# Fenced channel and provider leases

This is a derivative port of Dispatcharr's live owner and provider-connection
contracts. Original authorship belongs to upstream Dispatcharr's authors and
contributors; see [ATTRIBUTION.md](../../../ATTRIBUTION.md).

`Coordinator::connect(redis_url, LeaseConfig)` creates one multiplexed Redis
connection with bounded outstanding commands and finite command deadlines.
`acquire(AcquireRequest)` atomically claims a channel and its optional limited
provider/credential slots. It returns a non-cloneable `Lease` with a unique owner
token and monotonically increasing fencing generation. Redis time is authoritative.

`Lease::heartbeat()` refreshes only the exact current owner, never reacquiring an
expired lease. Automatic heartbeats stop and mark `lost()` on any failure. The
engine must cancel its upstream immediately when the loss watch changes: Redis
fencing alone cannot forcibly close an existing external-provider socket.
`release()` is single-use; `Drop` attempts async release. Crashes leave an expiry
record for the bounded periodic reaper, and every acquisition reaps expired records
before deciding capacity. Expired release records survive owner-key expiry, so
profile and credential counts can be recovered exactly once. Stale releases cannot
delete a successor owner or release its reservation.

Shared legacy keys:

* `live:channel:{channel_id}:owner`: holds the unique Rust owner token with TTL.
* `profile_connections:{profile_id}`: increments only for `max_streams > 0`.
* `server_group_connections:{group_id}:{fingerprint16}`: optional credential pool
  counter supplied by the trusted resolver. It uses the original profile limit.
* `profile_credential_release:{profile_id}`: preserves the original release mapping.

Rust-specific ledger/index/fence keys live below `LeaseConfig.namespace` (default
`rust:coordination`). All replicas must use the same namespace. Do not generate a
new namespace for each production replica. Redis Cluster is unsupported because
the legacy keys span slots and Lua accesses ledger-referenced keys.

Profile and credential mutations are Lua-atomic with ownership. Values that could
cause Redis type/counter errors are read and validated before mutation, because
Redis Lua errors do not roll back earlier writes. Malformed or negative counters
fail closed rather than silently changing accounting. The default ledger reaper
handles 128 records per command; backlog cleanup may need repeated bounded calls.

`Lease::release_provider()` frees the current profile/credential allocation while
retaining owner/fence/heartbeat. `reserve_provider(ProviderReservation)` then
claims an eligible replacement. It rejects double reservation, keeps failed
backup attempts allocation-free, and lets a failed source move to a new profile
without opening an ownership gap. Full release decrements only the allocation
currently recorded in the ledger, preventing a second decrement of the old profile.

Compatibility limits remain explicit. Python sees Rust ownership/counters and
cannot claim the same owner key. This does not supply media forwarding between
Python and Rust, or between Rust replicas: a remote-owned channel must currently
be rejected or explicitly routed to its owner. Python's existing media/status
metadata are not synthesized by this crate. If all Rust processes stop, Python
alone does not know to reap Rust ledger records; stale Rust profile counts remain
until a Rust reaper restarts. A healthy reaper/control service is therefore required
for mixed-runtime crash cleanup. Shared credential fingerprints and account/profile
selection must be ported before enabling pooled provider accounts. This crate
accepts the already-derived credential key, never guesses a provider login.

For real Redis verification, run from the repository root:

```sh
python3 rust/crates/coordination/tests/run-redis.py --output /tmp/redis-test-new --http
```

The runner starts disposable Redis from a digest-pinned cached application image,
publishes a loopback-only ephemeral port, runs the ten ignored-by-default real
Redis tests with `REDIS_TEST_URL` and (with `--http`) four real Redis/HTTP handoff
tests, and removes only that named container. Raw output
and exit evidence are preserved. Core-only `cargo test` reports these tests as
ignored; it does not count them as passes. No fake Redis implementation is used.

Acquisition cancellation is supervised before Redis receives Lua. The command
permit travels with the pending lease through actual Redis completion, oneshot
result delivery and token-fenced cleanup; caller cancellation/deadlines do not
release it early. The real TCP delayed-response regression separately delays
ACQUIRE and cleanup RELEASE replies for live and VOD, proving that capacity is
not reused during either delay. Detached work is bounded by command admission.
A permanently unresponsive Redis connection retains those bounded permits until
it replies or disconnects; caller deadlines still return an unavailable error.
No immediate quota-release guarantee is possible during a Redis/network outage;
ledger expiry and a healthy reaper remain the recovery path.

Timeshift uses `acquire_timeshift` with a distinct namespaced archive-session
owner key. Live, VOD and Timeshift share the legacy provider/credential quota
counters and fenced expiry ledger. Real Redis scope tests verify no phantom
live owners and bidirectional provider-capacity competition. The delayed-reply
cancellation regression now covers all three scopes, both caller cancellation
and caller deadline (six variants), including pending cleanup RELEASE replies.
