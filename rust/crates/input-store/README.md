# Shared TS input store

This derivative implements the original Dispatcharr `StreamBuffer` Redis key
layout, publish-after-write index, timestamp positioning and discontinuity
sidecar. All original application credit belongs to the upstream authors and
contributors; see [ATTRIBUTION.md](../../../ATTRIBUTION.md). AGPL-3.0-only remains.

`InputStore::put(channel, owner, bytes, wall_timestamp, discontinuity)` atomically
compares the live coordinator owner, stores one complete packet-aligned chunk,
and publishes the index only after storage. `snapshot` returns owner/index and
bounded receive-time/discontinuity entries. `cursor_for_age` preserves the
original `ZREVRANGEBYSCORE` selection and oldest fallback with last-consumed index
convention. `get` returns `InputBytes` with immutable sequence, owner, timestamp
and discontinuity. `Bytes::from_owner` can carry its byte admission into a local
fanout ring or HTTP frame without a media copy.

Every publication uses exact legacy keys under
`live:channel:<id>:input:buffer:{index,chunk:<seq>,chunk_timestamps,discontinuities}`.
An additional `rust_owner` fence identifies the published data era. If the
coordinator owner changes, reads and snapshots immediately hide the previous
owner's data. The first publication by the new owner removes old tracked media,
keeps the counter monotonic and marks a discontinuity. Initial new-channel media
is not spuriously marked discontinuous. Cleanup checks both the current owner
and the buffer fence; it never deletes coordinator ownership, channel metadata
or provider reservation/counter keys.

Defaults permit 4 MiB chunks, 16 MiB retained media per channel, 4,096 chunks,
64 MiB of owned read/snapshot data, sixteen in-flight Redis operations and 1,024
bounded queued operations. Chunk TTL is sixty seconds. Configure the maximum
chunk size to match the engine's actual chunk bound when possible. Retention
removes oldest timestamp entries until both byte and count budgets fit; readers
must handle a missing/evicted sequence by repositioning and marking a gap. These
resource ceilings intentionally constrain original unbounded edge cases.

The shared cancellation-safe Redis transport pre-reserves media bytes before
GET and conservatively reserves metadata parsing space before snapshots. After
decoding, permits shrink to retained data capacity and remain with the returned
owner. A cancelled caller or deadline leaves the bounded supervised request
holding its permits until Redis actually responds or disconnects. No background
queue of abandoned large responses escapes accounting.

Strict fencing is enabled for Rust-published input. A Python producer does not
write the additional `rust_owner` fence, so this store deliberately hides that
legacy producer's unfenced media. Python readers can read Rust-written legacy
chunk/index/timestamp/discontinuity keys. Bidirectional mixed-producer cluster
parity is not claimed; adding an explicit legacy trust policy would require its
own contract and safety gates. Redis Cluster and automatic stalled-socket
recovery are not implemented.

Run component gates with:

```sh
python scripts/rust-hls-parity/http.py --suite input \
  --output artifacts/rust/NEW-INPUT-STORE-RUN
```

Six real Redis tests cover two-store binary reads and exact legacy keys,
timestamps/age/discontinuity; ownership takeover hiding stale bytes before any
new publication, stale cleanup rejection and preserved provider/owner keys;
byte/count eviction and TTL; and actual Redis `CLIENT PAUSE` proving cancelled
reads, final owned-frame clones and cloned snapshot entries retain admission;
and eighty age-position vectors compare against the actual unchanged upstream
Lua, including tied timestamps and oldest fallback. Evidence:
`artifacts/rust/m2-input-store-abandoned-1/evidence.json`. This is storage component
evidence. Engine publication, remote follower fanout, mixed runtime behavior,
application performance and the M2 migration gate require separate verification.

`cleanup_abandoned(channel, expected_old_owner)` supports a live observer after
owner loss: it deletes input keys only while coordinator ownership is absent and
the published Rust fence still equals that exact old owner. Acquiring a new owner
makes the operation a no-op even before that owner's first publication. This
also protects provider/coordinator keys. The real Redis abandoned-cleanup gate
covers all three interleavings.

The durable Rust-only catalog now covers an arbitrarily late restart after
all workers disappear. Publication atomically writes a bounded hash catalog,
due sorted set and persistent per-channel `rust_gc_fence` containing the exact
owner/index. Default admission is4096 channels (configurable1..65536); a new
channel beyond capacity fails before any media/index mutation. Normal fenced
cleanup removes its catalog row too. Every store starts a weak-reference reaper:
one batch of at most32 due rows every5 seconds, using the same supervised bounded
Redis transport. `reap_expired(batch)` also exposes this operation for testing.
No arbitrary Redis key scan is used.

The reaper preflights its bounded batch, requires absent coordinator ownership,
matching durable catalog/fence and unchanged index, and waits until the expiring
Rust fence, timestamps, discontinuities and latest media key are absent. It
removes only the persistent Rust-proven index and its GC bookkeeping. It never
modifies owner/provider/metadata keys or an untracked Python buffer. A new owner
before first publication, new Rust publication, and a changed legacy index all
protect successor state. An unchanged orphan becomes due two seconds after
chunk TTL; polling adds at most5 seconds with an empty backlog. A full4096-row
due backlog requires at most128 batches (640 seconds); this is a bounded recovery
queue, not an immediate whole-database cleanup promise.

Strict mixed producers remain unsupported: an unfenced Python writer reusing
exactly the same index after a prior Rust era cannot be distinguished from that
old era once all expiring keys disappear. Do not run both producers on the same
channel. Four real Redis GC cases include automatic late-restart cleanup,
successors/untracked legacy preservation, atomic catalog saturation and cancelled
reaper admission. Seven existing input/media/age tests also pass with the catalog
enabled. Evidence: `artifacts/rust/m2-input-durable-gc-1`. Full Compose crash
and application resource-recovery gates remain separate.

The actual two-Python-worker full Compose probe measured an existing crash defect:
a hard owner-process-group kill left the provider count at one and canonical
input index at four after ninety seconds, despite ownership, clients, metadata
and timestamp entries being gone. Shared playback and follower-issued global stop
both passed and decoded 74 video frames with audio. Evidence:
`artifacts/rust/m2-shared-ts-baseline-1/shared-ts.json`. Rust crash cleanup is an
explicit resource-recovery improvement under the approved cleanup/rollback gates;
it must meet its own numeric deadline. It is not a claim of identical crash leaks
or an application performance gain.

The frozen application image also passed the full Compose all-workers-down gate:
both replicas were killed, then65seconds elapsed past the input TTL. A passive
replica restart reclaimed index/provider leftovers automatically in5.406seconds
without any upstream reconnect. Both pre-crash streams decoded74video frames
with audio. This verifies the application wiring of the durable catalog and
Coordinator recovery, separately from live-follower recovery26.647seconds.
Evidence: `artifacts/rust/m2-source-compat-all-workers-down-1/shared-ts.json` and
`scripts/rust-integration/verified-all-workers-down-source-compat-1.json`.
