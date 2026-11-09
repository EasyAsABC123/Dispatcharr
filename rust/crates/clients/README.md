
Client admission and release run as supervised bounded tasks. Cancelling a
REGISTER caller or reaching its response deadline does not drop its local
admission while Redis still has the command queued. An undelivered successful
lease schedules fenced RELEASE; that cleanup retains the same admission until
Redis actually replies. Explicit-release cancellation and the final lease Drop
have the same guarantee. Drop uses the creation runtime even on a plain thread.
Held HTTP lease clones continue to retain admission after Redis cleanup. User-limit policy
decisions and legacy key layout are retained; internal input-era metadata adds
fenced dead-owner recovery.

Five real Redis regression gates passed at
`artifacts/rust/m2-client-admission-supervision-1/evidence.json`: paused cancelled
registration, registration deadline, cancelled explicit release, plain-thread
final Drop, and retained body lease clones. The local cap remains occupied while
commands are delayed. This is an admission/cancellation correctness fix, not a
throughput or complete metadata-queue audit. Other small metadata operations and
all-workers-down cleanup have separate remaining gates.

`stop_local_channel(channel)` removes this worker's accounting even when a body
retains an unpolled client lease. It signals all matching local watches, processes
at most 32 acknowledged client releases concurrently, and supervises the cleanup
through actual Redis replies. Pending registrations are tracked before ACK but
excluded from heartbeat updates; a stop waits for their admission result and
fenced cleanup, so they cannot recreate metadata later. Other channels and
workers are preserved. A caller deadline does not cancel that owned cleanup;
physical admission remains held by delayed replies and any retained body clones.
Registry references are upgraded under its mutex and filtered/dropped outside,
avoiding a final-Arc Drop reacquiring the same mutex.

Ten real Redis gates, including pending rejected admission and concurrent
cross-channel churn, passed at `artifacts/rust/m2-client-local-stop-1/evidence.json`.

Input-era recovery adds internal `rust_input_owner` metadata. REGISTER reads the
actual owner before any preemption writes. `bind_local_owner(channel, owner)`
updates only local token-matched empty/same-era rows; a queued registration reads
the current owner when it executes. `stop_abandoned_channel(channel, old_owner)`
requires ownership absent and deletes only exact old-era hashes, stop markers,
matching set members and bound HLS capabilities. Canonical metadata is removed
only when its `rust_token` matches that old owner. New owners, pre-owner empty-era
rows, other eras and worker heartbeats are preserved. All write-target types are
validated before mutation. Control tasks have32 in-flight slots and1,024 bounded
pre-send waiters; caller cancellation never cancels an already-sent operation.

Six real Redis era gates passed at `artifacts/rust/m2-client-era-2/evidence.json`.
These verify metadata-only ghosts, corrupted metadata/capabilities without partial
deletes, delayed/queued registration and cleanup, and successor/foreign safety.
They do not replace an immutable two-worker application crash witness.
