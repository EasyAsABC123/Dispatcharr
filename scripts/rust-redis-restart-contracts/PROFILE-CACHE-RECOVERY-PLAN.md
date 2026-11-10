# Output-profile cache recovery: next gate

All application and algorithm credit belongs to the original Dispatcharr authors and contributors; existing AGPL attribution is retained. This is an unimplemented plan, not a passing recovery claim.

Inventory165 identifies a seventh persistent Redis command owner: `OutputProfileWrites.ProfileCache` in `rust/crates/store/src/output_profile_writes.rs`. It retains a manually driven `MultiplexedConnection`. Connectivity failures are intentionally ignored by its cache helpers, so the SQL mutation can succeed while future invalidations still use the dead connection. The accepted six-owner component tests do not cover this object.

## Establish the failure before changing it

Use one owned copied PostgreSQL database and an owned restartable Redis service. Keep the same `OutputProfileWrites`, auth/settings reader, and application process throughout. Warm actual CoreSettings cache keys, perform a normal profile mutation and record exact SQL references, invalidation keys and generation values. Restart only Redis without persistence; restore only the named stale fixture cache rows, never reconstruct the service under test. Perform another profile mutation through its actual HTTP route and compare the original/native response, committed SQL effects, deleted cache rows, changed group versions and a subsequent reader result. Separate the first post-restart operation from eventual recovery; a bounded retry is not first-call parity.

Retain the unmodified failure, immutable Rust inputs and tool hashes. Test both `stream_settings` and other affected groups as the actual Source delete/create/update behavior requires. Cover shared peer readers, rather than declaring recovery from Redis PING or HTTP201 alone. Continue to compare authentication/permission/server-command failures separately from the connectivity errors that Source ignores.

## Preserve command ownership and shutdown

A direct replacement with redis0.32.7 `ConnectionManager` is insufficient without an independently proven shutdown mechanism. The existing `physical_reply_ownership` fixture holds Redis replies, cancels observers and requires `OutputProfileWrites.shutdown` to close its actual driver and release both writer and physical-command admission. The manager's public API does not expose the manual driver's stop handle.

One candidate implementation is a shared bounded connection-generation holder retaining the current manual connection and every live driver handle. Connection creation is singleflight, bounded and inside the already owned physical operation, with the originating writer retained. Commands clone a generation and execute once. A settled I/O failure invalidates only its matching generation; a later logical operation can establish a new connection. An uncertain DEL or SET is never automatically replayed. Other operations retain their old connection/permit until their actual reply or disconnection.

Do not hold the connection-state mutex while awaiting a Redis command. Bound concurrent connection waiters using the existing 16 physical slots; preserve the four writer slots. Retire finished driver handles. Any bound on simultaneously live generations must be explicit and tested, rather than permitting an unbounded driver registry during repeated reconnects.

Close reconnect admission before shutdown drains work. Prevent a connecting operation from installing a fresh driver after shutdown has closed the previous handles. At the existing deadline, stop all owned drivers and await their termination; report remaining SQL or connection work honestly. Preserve the existing no-cache and overload classifications. A Source DEL connectivity failure still permits its separate SET attempt; each logical command keeps its own physical outcome.

## Acceptance matrix

1. Actual paired HTTP mutation, SQL effects and peer-reader cache generations before/after the Redis-only restart, with the same service instances and separate first-call/eventual results.
2. Same-generation simultaneous readers and mutations share one reconnect; a late failure from an old generation cannot invalidate a newer healthy connection.
3. Disconnect after a DEL or SET reaches Redis: exact mutation count, no uncertain replay, retained writer/physical leases.
4. Held replies, canceled caller, logical timeout, all 16 physical slots and four writers, overflow behavior and recovery after reply release.
5. Held reconnect handshake and held command reply during shutdown, including the race between creating a driver and closing admission; no post-shutdown driver or leaked writer.
6. Existing output-profile HTTP/SQL/error contracts, cache permission controls and complete strict workspace checks on the selected final source.

Run this matrix only after the category performance quiet window is explicitly released. No performance gains or application-wide Redis recovery follow from this plan.
