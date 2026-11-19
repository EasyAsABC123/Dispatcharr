# Rust archive and scheduler checkpoint

The original Dispatcharr and dependency authors retain full credit. This
checkpoint follows the [Candidate71 packaged archive and operator
comparisons](rust-milestone-batch71.md).

An archive request without a session ID can reuse a remote idle Native
session when its programme, authenticated user, address and User-Agent match.
The read-only lookup identifies the current owner, and the Redis claim
rechecks that owner, fingerprint and stored provider descriptor before
transferring the lease. A stale observation cannot create a replacement
archive. Real Redis covered the new fingerprint predicate plus twelve pool
and two cancellation regressions. The packaged no-ID comparison also passed.

Main readiness for an explicitly enabled Beat now waits for its first
settled scheduler tick. The focused test covers the observed, starting,
stopped and faulted states and the existing Beat-off behavior. Selected
jobs and Beat remain opt-in, and coordinated scheduler ownership and normal
deployment readiness still require their integrated M6 checks.

[Focused evidence](../scripts/rust-integration/milestone-batch72/focused.json)

The pinned Original reference for the second request without a session ID
returned HTTP 200 and the same 8,192-byte archive prefix as the first request.
Distinct serving workers were observed. The original session and stored
profile were preserved, the reservation dropped between requests, and the
owned fixture was removed.
[Original no-ID comparison](../scripts/rust-integration/milestone-batch72/source-no-sid-idle.json)

## Retained serializer contracts

Ninety-six retained Original HTTP observations now reference 23 additional
POST, PUT or PATCH operations. They cover malformed inputs and role
outcomes. There are 211 partial Source assignments and 587 unassigned HTTP
references. Repeated generation produced identical bytes. All 26 focused
checks passed, and the portable suite passed 365 checks with one skipped.
[Register verification](../scripts/rust-integration/milestone-batch72/m0-serializer-checks.json)

## Packaged no-session-ID handoff

Candidate72 matched the pinned Original when the peer worker omitted the
session ID. Both distinct Rust Main workers returned the same 8,192-byte
prefix as Python. The peer reused the archive session and stored profile
while the provider count followed zero, one, zero, one, zero. Independent
checks verified cleanup of the owned project.
[Build receipt](../scripts/rust-integration/milestone-batch72/candidate-build.json)
· [Native no-ID result](../scripts/rust-integration/milestone-batch72/native-no-sid-idle.json)
