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
and two cancellation regressions. Packaged no-ID playback is a separate check.

Main readiness for an explicitly enabled Beat now waits for its first
settled scheduler tick. The focused test covers the observed, starting,
stopped and faulted states and the existing Beat-off behavior. Selected
jobs and Beat remain opt-in, and coordinated scheduler ownership and normal
deployment readiness still require their integrated M6 checks.

[Focused evidence](../scripts/rust-integration/milestone-batch72/focused.json)
