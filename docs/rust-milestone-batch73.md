# Rust archive routing and retained contracts checkpoint

The original Dispatcharr and dependency authors retain full credit.

For an explicit idle session, Rust now selects the stored provider profile,
account and stream among the currently authorized candidates. The fenced
claim rechecks the observed owner. The loopback HTTP and Redis control
filled the primary provider, started the archive on the alternate provider,
then freed the primary. Reopening kept the original alternate provider.
The test and fourteen existing pool and cancellation controls passed.
The paired packaged default-rotation check remains open.
[Focused check receipt](../scripts/rust-integration/milestone-batch73/stored-profile-focused.json)

The M0 register now links 132 retained HTTP observations to fourteen
additional operations across M3U reads, channel producer POSTs and VOD
JSON-suffix GETs. This yields 225 partial assignments and 573 unassigned
HTTP references. The portable suite passed 366 checks with one skipped.
An independent capture review matched all 132 route observations.
[Register checks](../scripts/rust-integration/milestone-batch73/m0-retained-http-checks.json)

The [complete parity plan](rust-complete-parity-plan.md) tracks the remaining
functional, performance, soak and rollback gates.
