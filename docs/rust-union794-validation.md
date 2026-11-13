# Combined notification and M3U source validation

Original Dispatcharr authors and dependency contributors retain all application,
algorithm and licensing credit. React remains unchanged.

This source combines the [notification credential fix](rust-notification-credentials.md)
and [bounded M3U known-failure component](../scripts/rust-job-contracts/scanner-logo/m3u-known-failures/README.md)
without overlapping edits. The frozen source contains 794 Rust inputs and five
compile-time goldens; its exact [799-input map](../scripts/rust-integration/union794-source-inputs.json)
and [whole-workspace checks](../scripts/rust-integration/verified-union794-source-checks-1.json)
are retained. Formatting, strict all-target Clippy and default workspace tests
passed: 591 tests passed and 710 fixture-dependent tests were ignored in that run.
The ignored tests are not counted as fixture passes.

Source20 and Native20 qualify the five personal notification actions against real
JWT/API-key behavior while preserving administrative write checks. Source uses
APIClient and normal middleware; Native uses a real local listener. The Native
capture recorder's raw `passed:false` remains distinct from its passing controller
and effect qualification. Public credential projections preserve original hashes;
the separately reconstructed projected reference was not the executed input.

The M3U component preserves paired guest189/8 with host1 cleanup-parser failure,
106 completed checks in the first eight physical phases, the failed107th check,
the corrected remaining two cases21, three lease controls and every predecessor
failure. It adds only the observed ENOENT and CreateGroups/P0001 completion domain.
Long-lived Source renewal, literal lock-token and pending-filesystem-syscall parity
remain open. Neither component activates Main/default workers.

The selected functional image remains [image54](rust-image54-validation.md).
The later [Compose SDK check](../scripts/rust-integration/sdkaca6/README.md)
passes at committed `aca6d900`: 591 Rust tests, 361 Python tests plus one skip,
and 1,806 TS helper comparisons. Its 710 ignored Rust fixture tests remain
unexecuted in that default run. All 8,273 source files and 1,796 actual image
COPY inputs are bound; independent cleanup passes. The combined-source
[image55 build](../scripts/rust-integration/image55-build/README.md) also passes,
with installed Original credit and React assets unchanged. Package/Main replay
of this later image is the next application gate.
[Image53 performance](rust-image53-live-performance.md) remains bound to its measured
image and workload; no numerical gain transfers to this source. Full milestones,
whole-graph performance, soak and local cutover/rollback remain open.
