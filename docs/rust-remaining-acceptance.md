# Remaining rewrite acceptance work

The complete backend rewrite is not accepted yet. The React frontend remains
unchanged, and the original Dispatcharr authors retain all application and
algorithm credit. The [milestone ledger](rust-milestones.md) defines the formal
M0–M9 gates; the items below identify the next acceptance work, rather than a
percentage estimate from source-file or test counts.

## Current evidence

The selected packaged application is [image57](rust-image57-validation.md).
Its 28 packaged groups, SDK checks and three scoped Main checks qualify. An
additional catalogue comparison has qualified responses but retains its original
controller failure. Later library changes are not executions of this image.

The [image57 live experiment](rust-image57-live-performance.md) measures nine
paired trials at 1, 10 and 100 viewers. Median CPU reductions are 69.33%, 78.85%
and 91.17%; peak working-memory reductions are 52.08%, 46.30% and 49.63%.
The worst paired throughput is 1.55% lower than Original. This covers the live
HTTP backend and common PostgreSQL, Redis and deterministic origin, excluding
background workers and the rest of the application graph.

The current combined source passes 774 default workspace tests, with 741
external-fixture tests ignored; [actual waits](rust-pluginrepo-test-support-validation.md)
identify the exact snapshot. Ignored tests are not acceptance results.
Fresh [Core bounded-reader/error-body cases](rust-core-bounded-rest-component.md)
and [image59 Main header/HEAD cases](rust-image59-build-worker-header.md) pass
their selected functional checks. Image59 has no new full package matrix or
performance acceptance.

## Work and evidence needed

| Work | Next executable gate | Completion evidence |
| --- | --- | --- |
| M0 contract coverage | Continue mapping executable Original role, method and input cases against the fixed application inventory | Complete coverage register with unresolved methods and missing cases resolved; no inference from passive metadata |
| M1–M3 streaming | Extend qualified streaming components across the remaining formats, profiles, failures and recovery boundaries | Decoder-checked playback, counters, timers, teardown and paired measurements on the same source |
| M4 VOD | Assemble the now-qualified HEAD correction and fix the measured malformed-UUID React fallback; qualify ranges, seeks, catchup and remaining session boundaries | Original/Rust status, headers, bytes, origin behavior, ownership and resource comparison |
| M5 Core and accounts | Extend the fresh qualified combined Core service to special settings keys; reconcile overlapping read routes before Main assembly | Fresh combined-source API, permission, mutation, response-clone and shutdown tests, followed by Compose/React workflows and API measurements |
| M6 DVR and jobs | Execute the compiled Native DVR candidate against the real Original recording reference; extend remaining job families and configured graph behavior | Actual media/decoder, SQL, Beat, broker/result, event, active-key and child-process comparison, failure recovery and worker-inclusive measurements |
| M7 output and integrations | Assemble the qualified SQL ownership repairs and complete missing/stale/refresh and remaining protocol behavior | Fresh packaged/Main protocol and integration tests with exact source binding and measurements |
| M8 plugin and operations | Assemble the qualified HTML error correction; resolve or explicitly close the first physical case's missing full post-state audit after publishing the fresh normal/physical results; finish restore, schedules, admin and plugin execution | Source-derived normal/fault tests, database/cache audits, owned cleanup, rollback and full-service resource accounting |
| M9 final application | Package the final combined source, then run the complete workflow, restart, soak, cutover and rollback rehearsals | All prerequisite milestones accepted on the delivered revision, with reproducible Compose runs and full-graph performance evidence |

Fresh Original observations established HEAD401 and two HTML500 compatibility
defects. The Rust corrections now pass separate fresh selected comparisons;
these results do not close full VOD or plugin acceptance.

The expected SPA index was absent at the probed path in the pinned Original
image. That extraction failure remains recorded. A separately declared build
using the unchanged Original React source returned React200 for a malformed VOD
UUID. The [Main correction](rust-main-vod-react-fallback.md) passes workspace checks and awaits its fresh image60 witness; this context
does not establish equality with unobserved installed Original bytes.

Each new runtime composition needs its own checks. Historical failed controllers,
missing observations, source hashes and cleanup receipts remain preserved.
Measured gains are not transferred between source revisions or workloads.
