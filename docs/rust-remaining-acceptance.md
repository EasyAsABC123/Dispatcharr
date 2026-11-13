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

The newer [bounded REST library](rust-rest-read-ownership.md) passes 741 workspace
tests, with 737 external-fixture tests ignored. Ignored tests are not acceptance
results. Its two physical SQL cancellation checks and the seven
[Core settings ownership checks](rust-core-settings-native-component.md) belong
to separately recorded historical component builds.

## Work and evidence needed

| Work | Next executable gate | Completion evidence |
| --- | --- | --- |
| M0 contract coverage | Continue mapping executable Original role, method and input cases against the fixed application inventory | Complete coverage register with unresolved methods and missing cases resolved; no inference from passive metadata |
| M1–M3 streaming | Extend qualified streaming components across the remaining formats, profiles, failures and recovery boundaries | Decoder-checked playback, counters, timers, teardown and paired measurements on the same source |
| M4 VOD | Correct the measured invalid-credential HEAD mismatch; qualify ranges, seeks, catchup and remaining session boundaries | Original/Rust status, headers, bytes, origin behavior, ownership and resource comparison |
| M5 Core and accounts | Adopt bounded REST reads into the actual service; preserve ownership of admitted error bodies; reconcile overlapping read routes before Main assembly | Fresh combined-source API, permission, mutation, response-clone and shutdown tests, followed by Compose/React workflows and API measurements |
| M6 DVR and jobs | Execute the compiled Native DVR candidate against the real Original recording reference; extend remaining job families and configured graph behavior | Actual media/decoder, SQL, Beat, broker/result, event, active-key and child-process comparison, failure recovery and worker-inclusive measurements |
| M7 output and integrations | Assemble the qualified SQL ownership repairs and complete missing/stale/refresh and remaining protocol behavior | Fresh packaged/Main protocol and integration tests with exact source binding and measurements |
| M8 plugin and operations | Correct confirmed HTML error and duplicate-key differences; execute physical repository ownership cases; finish restore, schedules, admin and plugin execution | Source-derived normal/fault tests, database/cache audits, owned cleanup, rollback and full-service resource accounting |
| M9 final application | Package the final combined source, then run the complete workflow, restart, soak, cutover and rollback rehearsals | All prerequisite milestones accepted on the delivered revision, with reproducible Compose runs and full-graph performance evidence |

Fresh Original observations have established that invalid credentials on the
selected VOD HEAD request return 401, whereas the retained Rust checkpoint
returned 405. Two fresh plugin observations also establish HTML 500 responses
for malformed repository input and duplicate persisted API keys. These are
compatibility defects to fix, not accepted differences.

The expected SPA index was absent at the probed path in the pinned Original
image. That extraction failure remains recorded. A separately declared frontend
build context may supply a real template for another experiment, but cannot
establish equality with unobserved installed Original bytes.

Each new runtime composition needs its own checks. Historical failed controllers,
missing observations, source hashes and cleanup receipts remain preserved.
Measured gains are not transferred between source revisions or workloads.
