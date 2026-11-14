# Remaining Rust migration work and parallel ownership

Original Dispatcharr authors and dependency contributors retain full application,
algorithm and licensing credit. React remains unchanged. This work allocation
records open acceptance gates; it does not declare any complete M0–M9 milestone.
The authoritative ledger is [rust-milestones.md](rust-milestones.md).

**Current execution policy:** [milestone-wide validation](rust-milestone-testing.md)
supersedes the per-case execution/admission queues below. Owners combine the
remaining work into complete milestone batches, reuse existing runners and
fixtures, and expose unimplemented cells as blockers. Narrow tests diagnose
failures; they are no longer the unit of delivery. The rows below remain a
capability inventory and historical component checkpoint.

The delivered opt-in component composition is
[current882](../scripts/rust-component-checkpoints/current882/README.md).
Its unsigned plugin comparisons and recording SQL-rejection correction are
source-bound component evidence. The selected full package/performance reference
remains [image57](rust-image57-validation.md); [image62](rust-image62-build.md)
has separate immutable-build and installed-file evidence. Later library changes inherit neither
Main acceptance nor measured performance gains.

| Milestone | Owner and execution queue | Next acceptance gate |
| --- | --- | --- |
| M0: executable baseline contracts | Primary agent, with each component owner | Extend the 131 partial HTTP Source definitions and eight non-HTTP assignments against the fixed inventory; distinguish static candidates, observed dispatch and fully verified behavior. |
| M1: bounded streaming core | Primary agent, with streaming owner | Preserve exact-source technical core evidence, complete prerequisites and test relevant changes; do not repeat a benchmark merely to update prose. |
| M2: live TS | Streaming agent (`stream_http`), after current Core integration | Finish authenticated capacity, upstream sharing, disconnect/rejoin, counter, timer and teardown comparisons with actual media and physical lifecycle evidence. |
| M3: formats and profiles | Streaming agent (`stream_http`), following M2 dependencies | Complete HLS/fMP4/profile, failover, cancellation and fanout matrix, real decoding and relevant paired measurements. |
| M4: VOD/timeshift | Protocol/operations agent (`source_contract_finish`), after current plugin ownership gates | Complete ranges, seeks, catchup and session boundaries; retain unresolved counters/finalization/sharing differences and test packaged Main behavior. |
| M5: control APIs | Core/streaming agent (`stream_http`) | Run the current combined Native special-settings cohorts against Source51, then late-reply/caller-loss/frame/shutdown gates; wire actual Main consumers and test React/account/channel workflows and API performance. |
| M6: ingestion and jobs | Recording/jobs agent (`store_auth`) | Execute longer audio/video Native recording and held-result gates; finish Source-backed error/recovery/profile/scheduler/Series/default-job families and worker-inclusive measurements. |
| M7: outputs and integrations | Protocol/operations agent (`source_contract_finish`), with streaming dependencies | Finish Xtream/HDHR/XMLTV/M3U/realtime/WebSocket contracts, missing/stale/refresh behavior and recovery; qualify combined packaged workflows and workload measurements. |
| M8: operations/plugins | Protocol/operations agent (`source_contract_finish`) | Finish frontend-disconnect, SQL Ready and concurrent-fetch ownership; qualify HTTPS/DNS/signature domains, discovery/install/run, backup/restore/rollback, sessions/CSRF and environment/startup/shutdown workflows. |
| M9: final acceptance | Primary agent with all three agents | Assemble the final committed source, complete prerequisites and full workflow/package/SDK checks, then restart/soak/cutover/rollback and representative full-graph paired performance. |

## Current executable queues

- **Plugin/protocol owner:** the portable Source25/Native25 and two selected
  physical cases are published in the
  [unsigned-manifest packet](../scripts/rust-pluginrepo-contracts/unsigned-manifest-component/README.md).
  Next are four concrete Native-only cases: real frontend socket loss, committed
  refresh held before Ready, cancelled refresh with late ErrorReady, and four
  retained fetch/auth frames with fifth-admission rejection and later reuse.
  Broader transport/signature and operational domains follow their Source gates.
- **Core/streaming owner:** Source51 captured all six special settings groups,
  including real unconsumed broker publications and old-key/new-key distinctions.
  The private candidate and real RealtimeBus fixture peer pass their affected
  checks; the combined current-source executor is being finalized. Cache warming
  must use real bounded readers. A persistent proxy cache retains one actual
  writer token, leaving three slots; helper parity does not establish Main knob
  freshness. Late replies, caller loss, frame retention and shutdown remain tests.
- **Recording/jobs owner:** the measured P0001 rejection continuation is delivered.
  Original's separate nominal61-second audio/video recording produced56.64-second
  media, decoded both streams and recorded four renewals. A pinned deserializer
  verified all282 retained event associations without replay. The private Native61
  composition preserves the existing30-second constructor and prime correction;
  its full workspace passes826 tests with747 ignored. The first positive61 and
  held post-end result contexts exited1 while observing capture; the refusal
  context also exited1. All three fixtures cleaned up. The refusal output exposes
  the cleanup service's remaining30-second identity restriction; that is a real
  runtime gap to correct before a new frozen candidate. The two positive runs
  did not retain the early application result, so their identical cause is an
  inference, not an observed outcome. Existing media/clock criteria stay fixed;
  arbitrary profiles and durations remain open.
- **Primary agent:** review and publish scoped source/evidence, maintain the M0
  crosswalk, integrate each qualified lane, then qualify actual Main/React,
  package/SDK and operational workflows. Performance evidence must be measured
  on the delivered source and workload; image57 gains do not transfer to workers
  or later components.

The [ordinary VOD access-flag cohort](../scripts/rust-contracts/vod-access/README.md)
adds seven partial Source GET definitions from four exact successful Original
workflows already retained in the full3,362-test run. This closes no Native gate
and performs no new Source replay. Baseline prolonged-outage and supplemental
metric captures already exist; preserve their documented limits and failures
instead of rerunning them to reconcile stale prose.

A missing authority requires a bounded Source capture. Reuse valid observations;
never replay a passed Source merely to improve a failed controller's status.
Preserve failed attempts, missing observations, raw clocks and separately
qualified corrections. Every new behavior needs reviewable expected effects and
an immutable input definition before execution.

## Shared execution and storage constraints

Three worker agents run alongside the primary agent. Coding, controller work and
read-only review proceed concurrently in frozen source roots. Root coordinates
all application dispatch, with at most three ordinary Docker fixtures. Resource
experiments require a quiet window; shared-stream/all-workers-down/retention
checks run alone after other heavy work settles.

Use the existing shared Cargo target **in place**:
`artifacts/rust/m7-xc-vod-info-physical-compile-29/target`.
Root grants an exclusive compile reservation, and the owner releases it after
actual waits, unchanged-source checks and required outside-ELF copies. Do not
create a new target per validation run or copy/transplant compiler caches. Use
locked/offline dependencies, two build jobs, debug information disabled and
incremental compilation disabled for routine checks. Preserve frozen source,
results, logs and explicitly recorded outside binaries. The completed cleanup
removed195 targets and Docker build cache while preserving validation images;
new work must not recreate that accumulation.

Every fixture binds Docker/Compose tooling, image/source/input hashes, fresh
owned namespaces, fixed budgets and cleanup receipts. The primary agent checks
actual exits, SQL/cache/media/clock effects and physical resource settlement
before integration. Preserve unrelated working-tree edits; stage only reviewed
paths, commit completed work and verify the receiving fork ref after pushing.
A component pass, ignored test, preparation control or build alone cannot close
an application milestone.

Detailed remaining inventories:
[streaming/integration](rust-streaming-remaining.md),
[APIs/operations](rust-api-operations-remaining.md), and
[ingestion/jobs](rust-ingestion-remaining.md). Their older observations retain
separate historical source bindings; current scopes are identified above and in
[rust-remaining-acceptance.md](rust-remaining-acceptance.md).
