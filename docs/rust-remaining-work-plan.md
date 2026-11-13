# Remaining Rust migration work and parallel ownership

Original Dispatcharr authors and dependency contributors retain full application,
algorithm and licensing credit. This is a work allocation, not a declaration of
parity or milestone completion. React remains unchanged.

The authoritative acceptance ledger is [rust-milestones.md](rust-milestones.md).
Image53 has scoped package/Main and live-performance evidence. The later image54
build, SDK, all 28 selected corrected package groups and three scoped Main gates
pass. Its portable evidence and reproduction instructions are in
[rust-image54-validation.md](rust-image54-validation.md). Performance figures stay
bound to the measured image/workload.

| Milestone | Owner | Remaining work | Acceptance evidence |
| --- | --- | --- | --- |
| M0: executable baseline contracts | Primary agent | Finish operation-to-Source-test assignments and unresolved behavior contracts; distinguish observed dispatch, static candidates and partial role coverage. Reuse qualified captures where applicable. | Deterministic register with executable fixtures/assertions/results for required behaviors; exact Source provenance and rejection controls. |
| M1: bounded streaming core | Primary agent, with streaming agent for dependencies | Preserve the already passed technical implementation/benchmark gates; resolve the formal M0 prerequisite and check later changes where relevant. | Existing exact-source core evidence plus completed prerequisites; no redundant core benchmark solely to update prose. |
| M2: live TS | Streaming agent (`stream_http`) | Complete normal-capacity lifecycle, sharing, disconnect/rejoin, limits and remaining authenticated playback contracts. | Original/native behavior and physical lifecycle checks; decoder evidence and scoped resource acceptance. |
| M3: formats and profiles | Streaming agent (`stream_http`) | Close remaining HLS/fMP4/profile, failover, cancellation and fanout gaps. | Format/profile contract matrix, real decoding, recovery/cleanup and relevant performance evidence. |
| M4: VOD/timeshift | Streaming agent (`stream_http`) | Complete range/seek/catchup and session lifecycle; retain explicit counter/finalization/sharing differences until resolved. | Exact packaged Main witnesses, physical ownership/cancellation, decoding and relevant comparisons. |
| M5: control APIs | API/operations agent (`source_contract_finish`) | Qualify notification SQL cancellation/commit behavior and packaged mutations; close forms, renderer, credential, concurrency and remaining account/settings/channel API gaps. | Source-bound requests/effects, permission/credential cases, actual SQL/HTTP settlement, React workflows and API benchmarks. |
| M6: ingestion and jobs | Ingestion agent (`store_auth`) | Finish M3U known-failure/native-only child execution and physical collector; configured Series/recurring positive rules; remaining guide/matching/default-task behavior. | Actual producer/native worker/Original result-consumer chain, full SQL/cache/event/file/lock effects, physical fault controls, import/playback comparisons. |
| M7: outputs and integrations | Streaming agent (`stream_http`) | Complete remaining Xtream/HDHR/XMLTV/M3U/realtime/WebSocket/integration contracts and recovery behavior. | Protocol and integration suites, packaged workflows, physical cleanup and workload-specific performance. |
| M8: operations | API/operations agent (`source_contract_finish`) | Complete backup/restore/rollback, admin/session/CSRF, plugin/context, environment precedence and startup/shutdown behavior. | Real operational workflows and failure recovery, exact default-state audits and whole-service resource accounting. |
| M9: full acceptance | Primary agent, with all three agents | Integrate qualified changes; finish package/Main gates, full parity crosswalk, soak, packaging and local cutover/rollback rehearsal. | Final committed-source host/Compose checks, complete prerequisite ledger, representative whole-graph performance/soak and repeatable rollback evidence. |

## Current executable queues

- **Streaming/integration:** image54 package/Main evidence is qualified. Capture
  bounded Original profile and Xtream observations are now captured (24 model,
  38 framework and 38 TCP cases, with failed predecessors retained). Implement and
  verify the remaining format/profile/output and physical SQL ownership gaps.
- **API/operations:** physical notification cases preserve connection-drain
  failures in cases 2, 4 and 6; the composite remains negative. The new 20-request
  Original JWT/API-key capture and matching Native20 effects pass. The personal
  permission fix is in the qualified source union; preserve separate administrative
  write checks and finish the dependency shutdown/other API and operational gaps.
- **Ingestion:** the M3U candidate passes affected-crate checks and all 189
  eight-domain guest comparisons. Its host cleanup-parser failure remains
  recorded. Three lease controls and the corrected remaining two physical cases21
  pass. Configured Series/recurring Source11 observations are captured with an
  explicit missing-SQLSTATE instrumentation field; Native implementation, clocked
  scheduling and complete child execution remain separate work.
- **Primary agent:** review/integrate the three tracks, finish Source-contract
  assignments, keep evidence and milestone status current, and commit/push scoped
  completed work with receiving-ref verification.

Each owner maintains an itemized checklist and advances implementation and tests
in dependency order. A missing authority requires a bounded Source capture;
existing valid observations are reused without inventing coverage. New behavior
must have reviewable expected responses/effects before execution. Failed attempts
remain recorded alongside separately qualified corrections.

## Shared execution constraints

Three worker agents run alongside the primary agent. Independent coding, compile
and offline review proceed concurrently in separate frozen source roots. Docker
fixtures share a global maximum of three ordinary slots. The shared-stream,
all-workers-down and retention checks run alone after other heavy work settles.
Resource experiments require a coordinated quiet window.

All fixture dispatches bind the Docker/Compose tooling environment and run a
read-only executable/socket/daemon check before claiming outputs. The initial
image54 host-tooling failures remain failures; the corrected cohort has fresh
outputs, state and clearance. Guide UTC guards, reference freshness, workload
budgets and request definitions stay fixed.

Agents preserve unrelated working-tree edits and use scoped handoffs for shared
runtime changes. The primary agent reviews source/input maps, actual process
exits, effects and cleanup, then integrates and publishes completed changes.
A component pass, ignored test, preparation control or build alone cannot close
an application milestone.

Detailed owner queues: [streaming/integration](rust-streaming-remaining.md), [APIs/operations](rust-api-operations-remaining.md), and [ingestion/jobs](rust-ingestion-remaining.md). These retain each known gap, its existing authority and the next finite acceptance action.
