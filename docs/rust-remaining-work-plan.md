# Remaining Rust migration work and parallel ownership

Original Dispatcharr authors and dependency contributors retain full application,
algorithm and licensing credit. React remains unchanged. This work allocation
records open acceptance gates; it does not declare any complete M0–M9 milestone.
The authoritative ledger is [rust-milestones.md](rust-milestones.md).

The [November 16 acceleration plan](rust-migration-acceleration.md) specifies
parallel ownership, the integration queue, validation order and the next
production-workflow batches. Use it with the milestone-wide policy below;
historical candidate paragraphs are evidence, not the current dispatch queue.

**Current execution policy:** [milestone-wide validation](rust-milestone-testing.md)
supersedes the per-case execution/admission queues below. Owners combine the
remaining work into complete milestone batches, reuse existing runners and
fixtures, and expose unimplemented cells as blockers. Narrow tests diagnose
failures; they are no longer the unit of delivery. The rows below remain a
capability inventory and historical component checkpoint.

## Acceptance evidence by package

The `d426d322` application passed Channel Profile JSON aliases and ordinary
VOD logo metadata, media and category reads. Packaging verified the React
and attribution inputs. Fourteen of its 127 required Compose checks passed.
[Current required checks](rust-m9-d426d322-required-fourteen.md)
[Profile JSON aliases](rust-m0-channel-profile-json-aliases-paired.md) ·
[Asset and category reads](rust-m5-m4-d426d322-owned-assets.md)

The earlier `814b9403` application passed the default Main Standard provider
cadence edit and four ordinary Core backup schedule boots.
[Rust cadence](rust-m6-ordinary-active-std-cadence.md) ·
[Core backup startup](rust-m8-814b9403-core-backup-startup.md)

The earlier `fdede54c` package also passed 14 required Compose checks,
a two-hour playback soak, and warm copied-data fallback.

| Milestone | Verified scope and remaining acceptance |
| --- | --- |
| M0 | The Stock resolver selected the explicit route in 12 ordinary recording cases. Both backends matched 16 Channel Profile JSON variants and four canonical controls per arm. Complete executable role and method coverage remains open. [Recording routes](rust-m0-recording-hls-router-reachability.md) · [Channel Profile JSON aliases](rust-m0-channel-profile-json-aliases-paired.md) |
| M1 | On `d426d322`, two-worker sharing, disconnect and ordinary stop, retained history across ten channels and paced readers passed. The bounded history test served 100 late reads. Broader lifecycle and buffer acceptance remain open. [Current viewing evidence](rust-m1-m3-d426d322-ordinary-viewing.md) |
| M2 | On `d426d322`, each backend served 100 simultaneous authenticated viewers with A/V decode and one active upstream connection during overlap. Current matched resource comparison remains open. [Current functional playback](rust-m1-m3-d426d322-ordinary-viewing.md) · [Earlier performance measurements](rust-m2-100-viewer-measured.md) |
| M3 | On `d426d322`, both backends served two concurrent viewers through one upstream per HLS and fragmented MP4 run. Both decoded A/V and the remaining fMP4 viewer progressed after the first disconnected. Broader profile acceptance remains open. [Current viewing evidence](rust-m1-m3-d426d322-ordinary-viewing.md) |
| M4 | On `d426d322`, ordinary VOD seeks, calendar ranges, same-client reconnect and default timeshift archive playback passed with exact teardown. Wider recording and on-demand lifecycle acceptance remain open. [Current media group](rust-m4-d426d322-ordinary-media-group.md) |
| M5 | Group Manager OPTIONS, ordinary Channel response order and seven ordinary asset and category reads have paired evidence. Wider account, settings and route coverage remains open. [Channel evidence](rust-m5-fdede54c-channel-json-order.md) · [Asset and category reads](rust-m5-m4-d426d322-owned-assets.md) |
| M6 | The Standard provider cadence edit passed on `814b9403`. On `d426d322`, local gzip Refresh All passed with the Original Celery consumer, and scheduled backend cleanup passed with Rust Selected Jobs and Beat. The complete default worker and scheduler graph remains open. [Cadence](rust-m6-ordinary-active-std-cadence.md) · [Current jobs](rust-m6-d426d322-ordinary-jobs.md) |
| M7 | M3U, XMLTV, programme Grid and Xtream exports passed on `fdede54c` alongside HDHomeRun. On `d426d322`, 12 ordinary Connect reads and the paired authenticated WebSocket hello, IP snapshot and Stats update passed. Broader protocol and realtime acceptance remain open. [Connect](rust-m7-d426d322-connect-positive.md) · [WebSocket](rust-m7-d426d322-websocket-positive.md) |
| M8 | Default backup inventory and download matched on `d426d322`. Private Main preserved the saved value and signed session across restart on its earlier package. Wider admin, plugin and operational acceptance remain open. [Backup inventory and download](rust-m8-d426-postgresql-backup-inventory.md) · [Private admin](rust-m8-2581-private-admin-settings-restart.md) · [AIO login](rust-m8-stock-aio-admin-login.md) |
| M9 | On `d426d322`, 14 of 127 required Compose checks and the copied-data warm fallback passed. The earlier `fdede54c` package passed the two-hour soak. Strict cold remains open because Original startup rewrites four timestamps included in exact equality. The complete matrix and matched-work performance remain open. [Current Compose tally](rust-m9-d426d322-required-fourteen.md) · [Current warm fallback](rust-m9-d426d322-copied-postgresql-operator.md) · [Cold rollback](rust-m9-strict-cold-startup-operands.md) |
| M10 | The `d426d322` release executables compiled natively on ARM64. Earlier scoped checks cover React, media, Comskip, PostgreSQL, GPG, the Python host and DRF static assets. The inherited FFmpeg build source and seven tool notices are linked to the Original image. Integrated Compose, plugin execution and controlled rollout remain open. [Current Rust build](rust-m10-native-arm64-rust-build-d426.md) · [FFmpeg provenance](rust-m10-native-arm64-ffmpeg-source-notices.md) · [Comskip](rust-m10-native-arm64-comskip-debian.md) · [Python](rust-m10-native-arm64-python-host-debian.md) |
| M11 | The default switch and operator handoff depend on release acceptance and a rollout decision. |

The original Dispatcharr authors and dependency contributors retain full credit.

## Historical checkpoints

Current implementation and executable coverage are reconciled in the
[worker graph](rust-default-worker-graph.md) and
[47-declaration producer review](rust-source-task-coverage.md). Candidate38
registers 32 literal application task names plus backend cleanup and dynamic
plugins. Its [Native52 matching cohort](rust-channel-matching-native.md) passes
all 14 cases in the explicit no-download/absent-model/empty-plugin domain.
Candidate37's Native46/47 whole runs retain five passing contexts and two failures;
Native48 qualifies their affected recording-event slice on that earlier image.
The contract register now has 136 partial HTTP Source assignments, including
exact Source51 admin matching bindings; broader case/role coverage remains open.
None of these observations closes full milestones or matched-work performance.
The candidate paragraphs below are historical checkpoints, not current missing-
registration claims or a current execution queue.

The historical source [Candidate28](rust-milestone-batch28.md) passes all four SDK
gates with1,088 tests, zero failures and783 ignored. Guide scopes/output, builtin
task handoff, process ingress and ordinary Channel/account compatibility are
joined; Docker, Main/physical and performance acceptance remain required.

The preceding [Candidate27](rust-milestone-batch27.md) passes all four SDK
gates with1,078 tests, zero failures and782 ignored. Normal SD Connect/scoped
plugin callers, unknown-SQL abort/readiness and numeric preview substitution
are integrated. Docker/package checks pass; Main and actual plugin lifecycle
proofs remain required.

[Candidate26](rust-milestone-batch26.md) passes1,075 SDK tests and its corrected
Docker/package checks at `d96d5d13`. Python model/framework compatibility and
remaining guards are explicit. Actual execution audits and four-action/child-PG/
transaction acceptance remain open. Candidate25's fixed resource preflight
stopped before measurement on fixture permissions; no gain follows from it.

The preceding [Candidate25](rust-milestone-batch25.md) is committed as
`6612ec14757bdfb7f31a3e546ed9a6cbd8a7ad7d`:1,061 SDK tests pass, zero fail
and775 remain ignored. Docker/package and all eighteen joined physical controls
pass. Selected Source/Native application workflows are qualified across the
original batch and corrected complete Redirect context, with exact cleanup and
original failed assertions preserved. Full milestone and resource acceptance
remain open.

The preceding packaged source is [Candidate24](rust-milestone-batch24.md),
`df815fbb3d312d191654bf4079f337ecae216430`. Its1,053 default tests and all four
SDK gates pass;767 tests remain ignored. Docker/package checks and eleven actual
physical output controls pass across identified attempts. Its Source-first Main
batch exits1 with exact cleanup. Both backends pass SD8, auto5 and recording
profiles4; Native also passes compact3/API6/output6. Remaining observer/scenario
and regex-state failures are preserved with prepared corrections. No full
milestone or new application performance gain is accepted.

[Candidate25](rust-milestone-batch25.md) joins common Redirect
reservations and ordinary proxy-to-Redirect alias sharing with guarded plugin
runtime/SQL/result infrastructure. All four SDK gates pass:1,061 tests pass,
zero fail and775 remain ignored. Its scoped package/application evidence is
linked above. The next joint engineering batch adds inverse/scoped preview,
remote/staged XMLTV, ordinary guide output and plugin lifecycle activation;
compiler and physical acceptance remain separate requirements. Enabled plugin Main
activation and remote XMLTV downloads remain separate active engineering work;
a compiled runtime module does not accept either feature. Next application
validation must execute the corrected affected workflows together. Earlier
Source compact/API successes qualify their outputs, but their global numbering
state must be reconstructed before comparing subsequent regex results.

The preceding source candidate is [Candidate19](rust-milestone-batch19.md): all four
SDK gates pass with 1,023 tests, zero failures and 758 ignored tests. Normal
auto-sync, large metadata, shared channel deletion, dependency-only regex,
remote stream health and Source-equivalent Beat timing are joined. Docker and
archive/credit, crypto, PostgreSQL and paired VLC useful-core checks pass. The
final-image regex import exposed Debian site-directory discovery; the third
image passes final package checks. Actual Main startup then fails the registry
ceiling corrected in20. Combined application and paired resource acceptance
remain required.

The preceding code is [Candidate18](rust-milestone-batch18.md):1,008 workspace tests,
compilation and strict lint pass; Docker build and asset/credit, GPG and
PostgreSQL probes pass. Actual VLC remux exposed missing relocated plugin
discovery; its launcher correction requires a new image and acceptance.
Combined application acceptance remains pending. The preceding built runtime is
[Candidate16](rust-milestone-batch16.md),
`921c5c40b2eb7a7073a0006b43fae86b41cd2728`. Its
[functional record](rust-batch16-functional-observations.md) includes paired
shared viewing and seven paired XC business cells, preserving strict fixture
failures. Its separate resource batch does not inherit older measurements.
The [Candidate15 application record](rust-batch15-functional-observations.md)
retains ten passing Native contexts and separate six-file media qualification,
alongside literal fixture failures and the proxy-routing gap corrected in16.
All full M0–M9 milestones remain open.

Current parallel ownership is concrete:

- Streaming owns remaining M2/M3 sharing, Redirect/preview/rejoin/exceptional
  transport and the dependent M7 media-output contracts.
- Jobs owns M6 remote XMLTV/download/streaming import, guide retry characterization,
  ingestion hooks and remaining producer/scheduler contracts; it also owns the
  EPG portion of M7.
- Operations owns M8 enabled plugins, registered jobs/actions, bounded SQL/result
  compatibility, operations and physical lifecycle. M4 session/VOD finishing is
  queued with this owner after the enabled-plugin engineering batch.
- The primary agent owns M0/M5 inventory closure, candidate integration, whole
  affected workflow batches, Docker packaging, shared resource scheduling,
  performance and M9 final acceptance/publication.

SQLite, complete contract/role coverage, enabled plugin execution, wider
transport/fault coverage, final soak and rollback remain required. Physical
containers, compatibility actors, media children and shared services belong in
the declared performance boundary. Prepared or guarded behavior is not accepted
parity. Existing Source observations are reused only with the actual state
prerequisites required by subsequent assertions.

The following candidate1/2 paragraphs retain their historical scopes.

The earlier [combined milestone candidate](rust-milestone-batch1.md) is committed
as `04026c158c8211da123b94ec67b82812f3223321`:904 Rust inputs/five coupled vectors,
whole-workspace formatting/check/strict lint/default tests passed (872 passed,
751 ignored). Its Docker image builds and passes production-ELF, UID65532 and
unchanged React/credit checks. The [native fresh-install/React restart](rust-milestone-batch1-clean-install.md)
and [packaged safe17 CLI](rust-milestone-batch1-safe17.md) workflows pass;
the remaining package/workflow acceptance is pending.
The [global batch registry](../scripts/rust-milestone-batches/README.md) replaces
individual case queues and retains M0/M1/M9 gates as well as all owner cohorts.

The next [combined worker/profile/plugin candidate](rust-milestone-batch2.md)
passes whole-workspace checks with882 tests passed/751 ignored. Its36-path
integration adds configured-Series lineage, the normal version worker, shared
input-profile processes, resolver memory bounds and HTTPS/GPG paths. Packaged
results now include the [new-image React regression](rust-milestone-batch2-clean-install.md),
[33 shared-input media checks](rust-milestone-batch2-profile.md),
[nine job consumers and a real API parent chain](rust-m6-packaged-main-batch2.md),
and [41 plugin HTTP/SQL comparisons](rust-m478-batch-progress.md). These qualify
their stated subsets. P0001 failure completion, queued artwork, full cache/request
context, broader formats and other matrix cells remain open. No whole milestone
or new performance gain is accepted; earlier image results do not transfer.

The earlier opt-in component composition is
[current892](../scripts/rust-component-checkpoints/current892/README.md).
Its unsigned plugin comparisons and recording SQL-rejection correction are
source-bound component evidence. The selected full package/performance reference
remains [image57](rust-image57-validation.md); [image62](rust-image62-build.md)
has separate immutable-build and installed-file evidence. Later library changes inherit neither
Main acceptance nor measured performance gains.

| Milestone | Owner and execution queue | Next acceptance gate |
| --- | --- | --- |
| M0: executable baseline contracts | Primary agent, with each component owner | Extend the current partial HTTP and non-HTTP Source assignments against the fixed inventory; distinguish static candidates, observed dispatch and fully verified behavior. |
| M1: bounded streaming core | Primary agent, with streaming owner | Preserve exact-source technical core evidence, complete prerequisites and test relevant changes; do not repeat a benchmark merely to update prose. |
| M2: live TS | Streaming agent (`milestone_stream`) | Qualify current Main shared viewing, XC credential rotation, authenticated capacity, upstream sharing, disconnect/rejoin, counters, timers and teardown with actual media and paired resources. |
| M3: formats and profiles | Streaming agent (`milestone_stream`) | Complete HLS/fMP4, Redirect and output-profile lanes, VLC packaging/failover, cancellation and fanout matrix, actual decoding and paired measurements. |
| M4: VOD/timeshift | Operations agent (`milestone_ops`), after current plugin ownership gates | Complete ranges, seeks, catchup and session boundaries; retain unresolved counters/finalization/sharing differences and test packaged Main behavior. |
| M5: control APIs | Primary agent, with relevant component owners | Run the current combined Native special-settings cohorts against Source51, then late-reply/caller-loss/frame/shutdown gates; wire actual Main consumers and test React/account/channel workflows and API performance. |
| M6: ingestion and jobs | Jobs agent (`milestone_jobs`) | Qualify current XC provider/filter/catchup paths; complete remote STD streaming imports, large Groups backend metadata, auto-sync and hooks, complete default producer/task/scheduler coverage, then worker-inclusive measurements. Earlier Source/Native results retain their own scope. |
| M7: outputs and integrations | Streaming and jobs agents, with primary integration | Finish Xtream/HDHR/XMLTV/M3U/realtime/WebSocket contracts, missing/stale/refresh behavior and recovery; qualify combined packaged workflows and workload measurements. |
| M8: operations/plugins | Operations agent (`milestone_ops`) | Qualify current proxy/manifest and backup flows; complete disabled ZIP/discovery/settings/files, actual enabled Python-plugin host with ORM/tasks/events, sessions/CSRF and remaining admin/config/restore/rollback/physical lifecycle compatibility. A manifest refresh is not plugin execution parity. |
| M9: final acceptance | Primary agent with all three agents | Assemble the final committed source, complete prerequisites and full workflow/package/SDK checks, then restart/soak/cutover/rollback and representative full-graph paired performance. |

## Qualified groundwork for the milestone batches

The combined892 source passes860 default Rust tests with751 external-fixture
tests ignored. The [Core51 batch](rust-core-special-settings-source51-native51.md),
[four plugin ownership cases](../scripts/rust-pluginrepo-contracts/unsigned-remaining-physical/README.md)
and [bounded audio/video recording](rust-recording-audio61.md) now have published
source-bound evidence, including failed attempts. Main/default workers, broader
formats/profiles and whole-milestone performance remain open. These results do
not transfer to a full combined application acceptance.

The following queues describe the earlier component checkpoint; the current
execution units are the milestone-wide batches linked above.

## Historical component queues

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
