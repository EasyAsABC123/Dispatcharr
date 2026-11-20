# Plan to complete Dispatcharr Rust parity

Planning baseline: the published migration branch at 56afe8d63. Original
Dispatcharr and dependency authors retain credit for the application, models,
algorithms and protocols. The existing React UI, supported configurations and
persisted data are part of the compatibility contract.

The repository defines **M0 through M9**. M9 is the existing release-candidate
parity and cutover rehearsal gate. **M10 and M11 below are proposed release
stages**. They make a controlled rollout and the decision to switch defaults
explicit. They do not change the existing M0–M9 acceptance criteria.

## Starting point

The network-disabled M0 SDK passed all 119 tests, reproduced its register exactly,
and verified partial Original coverage for 797 of 798 HTTP declarations and
all 250 typed declarations. The new observations cover Celery task registration
and Django command discovery. Existing observations cover Django model and
schema migration behavior. A separate packaged Original run verified the
anonymous React root and both baked assets with an explicit disposable login
locale. The SDK binds that capture to the original root declaration.
[SDK receipt](../scripts/rust-contracts/sdk-live-typed250.json)
[Original packaged root](rust-m0-original-aio-root.md)

The historical media comparisons established one upstream for 100 simultaneous
live TS viewers and measured about 83% lower CPU and 71% lower peak working
memory at matched work. A separate two-hour soak completed twelve guide
refreshes with ten viewers sharing one upstream and provider slot. The release
candidate still needs integrated equivalents. The Auto Sync full-scan latency
check remains scheduled at the end of functional migration.
[100-viewer comparison](rust-m2-100-viewer-measured.md)
[Two-hour soak](rust-m9-guide-lock-diagnostic.md#two-hour-playback-and-refresh-soak)

The exact coordinator exports **127 required nodes**. The required roster has
44 playback and management nodes, 28 protocol and operations nodes, 48
provider and scheduler nodes, and 7 cross-milestone nodes. Each owner binds
the candidate, Source authority and declared effects for its executable
workflows. The current M6 owner admits two partial probes. Complete
provider, scheduler and performance workflows require bindings with their
normal runtime roles and full assertions.
[Coordinator guide](../scripts/rust-milestone-batches/README.md)

The C74 package passed nine distinct required workflow nodes across six
runs. The checks covered 11 stock React fresh-install, 17 safe operator,
22 shipped `createsuperuser`, 154 strict API, 64 current-programme and
43 guide-grid cases. Three MPEG-TS nodes passed the 24 two-worker checks,
14 delayed restart checks, and the functional retained-history fixture
with 100 late readers and 20 audio/video decodes. Each coordinator run
exited 2 with the larger roster still open.
[C74 package checks](rust-c74-clean-install-operator.md)

The [e852 permissions and management baseline](rust-e852-permissions-management-baseline.md) ran all 18 suites.
It recorded 1,023 passing checks and one response-body mismatch for a
malformed settings ID. Cleanup passed. The coordinator exited 2 with
this node failed. The nine C74 passes remain scoped to C74.

The [735 persisted password upgrade](rust-735-auth-hash-acceptance.md) passed 34 checks and
verified cleanup on 735, covering eight persisted credential variants.

The [a785 persisted password upgrade](rust-a785-auth-hash-acceptance.md) passed 34 checks across eight persisted credential variants against a fresh
Original fixture and independently verified Compose cleanup.

The [735 createsuperuser and typed comparisons](rust-735-cli-acceptance.md) passed 22 shipped
createsuperuser cases and 154 typed comparisons on 735. The coordinator
recorded two passing nodes and verified cleanup.

The [a785 createsuperuser and typed comparisons](rust-a785-cli-acceptance.md)
passed 22 shipped CLI cases and 154 typed comparisons with verified cleanup.

The [735 operator and guide acceptance](rust-735-group-acceptance.md) passed four required nodes
on 735. It verified 11 clean-install checks, 17 safe CLI cases, 64 current
programme cases and 43 guide grid cases with verified cleanup.

The [735 permissions and management baseline](rust-735-baseline-acceptance.md) passed all 1,024 checks
across 18 suites with verified cleanup. It matched the exact HTTP 404
body for the malformed settings ID. The coordinator recorded one passing
node on 735.

The [735 MPEG-TS functional checks](rust-m9-735-functional-streaming.md) passed
all three required nodes with verified cleanup. The run covered 24 two-worker
checks, 14 delayed restart checks and 100 retained-history readers with 20
audio and video decodes in the history fixture.

The shared verification package kept the existing React inputs and combined
the explicit VOD session change, opt-in Original Celery playlist refresh, and
missing-row backup settings recovery. The scoped Compose checks passed.
Fresh VOD admissions still differ at the measured provider-slot boundary.
The complete Native worker graph, packed application suite, and final
resource comparison remain open.
[VOD session evidence](rust-vod-explicit-session-id.md)
[Refresh All evidence](rust-m6-refresh-all-original-worker.md)
[Backup settings evidence](rust-m8-backup-missing-row.md)

## Orchestration

The integration agent owns the candidate branch, merge order, shared Cargo
target, fixture reservations, evidence ledger, Git publication and final release
decision record. Three subagents can run beside it at once. Use many scoped
assignments over the project by rotating those slots as work finishes. Work in
parallel across milestones even when final acceptance requires another
milestone's evidence.

| Persistent queue | Milestones | First work packets |
| --- | --- | --- |
| Playback and media | M1–M4 | Bind clean core write failure and retry, then live TS ownership/failover and the HLS/fMP4/profile and VOD/catchup cohorts. |
| Application and outputs | M5, M7 | Bind React account/settings/channel flows and role contracts. Continue protocol output, browser, realtime and integration parity separately. |
| Providers and durable jobs | M6 | Verify the recent XC fixes in the normal worker graph. Expand ingestion, guide and scheduler/physical-recovery cases in parallel packets. |
| Baseline and operations | M0, M8–M11 | Map executable Python evidence by route family. Close copied-data restore, datastore, plugin and operator gaps. Prepare final test and rollout prerequisites. |

Keep two implementation packets and one independent validation, baseline or
harness packet active where feasible. The integration agent can take the
remaining queue or unblock shared files. Reassign a slot when a packet reaches
review. Cross-check high-risk contracts with an agent other than the author.

Each packet names the base revision, contract IDs, exact Python reference,
production Rust entry point, owned paths, proposed checks and handoff. The
handoff includes actual exits and evidence paths, any behavior or binding still
missing, and the smallest next action. Use isolated overlays and scoped commits
to avoid overlapping edits. Reuse one Cargo target and pinned dependencies.
Where an implementation and a test depend on each other, prepare both in
parallel and run the check against the merged change.

Use focused tests during implementation. Reuse valid Python captures and
run the smallest new Python cohort only for required behavior without a reference.
The existing coordinator serializes shared resources and heavy fixtures.
Performance must have an exclusive quiet window. Reserve the full acceptance
campaign for the integrated release candidate.

## First assignments and blockers to resolve early

- **Playback agent:** Verify M2 ownership/failover and bind its React playback
  workflow. Characterize and implement the explicit HLS output-profile path,
  which still has a guard, and rotate into archive idle adoption. Confirm the
  Source reservation and busy-counter behavior before changing ownership.
- **Application agent:** Reconcile M5 and M7 owner rows with the current Main
  routes. Start with multipart provider actions, broader XC update and nested
  groups, and content-bearing account deletion. Then bind guide response
  negotiation, catalogue, HDHomeRun and realtime client workflows. Verify
  custom XMLTV behavior and nonzero XC EPG hash modes against the pinned
  implementation.
- **Baseline and operations agent:** Open the datastore track immediately.
  The retained file-backed SQLite migration on pinned Python fails at
  Channels 0038 with a syntax error near the regex operator. Native is currently
  PostgreSQL-only. Establish the Source default engine and a legitimate,
  complete existing or freshly created SQLite database. Qualify persistence,
  upgrade and restore with that authority. A partial or skipped migration
  cannot characterize the supported behavior. In parallel, map retained M0
  references and bind actual backup, admin and plugin workflows.
- **Integration agent:** Own the first XC through-Main packet and queue the
  default worker/Beat packet. Verify both user-agent paths, cache writes,
  per-profile recovery and error events. Verify startup and readiness when a
  worker faults. The selected worker and Beat flags currently default off.
  Validate normal deployment with one scheduler, required consumers present
  and the inherited queue drained without reordering or duplication.

Use the M235 `Main/lifecycle-fault-workflow` and `Main/React-cross-workflows`
entries, the M478 VOD recovery/range and protocol/operations entries, and the
M6 default-job-graph/physical-restart entries as the initial binding targets.
The datastore blocker only prevents the relevant acceptance gates. Media,
API, job, contract and plugin implementation keep moving alongside it.

## Remaining work by milestone

| Milestone | Work packages | Acceptance evidence |
| --- | --- | --- |
| **M0 · Executable contracts and baseline** | Group the remaining register work by route family and role. Reuse the pinned framework suite and verified captures when they establish the exact method, route, actor and assertions. Resolve dynamic methods and aliases from Django/DRF runtime routing. Map non-HTTP tasks, CLI, environment, schema, plugin and operational contracts. Freeze the relevant workloads, budgets and measurement boundaries. | Another developer can reproduce each required behavior in Compose. Stable IDs, actual methods/roles, fixtures, results and owning milestones are recorded. Keep partial references, unresolved methods, invalid observations and valid negative baseline runs distinct. Document any baseline metric without a valid observation and cover it if required. |
| **M1 · Bounded streaming core** | Reuse the bounded core technical evidence. Bind the current candidate's clean write failure and retry, sequence/index consistency, discontinuities, source switch, lagging and fast readers, cursor alignment, wakeups, retention, idle expiry and shutdown. | Deterministic Rust and Compose tests show bounded media and in-flight references, no lost wakeups or premature index advance, and decoder-valid output across switches. Run the declared equivalent-work core comparison. M0 remains a formal prerequisite. |
| **M2 · Live TS** | Cover existing-client auth and privacy, provider/profile and channel limits, same-channel sharing and multi-worker ownership, reconnect, alternate loss/recovery, cancellation and stat/stop cleanup. Bind the unchanged React playback and viewer stats flow. | Compare actual clients and origin/slot counts, live decode and resource release after churn and owner loss. The same channel and profile share the expected upstream pipeline. Validate no-warmup and fault behavior where required, plus the scoped fanout budgets. |
| **M3 · HLS, fMP4 and profiles** | Exercise cold and warm HLS, playlist/segment/capability and TTL/stop semantics, packet discontinuity and source switching, fMP4 init/fragments, native and external FFmpeg/VLC profiles, share boundaries and process termination. Verify the new HLS timing and provider CA changes through Main. | Each supported format passes player/decoder checks and failure scenarios. Shared viewers use bounded pipelines and external processes. Per-format CPU/RAM/throughput/latency include the media processes. |
| **M4 · VOD and timeshift** | Cover method/header/body Range semantics, seek/decode, redirects and credentials, alternate providers, session limits/retirement, profile restrictions, XC movie/series addressing and archive/catchup variants. Close missing-ID, upstream errors and restart scenarios. | Compare wire status/headers/ranges, player seeking, provider connections and cleanup under load. Run the declared VOD/catchup resource comparisons on the release image. |
| **M5 · Management APIs and React** | Cover bootstrap, login/logout/password hashes, JWT refresh, API keys, proxy identity and permissions. Finish accounts, users, groups, settings, profiles, channels, logos, filters, pagination, coercion/errors and existing database behavior. Exercise settings through their real runtime consumers. Repeat the channel range and HEAD cases through Main. | Compare expected status/body/headers and authorization for actual roles, SQL effects and IDs. Run browser workflows on both backends with the existing React build and copied datastore fixtures. Meet API load and idle-footprint budgets. |
| **M6 · Providers, guide and jobs** | Verify XC create/group/category/refresh/delete, configured and fallback UA effects, retry exhaustion between profiles and WebSocket ordering/precision. Expand M3U local/remote/compressed imports, XC/VOD, XMLTV/Schedules Direct, matching/artwork and guide changes. Exercise durable Beat/default worker startup, schedule/timezone/DVR callbacks, queue lineage, FIFO, retries, cancellation, restart and physical cleanup. | Compare database rows/sequences, provider requests, cache/WS effects, actual task publication and terminal results. Prove jobs survive process/connection loss with supported retry or cancellation, and no duplicate ownership or partial-data loss. Measure refresh and playback-during-refresh. |
| **M7 · Protocols and integrations** | Complete M3U/XMLTV/Xtream output, HDHomeRun discovery and lineup, EPG and catalogue client workflows, telemetry/stats, notifications, WebSocket auth/lifecycle and integration delivery. Verify recent multicast and HDHomeRun fixes in the packaged application. | Compare wire bytes/ordering where the protocol requires them. Use actual clients and authorized, standard and anonymous paths where applicable. Confirm dependent state and realtime effects through container networking, with fanout/generation/delivery budgets. |
| **M8 · Operations and compatibility** | Verify fresh install and upgrade, SQLite and PostgreSQL support, archive round-trip and rollback, admin/session/CSRF, logs, static/media and CLI/env behavior. Cover existing Python plugin imports, settings and actions, task callbacks, managed archives, and real dependency/runtime behavior. Test Latin-1 and Unicode backup filenames on the actual download route. | Copied databases preserve IDs, hashes, media references and schedule state. Restore and supported operator workflows pass on both backends. Fixture plugins make real API/task calls. Measure the service including its compatibility workers. |
| **M9 · Integrated test and release rehearsal** | Freeze one reviewed release commit, SDK/source map, immutable image and configuration. Bind M0–M8 and verified datastore scope, then run the whole Compose matrix and unchanged frontend against the release image. Run the paired performance campaign, at least two hours of soak with viewing and refresh activity, and restore/rollback rehearsal on copied data. | All required contract/functional/measurement cells pass, with actual exits, matching useful work, resource limits and verified cleanup. Record fresh-install and upgrade paths, release image, configuration, restore/rollback commands and expected results. One passing component check or historical benchmark cannot close the release. |
| **M10 · Proposed controlled rollout** | After M9, rehearse the chosen deployment at production scale with isolated state or a designated canary. Set traffic and time bounds, monitoring and rollback thresholds before starting. Prevent competing schedulers, database writers and upstream sessions. | Confirm observed playback, ingest, admin, plugin and recovery behavior with the release image. Verify monitoring, data integrity, stop criteria and rollback readiness. Any real deployment requires an explicit release decision. |
| **M11 · Proposed default switch and handoff** | Use M10 evidence to decide whether to switch the default. Prepare release notes, compatibility matrix, upgrade/rollback runbook, dashboards and operator handoff. Keep a tested fallback available during the declared rollback period. | Verify the released digest and default configuration, successful fresh install/upgrade, source credits, operator ownership, release evidence and post-cutover monitoring. Retire the fallback only after a separate decision backed by the agreed retention and rollback evidence. |

## How the final campaign will run

1. **Prepare bindings while the implementation lanes run.** Each required cell
   has a Source reference, real application entry point, supported actor and
   datastore, fixture/reset rule, compatibility assertions, time/resource
   budget and cleanup. Identify precisely when a new Source run is required.
   Track availability as implemented, missing implementation, fixture/binding
   blocked, or evidence missing. Track execution separately as pass, fail,
   blocked or unrun.
2. **Freeze and qualify the candidate.** Check formatting, strict lint and whole
   workspace tests. Build the immutable Docker image once and verify packaged
   Main, normal worker graph, UID, runtime dependencies, React and credits. Bind
   each final group to this exact image and supported configuration.
3. **Run integrated functional parity.** Schedule independent cases in parallel
   through the existing coordinator. Keep required causal chains together,
   including queued descendants and persistence across restarts. Retain literal
   differences. Qualify fixture clocks, generated IDs, transport or concurrency
   variation only with an explicit per-case invariant and retained evidence.
   Unresolved business differences, ignored required tests and missing
   observations cannot pass. Resolve unsupported-scope questions with pinned
   Python or an explicit product decision before closing a requirement.
4. **Measure and rehearse on the accepted functional snapshot.** Compare paired
   Python and Rust runs at workloads and budgets declared before execution.
   Verify equivalent useful media/work, correctness, latency, throughput, CPU
   and memory. Account for database, cache, compatibility workers, media
   children and observer costs. Workflow parity precedes the two-hour soak and
   copied-data rollback rehearsal. The coordinator requires workflow and soak
   before whole-service performance. The owner-specific performance gates
   still apply. An image change requires rerunning the affected evidence.
5. **Publish the result.** Record passed requirements and any remaining blockers,
   the exact image and release instructions, the measurements with their scope,
   and the maintenance handoff. A passing M9 rehearsal establishes parity for
   the declared supported scope. M10 and M11 record actual rollout decisions
   and outcomes separately.

Use the [milestone ledger](rust-milestones.md), the
[latest development checkpoint](rust-milestone-batch69.md), and the
[coordinator guide](../scripts/rust-milestone-batches/README.md) as the living
links between work, test bindings and evidence.

## M10/M11 operator handoff

M10 and M11 remain proposed. Record each decision against the exact image
and effective configuration selected for that stage. The release record should
carry the source revision, platform digests, configuration, decision and owner,
observations, and the fallback digest and runbook.

| Decision | Current evidence | What the operator needs to record |
| --- | --- | --- |
| **M10 · Select the release candidate and supported platforms** | Two amd64 packages passed 15 scoped PostgreSQL/VLC probes. ARM64 donor and Comskip identities passed offline verification. [Package evidence and native checks](rust-m10-amd64-pg-vlc-portability.md) | Select the exact M9-qualified digest and effective deployment configuration. Bind per-platform runtime checks for the supported scope, including native ARM64 package and dependency/attribution checks where ARM64 is supported. The amd64 component results do not establish full M9 or platform-wide readiness. |
| **M10 · Start and evaluate the rollout** | The current evidence is from isolated package and copied-data fixtures. | Record the isolated production-scale rehearsal or designated canary, traffic and time bounds, recovery targets, and playback, ingest, admin, plugin, resource and data-integrity thresholds. Identify the single writer, scheduler and upstream owner per scope. Record the actual observations and stop/continue outcome. Real deployment requires the release decision already called for in M10. |
| **M10 · Stop and restore the fallback** | Candidate110 and Candidate111 restored a transactional PostgreSQL copy, reused the front door and authenticated session, confined the Native edit to the copy, and matched the complete Original fingerprint before restart. Both strict controllers exited 1 after the same four timestamp changes reproduced by an independent Original restart. [Exact copied PostgreSQL rollback](rust-m9-copied-postgres-rollback.md) | Bind rollback to the selected release image and configuration. Record how to stop writers and workers, retain or reconcile writes accepted after cutover, resume the fallback, and verify service and data integrity within the declared recovery targets. The integrated strict M9 rollback gate remains open. |
| **M11 · Switch the default and transfer ownership** | Package and copied-data results supply bounded inputs to the M10 decision. | Use the actual M10 result to decide whether to switch. Verify the served release digest, effective default configuration, supported platforms and roles, and successful fresh install and upgrade. Hand over release notes, compatibility matrix, operator ownership, dashboards and the upgrade/rollback runbook. Retain the unchanged React UI and original application and dependency credits. [Attribution](../ATTRIBUTION.md) |
| **M11 · Monitor and retire the fallback** | The isolated PostgreSQL rollback evidence covers copied data and the recorded restart outcome. | Keep the tested fallback available throughout the declared rollback interval and record post-cutover monitoring and recovery evidence. Decide separately whether to retire it using the existing retention and rollback criteria. |
