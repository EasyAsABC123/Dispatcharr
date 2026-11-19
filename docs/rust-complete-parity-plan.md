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

M0–M8 are in progress and M9 is planned. M0 records 175 partial executable
Python HTTP references, with 623 operations lacking one. The 322 unresolved
method declarations overlap those categories. These are coverage counts. A
separate implementation audit establishes which Rust behavior is
missing. The latest parallel fixes have focused compilation, tests and
reference controls. The exact Candidate68 image completed all twelve XC
lifecycle calls and matched five task result values, with cache and
WebSocket differences remaining. The follow-up code needs verification through
Main. Candidate67 passed twelve scoped live-viewing budget pairs. New output
formats, active jobs and the final release image require their own evidence.

The existing coordinator exports **127 required nodes** shared across the
playback/API, protocols/operations, jobs and root owners. This is the starting
execution matrix. Check each node against the requirements below, including
shared workflows that cover multiple contract IDs. Record any required cell
without a production entry point or runnable binding as a blocker.

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
