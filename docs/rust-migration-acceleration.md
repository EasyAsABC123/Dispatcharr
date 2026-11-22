# Faster Rust migration delivery

This is the execution plan from the November 16 review. It changes how work is
organized, not the 1:1 backend scope, unchanged React UI, original authorship or
[formal milestone gates](rust-milestones.md#progress-ledger). Original Dispatcharr
and dependency authors retain full application and algorithm credit.
No new acceptance or application performance gain is claimed by this plan.

The candidate names and “Next” labels in this original review are historical;
use the [current ledger](rust-milestones.md#current-integrated-work) and
[worker graph](rust-default-worker-graph.md) for current scope. Matching14 and
Candidate40 aggregate/station/fresh-VOD acceptance are already recorded there;
qualified Source references are retained rather than replayed. The ownership,
one-heavy-workload rule and milestone-wide validation workflow below still apply.

At review, Git HEAD `9b6262e9` includes documentation and Source evidence. The
last qualified runtime is Candidate37 at `da75542b`, image
`sha256:83edf4700ef2be10b6812c8045d0d5fca94216861854067465dd36154f83bb88`.
Matching SDK50 is private and unbuilt, not a replacement qualified image.

## What is slowing delivery

The tracker records many successful components across different source epochs,
but every full milestone remains open. Components often need another round to
connect to Main, the default queue, actual settings consumers or lifecycle hooks.
Separate snapshots, evidence packets and fixture bootstraps add serial work.
The existing [batch coordinator](../scripts/rust-milestone-batches/README.md)
already provides resource reservations, continued independent cells and cleanup.
Use that infrastructure rather than building another orchestration framework.

There is also a feedback-order problem. The private matching SDK50 receipt has
formatting, compilation and workspace tests at exit 0, but strict lint at 101
for one Store type-complexity diagnostic. That candidate is not qualified.
Cheap affected-package feedback before freezing the integration candidate would
have exposed this without waiting for its entire workspace test phase.
Local evidence: `artifacts/rust/normal-graph-batch17-sdk-1/attempt50/checks.json`.

## November 22 build and acceptance review

The two Rust compiler steps took 708.9 seconds on `25dc1502` and 707.0
seconds on `9ace9f66` across the ordinary and copied-data images. Both
modes rebuilt 26 local crates. The copied-data build already reused the
ordinary compiler layer. The frontend, media, and helper layers were
cached. The largest measured opportunity is to reuse verified unchanged
Rust inputs across candidates. Measure content-aware cache invalidation
with distinct warmed cache namespaces and verify both binaries, image
provenance, packaged behavior, and same-path source changes. Savings
from this experiment are unmeasured.

The historical best required matrix passed 26 of 127 rows on `4558a85b`.
Of the 100 blocked rows, 47 have `m6/` identifiers. The separate
`Main/settings-task-consumers` row was also blocked. These are acceptance
counts for required workflows on that exact package.
The separate `25dc1502` report passed 21 on its own package.
[Formal matrix and scopes](rust-m9-4558a85b-formal-compose.md)

Prioritize the ordinary producer, worker, and scheduler graph. Recent
explicit Selected/Core pairs demonstrated actual scheduled backup and two
linked XMLTV schedules. The next scope is installed known schedules,
restart retention, and actual Scanner and Recurring jobs. Resolve normal
receiver readiness and single Beat ownership before treating opt-in
results as evidence for normal startup. Continue independent playback
and copied-data work while the scheduler change is prepared.
[Worker coverage](rust-default-worker-graph.md)

Two ordinary-only candidates spent 347.3 and 342.3 seconds in the Rust
compiler step. `66b02c4a` used one build for the XMLTV scheduler and the
HDHomeRun response fix. Its queued XMLTV-only build ended before contacting
the engine. The same built application also verified a separate Native
profile recovery workflow. Keep using one immutable image for related
runtime changes and shared fixture bootstraps with independently asserted
results. Test observer corrections on the same image. Use the coordinator's
full workflow binding early when the acceptance scope is already known.
[Combined Compose checks](rust-m0-m6-66b02c4a-xmltv-hdhr-checkpoint.md)

Reserve the final whole-matrix rehearsal, soak, and release resource
comparison for one candidate. The historical 100-viewer shared VLC MPEG-TS
comparison continues to demonstrate one upstream, equivalent decoded work,
82.6% lower CPU, and 71.0% lower peak working memory on its named image.
The current release-wide resource gate remains open.
[100-viewer measurement](rust-m2-100-viewer-measured.md)

## Parallel ownership with a bounded integration queue

Use four concurrent agents on this host: one integrator and three work owners.
Additional agents can review bounded questions in later waves; more simultaneous
builds and fixtures would compete for the same machine and increase storage.

| Owner | Deliverable | Integration boundary |
| --- | --- | --- |
| Integrator | One candidate, gate matrix, SDK/image/package run, release accounting | Own shared routing, registry, shared target and dispatch leases |
| Jobs owner | M6 ingestion, matching, natural producer-to-worker and child workflows | Own Store/job implementation paths; agree transport interfaces first |
| Playback owner | M2/M3 and dependent M4/M7 playback, formats, sharing and recovery | Own media/session/protocol paths; preserve actual default Main behavior |
| Operations/API owner | M5/M8 APIs, plugins, restore/admin and UI workflows | Own control/operations paths; coordinate shared router changes |

Assign each task concrete paths, interfaces, Source authority and its complete
workflow test before editing. The same owner supplies implementation and tests;
peer review concentrates on the interface or risk that actually changed. Owners
prepare the next integration batch while the integrator runs the frozen one.
Never edit the tested snapshot. Keep at most one ready next batch per owner to
avoid a growing collection of qualified but unintegrated components.

Update exact case bindings as part of each workflow handoff, using the existing
[contract register](rust-contract-register.md): method/action, role, fixture,
assertions, actual result and source/image identity. Its zero fully audited
operation rows describe incomplete mapping, not zero implemented behavior.
Do not leave acceptance accounting until the end or create more Source captures
where the missing work is binding existing observations.

## One repeatable development and validation loop

1. **Select workflows.** Pull required cells from the existing milestone matrices.
   Each cell names the production entry point, Source authority, setup/reset,
   assertions, owner, dependencies and current blocker. Preserve the complete
   required matrix even when only its executable cells can run.
2. **Develop in parallel.** Use focused unit/contract tests and affected-package
   check/lint for fast feedback. Batch related failures into one owner handoff.
   Do not add tests that merely mirror implementation or repeat proven checks.
   For matching, first fix the complex callback type with a named alias and run
   affected Store strict lint; do not suppress the diagnostic. Prepare the 11
   actual compiled SQL statements against the owned migrated schema before
   spending time on an image build. Each owner supplies a runnable affected
   cohort, including existing-runner preflight for observer access and required
   metadata, rather than another validator for every field.
3. **Integrate once.** Join reviewed changes, run the four SDK gates on frozen
   inputs using one shared Cargo target, then build one committed candidate
   image and run shared package checks once. Record all independent gate exits;
   any required failure prevents qualification. Rebuild only if runtime inputs
   change. Harness-only fixes can reuse the same image with new harness identity.
4. **Run complete affected workflows.** Use existing Compose fixtures and batch
   adapters through Main, normal authentication, the actual queue and children.
   Reuse bootstrap within a cohort with explicit reset boundaries. Independent
   failures continue when their fixture remains valid; uncertain cleanup stops
   dispatch. Record passed, failed, blocked and not_run separately.
5. **Repair narrowly, accept broadly.** Diagnose the smallest failed case,
   correct its owner, then run affected/dependent cases. Before accepting a
   milestone, run its complete required batch on the final candidate. Do not
   rebuild or replay unrelated Source cases merely to obtain a green report.
6. **Measure and deliver.** Run the milestone's representative paired resource
   phase in a quiet window. Commit implementation and compact evidence; push
   to `rust-fork/rust-backend-migration` and verify the receiving ref. Close
   only gates actually met; carry explicit required blockers forward.

A bounded harness improvement belongs inside the next matching batch. Source51
already reused one Compose context for 14 cases in 149.562 seconds, but issued
272 external command receipts totaling 106.939 seconds of command wall time.
That total includes necessary work; it is not removable overhead or CPU usage.
Its SQL helper starts Python/Django on each call, and Redis polling starts a
Compose/redis-cli command. First combine seed, receiver setup and before-state
reads into one existing SQL-helper call per case where session and ordering
semantics permit. Likewise combine settled after-state and receiver reads.
Compare the complete 14-case projections, frames, events and elapsed time.
Preserve transactions, journals, terminal barriers and cleanup. Try a fixture-
local Redis connection only if polling startup remains material; do not build a
new persistent control service. See the
[Source51 scope](rust-channel-matching-source51.md) and existing
[recording fixture](../scripts/rust-integration/recording-connect-cohort/README.md).

The host currently permits one heavy Cargo, application/media or Docker build
workload at a time. Keep that limit until measurements justify changing it.
The coordinator supports up to three ordinary fixtures, but dispatch parallel
functional fixtures only when each is actually lightweight and their resources,
ports, state and cleanup are independent. Benchmarks always run alone.

## Prioritize work that completes production behavior

| Order | Batch | Exit condition and unresolved scope |
| --- | --- | --- |
| Next | Finish the integrated matching candidate | Resolve SDK lint, physical SQL/schema checks, image/package qualification and the full retained-Source51/Native14 matching cohort. No replay of already qualified Source cases. Model-enabled inference, wider concurrency and plugins remain explicit required gaps. |
| Alongside | Default producer/worker graph | Map missing behavior to real required workflows; implement queue blockers and descendants before polishing isolated helpers. Unsupported known work is blocked, not a passing refusal. |
| Following | Current Main playback/control cohort | Auth, useful decoded media, fanout/upstream count, stats/settings consumers, unchanged React and disconnect/recovery on the same candidate; include the applicable M2/M3/M5 matrix. |
| Following | VOD/protocol and operational cohorts | Run actual range/seek/session and output contracts; backup/restore/admin/plugins under real configuration. Keep SQLite and extension execution visible rather than hiding them behind success subsets. |
| Per ready milestone | Resource comparison | Functionally qualified paired workloads, equivalent useful work and declared service boundary. Results bind to candidate and workload. |
| Final | M9 release rehearsal | Complete required matrix, fresh install/upgrade, at least two-hour soak, copied-data rollback and final full-service resource acceptance. |

These are integration waves, not a claim that the entire M6 must finish before
other owners work. Select the next wave by which ready implementation removes
required workflow blockers. Do not let the broad M0 inventory gate prevent
running or recording other milestones' functional and performance subgates;
formal dependency and full-parity requirements still apply.

For queue work, review SD station discovery, all-active-account refresh, VOD
ingestion and comskip against the [producer inventory](rust-source-task-coverage.md).
Distinguish genuinely missing natural consumers from inline-only decorated
helpers; an unmapped declaration is not automatically another worker to build.

## Stop spending time on the wrong work

- Reuse qualified Source observations unless a new fixture mutation, issued
  token or actual clock dependency requires fresh execution. Capture genuinely
  missing Source behavior together in one reference cohort.
- Classify failures before changing code: Rust behavior, observer/harness,
  infrastructure, pre-existing Source defect, or unknown. Preserve original
  failures. A Source defect is not automatically a Rust waiver; document its
  effect on parity and the still-failing criterion. Do not silently weaken
  decoder, cleanup, role or performance assertions.
- Give reproduced Source defects a concrete decision record and owner. The
  shared [restart audio failure](rust-restart-audio49.md) and
  [SQLite migration failure](rust-sqlite-compatibility.md) stay failed; neither
  should prevent independent live/API batches from running. End diagnosis when
  a causal reproduction and next implementation or baseline decision exist,
  rather than trying ungrounded settings indefinitely.
- Keep one run index with source/image/harness identities, commands and actual
  exits, assertions, relevant raw logs, resource observations and cleanup.
  Reference shared inputs by hash. Add another evidence validator or negative
  control only for a demonstrated failure mode or material risk.
- Use the existing full SDK gates for integration candidates, not every draft.
  Ignored fixture-dependent tests remain unexecuted until their actual workflow
  runs. A larger test count is not progress toward a missing behavior.
- Retain source, logs and necessary evidence binaries; retire reproducible old
  compilation output after confirming no active run depends on it. No new
  per-case Cargo target directories or compiler-cache copies.

## Performance and progress that can guide decisions

At each integration handoff report: required cells implemented, required cells
executed/passed, remaining blockers by owner, candidate identity, and the next
complete acceptance batch. Distinguish functional coverage from measured gains.
Do not report an overall percentage from test counts or document volume.

Record gate duration, image builds per runtime candidate, repeated Source boots
with their causal reason, fixture setup time versus useful test time, harness
failures versus product failures, peak artifact storage and time from a ready
patch to production-workflow feedback. Collect these from existing receipts;
start with descriptive totals, not a new telemetry service or arbitrary targets.
Use the next two integration batches to establish whether the revised workflow
actually shortens delivery. No acceleration factor is promised in advance.

For application performance, retain the existing minimum three alternating
paired repetitions after warmup and the fixed workload budgets. Include backend,
workers, media children, database/cache and other declared participants. Record
throughput, errors, latency, CPU and correctly named memory quantities, including
post-disconnect retention. Historical image57 gains and coordinator-tooling CPU
improvements stay bound to their original experiments.
