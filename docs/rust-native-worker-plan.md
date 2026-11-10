# Native worker and scheduler acceptance draft

Original Dispatcharr authors and contributors retain full application and task algorithm credit and AGPL licensing. This is a code-reading plan prepared during the image28 guide measurement quiet window. It adds no worker, queue consumer, scheduler, public202 route or execution acceptance. The existing [transport plan](rust-native-job-transport-plan.md) and its Source55/envelope26/result67/nativeInterop65/physical2 evidence remain separately scoped.

## First supported handler and queue ownership

Use copied `rust_probe_backup_*` PostgreSQL databases, isolated broker8/result9, fixture-only secrets and an internal network. Production Celery queue `celery` contains multiple task families: an allowlist in a consumer is insufficient, because reserving and acknowledging an unsupported message can lose another worker's task. Before a native consumer runs, capture actual Source reservation/unacked structures, routing, acknowledgment and requeue transitions in a dedicated owned queue. Any production queue partition needs its own routing/interop decision; silently moving original publications to a new queue is not1to1 parity.

The first actual execution candidate is `apps.backups.tasks.create_backup_task`, using the already cross-loaded PostgreSQL17 archive component. Its Source wrapper catches service errors and returns a SUCCESS backend record containing either `{status: completed, filename, size}` or `{status: failed, error}`. A service error must not be rewritten into CeleryFAILURE. Actual uncaught worker exceptions and Source date_done/children/traceback handling require independently captured result-writer cases. Do not construct a result record from the HTTP status mapper.

Bound queue envelope bytes, supported task arguments, concurrent worker slots, active database connections, child processes, diagnostic pipe bytes, archive spool bytes and physical Redis commands before implementation. A reserved task keeps its task owner and worker admission through actual database/Redis replies and child reap, including caller cancellation, stop and failed drain. Idle workers use a bounded blocking receive rather than periodic database polling. Unsupported envelopes must follow a captured non-lossy boundary; do not invent successful acknowledgment.

## ACK, crashes and result consistency

The pinned Source defaults are earlyACK, track_startedFalse and prefetch4. Source55 observed that an active blocked task no longer had its reservation in unacked storage and a replacement worker did not immediately replay it. That observation does not prove native acknowledgment scripts, one-hour visibility redelivery, exactly-once execution or a full crash state machine.

Capture Source at reservation-before-ACK, ACK-before-handler, child-start, database/archive-complete-before-result, result-accepted-before-reply, and shutdown with prefetched work. Kill only owned fixture processes, retain queue/unacked/result/ZIP/database evidence and task identities, then restart the original worker. A backup may be created without a visible result at a crash boundary. Do not automatically rerun such a task or claim exactly-once service behavior. Match observed early-ACK loss where required; stronger recovery or deduplication needs an explicit documented compatibility decision.

Native result publication must retain its own physical-command and task leases until the actual Redis reply. Use the captured result namespace and3600-second expiry, preserve broker/backend separation, and prove duplicate task IDs, stale writers, wrong key types, lost/paused replies and process restart. A delayed reply can be ambiguous. Record that state rather than returning fabricated failure or success. Task-token HMAC and existing authentication precedence stay outside the worker and remain unchanged.

## Index handler follows its own contract

`apps.epg.tasks.build_programme_index_task(source_id, _defer_retry=0)` acquires the Source per-source file lock. When busy it publishes a new task with the same positional source ID, incremented `_defer_retry` and the Source countdown, then returns; at the Source deferral maximum it returns without a new publication. That is a new message and successful null return, not an inferred CeleryRETRY state. With the lock held, a renewer spans `build_programme_index`, and stop/release run in finally.

The first index publisher must capture direct and busy/deferred protocol2 messages, exact task name/options/ETA/timezone and allowed scalar argument conversions before expanding the current backup-only enum. The caller's retained lookup context spans durable publication; ambiguous publication cannot be reported as a successful Parsing response. Existing current-file/index components do not establish that dispatch or a native worker exists.

For execution, compare actual Source file/index SQL outcomes on nonempty guide data, including missing/corrupt files, busy locks, defer exhaustion, renewal loss, cancellation, partial output, stale source replacement and database failure. Keep a task/file owner through physical operations and child cleanup. Preserve Source ordering and null/exception outcomes; do not promote the component solely because its scanner or SQL sink passed separately.

## Scheduler is a separate milestone

Source uses `django_celery_beat.schedulers.DatabaseScheduler`; scheduled backup behavior also creates the ZIP before retention cleanup. Capture database schedule rows, crontab/timezone semantics, last-run and total-run updates, due-time boundaries, disabled entries and concurrent scheduler ownership before implementing a native scheduler. Fixture startup must keep unrelated DVR/provider/plugin/version tasks quarantined using the original fixture rules.

Prove restart at claim, publish and schedule-row-update boundaries with the actual original scheduler, then a native implementation. Scheduled retention keeps newest-first ordering, disables deletion for nonpositive counts, and catches individual delete errors while continuing. A local timer is not durable scheduling. First-install schedule initialization and production schema upgrades remain separate migration gates.

## Promotion sequence

1. Original-only reservation/ACK/result-writer and direct/deferred index capture, with bounded private raw records and sanitized public relations.
2. App-unwired native consumer/result writer, one owned queue and CreateBackup only; real Source-produced message input and Source status-reader compatibility.
3. Copied-database ZIP cross-load plus child/Redis/database cancellation, admission, shutdown and crash/restart matrix. No original/default database restoration.
4. Index task publication, then lock/defer/renewal/SQL/file execution gates; preserve its distinct wrapper semantics.
5. Durable application dispatch/status202 only after actual execution, task authorization, restart and physical ownership evidence. Successful restore additionally requires coordinated reader/writer quiescence, archive validation and schema/upgrade recovery, and is not authorized by CreateBackup acceptance.
6. Original/native scheduler database and restart interoperability, separately bound performance workloads and immutable packaged gates.

No tests, fixtures, builds or runtime changes were executed to produce this draft. All acceptance remains pending the named evidence. Guide performance evidence and policy are untouched.

## First fixture execution evidence

The first Create worker component has now completed its isolated gates; this paragraph supersedes the draft's earlier no-execution status only for this named scope. [Copied-database fixture2](../scripts/rust-job-contracts/verified-native-create-worker-2.json) binds24 real checks, exact frozen Rust/native binary and original producer/status/archive readers. [Physical resources1](../scripts/rust-job-contracts/verified-native-create-worker-resources-1.json) binds two actualRedis reply/cancellation/admission/drain controls, five frozen library tests and all-target strict Clippy. Their runtime source hashes match; the second snapshot adds resource tests. All disposable projects were removed and the default database remained unchanged. Attempt1's incorrect startup-task oracle and a validation invocation without the pinned toolchain environment remain retained as failures; fresh corrected runs supply the accepted evidence.

A Source-produced Create task ran through the native archive service using original PostgreSQL17 client tools in the fixture. Source AsyncResult/status and ZIP/pg_restore readers accepted the native result/archive; actual3600TTL and separately labeled short expiry were checked. Native process loss after earlyACK, before archive execution, left no result and was not immediately replayed by a replacement. Unsupported Restore bytes remained at the queue head without execution. Injected handler failure confirms SUCCESS/returned-failure semantics; **actual native archive diagnostic strings still differ and are not accepted as Source parity**. Source UTC fixture clocks do not establish general timezone configuration. The physical resource handlers deliberately avoid archive work, so their gates do not establish process-group hard-kill/spool recovery during pg_dump.

No native application202 route, production constructor, scheduler, destructive restore or index worker was promoted. General Kombu reservation/unacked/priority/ETA compatibility, long visibility recovery and successful restore quiescence remain open. The [original index worker109](../scripts/rust-epg-parity/verified-index-worker-original-109.json) supplies a distinct later protocol reference; Create's dictionary result must not be reused for index null/children/lineage semantics.

## Immediate index interoperability component

[Native index fixture2](../scripts/rust-job-contracts/verified-native-index-worker-2.json) now binds18 real copied-database checks, eight frozen protocol/library controls and strict all-target Clippy. Actual Source tasks execute through the native IndexService with identical stored index data. A native busy parent durably publishes the five-second child and writes SUCCESS/null with the Source child tuple; the actual original worker consumes that ETA child, and descendant parent metadata remains readable. Missing source/file and explicit defer480 return normal null. [Per-execution dispatcher115](../scripts/rust-epg-parity/verified-index-dispatcher-override-115.json) separately proves immutable concurrent traces and caller retention through a real delayed Redis reply.

The native consumer deliberately retains all ETA/parented messages at the head rather than claiming unsupported reservation semantics. Original109 did not retain the complete raw unacked tuple/score and post-restore redelivery envelope, so native ETA consumption and production driver/app dispatch remain open. Index exceptions are not rewritten into a backup failure dictionary or an invented null success. The first failed fixture remains retained: creating an active missing-file test source triggered Source's real post-save refresh publication, which the native consumer correctly refused. Fresh inactive fixture sources corrected that setup without changing native runtime bytes. All projects were removed and the default database stayed unchanged. No new performance claim follows.

[Original ETA117](../scripts/rust-epg-parity/verified-index-eta-original-117.json) subsequently supplies26 passing raw reservation/redelivery controls. The original unacked hash uses delivery_tag fields and JSON `[message,exchange,routing_key]`, paired with wall-clock scores in unacked_index and no TTL on either key. Warm restoration changes only the two redelivered flags; hard loss preserves a future reservation. Explicitly aging the owned score by3605seconds lets the unchanged Original restore function requeue it, and an actual restarted worker completes the same task/lineage. This controlled transport observation does not prove real one-hour expiry, original polling cadence or a native ETA consumer. It supersedes the earlier missing-capture statement only for those recorded fields.

## Index-only constructor and concrete public dispatcher

[Constructor interoperability1](../scripts/rust-job-contracts/verified-index-production-constructor-1.json) passes19 real checks through `Jobs::connect_index`, the concrete initial-publication dispatcher and shared IndexService executor. [Public-dispatch resources1](../scripts/rust-job-contracts/verified-index-public-dispatch-resources-1.json) passes two real Redis reply/caller/admission/drain controls. Both frozen trees have the same runtime hash. All actual execution still uses explicitly owned copied databases, and cleanup/default-database checks pass. The new connection API has no fixture-environment precondition and grants only index publication/execution capability; Create/Restore publication is rejected. Independently configurable broker/backend URLs are exposed, with the pinned local Redis layout proven here; broad transport/TLS/Sentinel configurations remain separate gates.

The CurrentLookup dispatcher returns success only after a real publication reply. A timed-out or aborted caller keeps its context and publication admission until the underlying command reply; it never returns a successful Parsing dispatch for an ambiguous publication. Closing/draining initial publication leaves shared worker child/result I/O open until the worker's separate drain. These are library components, with no app/main hook or application202 promotion. The new frozen unit/Clippy validation is queued until the coordinated GRID quiet window ends; pre-capture all-target Clippy and the actual19+2 gates are retained separately. Native ETA reservation/redelivery execution and production startup/shutdown integration remain pending.

## Post-quiet ETA and shared-lifecycle gates

The quiet-window draft `job-transport/src/eta_draft.rs` is deliberately unregistered and uncompiled. Its normalized Source117 dataset contains three actual reservation tuples and two restored envelopes. It validates the observed message/exchange/routing-key tuple, delivery-tag identity and paired `redelivered=true` fields, retaining exact existing bytes for owner comparisons. Draft reserve/ACK/restore scripts are not execution evidence. Restore placement has no selected default: Source117's single ETA cannot establish list end or multi-reservation order. The queued pinned Kombu implementation/ordering capture must settle that before integration.

After quiet, first validate the new public constructor/dispatcher frozen trees and the shared-worker lifecycle change. Family shutdown must close/drain only its own admission; a caller-owned final shared Jobs close follows all producer/worker drains under one absolute deadline. Run both directions against actual Redis: idle Index shutdown while Create still needs its result; idle Create shutdown while Index still needs a child/result. These handlers isolate physical ownership and do not substitute for actual archive/index algorithm tests.

Then register the ETA codec only for its Source corpus/negative controls. Compare actual broker queue/unacked/unacked_index states for reserve, due-time earlyACK, warm restore and Source/native interoperation, including exact tag/tuple/TTL geometry and redelivery fields. Pause each mutating reply after server acceptance, drop/timeout the caller, and verify task/worker/physical ownership plus failed-drain behavior. Hard process loss while awaiting ETA must leave the reservation; original/native recovery requires separately labeled fixture-aged-score and actual visibility observations. Source117's forced old score is not a one-hour wait or real worker polling-cadence proof.

A production worker needs separate bounded pending-ETA, prefetched and active admissions: an ETA timer cannot occupy the only execution slot and stall unrelated immediate tasks. No input ACK occurs before the captured pool-acceptance boundary. Warm shutdown restores reserved, unstarted work and drains active work, retaining each owner through the actual reply. Unsupported task families stay untouched. Task exceptions, priority/routing configurations, transport faults and startup/restore failure boundaries remain explicit until captured and tested. Only after these gates should the app connect the real CurrentLookup dispatcher and worker lifecycle; application202 and destructive restore remain separate milestones.

The reserved index scheduler now has a separate frozen component proof in
`scripts/rust-job-contracts/verified-native-index-eta-worker-1.json`:20 checks,
10 library tests and strict all-target Clippy. An actual Original ETA task is
reserved with the captured Kombu tuple/tag/zset geometry, remains PENDING before
due, and executes the real IndexService at or after its ETA. Eight future tasks
leave the active execution slot free; warm close restores the Original119
reverse-delivery order and only the two captured redelivery fields change.
Actual Original workers consume these restored envelopes. Process exit and hard
loss retain reservations; the recovery witness explicitly ages the owned score
by3605seconds and invokes Original `restore_visible`. This proves neither an
elapsed one-hour timeout nor autonomous restore polling.

Independent native124 tests additionally cover frame/count/byte admission,
resident Candidate clones, cross-connection receipt ownership, empty-head races,
large foreign replacements and retention through actual paused Redis replies.
These are reservation/resource proofs, separate from the timer proof.

A new application-unwired `IndexDriver` draft settles intake before scheduler
close. A logical reserve timeout waits for physical completion, then reconciles
the exact stored owner tuple before execution. Its actual Redis lifecycle tests
are pending under `m6-index-driver-resources-1`; compilation alone is not runtime
acceptance. Automatic visibility recovery and the full CurrentLookup HTTP
Parsing-to-programme/dispatch500/path-context sequence remain explicit gates
before application attachment. Shared Jobs closes only after all worker families
and initial publishers have drained. No public202 or restore-worker authority is
added by these components. Original Dispatcharr, Celery and Kombu authors retain
their application and transport algorithm credit.

The later stageable component closure is recorded in
`scripts/rust-job-contracts/verified-final-index-worker-closure-3.json`.
Constructor19, ETA20,11 individual physical-owner tests and3 independent ETA
boundary tests were replayed against one retained driver2 tree. The first intake
fixture failed because its single command slot let intake compete with execution;
the constructor now requires at least active+1 slots and exposes executor errors.
The first combined physical replay also preserved deliberate task heads between
independent unit cases; its failures are retained. The successful replay isolates
each case using only the disposable project's broker8/result9 namespaces.

A final revision adds checked arithmetic before validating public scheduling
counts, including usize::MAX tests, and clarifies result-outcome comments.
Both handler algorithms and result-write paths remain unchanged. Exact source
deltas, selected successful tool snapshots, graph inputs and coupled ingest test
files are in the closure manifest. Final frozen validation passes11 library tests
and strict all-target Clippy.

Actual Original duplicate2 establishes16 checks: repeated task IDs reexecute the
index handler in the same and a restarted worker, while the existing SUCCESS
metadata and its decreasing TTL remain unchanged. Final native duplicate1 repeats
those checks and adds8 actual immediate/ETA SQL/result checks. The native handlers
already execute before their metadata-only SUCCESS guard; no execution skip was
present or removed. `AlreadySucceeded` describes suppression of a result rewrite.
The first Source duplicate fixture incorrectly expected a new result date and is
retained as a failed fixture, rather than changing Original behavior.

These proofs still do not attach an application driver or implement autonomous
visibility recovery, exception result writing or public202. The CurrentLookup
HTTP Parsing-to-programme lifecycle and exact failure/path/cache context must be
proved before attachment. Original Dispatcharr authors retain full application
credit; Celery/Kombu authors retain task transport/backend algorithm credit.
