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
