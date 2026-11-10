# Original EPG orphan cleanup contract

All application behavior and algorithm credit belongs to the original Dispatcharr authors (AGPL). Django and django-celery-beat authors retain their framework credit and licenses.

`verified-original-epg-orphan-cleanup-1.json` binds 79 checks: eleven direct helper cases and nine actual Original Celery worker cases. The pinned application revision is `1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430`; the image packages are Django 6.0.9, django-celery-beat 2.9.0 and psycopg 3.3.6. The fixture uses a copied database and owned Redis DB8/DB9, quarantines unrelated task families and leaves the default database unchanged. All three database fixtures and the read-only collector capture cleaned up successfully.

The earlier narrower successful captures remain intact: `m6-original-epg-orphan-sql-1` (21 checks) and `m6-original-epg-orphan-worker-1` (52 checks). The strongest capture is `m6-original-epg-orphan-marker-1` (79). These are Source contracts, not native-helper passes. No failed Source attempts were discarded. A local publication-tool path normalization error was corrected before the sanitized summary was produced; it did not run a database or worker.

## Transactions and effects

The helper looks up `epg_source-refresh-{source_id}`, reads its interval and counts references before deletion. Missing tasks return false. It deletes the task through Django's collector, including explicit SET_NULL updates to EPGSource and M3UAccount references. The physical foreign keys do not perform those updates automatically. It deletes an interval only when its previously observed reference count was one; shared intervals and crontab schedules remain.

A sole-interval deletion has three independently committed phases:

1. Collector transaction: Beat marker update from PeriodicTask `pre_delete`, both SET_NULL updates, task deletion, commit.
2. PeriodicTask model override: a second Beat marker update after `super().delete()` has committed.
3. Interval collector transaction: third Beat marker update from IntervalSchedule `pre_delete`, interval deletion, commit.

The marker is the singleton `PeriodicTasks.ident=1`, created if absent. Owned sequence counters and triggers fail exact marker attempts 1, 2 and 3; their values establish the ordering independently of timestamps.

| Fault | Helper return | Committed effects |
| --- | --- | --- |
| Task DELETE or first marker | false | Original task, references, interval and marker remain |
| Second marker | false | Task removed, references cleared, first marker committed; interval remains |
| Interval DELETE or third marker | false | Task removed, references cleared, second marker committed; interval remains |
| No fault, sole interval | true | Task and interval removed; third marker committed |
| No fault, shared interval | true | Target task removed; shared interval and other task remain |

The actual Original worker returns `SUCCESS` with `EPG source {id} does not exist, task cleaned up` for every missing-source case, including all helper failures. It returns no children or traceback; fixture result TTL is 3600 seconds. The refresh lock is released. The string describes the wrapper result, not proof that all cleanup succeeded.

## Evidence boundaries

The direct helper has actual SQL traces and before/after row/FK/marker observations. The worker's query observer is empty because Source resets its database connection; worker evidence covers actual results, rows, marker attempts, locks and result TTL. It does not provide a worker SQL trace.

`m6-original-epg-orphan-collector-1` separately captures the pinned Django Collector `collect`, `delete`, `add_field_update`, SET_NULL, CASCADE and Model.delete implementations using a network-disabled, read-only, unprivileged container. Package source is private evidence; public summaries bind its hashes. The original beat models/signals and app helper are separately hash-bound.

No concurrent insertion/deletion, cancellation, ambiguous commit, generic collector graph, other schedule garbage collection or native helper behavior is accepted here. The next native component must preserve these partial commits and retain physical SQL ownership through commit/rollback after waiter cancellation. Unsupported or ambiguous outcomes must remain operational errors. The existing immediate native worker76 remains unchanged and app-unwired.

## Unwired native component

`verified-source-native-epg-orphan-cleanup-1.json` records 125 checks: the original 79 plus eleven direct Source/native comparisons (four checks each) and two resource assertions. Two actual resource scenarios are exercised by the latter assertions. Frozen ingest all-target strict Clippy and two pure unit tests also pass. The complete archive is retained; selection permits only `orphan_cleanup.rs` plus its exact cfg(test) module append, with no Cargo changes. Unrelated ProfileCache/sibling bytes included in the archive receive no acceptance from this component.

The direct comparisons reuse exactly the same synthetic row IDs and fault definitions after a guarded fixture reset. Only Beat marker wall time is normalized to changed versus unchanged: native uses a fixed 2021 clock and Source uses its actual clock. All task/interval/crontab rows, FK values, helper booleans and marker attempt counts match. The original and native worker76/FileJobRunner runtime files are byte-identical to the accepted worker76 snapshot.

The owned native supervisor has fixed admission and retains caller/permit through SQL. Aborting a waiter while its real SELECT FOR UPDATE blocks behind an owned marker lock does not free admission; shutdown reports incomplete until the lock is released. The subsequent task DELETE P0001 rolls back before drain, preserving task/interval/marker and returning pool1 without an open transaction. A second test holds the only pool connection: aborted acquisition likewise retains caller/admission until actual acquisition and completion. Closed admission rejects new work.

The helper converts only the observed P0001 error after settled rollback into helper false. Unknown SQL/transport failures and ambiguous outcomes remain operational. This does not claim a deliberately paused physical ROLLBACK reply, general concurrent marker/graph semantics, a generic Django collector, or worker/application integration. Repeated phases use SELECT FOR UPDATE then UPDATE or INSERT; an UPSERT was rejected during design because it would fire both INSERT and UPDATE triggers on existing markers and change the observed fault ordering.

## FileJob and immediate fixture worker integration

`verified-source-native-epg-orphan-worker-1.json` selects the reviewed frozen integration3 closure. The helper is now a regular ingest service, called by FileJobRunner's MissingSource branch with its retained per-run lease context. Existing `run`/`run_with_dispatcher` signatures and default/immutable dispatcher behavior remain. Shutdown drains the helper within the same remaining deadline. The fixture worker's former orphan-exists rejection is removed; coarse `Failed` and all unclassified failures remain operational.

The final gate passes 161 orphan checks plus 80 targeted refresh regression checks. Nine actual Original publications are executed by the real native FileJobRunner, normal result writer and then decoded by Original AsyncResult. Source/native task UUIDs are deliberately different, with each result validated against its own publication. Restored row IDs and all ordinary row/FK values are identical; marker wall time and result timestamps/TTL follow the stated volatile normalizations. The cases cover no task, sole/shared intervals, cross references, task and interval deletion faults, and marker phases 1/2/3. All known settled P0001 helper-false cases still return the Source cleanup string.

Two worker cases hold the actual singleton marker row lock, abort the logical run waiter and verify that the caller, worker slot, FileJob job and refresh lease remain owned until SQL settles and the actual normal result is written. One is ordinary sole deletion; the other fails at marker phase2 after task deletion commits. An aborted waiter's return is **unobserved**, recorded as `None`; completion is proved by the actual bounded result read and Original AsyncResult, not by inventing a Completed return value.

The targeted regression covers eleven existing refresh cases, three unsupported head controls and three real Redis worker resource tests. It verifies imports, child lineage/default dispatcher isolation, deferral, repeat-ID result preservation, ordinary skips and continued operational handling of coarse Failed. Frozen all-target strict ingest/job Clippy and 61 default units (28 ingest, 33 job) pass. Eighteen external tests are intentionally ignored by that default unit run and are not counted as passes.

Attempt1's aggregate failed only because a cloned regression controller wrote the old report filename; its actual 161/80 checks passed and the failure remains intact. Attempt2 corrected that filename and passed, but its held-waiter audit synthesized a Completed label after abandoning the JoinHandle. Attempt3 changes only that test-peer audit to truthful unobserved `None` and repeats the full integration successfully. All three projects are independently confirmed absent and all default database audits pass. No runtime code was changed between attempts2 and3.

The selection includes four runtime/test files and one narrow ingest module append, with no Cargo graph changes. It does not accept unrelated sibling inputs, a default consumer, scheduler, HTTP202, native refresh ETA intake, generic SQL/collector concurrency, or the deliberately paused physical ROLLBACK reply that remains untested.
