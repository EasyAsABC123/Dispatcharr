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
