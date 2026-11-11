# Bounded known multi-interval scheduler

Original Dispatcharr authors retain application/algorithm credit and AGPL licensing. Celery, django-celery-beat and Kombu authors retain framework credit and licenses. Exact installed notices/provenance are retained with the final proof; root owns the attribution bundle.

The expanded known-interval constructor/driver passed, on one exact source/test map:

- 184 functional checks: 68 actual Original reference checks and 116 native comparisons.
- 58 real-worker checks: 44 actual Original reference checks and 14 native comparisons, including four native results decoded by actual Original AsyncResult and SQL counts `[2, 2]`.
- All 108 earlier single-entry checks, including expiry, completed one-off, future start and actual Original worker compatibility.
- 47 startup/resource group checks with five actual physical tests, including a new two-entry delayed-reply/caller-abort/final-sync case.
- Strict all-target job Clippy and 40 default units; external fixture tests remain explicitly ignored by default.

`verified-source-native-scheduler-multi-final-1.json` binds the four successful runs, exact original/source/tool/binary hashes, unchanged default databases and complete owned-project cleanup. Repeated reference groups are not summed as unique cases. No performance acceptance or main/default scheduler enablement is inferred.

## Behavior preserved

Raw model load order is retained separately from the keyed cache. Source's actual query has no PeriodicTask ORDER BY, and the fixture predeclared reversed-lexical names to expose accidental name sorting. The first native loader used related-table hash-right joins, which reversed the parent row chains. Failed runs 1–3 retain the strict mismatch and both actual EXPLAIN plans. Correlated related-schedule PK lookups now preserve the parent scan order like the captured Original queryset/lazy related reads; output order and acceptance were unchanged.

The bounded event collection chooses by actual sampled heap time, fixed Source priority five, then object identity. It does not sort task names. Real-clock equal-due cohorts match exact ordered frames for both creation/family orders. True equal heap-time/priority ties have a separate predeclared witness: each runtime selects its own lowest object identity, matching Source's explicitly arbitrary final comparator. Frames remain exact by task identity in that case; a stable cross-process label order for opaque identity ties is not claimed.

The actual corpus covers future/due selection, one-off completion/reset, save+sentinel disable, delete+sentinel, heap rebuild, real LPUSH denial, two-entry elapsed Beat ordering, graceful close, and before/after-sync process loss. A before-sync loss preserves the Original duplicate publication window; after-sync restart selects the remaining due row. No task is reinserted to manufacture results, no unrelated task is executed, and no recording rule/remote provider is run.

Dirty state supports bounded successful multi-row persistence. Save reads the current cached model state, so completed one-off count resets are not overwritten by older dirty snapshots. Source dirty names are an unordered set; native does not invent lexical-name ordering. Multi-dirty SQL-error partial-commit ordering remains an explicit open gate. A missing/recreated-name identity mismatch is not silently written through.

The new preflight initially rejected an already-expired known row before Source's disable transition. The retained single-regression failure identified this. Known family/interval authority remains strict, while unused publication options are validated only when the due decision permits publication. The unchanged 108-case reference then passed.

## Bounds and scope

The existing 256-entry, bounded-field, 1 MiB server aggregate reply guards and two owned admissions remain in force. Related data is read within the same MVCC statement. The five actual physical tests preserve byte-reply bounds, SQL rollback/held replies, caller and admission retention across logical deadlines and cancellation, single-deadline shutdown, and final physical drain. The new two-entry case observed two actual publications, count one on each row, truthful incomplete short shutdown and later full cleanup. These are not total-process RSS or distributed scheduler ownership claims.

The six actual eager defaults are listed in `DATABASE-SCHEDULER-MULTI-PLAN.md`; none is an alias for the two supported Refresh/Series families. Unknown task families/options and calendar schedules remain explicit unsupported. Configured Series recording rules, calendar/timezone/DST semantics, arbitrary database plan ordering, multi-dirty SQL-error ordering and scheduler main/packaged startup are still open. No milestone ledger or default intake flag changes here.
