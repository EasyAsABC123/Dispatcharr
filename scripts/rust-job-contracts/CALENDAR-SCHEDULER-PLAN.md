# Known calendar lane — Source-first plan

Original Dispatcharr/Celery/django-celery-beat authors retain credit and licenses. Existing calendar11 and all interval proofs remain immutable. No main/default intake or attribution file is changed by this plan.

The pinned Original loads system ZoneInfo data `2026e`; its Python tzdata package is `2026.6` and is only a fallback. Cached `chrono-tz 0.10.4` embeds `2025b`. The implementation will use existing chrono and exact bounded captured Source TZif data for UTC, New York, Lord Howe, Kathmandu and Paris, with explicit transition/date bounds. No Cargo graph addition is needed. Other zones/ranges remain unsupported rather than silently using older rules.

Capture before implementation:

- Preserve the exact eleven legacy ModelEntry vectors, whose injected nowfun returns fixed-offset datetime objects.
- Capture matching ZoneInfo-clock vectors and real eager database query/tick/persistence for known Refresh/Series crontab rows. Clock dependency injection must be explicit: Original methods, broker publication and SQL sync remain unchanged.
- Source database filtering uses server-local hour ±2 plus cleanup hour4 and a fixed 2023-01-01 timezone offset truncated toward zero to hours. Capture that behavior separately from actual DST offset calculation.
- Exercise UTC hour4/default shape, wildcard/list/step/day fields, five captured timezone files, spring gaps/fall folds, start/expiry/one-off transitions and timezone/sentinel reload.
- Run actual Beat process with controlled clock files for DST transitions; distinguish this from elapsed-time DST or OS clock changes. Run a real-clock wildcard known-rows Beat publication as an elapsed process reference.

Important boundaries: real TzAwareCrontab.nowfunc returns ZoneInfo. The pinned Celery remaining() explicitly converts both endpoints to UTC. Both clock modes preserve gap delays, but a due recomputation from a fixed-offset now can differ from ZoneInfo across the following DST transition (first fall fold: 86400 vs 90000). Keep both modes labeled; do not rewrite the old eleven outputs or normalize differences after a native run.

A future `expires` option affects the protocol expiry header and worker revocation/result semantics. Capture it, but do not strip or silently accept it into native consumers without real Original REVOKED/expiry gates. Unknown publication options, default families and configured Series rules stay unsupported. The calendar scope can first prove expired disable and normal header-free known tasks.

Multi-dirty SQL-error partial-commit order remains separately open: Source uses an unordered dirty set and stops after the first database exception. Normal multi-dirty persistence is proved; calendar reference cases will isolate dirty failures instead of inventing lexical commit order. Capture that remainder separately if an observed calendar failure requires it.
