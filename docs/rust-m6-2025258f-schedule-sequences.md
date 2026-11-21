# 2025258f backup and plugin startup sequence state

Pinned Original restarted once with existing enabled backup and plugin
schedule rows. The exact Rust package repeated the same-data transition
through an initial boot and a fresh restart. Both rows kept their IDs
and relation IDs. Their update clocks and the Beat marker advanced.

Every boot preserved the observed PostgreSQL sequence identities and
states for periodic tasks, intervals, crontabs, and core settings. Both
Rust Main shutdowns exited 0. All owned fixture roles retired. The earlier
strict copied-rollback outcome stays attached to its separate rehearsal.
[Paired sequence receipt](../scripts/rust-integration/m6-2025258f-schedule-sequences.json)

The original Dispatcharr, PostgreSQL, Django, and django-celery-beat
authors and contributors retain full credit. [Attribution](../ATTRIBUTION.md)
