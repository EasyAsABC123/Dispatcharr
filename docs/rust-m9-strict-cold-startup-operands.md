# Strict cold PostgreSQL rollback operands

The pinned Original copied rollback gate compares the complete quiescent
PostgreSQL copy immediately before startup with the same copy after the
ordinary uWSGI web process becomes ready. The manifest hashes sorted
full-row JSON per table, including timestamp columns, and checks columns,
indexes, constraints and sequence states. The paired strict comparator
exited 1. The independent Original-only witness verified exact equality
before startup and recorded the four resulting writes. The authenticated
read after readiness added no changes.
[Existing copied rollback evidence](rust-m9-copied-postgres-rollback.md)

| Existing row | Field | Write after preboot snapshot | Increase over preserved copy |
| --- | --- | ---: | ---: |
| Backup schedule | `date_changed` | 1.022055 s | 37.346273 s |
| Beat marker | `last_update` | 1.348266 s | 37.386969 s |
| Developer notification | `updated_at` | 1.307332 s | 37.382454 s |
| Plugin refresh schedule | `date_changed` | 1.347882 s | 37.386898 s |

All four writes occurred in the 1.897547-second startup bracket. Each
observation used one read-only repeatable-read transaction. Other
observed row fields, schema and sequence values matched.

The pinned app lets the first lazy uWSGI worker initialize. Backup
startup always syncs the configured schedule. The nonzero plugin
refresh interval takes the periodic-task upsert path as well.
[Worker selection](https://github.com/Dispatcharr/Dispatcharr/blob/1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430/dispatcharr/app_initialization.py#L31-L48) · [Backup startup](https://github.com/Dispatcharr/Dispatcharr/blob/1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430/apps/backups/apps.py#L13-L39) · [Backup schedule](https://github.com/Dispatcharr/Dispatcharr/blob/1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430/apps/backups/scheduler.py#L107-L133) · [Plugin startup](https://github.com/Dispatcharr/Dispatcharr/blob/1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430/apps/plugins/apps.py#L90-L116) · [Schedule upserts](https://github.com/Dispatcharr/Dispatcharr/blob/1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430/core/scheduling.py#L100-L136)

Django 6.0.9 saves an existing row on `update_or_create` and includes
`auto_now` fields. django-celery-beat 2.9.0 writes `date_changed` and
updates the singleton Beat marker from the current clock. Core startup
also upserts the existing developer notification, whose `updated_at`
uses `auto_now`. The receipt pins the installed source lines against
the distribution records.
[Core startup](https://github.com/Dispatcharr/Dispatcharr/blob/1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430/core/apps.py#L23-L58) · [Notification upsert](https://github.com/Dispatcharr/Dispatcharr/blob/1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430/core/developer_notifications.py#L328-L349) · [Notification model](https://github.com/Dispatcharr/Dispatcharr/blob/1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430/core/models.py#L1033-L1038)

With these operands and an advancing ordinary clock, reordering the
replay cannot make the preboot and ready fingerprints equal. Sampling
after startup changes the baseline. Disabling schedule rows or bypassing
production web startup changes the workload. The strict cold gate stays
open under its existing criterion. The separate warm replay keeps the
same Original generation across the switch.
[Warm copied fallback](rust-m9-fdede54c-warm-copied-rollback.md) · [Redacted clock evidence](../scripts/rust-integration/m9-strict-cold-startup-operands.json)

Original Dispatcharr, Django, django-celery-beat and PostgreSQL authors and contributors retain full credit. [Attribution](../ATTRIBUTION.md)
