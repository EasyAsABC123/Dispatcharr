# Disable a backup schedule in ordinary Rust Main

The original Dispatcharr, Django, Celery and django-celery-beat authors retain full credit. The React UI is unchanged.

Candidate116 `8ebc3eb6985c72d578d5c74db81149fe7c3c2dbe` passed the stock six-field disabled save through `PUT /api/backups/schedule/update/` and its immediate `GET /api/backups/schedule/` readback. The pinned Original was tested separately. Both returned HTTP 200 and identical JSON in each of the three cases. The Native run used ordinary Main with selected workers and Beat disabled.

| Initial named backup task | Named task after save | Cron after save | Beat marker |
| --- | --- | --- | --- |
| Own Cron | Removed | Orphan removed | Advanced during save |
| Shared Cron | Removed | Retained | Advanced during save |
| No task | Absent | Unrelated Cron retained | Unchanged |

Each save merged the existing backup settings group, preserved unrelated settings and task rows, invalidated the cached group and advanced its persistent version within the observed request interval. The readback made no further scoped state changes. Existing broker frames and result keys were unchanged. Anonymous and viewer writes returned 401 and 403 respectively and left the fixture unchanged.

The exact package and pair controllers exited 0. The package passed formatting, locked Main compilation, both named backup tests, strict Store and job-transport Clippy and exact-image admission. Original and Native owned Compose resources and tracked process generations were removed. The [redacted receipt](../scripts/rust-integration/verified-backup-schedule-disable-c116.json) records the six verified Rust postimages and scoped evidence.

This qualification covers an existing settings row and a disabled stock save. Schedule enablement, schedule reconfiguration while enabled, missing rows, worker execution, backup and restore coverage, and whole-M8 acceptance require separate evidence. The request interval bounds database and cache timestamps. There was no performance measurement.
