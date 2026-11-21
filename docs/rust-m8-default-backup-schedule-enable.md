# Default backup schedule re-enable

In a fresh Docker Compose comparison using copied PostgreSQL and Redis stores, an administrator used the unchanged Backup Manager's full schedule payload to re-enable a disabled weekly backup. Original 1241b3f returned HTTP 200, stored Tuesday at 14:10 in the configured timezone, and created one enabled periodic task with a three-archive retention setting. Rust Core 0b48cf4b returned HTTP 501 and preserved the disabled schedule without creating a periodic task. Both readbacks confirmed the observed result. [Redacted execution receipt](rust-m8-default-backup-schedule-enable.json)

Both applications accepted the ordinary initial and final disable requests. Their unrelated settings, queue lengths, result keys and archive directories stayed stable over the operation. Every owned container generation retired, and both Compose project inventories were empty. The paired controller exited 1. Independent verification exited 0 and confirmed the exact difference and cleanup.

The Core enable path remains open. Earlier explicit Selected worker and Beat coverage observed one due backup after restart, a successful archive and rotation of the older archive. This comparison covered default schedule admission and cleanup. It claimed zero formal nodes and measured no performance.

The original Dispatcharr authors and contributors retain full credit. Celery, Kombu, django-celery-beat, PostgreSQL, Redis and the other dependency authors retain credit for their work.
