# Selected daily UTC backup and retention on 4f9e3916

Pinned Original and packaged Rust each accepted a daily UTC backup
schedule through the admin API. The schedule survived a real restart
before its due time. Both schedulers then published one backup and
persisted one run. The admin status matched the stored SUCCESS result.

Each arm kept its new PostgreSQL format-2 archive and removed one older
archive. The scoped SQL rows stayed intact. Both arms exited 0 and
independently verified that their owned processes and Compose resources
were gone. Rust used Selected Jobs and Beat with the standalone backup
owners absent.

This covers daily scheduling and retention on the tested image. Full M8,
default scheduler behavior, and performance remain open.

The tested manifest SHA-256 is `da9b42e61ed4dab91510f30df638f4ace610ebd3623ba5f8462caa35379c22cd`.
[Redacted receipt](../scripts/rust-integration/m8-selected-scheduled-backup-4f9e3916.json)

The original Dispatcharr, Django, Celery, django-celery-beat and PostgreSQL contributors retain full credit.
[Attribution](../ATTRIBUTION.md)
