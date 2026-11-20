# Scheduled XMLTV refresh and guide jobs on 4f9e3916

Pinned Original and the packaged Rust Selected scheduler each published one
naturally due XMLTV refresh. Each fetched the owned guide once, wrote one
programme and the matching index, and settled the Programme Index and
Series descendants. Each persisted one Beat run and left its ready and
reserved queues empty. Both arms exited 0 and independently verified
cleanup of their owned resources.

This covers the scheduled XMLTV workflow on the tested image. Full M6,
default scheduler activation, and performance remain open.

The tested manifest SHA-256 is `da9b42e61ed4dab91510f30df638f4ace610ebd3623ba5f8462caa35379c22cd`.
[Redacted receipt](../scripts/rust-integration/milestone-batch113/4f9e3916-m6-scheduled-xmltv.json)

The original Dispatcharr, Django, Celery, django-celery-beat and PostgreSQL contributors retain full credit.
[Attribution](../ATTRIBUTION.md)
