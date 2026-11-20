# Stock backup schedule disable with a missing settings row

The original Dispatcharr, Django and django-celery-beat contributors retain
full application and workflow credit. [Attribution](../ATTRIBUTION.md)

With the backup settings row absent in disposable PostgreSQL, an admin
could read the stock defaults from both backends without creating a row.
Anonymous and viewer write attempts returned HTTP 401 and 403, respectively,
and left the state unchanged.

The stock six-field disabled save returned HTTP 200 with identical response
bytes on Original and on the shared Rust package. Each backend created one
settings row with the submitted values. The following GET matched the result.
The backup group cache entry was invalidated and its version advanced within
the request window. The named task remained absent. The Beat marker,
unrelated settings and cache, existing queued work and results stayed unchanged.

Rust records the final disabled settings in one atomic database operation.
The comparison checked the stored row and cache effects after the HTTP response.
The previous proven existing-row workflow remains covered separately.

The initial shared-image launcher used a directory outside the fixture owner
and stopped before Docker. The corrected run reused the same image, passed all
assertions and retired all four owned container and process generations.
The retained Original run exited zero with both owned roles retired.
The scope covers one ordinary stock save. Backup execution, enabling a
schedule, full operations parity and performance remain separate gates.
[Validation receipt](../scripts/rust-integration/milestone-batch113/backup-missing-row.json)
