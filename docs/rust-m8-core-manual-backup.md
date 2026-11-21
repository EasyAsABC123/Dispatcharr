# Shared Core manual backup creation and status

The paired Docker Compose replay used Rust `a5be97cb9ed7335a9fcb9e74cedc3f3a4affd762`
and pinned Original Dispatcharr. With `RUST_CORE_SCHEDULE_OWNER=rust`, Rust
accepted the ordinary admin backup request through the same Celery receiver
used for scheduled backups and backend cleanup.

Both backends returned HTTP 202, served signed task status and archive
download, and recorded the matching Celery task as SUCCESS. Both declined
unsigned anonymous status with HTTP 401 and an invalid task token with
HTTP 403. Each generated a PostgreSQL format-2 archive. The replay independently
verified retirement of two Original and four Rust-side containers.

The full Rust workspace passed formatting, strict Clippy and 1,548 tests
over 193 targets, with 814 fixture tests ignored.

The exact build manifest SHA-256 is `87eeb7a083972e850b5a6d22482e48c8208ec6bae2324eef774ce21baab897de`.
Manual restore, default worker ownership and comparative performance remain
open. [Scoped receipt](../scripts/rust-integration/m8-core-manual-backup-compose.json)

The original Dispatcharr, Django, Celery, Kombu, PostgreSQL and other
dependency authors retain full credit.
