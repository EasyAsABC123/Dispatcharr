# PostgreSQL backup restore on the shared Rust Core worker

The exact Rust image at `3843b61af0ad8493a9004316836a18c65e7001a2` restored an Original-created
PostgreSQL archive and then a Rust-created recovery archive against a disposable
copy. Both restores returned HTTP 202 and reached their signed task's Redis
SUCCESS result. Fresh admin sign-in, settings and channel readback matched the
expected data after each restore. Main and PostgreSQL kept their process
generations, and the separate control database stayed unchanged.

The same image kept a claimed `pg_restore` child alive across Main's SIGTERM.
It completed the signed task and matched the restored SQL before the owned
Compose project fully retired.

A separate natural-overlap case kept the accepted restore queued when
SIGTERM arrived during an active backup creation. The create settled, the
queued restore had no terminal result and the target SQL stayed unchanged.

The Compose fixture enabled `RUST_CORE_SCHEDULE_OWNER=rust` and
`RUST_CORE_BACKUP_RESTORE_OWNER=rust`, set `RUST_SHUTDOWN_SECONDS=136` and
used a 150-second container stop grace. Create, restore and status used the
one shared worker. Both existing schedule update paths retained their owners.
The workspace passed formatting, strict Clippy and 1,554 tests.
Enabled plugin archives, broader failures, the default worker graph and
comparative performance remain open. [Exact scoped receipts](../scripts/rust-integration/m8-core-postgres-restore.json)

The original Dispatcharr, Django, Redis, Celery, PostgreSQL and dependency
authors retain full credit.
