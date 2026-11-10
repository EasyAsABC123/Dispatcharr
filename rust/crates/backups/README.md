# Database-only backup component

Original Dispatcharr authors retain full application credit and the AGPL license. This crate ports the original `apps/backups/services.py` database-only v2 ZIP format. It is a standalone component: the application does not depend on it, and no backup HTTP endpoint or durable job is enabled by registering this workspace member.

The original archive contains `database.dump` (PostgreSQL custom dump) and `metadata.json`. Media, uploaded logos, recordings, plugins, Redis state and deployment secrets are excluded. A database roundtrip preserves references to these files; it does not transfer the files or preserve a separately changed JWT secret.

The first actual original/native proof is recorded in [the sanitized cross-restore summary](../../../scripts/rust-integration/verified-backup-cross-restore-6.json). Three fresh, explicitly named copied databases were created inside an isolated original PostgreSQL17 fixture. An archive created by the unchanged original service was restored by Rust. A Rust-created archive was then restored through the unchanged original Celery task, including Django migrations. Both directions preserved all logical rows, table/column definitions, indexes, constraints and sequence states; 26 tables were nonempty. Original password, API-key, group and channel-profile checks passed. The original default fixture database remained unchanged, and the entire project was removed.

Real PostgreSQL TCP authentication and insufficient table-privilege failures were also compared with the original service. The source fixture intentionally trusts its local Unix socket, so the password-rejection cases explicitly select TCP. Failed dumps preserve their source database. Diagnostic contents and dump contents remain in ignored private artifacts; checked-in evidence contains only scalar results and hashes.

Run the guarded fixture with the normal local toolchain and Docker environment:

```
python scripts/rust-integration/run_backup_contracts.py --output artifacts/rust/m8-backup-cross-restore-unique
```

The runner captures the exact Rust/tool bytes before building. The integration target is ignored during ordinary `cargo test`; it requires the explicitly guarded original container, original PostgreSQL17 clients and three disposable databases. Three ordinary native tests verify that dropping an observer retains worker admission, diagnostic prefixes stay bounded while complete-stream decoding/error detection continues, and active cancellation/deadlines reap the owned process group without hanging on inherited output pipes.

One worker is admitted per `Admission`; additional work returns `Busy`. Archive copying uses 8KiB buffers, child diagnostics have bounded retained prefixes, child output files have a hard file-size limit, central-directory bytes/entries are checked before ZIP allocation, metadata has a separate limit, and scratch directories use mode0700 with owned cleanup. Limits are explicit additions to the original implementation; full malformed-input compatibility is not claimed. A restore authority accepts only `rust_probe_backup_*` fixture database names, so this component cannot currently authorize production restore.

Seven actual archive boundaries also match: missing metadata/dump, wrong engine, nonobject metadata, ignored format/version, default type/file fields, and invalid pg_restore data. The last case returns service success after leaving the target schema empty in both implementations. This is preserved failure behavior, not a successful data restore.

Remaining gates include corrupt/unsafe/duplicate ZIP and broader archive boundaries, PostgreSQL TLS and the packaged PG17 ELF closure with notices, platform/timezone behavior, SQLite, old-schema upgrades, application quiescence and restart coordination, persisted jobs/status/token contracts, scheduling/retention and the unchanged backup UI. The original warning-only `pg_restore` behavior requires testing physical database effects, because an apparent success can follow destructive schema cleanup. No backup performance gain or full M8 completion is claimed.
