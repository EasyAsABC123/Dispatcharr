# M8 backup, restore, settings and CLI inventory — draft

Original Dispatcharr authors and contributors retain full application credit and AGPL licensing. This is a read-only source inventory prepared during the coordinated performance quiet window. No original process, Cargo test, Docker stack, backup, restore or database command was executed. Nothing here is an accepted migration milestone or a new passing test report.

Following this historical inventory, a standalone, app-unwired PostgreSQL archive component and a real copied-database cross-restore were implemented. [The scoped component evidence](../rust-integration/verified-backup-cross-restore-5.json) records original→Rust and Rust→original restore success, exact logical data/schema/sequences, credentials and real failed-auth/privilege cases. Jobs, HTTP backup flows, SQLite and upgrades remain open; the inventory below describes the original audit rather than claiming those gates are complete.

The inspected contracts are `apps/backups/{services,api_views,api_urls,tasks,scheduler,apps}.py`, `apps/backups/tests/test_backups.py`, `core/{models,signals,scheduling}.py`, `core/management/commands/`, `apps/proxy/management/commands/proxy.py`, `dispatcharr/settings.py`, `docker/entrypoint.sh` and `docker/init/03-init-dispatcharr.sh` at the migration's pinned original source revision. HTTP routes are included under `/api/backups/` by `apps/api/urls.py`.

## Actual archive contract

Despite the service docstring mentioning data directories, the implementation writes only two ZIP members:

| Database | Database member | Other member |
| --- | --- | --- |
| PostgreSQL | `database.dump`, produced by `pg_dump -Fc -v -f <temporary file>` | `metadata.json` |
| SQLite | `database.sqlite3`, produced using the SQLite online backup API | `metadata.json` |

The ZIP uses DEFLATE and ZIP64. Metadata is an indented JSON object with `format: "dispatcharr-backup"`, `version: 2`, `database_type: "postgresql"` or `"sqlite"`, `database_file` and a UTC ISO timestamp `created_at`. The filename is `dispatcharr-backup-YYYY.MM.DD.HH.MM.SS.zip`, using the cached system timezone via pytz; invalid timezone resolution falls back to UTC. Filename precision is one second and the archive is opened with write/truncate semantics, so same-second jobs can collide. No atomic-final-rename or cleanup of a partial archive is implemented.

`BACKUP_ROOT` defaults to `/data/backups` and is created on access. Listing includes only names matching `dispatcharr-backup-*.zip`, sorted reverse lexicographically, returning `name`, byte `size` and UTC ISO file-mtime `created`; it does not read archive metadata. Uploaded archives with other names are accessible by filename but do not necessarily appear in this listing. Retention uses this filename order rather than file mtime.

### What is preserved, and what is outside the ZIP

The database dump includes application rows, IDs, relationships, password hashes, API keys, provider credentials/URLs, user custom properties, settings, migration history and any database-backed task/scheduler records. These are sensitive backup contents; public evidence must contain fixture-only hashes/counts, not exported rows or actual archives.

No loop copies `/data/logos`, `/data/uploads/m3us`, `/data/uploads/epgs`, `/data/m3us`, `/data/epgs`, `/data/recordings`, `/data/plugins`, `/data/models`, `/data/scripts`, `/data/cache`, `/data/logs`, `/app/media`, database physical files, Redis data, deployment environment or TLS certificate/key files. Database references to those paths survive, but the files themselves do not come with the backup. Source initialization creates these directories separately.

`/data/jwt` is also excluded. The original entrypoint generates a missing secret file using restrictive umask and an atomic rename, then exports its CR/LF-stripped bytes as `DJANGO_SECRET_KEY`. Existing deployments retain this file independently. The same secret signs JWT-related application data and the backup task/download tokens. Restoring rows does not transfer that deployment secret; a new secret changes token validity. The current Rust binary requires its secret from the environment; secret-file generation is not established by the existing pure-native browser proof.

## Restore sequence and failure boundaries

1. Require the named backup path to exist, create a temporary extraction directory, and call Python `ZipFile.extractall()`.
2. Require `metadata.json`, parse it as JSON, then resolve `database_type` (default PostgreSQL) and `database_file` (default `database.dump`). Require the selected member path to exist.
3. Reject a different current database engine. The restore code does **not** validate the metadata `format` or `version` fields. Missing/incorrect JSON types follow Python exceptions rather than a defined serializer.
4. PostgreSQL: run `psql -c 'DROP SCHEMA IF EXISTS public CASCADE; CREATE SCHEMA public; GRANT ALL ON SCHEMA public TO public;'`. Any nonzero cleanup return fails. Then run `pg_restore --no-owner ... -v <dump>`. A nonzero restore return is treated as failure only if lowercased stderr contains `fatal` or `could not connect`; other nonzero results are logged as warnings and treated as success. This can report completion after partial restore. There is no rollback or pre-restore PostgreSQL safety dump.
5. SQLite: if the current file exists, copy it to a `.db.bak` sibling; copy the restored file over the target; verify by opening it and querying `sqlite_master`; on a SQLite verification error, copy the saved file back when present. Failures before that verification handler have their own partial effects. Parent directories are created.
6. The Celery restore task then calls Django `migrate --noinput` and only afterward returns `{status: "completed", filename}`. Any caught exception returns `{status: "failed", error: str(exception)}`; the task itself normally returns a result rather than raising. Services alone do not run migrations, close/rebuild every worker pool, invalidate all shared caches, restore files or clear playback/session state.

PostgreSQL commands use argv arrays, not shell interpolation. Host, port, user and database are separate arguments. Password is supplied in `PGPASSWORD` only when configured, otherwise removed from the inherited environment. Inherited `PGSSLMODE`, `PGSSLROOTCERT`, `PGSSLCERT` and `PGSSLKEY` are removed and rebuilt solely from configured Django database OPTIONS. Source TLS settings derive from `POSTGRES_SSL` and the four `POSTGRES_SSL_*` settings. Actual client tools and server-version compatibility belong in the Rust runtime-image gate.

Archive/extraction limits, metadata-selected absolute/parent-relative paths, symlinks, corrupt archives, decompression amplification, disk-full and interrupted subprocess behavior have no comprehensive contract tests here. The original metadata-selected path is joined without an explicit containment check. These require a disposable boundary probe and an explicit compatibility decision before a native restore route is enabled; do not silently declare all malformed archives compatible.

## HTTP, authentication and task state

| Route relative to `/api/backups/` | Method and behavior |
| --- | --- |
| empty | GET, IsAdmin, list array; caught errors500 `Failed to list backups: ...`. |
| `create/` | POST, IsAdmin, enqueue create task;202 `detail`, `task_id`, `task_token`; enqueue error500. |
| `upload/` | POST, IsAdmin, multipart/form parsers, field `file`; missing file400. Basename/safe-upload-path validation; collision suffix `-1`, `-2` before extension; chunked file write;201 `detail`, `filename`. No ZIP validation or explicit service-level file-size limit. |
| `schedule/` | GET, IsAdmin, typed schedule settings. |
| `schedule/update/` | PUT, IsAdmin, settings write followed by scheduler synchronization; ValueError400, other errors500. |
| `status/<task_id>/` | GET, AllowAny with manual admin-or-token check; task-result interpretation described below. |
| `<filename>/download-token/` | GET, IsAdmin; reject suspicious filename, require existence, return token. Existence alone does not prove regular file or readable archive. |
| `<filename>/download/` | GET, AllowAny with manual admin-or-token check; path/file checks, ZIP content type/length and attachment disposition. |
| `<filename>/delete/` | DELETE, IsAdmin; unlink regular file;204 response, missing file404, other errors500. |
| `<filename>/restore/` | POST, IsAdmin; check filename/existence, enqueue task;202 same task fields; no content prevalidation. |

IsAdmin means active authenticated account, UI network policy and `user_level >= 10`; staff/superuser flags alone do not replace the level check. The AllowAny status/download handlers manually check authentication and level and do **not** call IsAdmin/UI permission logic. DRF authentication still runs before these handlers, so invalid supplied credentials can fail before a valid query token is considered. Capture this precedence rather than reusing an administrator helper unconditionally.

Tokens are the first32 hex characters of HMAC-SHA256 using `SECRET_KEY`, with the task ID or filename as the exact message and constant-time comparison. There is no timestamp, expiry or separate task/download domain. A truthy supplied invalid query `token` returns403 even for an authenticated admin; absent/empty token falls back to a manual admin check and returns401 `Authentication required` when it fails.

`AsyncResult.ready()` is checked before `failed()`. A ready result is retrieved and interpreted as a mapping: status `completed` yields `{state: "completed", result: ...}`, otherwise `{state: "failed", error: ...}`. A nonready task returns lowercase backend state. Backend/result exceptions become500 `Failed to get task status: ...`. Missing/expired result storage can appear pending; do not synthesize a successful native task for an unimplemented job. Task tokens exist specifically so the browser can keep polling while restore changes its login state.

Download uses `X-Accel-Redirect: /protected-backups/<filename>` when `USE_NGINX_ACCEL` lowercases to `true`; otherwise a streaming iterator reads2MiB chunks. Only content bytes/selected semantic headers should be compared across native streaming and proxy configurations. Cancellation/held-body resource tests remain required.

Filename checks and exception handling differ by route. Token/download/restore explicitly rethrow Http404. Delete's generic catch can turn an explicitly raised invalid-filename Http404 into500 `Delete failed: ...`; capture actual HTTP before “correcting” this quirk. Download resolves paths but uses string-prefix containment, while token/restore do not share that resolution check. The Rust port must characterize symlink and sibling-prefix boundaries separately.

## Schedule and settings contracts

Default backup settings: enabled true, daily,03:00, weekday0(Sunday), retention3, empty advanced cron. Stored group keys are `schedule_enabled`, `schedule_frequency`, `schedule_time`, `schedule_day_of_week`, `retention_count`, `schedule_cron_expression`; API names omit `schedule_` except cron expression. A missing/nonobject group uses defaults; a stored object is read as-is with per-field `.get` defaults. Reads use Python bool/str/int conversions, not strict JSON schema coercion.

Updates validate frequency daily/weekly; time must split into two integer-convertible parts but the first validation does not itself bound hour/minute; weekday0..6 and retention>=0 use Python int. `enabled` uses Python truthiness (for example a nonempty string is true). Unknown keys are ignored. Updates merge/save the group **before** syncing the periodic task, so later cron/scheduler failure can leave settings saved even when the API returns400/500.

Enabled schedules maintain `backup-scheduled-task` pointing at `apps.backups.tasks.scheduled_backup_task`, JSON kwargs with retention count, and a cron schedule in the cached system timezone. An advanced truthy cron expression overrides simple daily/weekly settings. Shared scheduling parses exactly five whitespace-separated parts and updates Celery Beat rows, deleting orphaned old interval/crontab rows only when unreferenced. Disabling removes the task and cleans orphan schedules. Backup app startup attempts to ensure default settings and synchronize the task; failures warn rather than fail startup. A worker/management/dev initialization guard can skip this startup behavior.

Scheduled backup creates the archive before retention cleanup. Retention<=0 keeps all. Cleanup walks the reverse-name-sorted matching listing beyond the retained count, catches individual delete failures and counts only successful deletions. Manual creation has no retention cleanup. A native persisted job/scheduler bridge is prerequisite to enabling these writes, not just a matching settings response.

Core settings elsewhere have distinct callbacks: group/version cache invalidation; stream-settings default-UA invalidation; proxy local-cache clearing; system display timezone/log collector updates; network condition-cache clearing and developer-notification synchronization. Serializer-only effects such as DVR rescheduling and M3U rehash locks/jobs must be inventoried separately. Direct backup group saves do not imply all those unrelated effects.

## CLI and startup inventory

| Original operation | Exact relevant contract and Rust gap |
| --- | --- |
| `manage.py reset_network_access` | Get the existing network_access row, replace value with `{}`, save. Missing row raises; save fires normal cache/developer-notification effects. No Rust management CLI currently exists. |
| `manage.py reset_user_network [--user USER]` | Iterate all/exact-username users, remove `allowed_networks` when present, save only custom_properties, print changed count. Missing username changes zero rows. Legacy truthy nonobject properties can raise partway through; no transaction wraps all users. |
| `manage.py dropdb` | PostgreSQL only; print irreversible warning; input lowercased must equal `yes` (not stripped), otherwise abort successfully. Close Django connection, connect maintenance database postgres with TLS options/autocommit, quote database identifier, DROP IF EXISTS then CREATE. Error exits1; success tells operator to migrate. Never exercise against a deployment database. |
| `manage.py kill_processes` | Enumerate processes and kill names/command lines containing ffmpeg or streamlink case-insensitively; skip vanished/access-denied/zombie, print each and count. Broad host/container process matching differs from owned native engine shutdown; compatibility and containment need an explicit decision. |
| `manage.py proxy start/stop/restart --type hls/ts/all --channel ID --url URL` | Uses in-process Django app proxy objects; start requires URL. Missing services/errors are caught and printed to stderr without a CommandError. Restart recursively calls stop then start; reported process exit alone is insufficient evidence. |
| Django builtins | migrate, collectstatic, createsuperuser, changepassword, dumpdata/loaddata and other inherited commands require a supported-command allowlist and documented replacement. Existence in Django is not proof a Rust equivalent is already available. |
| Original container startup | AIO/modular environment defaults, UID/GID/directory handling, persistent secret, DB/TLS readiness, migrate with cache skipped in AIO, collectstatic, nginx/uWSGI/Celery supervision. Current native empty-schema initialization is explicitly opt-in and does not upgrade a restored older nonempty schema. |

The Rust app currently starts from environment configuration rather than a management-subcommand parser. It uses PostgreSQL SQLx; the original `DB_ENGINE=sqlite` branch and SQLite backup/restore need a separate port. Database URL/TLS settings must be checked against original libpq argument/env semantics rather than assumed equivalent. Existing native fresh-install/React/restart and media shutdown proofs are useful prerequisites but do not establish backup or CLI compatibility.

## Existing tests and absent roundtrip evidence

`apps/backups/tests/test_backups.py` covers password/TLS environment construction, ZIP member/metadata shape with mocked dumps, listing/deletion, missing archive/metadata/database and type mismatch, API roles/status/upload/download/task initiation, schedule validation/rows/timezones/orphan cleanup, retention and scheduled task result shapes. PostgreSQL and SQLite service restore tests patch the actual restore functions; the fixtures contain placeholder dump bytes. API task tests largely mock `.delay()` or AsyncResult. They do not prove pg_dump→pg_restore or real SQLite copied-data roundtrip.

No existing Rust backup implementation or actual copied-database backup roundtrip report was found in the inspected Rust crate/tool paths. [M8's milestone requirements](../../docs/rust-milestones.md) already require backup roundtrips preserving IDs/hashes/media references and restored data working in both implementations. A full original Python test-suite pass must not be relabeled a real restore proof.

## Minimal native port sequence and acceptance gates

1. Capture original job-free role/token/list/upload/download/delete behavior in disposable private volumes, including path/UTF8/duplicate-name/proxy-stream/failure boundaries. Add bounded disk streaming, owned admission and cancellation tests. Publish only redacted scalar/hash reports.
2. Implement typed v2 ZIP metadata/streaming archive construction and an owned subprocess runner with explicit libpq arguments/env, client-version inventory, bounded retained stderr, child reaping, disk limits and shutdown handling. Preserve successful Source archives without claiming unsafe/unbounded malformed-input equivalence.
3. Add real PostgreSQL copied-data roundtrips with two disposable databases and independent data manifests: original-create→native-restore and native-create→original-restore. Include users/password hashes/API-key authentication, provider/channel IDs and relationships, Unicode/numeric/null JSON, CoreSettings, migration records and sequences. Keep the same deployment secret for interoperability, then separately verify different-secret token rejection. Compare media references and separately managed fixture files; assert that a database-only archive does not magically restore missing media.
4. Preserve task202/status/token/result contracts with a real persisted native worker. Prove task polling across changed admin credentials and service restart, concurrent backup filename behavior, failure/cancel cleanup and all active service processes/resources. Do not enable restore until post-restore schema upgrades, pool/cache/session lifecycle and migration-version compatibility are concretely handled.
5. Port settings/schedule persistence and native execution semantics, including timezone/DST, malformed update partial writes, orphan schedule cleanup and retention failure boundaries. Run source-versus-native scheduler SQL/clock tests and packaged browser create/upload/download/restore workflows.
6. Port or explicitly retain a tested compatibility runtime for supported CLI, SQLite, plugins/admin and upgrades. Test destructive operations only on owned disposable copies. Run full-service idle/active resource measurements including the worker and PostgreSQL tools after functional parity; no performance claim follows from this inventory.
