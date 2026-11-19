# SQLite compatibility remains open

SQLite remains part of the requested one-to-one rewrite. The Native application currently uses PostgreSQL and explicitly rejects SQLite as unfinished. The actual unchanged Original production migration command now fails on fresh file-backed SQLite; application, backup/restore and Native SQLite acceptance remain open.

Original Dispatcharr authors and contributors retain full credit for the application and algorithms. Django, Python/SQLite, PostgreSQL, Celery/Kombu and other dependency authors retain their credit and licenses.

## Exact Source selection

Authority is captured Original commit `1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430`, archive SHA `f2323ac705be2af5d1c264f26b1d297cb2bede53874e6bceccee4bf6603fa7ff`.

| Entry point | Captured behavior |
|---|---|
| Production settings with exact `DB_ENGINE=sqlite` | Django SQLite backend at fixed `/data/dispatcharr.db` |
| Unset or differently cased production value | PostgreSQL settings branch |
| `python manage.py test` | Selects separate `dispatcharr.settings_test`, normally isolated PostgreSQL |
| Test settings with `TEST_USE_SQLITE=1`, `true` or `yes`, ignoring case | In-memory SQLite `:memory:`; independent of production selection |

The production file selector is real Source code. Selecting an engine alone does not establish that every application operation works on it. The in-memory test fallback cannot qualify file-backed deployment or backup behavior.

## Backup behavior and portability uncertainty

Source SQLite create uses Python's actual `sqlite3.Connection.backup` API. Its version-two ZIP contains `database.sqlite3` and metadata declaring SQLite. Restore checks the engine type, copies an existing file to `.db.bak`, copies the extracted database over the configured path, and queries `sqlite_master` for readability. A `sqlite3.DatabaseError` triggers restoration of the saved `.bak` when available. This checks readability rather than the complete application schema.

The actual restore task subsequently runs Django migrate. Caught errors produce Celery backend SUCCESS with a `status=failed` dictionary. A successful file copy therefore does not establish a successful restore task. Source does not explicitly retire every application connection or reconcile WAL/SHM sidecars in this service; concurrent readers, writers, file permissions and failure recovery need observation. Relevant Source unit tests mock SQLite dump/restore branches and do not supply physical acceptance.

The deployment and direct CLI results establish different behaviors:

- The default Dockerfile entrypoint provisions or waits for PostgreSQL and launches migrate/web through `su -`. Its explicit environment whitelist omits `DB_ENGINE`. In the isolated all-in-one run below, PID 1 received `DB_ENGINE=sqlite`, while login-shell settings and all six observed uWSGI process generations lacked it. Django selected PostgreSQL.
- Several migrations provide non-PostgreSQL fallbacks. Actual fresh migration first fails at Channels0038 with `django.db.utils.OperationalError: near "~": syntax error`. The full VOD0003 migration was not reached. Isolated SQLite dialect checks on its exact SQL are recorded below.

## Actual Original all-in-one entrypoint

The default entrypoint ran from the hash-verified pinned Original archive in a
trimmed dependency image. The isolated container used fresh memory-backed
`/data`, networking disabled, and no published ports. PID 1 received
`DB_ENGINE=sqlite` and the uWSGI processes reached the application login shell.

| Observation | Actual result |
|---|---|
| Application HTTP | Loopback GET `/api/accounts/initialize-superuser/` returned 200 |
| Login shell and all six observed uWSGI process generations | `DB_ENGINE` absent |
| Effective Django backend | `dispatcharr.db.backends.postgresql_psycopg3` |
| Physical PostgreSQL query | Owned UTF-8 database reached with 184 applied migrations |
| Physical database paths | `/data/db/PG_VERSION` present, `/data/dispatcharr.db` absent |
| Wait and teardown | Attached startup exited 0 after controlled stop, no OOM, exact owned container removed |

The first attempt exited 1 before HTTP readiness because the trimmed dependency
image initialized PostgreSQL with SQL_ASCII. The successful second attempt
set `LANG=C.UTF-8` in the disposable login environment. Both attempts kept the
pinned Source entrypoint and database environment handoff intact. The
[selection receipt](../scripts/rust-integration/sqlite-source-aio-2/result.json)
records actual exits, file hashes, observations and cleanup. It qualifies
PostgreSQL selection in this all-in-one setup. File-backed SQLite deployments
still require their own Source acceptance.

## Actual fresh Source CLI result

The pinned dependency image `sha256:34f9011514edd31ef0ff111b5f256d2e37614fcb08a6ee1ed9eccec9251a856f` ran the unchanged `python manage.py migrate --noinput` against an independent empty `/data`, with production settings, exact `DB_ENGINE=sqlite`, and the ordinary AIO pre-Redis flag `DISPATCHARR_SKIP_REDIS_CACHE=1`. The preflight confirmed `django.db.backends.sqlite3` and `/data/dispatcharr.db`. The container had no network, servers or workers; this command overrides the image entrypoint and does not qualify its login-shell propagation.

The real command exited **1** after **4.506 seconds**, first failing in `apps/channels/migrations/0038_add_catchup_fields.py:9`, `backfill_stream_catchup`. The read-only observer exited zero: SQLite `quick_check` returned `ok`, 44 tables existed, and `django_migrations` contained 158 applied entries ending at `dispatcharr_channels.0037_auto_sync_overhaul`. A readable partial database is not a usable current application schema. No fake migration, SQL patch, skipped migration or test-settings fallback was used.

Both output streams reached EOF, the CLI process group settled, output was not discarded, the container exited 1 without OOM, and both owned CLI/preflight containers were removed with final labelled-container absence. Attempt 1 is retained separately as a pre-Django file-permission failure; its migration never ran. Attempt 2's unchanged controller report contains an unset `migration_run:false` field; the derived result records actual execution from migration output, traceback and applied-prefix observations, preserving that raw report rather than rewriting it.

The [actual CLI packet](../scripts/rust-integration/sqlite-source-cli-1/result.json) contains the result, raw command output, read-only schema/prefix, failure provenance and cleanup receipts. Original Source application behavior and Native parity remain unqualified beyond this CLI observation.


## Historical migration provenance

All six retained Git versions of Channels0038, including its introduction,
contain a PostgreSQL `::jsonb` cast without a backend guard. The exact pinned
Original and published migration worktree contain no tracked SQLite database
fixture or backup archive. The retained owned file passes `quick_check` and
ends at Channels0037 after 158 recorded migrations.

A separate in-memory SQLite 3.54.0 check ran the exact VOD0003 SQL statements
against minimal tables. Its initial INSERT parsed. Its forward UPDATE failed
near `m`, and its PostgreSQL `DO` statement failed near `DO`. The full Original
migration still requires its own application fixture.
[Provenance receipt](../scripts/rust-integration/sqlite-source-scope/provenance-9a017508.json)

## Required characterization gates

1. The ordinary Source all-in-one entrypoint observation now captures actual engine selection, startup exits and physical PostgreSQL activity. Keep credentials and raw process environments private.
2. The real public `python manage.py migrate --noinput` with production settings, exact `DB_ENGINE=sqlite` and fresh owned `/data` has recorded the first failure and read-only applied prefix. Retain these failures when testing other legitimate SQLite data paths.
3. Run actual REST create/status/download/restore only after unmodified bootstrap succeeds or a legitimate owned SQLite application fixture has verified Original version/history. Otherwise retain failures and mark dependent cells blocked/not_run. No such existing application file is supplied by the captured archive. A usable baseline must then cover same-instance login/settings/channel restoration, real task/migrate results, failure recovery and physical reader/writer/shutdown behavior.

The current Rust gap includes PostgreSQL-specific Store/SQLx types and schema, PostgreSQL backup tools, and PostgreSQL session/server fencing. SQLite requires coherent persistence, migration/history, connection/file/journal and task/Beat/plugin lifecycles across the actual Native application. Adding an archive member cannot establish that compatibility. Source uncertainty does not waive the requirement or authorize a speculative engine implementation.

Static references and the original prepared gates remain in [authority](../scripts/rust-integration/sqlite-source-scope/authority.json), [source hashes](../scripts/rust-integration/sqlite-source-scope/source-hashes.json), and the [historical gated matrix](../scripts/rust-integration/sqlite-source-scope/gated-matrix.json). The actual CLI packet supersedes that matrix's unexecuted fresh-migration cell only. The next gate requires a legitimate complete Source SQLite application fixture with recorded version and migration history before dependent REST backup/restore acceptance. The captured archive supplies no such file. M9’s datastore scope gate remains open. Native equivalence and equivalent-work performance remain blocked; all full milestones remain open.


## Verified historical Original SQLite database

Original v0.11.2 at `d0ebfb57c4bde04d047a555f66b1da92104eea8f` ran its
unmodified SQLite migrations against a disposable file. The official
linux/amd64 image digest was
`sha256:e7ed9a28f57c51ccedc2f113f4fd857eee22ff4d6b6be5dbea4e995ceeaf371c`.
The fixture independently verified the exact 261-file Git source archive.

The historical app completed ordinary ORM writes, administrator setup,
password login and channel reads across a process restart. The closed
database had 49 tables and 132 recorded migrations. Read-only inspection
passed integrity and foreign-key checks, and no journal or WAL sidecars
remained. The owner verified the container exited 0 and was removed.

An independent inspector copied the resulting database and checked the
copy in SQLite immutable mode. Both file hashes matched throughout. This
qualifies the historical file as input for a future legacy import check.
The Native SQLite runtime and import remain open, and the current pinned
Original still has its separate fresh-schema migration failure.

[Historical fixture and copy receipt](../scripts/rust-integration/legacy-sqlite-source-v0.11.2.json)
