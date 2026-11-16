# SQLite compatibility remains open

SQLite remains part of the requested one-to-one rewrite. The Native application currently uses PostgreSQL and explicitly rejects SQLite as unfinished. This packet contains static Original Source authority and a prepared characterization matrix; no SQLite application, migration, backup/restore workflow or performance result has been executed or accepted.

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

Two static findings require runtime characterization:

- The default Dockerfile entrypoint provisions or waits for PostgreSQL and launches migrate/web through `su -`. Its explicit environment whitelist omits `DB_ENGINE`; checked-in Compose examples use PostgreSQL. **Inference:** a container's SQLite variable may not reach the actual login-shell application processes. Effective packaged selection remains unmeasured.
- Several migrations provide non-PostgreSQL fallbacks, but VOD0003 contains unguarded PostgreSQL `DO`/catalog SQL and Channels0038 contains PostgreSQL regex/casts. **Inference:** fresh SQLite migration is likely blocked. The exact first failure and applied prefix remain unmeasured.

## Required characterization gates

1. Run the unchanged ordinary Source entrypoint in an owned environment and capture actual effective engine/path, startup exits and database activity. Record PostgreSQL selection as a deployment observation, never SQLite acceptance. Keep credentials and raw process environments private.
2. Run the real public `python manage.py migrate --noinput` with production settings and exact `DB_ENGINE=sqlite` against an independent owned `/data`. Record actual exits, first failure, applied migration prefix and read-only file/schema observations. Declare the real AIO pre-Redis flag where applicable. Do not use `--fake`, skipped migrations, SQL patches, generated app schemas or migration overrides.
3. Run actual REST create/status/download/restore only after unmodified bootstrap succeeds or a legitimate owned SQLite application fixture has verified Original version/history. Otherwise retain failures and mark dependent cells blocked/not_run. No such existing application file is supplied by the captured archive. A usable baseline must then cover same-instance login/settings/channel restoration, real task/migrate results, failure recovery and physical reader/writer/shutdown behavior.

The current Rust gap includes PostgreSQL-specific Store/SQLx types and schema, PostgreSQL backup tools, and PostgreSQL session/server fencing. SQLite requires coherent persistence, migration/history, connection/file/journal and task/Beat/plugin lifecycles across the actual Native application. Adding an archive member cannot establish that compatibility. Source uncertainty does not waive the requirement or authorize a speculative engine implementation.

Exact references and next steps are recorded in [authority](../scripts/rust-integration/sqlite-source-scope/authority.json), [source hashes](../scripts/rust-integration/sqlite-source-scope/source-hashes.json), and the [gated matrix](../scripts/rust-integration/sqlite-source-scope/gated-matrix.json). Native equivalence and equivalent-work performance remain blocked on a concrete Source baseline and implementation; all full milestones remain open.
