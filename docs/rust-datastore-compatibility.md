# Required datastore compatibility

Original Dispatcharr and dependency authors retain full application and license
credit. The target remains pinned Original revision
`1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430`.

SQLite is part of the existing application, rather than an optional future
feature: `dispatcharr/settings.py:288` selects Django SQLite at
`/data/dispatcharr.db` when `DB_ENGINE=sqlite`. The alternative configures
PostgreSQL. Lines281–286 and329–347 also configure PostgreSQL TLS mode, CA and
client certificate/key from `POSTGRES_SSL*` variables. These facts come from
the pinned Git source; no new database application or performance test ran for
this audit.

| Supported configuration | Current Rust implementation | Remaining acceptance |
| --- | --- | --- |
| PostgreSQL | SQLx PgPool, existing-schema workflows and fresh schema initialization; current Compose cohort uses PostgreSQL17 | Full required API/job/backup/restore/rollback matrix and version/error compatibility |
| SQLite selected by DB_ENGINE | Missing: Store uses PgPool, Cargo enables only the PostgreSQL SQLx driver, and Main's database_url ignores DB_ENGINE | Backend selection, schema/migrations, every required SQL/JSON/time/constraint behavior, existing dispatcharr.db, backup/restore and concurrent API/job workflows |
| PostgreSQL TLS environment | Original POSTGRES_SSL* translation is absent in Main's URL construction; a caller-supplied DATABASE_URL is a separate configuration path | Verify all retained environment settings, TLS modes, certificate trust, client authentication and errors against pinned Source |

The 1:1 requirement already determines that these supported configurations must
be preserved; no new product-scope decision is needed. Passing PostgreSQL-only
Compose tests cannot close this gate or M5/M8/M9. Existing image57 performance
and later component checks retain their original PostgreSQL workload scope.

Implementation should introduce explicit backend selection and backend-specific
repository behavior while preserving domain DTOs, shared admission and retained
physical operations. Do not translate PostgreSQL queries mechanically: current
code exposes PostgreSQL rows/pools, JSON operators, sequences and error metadata.
Use one reusable Source/Native cohort per supported backend with grouped CRUD,
job and restore workflows, including existing data and exact error effects.
SQLite tests must use the actual unchanged React and Rust production entrypoint;
a converted PostgreSQL copy alone does not establish SQLite support.

Until implemented, deployment documentation must continue to describe the Rust
port as incomplete and PostgreSQL-scoped. Keep the executable datastore gate
open and preserve all existing SQLite data.
