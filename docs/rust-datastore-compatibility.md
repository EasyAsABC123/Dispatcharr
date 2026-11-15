# Required datastore compatibility

Original Dispatcharr and dependency authors retain full application and license
credit. The target remains pinned Original revision
`1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430`.

SQLite is part of the existing application, rather than an optional future
feature: `dispatcharr/settings.py:288` selects Django SQLite at
`/data/dispatcharr.db` when `DB_ENGINE=sqlite`. The alternative configures
PostgreSQL. Lines281–286 and329–347 also configure PostgreSQL TLS mode, CA and
client certificate/key from `POSTGRES_SSL*` variables. These facts come from
the pinned Git source. The initial audit was static; the subsequent
[candidate5 observations](rust-batch5-ops-acceptance.md) and
[candidate7 twelve-case acceptance](rust-batch7-tls-acceptance.md) records actual
driver and Main executions. No datastore performance gain is accepted.

| Supported configuration | Current Rust implementation | Remaining acceptance |
| --- | --- | --- |
| PostgreSQL | SQLx PgPool, existing-schema workflows and fresh schema initialization; current Compose cohort uses PostgreSQL17 | Full required API/job/backup/restore/rollback matrix and version/error compatibility |
| SQLite selected by DB_ENGINE | Missing: Store uses PgPool and Cargo enables only PostgreSQL. Main now rejects explicit SQLite selection before connection or DDL; this guard does not implement SQLite | Backend selection, schema/migrations, every required SQL/JSON/time/constraint behavior, existing dispatcharr.db, backup/restore and concurrent API/job workflows |
| PostgreSQL TLS environment | Main now translates POSTGRES_SSL* settings and validates certificate paths. Candidate7 matches all 12 retained PostgreSQL cases, including verify-ca hostname/IPv6 and both allow branches | Retain the actual Main twelve-case acceptance; complete remaining modes, certificate and error branches, including unmeasured allow I/O/EOF and prefer fallback behavior, against pinned Source |

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
