# Candidate 38 matching SQL schema check

The actual SDK51 Store test executable prepared all 11 production matching SQL statements against the unchanged pinned Original Dispatcharr migrated PostgreSQL schema. Server parameter types matched the production Rust bind types, and result columns matched the actual Rust row decoders. Deliberately wrong types for the same channels and bulk-candidate SQL were rejected by PostgreSQL (42804 and 42883).

The connection was read-only with statement caching disabled and a five-second statement timeout. Ready and close both completed. No matching DML, tasks, media, or model inference ran. This establishes schema/type compatibility; whole matching workflow parity remains a separate application batch.

The existing fixture started exactly one pinned Original baseline, stopped its web process, and restored the unchanged migrated DDL into an owned disposable database. The default database manifest remained byte-for-byte unchanged. All 44 host commands completed their actual waits and pipe collection. Exact CID, project containers/networks/volumes, and all eight unique positive PID generations (nine captured rows include the repeated leader) were absent after cleanup (1.300 seconds within 90 seconds).

`compiled-statements.json` contains the actual server observations, SQL bytes, and negative controls. `bindings.json` binds the completed SDK51 executable, input map, schema dump, and pinned Original image. `peer-execution.json` retains the bounded actual child wait and EOF receipts. `execution.json` distinguishes the host tool terminal receipt from the child receipts. Private absolute paths identify provenance and are not portable launch instructions.

Original Dispatcharr and dependency authors retain full application, algorithm, schema, and library credit.
