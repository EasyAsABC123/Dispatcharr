# Historical SQLite Channel Groups repository test

The pinned Original revision reaches the same Channels0038 SQLite syntax
error on fresh and authentic historical files. The historical copy advanced
from 132 to 163 migrations before the command exited 1. Channels0038 uses
PostgreSQL regex and casts. A separate local parser check also rejected the
pinned VOD0003 PostgreSQL anonymous blocks and target aliases. The latter
does not represent a full VOD migration. Three inspected closed retained
SQLite files contained neither a verified current migration history nor a
current Original application boot. The v0.11.2 authority remains usable
with its historical application. [Current SQLite authority](rust-sqlite-compatibility.md)

A private Rust SQLx 0.8.6 component ran on a byte-identical disposable copy
of that v0.11.2 database. Concurrent creates of the same Unicode group name
committed exactly one row. The component read and renamed it, closed and
reopened its pool, read the committed name and deleted it. It preserved the
linked historical group, the schema and all 48 application tables after
deletion. Only the group sequence advanced, by one. Integrity and foreign
keys passed and the test copy was retired. The unbundled offline build
exited 101 due to an unavailable bindgen dependency in the cache. The
bundled Rust build, test and strict Clippy exited 0.

The exact audited Native application uses PostgreSQL. Its Channel Groups
handler has a narrow repository boundary around typed rows and complete
mutation transactions. Current SQLite HTTP acceptance still requires a
legitimately migrated current schema, authentication, full CRUD across a
restart, provider and channel associations and matching PostgreSQL
regressions. The historical SQLite-to-PostgreSQL bridge remains separately
qualified. [Scoped evidence](../scripts/rust-integration/sqlite-historical-group-crud.json)

Original Dispatcharr, Django, SQLite and SQLx authors and contributors
retain all application and dependency credit.
