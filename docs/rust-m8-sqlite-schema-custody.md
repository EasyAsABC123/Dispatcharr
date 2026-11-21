# SQLite schema custody after migration

Both disposable runs used pinned Original `1241b3f7ffb2` with the two
SQLite-only migration adapters for Channels0038 and VOD0003. The guest
verified the `dispatcharr.settings` module, Django's SQLite backend, and
`/data/dispatcharr.db` before emitting `production_SQLite_selected`.
The fresh database advanced from zero to 184 migrations and the cloned
v0.11.2 database from 132 to 184. Both ended with 63 tables on SQLite
3.46.1. Django migration and migration check exited 0, the migration
history and integrity and foreign-key checks passed, and both owned
containers retired. The historical master copy was unchanged.

The read-only captures found the same ordered app and migration names, DDL
objects, typed table structures, 20 bound SQL query shapes, and seven
Django-compiled query shapes. The inventory excluded application row
data, bind values, and database files. The checks observed SQLite statement
preparation and ORM compilation. They establish schema custody for this
adapted Original fixture. Unmodified migration, web startup, and Rust
SQLite runtime remain open.

The initial sequence exited 1. The second sequence also exited 1 even
though both physical runs exited 0. Its reducer expected a field name
different from the one the sealed guest emitted. An offline replay against
the actual field ran all original schema comparisons and exited 0. The
earlier results remain in the evidence.

[Redacted schema receipt](../scripts/rust-integration/m8-sqlite-schema-custody.json)

Original Dispatcharr, Django, SQLite, and dependency authors and contributors retain full credit. [Attribution](../ATTRIBUTION.md)
