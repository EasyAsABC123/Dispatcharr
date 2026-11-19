# Historical SQLite to PostgreSQL and rollback rehearsal

The original Dispatcharr, Django and dependency authors retain full
application, schema and workflow credit.

The fixture seeded a writable copy of the Original v0.11.2 SQLite database
and transferred all 376 verified rows across 48 tables into the historical
PostgreSQL schema. A forced late check violation left all 48 tables and 46
sequence states unchanged. Current Original then applied the remaining 52
migrations to reach 184, preserving all 132 historical recorder rows.

Current Original, ordinary Rust Main, and historical Original on the
separate rollback copy accepted the copied credential and returned the
same owned channel and recurring rule. Each denied anonymous and
wrong-password access as expected. Rust's recurring-rule GET uses the
normal authorization and recording owner. All nine containers, one
network, one volume, and their owned process generations retired. The
controller exited 0.

The acceptance covers historical SQLite migration to PostgreSQL and the
pre-cutover rollback copy. Native SQLite and full M8 acceptance remain open.
[Historical bridge and runtime receipt](../scripts/rust-integration/milestone-batch111/legacy-sqlite-postgres.json)
