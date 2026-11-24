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

## Backup filenames and current Original SQLite upgrade

The exact Candidate111 and pinned Original Compose comparison passed 21
filename pairs for 42 requests. It covers signed and authorized reading and
the status and response details for each tested filename and role. Both
backends and all owned resources retired. Backup job execution has its own
acceptance scope.

A separate pinned Original run attempted the current migrations against
another disposable copy of the same historical v0.11.2 SQLite fixture. It
advanced from 132 to 163 migration records, then exited 1 at the PostgreSQL
syntax in Channels 0038. The immutable historical SQLite master stayed
unchanged. The SQLite-to-PostgreSQL rehearsal above remains the verified
bridge for this historical fixture.

[Scoped filename and Original upgrade evidence](../scripts/rust-integration/milestone-batch111/backup-filenames-and-original-sqlite.json)

## SQLite backup archive interoperability

The pinned Original backup service and the Rust crate exchanged v2
SQLite archives in both directions using disposable files. Each
created a ZIP with `database.sqlite3` and `metadata.json` while the
source was open in WAL mode. Restores on closed copies preserved rows,
foreign keys, sequence values, and the existing media reference. Both
retained the replaced database as a separate file. A malformed SQLite
payload triggered Original's rollback and Rust's staging rejection.

Twelve focused Rust backup tests passed. The paired component check
used synthetic data and a fixture restore owner. The normal packaged
worker, signed login after restore, and historical schema upgrade still
need separate evidence.

[SQLite component receipt](../scripts/rust-integration/milestone-batch111/backup-sqlite-roundtrip-component.json)
