# Copied SQLite login and Channel Groups

Pinned Original `1241b3f7ffb2` and a private Rust Axum harness on
`cc4949173b55` completed the same ordinary login and Channel Groups
workflow on separate copies of one pre-login SQLite file. The physical
run and independent reducer both exited 0.

The fixture started with a synthetic v0.11.2 database. Two SQLite
dialect adapters let Original Django migrate from 132 to 184 migration
records and from 49 to 63 tables. Original seeded three local actors
and their groups and associations once through its ORM. A consistent
pre-login backup became the byte-identical starting point for both
runs. The historical master stayed unchanged.

Both listeners issued three matching successful token response shapes
with the same `Allow` value. All three role-scoped collection responses
matched in status, `Allow`, and full typed JSON, including returned
group identities, effective counts and nested provider details. Each
listener reopened its own SQLite copy and accepted the same
administrator bearer it had already issued. The post-restart query
returned the same collection and left the login clocks and audit
counts unchanged. Both runs verified cleanup.

The earlier synthetic Rust smoke also completed and cleaned up. Its
first evidence reducer exited 1 because the expected test name was
abbreviated. The corrected reducer independently verified the existing
physical run without repeating it. The recorded exit 1 remains part of
that evidence.

This scope uses adapted direct Original and test-only Rust routing.
Packaged Main selection, Native SQLite migrations, backup/restore,
other application routes and comparative performance remain open.
[Redacted evidence](../scripts/rust-integration/m8-copied-sqlite-login-groups.json)

The original Dispatcharr authors and contributors retain full credit.
[Attribution](../ATTRIBUTION.md)
