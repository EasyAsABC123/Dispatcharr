# Rust release datastore scope

On November 24, 2026, Justin selected PostgreSQL for the ordinary Dispatcharr service and a separate opt-in utility for migrating copied SQLite data. The utility operates on copies and must leave the input data unchanged. Live SQLite service and same-engine SQLite backup and restore are outside this release scope.

The pinned Original release `1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430` stops at its PostgreSQL-specific Channels 0038 migration when run directly against SQLite. The failure occurred on both a fresh SQLite database and a verified v0.11.2 copy. The retained evidence still describes the behavior of that Original release.

The current Rust candidate `3d8f96725ed90f5d37e406492e264e7c4d37f89a` passed the scoped PostgreSQL copied restore and retention workflow. The separate SQLite operator passed five isolated cases, including upgrading a copy to 184 migration records and refusing unknown history and a staged collision with unchanged input. [PostgreSQL workflow evidence](rust-m8-amd64-postgres-workflow-3d8f9672.json), [copied SQLite operator evidence](rust-m8-amd64-copied-sqlite-operator-3d8f9672.json)

The current 127-row ledger remains available as historical evidence for this decision. Its three live SQLite rows will retain their original dispositions in that ledger. The revised release graph will mark those rows out of scope and require a separate copied SQLite utility row. The full PostgreSQL workflow, backup lifecycle, rollback, soak and performance gates remain required. This record leaves the release on hold pending the remaining acceptance work. [Recorded decision and graph projection](rust-m8-m9-datastore-scope-3d8f9672.json)

Original Dispatcharr and upstream contributors retain full authorship, attribution and license credit.
