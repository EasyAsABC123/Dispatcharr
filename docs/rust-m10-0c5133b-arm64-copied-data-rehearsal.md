# M10 current ARM64 copied-data rehearsal

On Lappy, the exact current native package completed a disposable
Original → PostgreSQL logical copy → Rust → warm Original rehearsal.
The fixture generated one ordinary signed administrator and a synthetic
catalog in Original. Rust started on the copy, accepted the Original-issued
token, returned the same account and catalog, and completed a new normal
login. After Rust stopped, the already-running Original served the same
signed reads from its untouched database and process generation.

The archive dump and restore both exited 0. The copy preserved all 501
ordered column definitions across 62 tables, as well as constraints,
indexes, sequence definitions, row counts, and selected typed data.
PostgreSQL compacted 73 internal column numbers during the logical restore.
The first attempt failed on those physical numbers. The second reached
Rust login and failed because the census omitted the normal login audit
effect. The successful retry verified the single appended `login_success`
event for the fixture actor and the login timestamp. All other row counts,
selected data and schema in the copy remained stable.

The successful controller and Native stop exited 0. Independent teardown
retired nine containers, one network, and three volumes. This rehearsal
covers a disposable logical copy and signed-read rollback. The ARM64
write-reconciliation scenario, broader supported-data upgrade matrix,
and controlled rollout remain open.

Original Dispatcharr, its React frontend, and its dependencies retain
their existing attribution and licenses.

[Machine-readable receipt](../scripts/rust-integration/m10-0c5133b-arm64-copied-data-rehearsal.json)
