# b60763d1 PostgreSQL backup and restore

The ordinary AMD64 package passed the scoped signed backup and restore
workflow. The fixture verified archive creation, inventory, downloaded
content, a same-schema restore with durable database readback, and two
restore refusals with the exact retained Original messages. The regular
retention job moved the owned inventory from five archives to two, and
the observed result persisted through Main shutdown.

The physical test and independent exact teardown audit exited zero.
This workflow covers copied PostgreSQL data. SQLite and broader M8
acceptance remain open. Original Dispatcharr and all upstream
contributors retain full application and dependency credit.

[Machine-readable PostgreSQL qualification](../scripts/rust-integration/m8-b60763d1-amd64-postgresql-backup.json)
