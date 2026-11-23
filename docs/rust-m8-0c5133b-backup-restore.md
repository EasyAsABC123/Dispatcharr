# Backup inventory, download and same-schema restore on 0c5133b

Fresh Original wrote and downloaded one database-only PostgreSQL backup. Both backends authenticated, listed the singleton archive, downloaded it, and listed it again. The archive used Dispatcharr's v2 format and contained the regular `database.dump` and `metadata.json` entries. Rust restored that same archive into a separately owned PostgreSQL 17 target with the matching schema.

The Rust runner verified the signed task response, stored task identity, control state, SQL and application readbacks. The one restore owner was scoped to the disposable stack. The physical runner, independent reducer and exact teardown passed. The [aggregate receipt](../scripts/rust-integration/m8-0c5133b-backup-restore.json) identifies the exact package and archive.

This qualifies ordinary inventory, download and one same-schema restore on the current image. Broader backup formats, upgrade paths, operational roles, performance and complete M8 acceptance remain separate requirements.

Original Dispatcharr, PostgreSQL and dependency contributors retain full credit.
