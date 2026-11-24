# Current ARM64 backup worker recovery after database connection loss

On exact `2d3ffe53`, the fixture terminated only the identified `pg_dump` SQL backend while it waited on the copied database’s table lock. Two signed create requests returned 202. The first reached the ordinary signed failure state and produced no archive. A distinct retry on the same Main generation succeeded and produced a valid 37,351-byte two-member ZIP with a PostgreSQL dump. The fixture verified the signed download and delete, and independently checked retirement of six containers, two volumes and the network. [Redacted proof](../scripts/rust-integration/m8-2d3ffe53-arm64-backup-backend-loss.json)

Copied and control SQL and the synthetic control file stayed unchanged. The separate signed restore verified recording media. The complete canonical backup workflow, callback and job lifecycle, and SQLite upgrade remain open. [Same-package PostgreSQL restore](rust-m8-2d3ffe53-arm64-postgresql-group.md)

Dispatcharr’s original authors and the authors of its dependencies retain full credit. [Attribution](../ATTRIBUTION.md)
