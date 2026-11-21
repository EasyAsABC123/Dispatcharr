# d426 PostgreSQL backup inventory and download

Pinned Original and the exact Rust `d426d322b27a` package passed the
ordinary administrator backup inventory and direct download workflow on
separate disposable PostgreSQL fixtures. Rust used the default backup
root, with the optional worker ownership and accelerated delivery off.

Each arm first listed an empty directory. The fixture used Original's
management code to generate a v2 ZIP containing a PostgreSQL custom dump,
then seeded two named copies with modification times ordered opposite to
the filenames. The API listed both in reverse filename order with exact
byte sizes and UTC timestamps, including fractional precision. An
unrelated text file stayed out of the inventory.

The authenticated direct download returned the exact local ZIP bytes and
expected headers. A repeat inventory matched the first one. The reads
preserved both files and the scoped SQL rows. Each fixture removed the
seeded files and actor, and all six Compose roles retired.

This scope covers inventory and download. Asynchronous backup creation
and status, restoration, and comparative performance have separate
gates. [Redacted receipt](../scripts/rust-integration/m8-d426-postgresql-backup-inventory.json)

The original Dispatcharr and dependency authors retain full credit.
[Attribution](../ATTRIBUTION.md)
