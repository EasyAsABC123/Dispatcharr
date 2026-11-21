# Selected PostgreSQL backup and restore round trip

Pinned Original and exact Rust candidate `0f79dc28` each created and
downloaded a version 2 database-only PostgreSQL archive in separate
disposable Compose projects. Both create requests returned 202. The
signed status returned 200 and agreed with the actual worker result in
Redis. Each archive contained the PostgreSQL custom dump and metadata.

The same Rust Main uploaded the unmodified Original archive, verified
the downloaded bytes and restored it. The Rust worker then restored
the archive it had generated before the first restore, returning the
target to its initial scoped state. All three Native task identities
were distinct and their signed statuses agreed with actual successful
worker results. Both restore requests returned 202 and passed fresh
administrator login, settings and channel reads. The representative SQL
matched the expected account, channel, logo and settings data after each
restore. The Main and PostgreSQL process generations remained the same,
and the independent PostgreSQL control marker stayed unchanged. Both
projects stopped normally and all six observed roles retired, with
empty owned inventories.

The candidate used explicit Selected jobs, disabled Selected Beat,
dedicated Redis broker and result namespaces and direct download.
Standalone and taskless backup owners were absent. The evidence is
for `0f79dc28`, with no transfer to later packages. Current default Core
job ownership, scheduled work, enabled-plugin or older-schema restore,
media files and performance retain separate acceptance. The full M8 gate
remains open. [Scoped receipt](../scripts/rust-integration/m8-0f79dc28-selected-backup-roundtrip.json)

Original Dispatcharr, Django, Celery, PostgreSQL and other dependency
authors and contributors retain all application and dependency credit.
