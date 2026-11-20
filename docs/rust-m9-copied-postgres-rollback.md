# Copied PostgreSQL cutover and rollback

Original Dispatcharr authors and contributors retain full application credit.

The exact Candidate110 and Candidate111 rehearsals restored a transactional
copy, reused the same front door and authenticated session, and kept the
Native channel edit in the copy. Original's untouched database matched its
complete pre-cutover fingerprint before restarting. Both Compose projects
retired all five owned containers.

Both strict controllers exited 1 after restarting Original. The independent
Original-only restart reproduced four timestamp-only writes on the same rows.
One system notification, two named periodic tasks and the Beat change marker
updated. Schema, sequence values and row counts stayed unchanged. The later
authenticated read added no changes.

The results record the passed copied-data rehearsal and the strict post-restart
fingerprint failure for each image. The integrated M9 release gate remains open.

[Exact rollback receipts](../scripts/rust-integration/milestone-batch111/copied-postgres-rollback.json)

## C74 same-seed control and copied cutover

One quiescent Original donor supplied an identical transactional PostgreSQL
archive to two fresh projects. The Original-only control ran first and
retired before the C74 arm. C74 served the same authenticated session,
channel and media through the trial's unchanged loopback front door.
Its channel-name edit stayed in its own restored copy. The strict Original
fingerprint still matched the quiescent baseline immediately before
fallback restarted Original.

Both Original starts changed exactly four preexisting timestamp cells.
The backup and plugin repository schedules changed `date_changed`,
the Beat marker changed `last_update`, and the developer notification
changed `updated_at`. The complete keyed projection of all other fields,
schema, sequences and row multiplicities stayed equal. The authenticated
read after Original became ready introduced no changes. The independently
observed strict controller exit was **1** because the complete
post-restart fingerprint differed. The separate four-cell diagnostic
passed. The integrated M9 and production fallback gates remain open.

The donor, Original-only arm and C74 arm removed two, two and five
owned containers respectively, with exact absence of all seven observed
host process generations and empty project inventories. An earlier
relative path caused the donor start and cleanup commands to exit 1.
The next run completed and retired the donor, then the deferred
Original arm failed before guest startup evidence and retired cleanly.
The final run used one complete read-only verification mount. The raw
engine error from the deferred failure remains unconfirmed.
[C74 same-seed rollback receipt](../scripts/rust-integration/milestone-batch113/c74-same-seed-postgres-rollback.json)
