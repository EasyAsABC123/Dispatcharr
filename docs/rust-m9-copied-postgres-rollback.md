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
