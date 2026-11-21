# Copied PostgreSQL rollback rehearsals

These Docker Compose exercises used disposable copies of PostgreSQL. The
Original Dispatcharr authors and contributors retain full credit, along with
the authors of the frameworks and other dependencies. Application and React
source were frozen for each run.

## Warm fallback with one acknowledged write

The rehearsal used packaged Rust `e740306c33c0b188591db904ba643d4479e068ea`
and image configuration `sha256:7e11df765c57e632ef3d60b2caccc460363eb79b9c241ba35a199b3d88b08f60`.
It verified the same signed session against Original, retired the old Rust
writer before reconciliation, acknowledged one fallback channel rename, and
reconciled the retained native write. Only the channel's `name` and
`updated_at` changed in the checked row. Unrelated rows and schema matched.
Original process generations remained stable and the owned resources retired.

The local fixture recorded 8.051 seconds of recovery. Comparative performance
and production recovery time remain unmeasured by this exercise. Preflight,
Compose, and independent reduction each exited 0.
Proof SHA-256 is `da776cc3b961b48d59f9a0484e8e4dd3de02e7b05b6d81ecf9d92a01a7e7a13a`.

## Cold boot from identical copied state

The separate diagnostic used packaged Rust `1423e79b6090af2ba47a7516ff4b29f39e9222fa`
and image configuration `sha256:4cd12e5ff12aad6750d092d96ad14b725d59872f8157f603a3c2755e541a792a`.
Both backends started from the same 244,623-byte donor copy. The four
observed differences were advancing boot timestamps for the backup schedule,
the plugin repository refresh schedule, the developer notification, and the
shared Beat schedule marker. Each change occurred between startup and
readiness. Non-clock rows, row counts, schema, indexes, constraints,
sequences, and authenticated read state matched in the checked comparison.
The owned donor and trial resources retired.

The strict Original preboot-to-ready equality observer returned 1 on
the fresh control. The narrower transition comparison, Compose owner,
and independent reduction returned 0. The strict cold rollback requirement,
full M9 acceptance, and production rollout eligibility remain open.
Proof SHA-256 is `5b994a2d0ba732a58e098aed2849ebb073747c4970a3b508a3b215ad0a5dd1ff`.
