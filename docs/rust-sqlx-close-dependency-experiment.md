# SQLx close dependency experiment

The pinned sqlx-core 0.8.6 maximum-one-connection baseline failed its deterministic after_release / first-close case: 96 physical observations over 10010 ms retained size=1, idle=1 and the original live SQLx socket/backend/relay. Its two control cases passed. The baseline host/controller exited 1 and test ELF 101. Last-handle-drop cleanup succeeded separately.

A private dependency-only final-idle-drain candidate passed the same three cases, each with one observation at 7 ms, and host/controller/ELF exited 0. These are bounded fixture observations, not timing measurements. Actual PostgreSQL Ready frames, socket/backend identity, all 11 table/sequence snapshots, default audits and process/project cleanup are preserved.

[Portable evidence and reconstruction](../scripts/rust-contracts/sqlx-close-dependency-experiment/README.md) retains exact upstream licenses, executed controller/input seal, original build and capture receipts, both raw archives, independent reviews, corruption controls and exact 898-file reconstruction. Historical ELFs are omitted explicitly. Compiler, freshness-preflight and ENOSPC negatives remain negative; successful candidate recovery is recorded as two commands in a fresh target rather than relabeling the original build.

No canonical Cargo/vendor or Main change is selected. Main does not explicitly call pool.close. Maximum greater than one, subpools, concurrent close and application shutdown remain unqualified. Notification physical cases 2/4/6 remain negative. Dispatcharr Original and SQLx contributors retain their licenses and credit.
