# XC live refresh followed by VOD ingestion

The original Dispatcharr authors and contributors retain full application,
workflow and algorithm credit.

Candidate95 completed the direct XC refresh cases on both packaged backends.
With VOD enabled and live data imported, the live worker emitted one VOD
child carrying the real parent and root identities. With VOD disabled or
the live provider empty, the fixture observed no VOD child. The real
worker results, ordered child deliveries, provider operations and scoped
SQL rows matched in the three direct pairs.

Original completed the scheduled case. Native's scheduled frame reached
Redis without the two task time limits. No matching Native worker result
appeared within 55 seconds. The Native lane exited 1 and both owned
projects passed exact cleanup.

The scheduler correction adds the Original 3,600-second hard and
3,500-second soft limits to the XC task frame. Formatting and strict
all-target job-transport Clippy passed. The corrected revision passed 287
job-transport tests with 113 external-fixture tests ignored. Its packaged
scheduled replay remains open in this checkpoint.

[Actual Candidate95 rows and failure](../scripts/rust-integration/milestone-batch95/xc-vod-candidate95.json)
· [Milestone ledger](rust-milestones.md)
