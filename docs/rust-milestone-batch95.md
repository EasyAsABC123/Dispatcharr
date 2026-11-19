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
job-transport tests with 113 external-fixture tests ignored.

Candidate96 (`68cd75283d4aea6ee19b963d1dbb20be5ce3614c`) passed the exact
scheduled-only Docker Compose replay. Beat published one live refresh per
backend with the `[3600, 3500]` limits and advanced the persisted run count
once. Both workers completed the live refresh, profile refresh and VOD
child. The captured child frames refer to the real live task as their root
and parent. Both backends saved the same owned stream and movie records,
with equal provider request counts.

Both lanes exited zero, settled their process groups and removed all five
owned container generations across their two projects. The outer queue
observed a zero exit for the pair. The package preserved all 497 React,
license and attribution inputs. This replay verifies the scheduled branch
of this workflow. The full M6 gate and performance remain open.

[Actual Candidate95 rows and failure](../scripts/rust-integration/milestone-batch95/xc-vod-candidate95.json)
· [Candidate96 scheduled Compose replay](../scripts/rust-integration/milestone-batch96/xc-vod-beat.json)
· [Milestone ledger](rust-milestones.md)
