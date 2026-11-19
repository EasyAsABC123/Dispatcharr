# XC capacity and VOD priority edits

The original Dispatcharr authors and contributors retain full application,
workflow and algorithm credit.

Rust accepts the existing React form's changes to Max Streams, VOD Priority,
or both, when the other account settings are preserved. The update uses the
existing account and profile writes. Changes to Max Streams update the
account's default profiles, preserving nondefault profiles and other
accounts. The focused controls verify the new request shape and keep mixed
changes outside the new admission scope.

The default Store suite passed all 530 tests, with 79 external-fixture
cases ignored. The source snapshot at `9460c5e07` then passed workspace
formatting, all 1,291 default tests and strict all-target workspace Clippy.
The locked suite left 810 external-fixture tests ignored. Its exact Docker
build froze the runtime packages and preserved all 497 React, license and
attribution inputs from Candidate83. The exact XC Compose pair passed six
Max Streams and VOD Priority edits on each backend. Each PATCH and GET
returned 200, including active and inactive accounts, stale default profile
synchronization and unlimited capacity. All SQL preservation checks passed.
Both controllers exited 0 with exact cleanup.

A separate pinned Original Connect Test fixture passed nine controls, with
two requests to its internal receiver and unchanged integration, subscription,
and delivery log rows and sequences. This is an existing backend action.
Native parity and new performance measurements remain open.

[Focused checks](../scripts/rust-integration/milestone-batch84/focused.json)
· [Docker build and workspace checks](../scripts/rust-integration/milestone-batch84/build-and-workspace.json)
· [Compose results](../scripts/rust-integration/milestone-batch84/packaged.json)
· [Milestone ledger](rust-milestones.md)
