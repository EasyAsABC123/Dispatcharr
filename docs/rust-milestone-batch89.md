# Saved Connect webhook Test action

The original Dispatcharr authors and contributors retain full application,
workflow and algorithm credit.

Rust exposes the existing admin-only Connect Test endpoint for saved
webhooks. It uses the existing form transport and retained request
ownership, reads only the selected config, and returns the Original dummy
event and receiver result. The pinned React frontend has no Test control.
Script integrations and transport-error detail remain open.

Nine pinned Original Compose checks established the role, request encoding,
response, receiver and no-write contract. Formatting, strict all-target
Store Clippy and all 530 Store library tests passed for the Native port.
The suite left 79 external-fixture cases ignored. The exact-image Native
Compose comparison is pending.

[Focused results](../scripts/rust-integration/milestone-batch89/focused.json)
· [Original Compose receipt](../scripts/rust-integration/milestone-batch84/packaged.json)
· [Milestone ledger](rust-milestones.md)
