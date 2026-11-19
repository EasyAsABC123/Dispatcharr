# Integration subscription replacement and XC refresh interval

The original Dispatcharr authors and contributors retain full application,
workflow and algorithm credit.

Native Main now accepts the admin subscription replacement that the existing
React Integrations page sends after creating or editing an integration.
It validates the full list before making changes, deletes stale subscriptions
and their dependent delivery logs, and applies each event in request order.
The stored-duplicate case preserves Original's recorded error and earlier
committed effects. The pinned Original passed all 17 scoped cases with no
delivery, controller exit 0, and exact Compose cleanup.

The Rust XC account update now accepts the stock interval change when the
account already uses interval scheduling. It reuses the existing task and
scheduling code. The paired fixture checks active and inactive accounts,
task enablement, reuse of shared schedule rows, deletion of orphan rows,
and preservation of other account settings.

All 526 default Store tests and strict all-target Store Clippy passed.
Seventy-nine tests requiring external fixtures were ignored in the default
run. The exact Docker image then passed all 17 paired subscription cases
and the active and inactive XC interval comparisons. Both fixture paths
verified cleanup. The separate Original integration-edit capture passed
18 scoped requests and preserved IDs, timestamps, subscriptions and logs.

[Packaged evidence](../scripts/rust-integration/milestone-batch81/packaged.json)
· [Focused checks](../scripts/rust-integration/milestone-batch81/focused.json)
· [Milestone ledger](rust-milestones.md)
