# Integration creation and XC account rename

The original Dispatcharr authors and contributors retain full credit for the
application, workflows and algorithms.

Native Main now accepts the admin JSON request that creates an integration.
It applies the observed validation before inserting the row and returns the
new integration in the same database operation. Creation and subscription
replacement remain separate operations in React. The Original create fixture
observed 16 requests, including three inserts and zero deliveries.

The Rust XC account update accepts the existing React playlist editor's
rename request. It preserves the account's credentials, scheduling, default
profile label and other settings when the effective change is the name.
The portable paired fixture checks the HTTP response and independently
verifies those database effects.

All 524 default Store tests and strict all-target Store Clippy passed.
Seventy-nine tests requiring external fixtures were ignored in the default
run. Both exact-image Compose comparisons then passed. Original and
Native matched all 16 integration-create cases and produced no deliveries.
The stock XC rename matched its PATCH and GET responses and preserved the
tested account, task, and profile fields. Independent cleanup passed.

The regular integration runner now includes the opt-in `connect_create`
suite. It can run on its own or after `connect_reads,connect_logs`.

[Exact-image Compose evidence](../scripts/rust-integration/milestone-batch80/packaged.json)
· [Focused checks](../scripts/rust-integration/milestone-batch80/focused.json)
· [Original create capture](rust-milestone-batch79.md)
· [Milestone ledger](rust-milestones.md)
