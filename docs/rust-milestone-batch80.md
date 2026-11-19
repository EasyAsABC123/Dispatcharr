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
run. The new paths are queued for exact-image Compose checks.

[Focused checks](../scripts/rust-integration/milestone-batch80/focused.json)
· [Original create capture](rust-milestone-batch79.md)
· [Milestone ledger](rust-milestones.md)
