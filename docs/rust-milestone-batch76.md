# XC orphan cleanup and archive fault controls

The original Dispatcharr authors and contributors retain full application and
algorithm credit.

The Rust account update now accepts the three orphan-cleanup modes exposed by
the existing React Live Group Filter. It keeps the other custom properties,
account state, scheduling and memberships. Scoped Rust tests exercise all
three modes on active and inactive accounts, with VOD properties and with
null prior properties. Paired packaged acceptance still needs to verify the
three PATCH and follow-up GET results against Original and the seeded SQL rows.

The archive regression parks the peer's second Lua snapshot before passing it
to Redis. It changes the programme generation, changes the stored profile, or
disconnects that Redis connection. All three cases check the exact 503,
unchanged active ownership and provider quota, no fallback archive claim, and
no additional origin request. The eight media tests passed with real Redis, and
the disposable Redis instance was removed. Strict all-target Clippy passed on
the touched Rust packages.

[Focused Rust and Redis evidence](../scripts/rust-integration/milestone-batch76/focused.json)
· [Milestone ledger](rust-milestones.md)
