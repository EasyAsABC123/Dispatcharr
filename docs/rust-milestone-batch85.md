# Channel editor guide links and modular PostgreSQL TLS

The original Dispatcharr authors and contributors retain full application,
workflow and algorithm credit.

The existing React channel editor calls Set EPG before updating the channel.
The Rust action links or clears the guide field without changing the channel's
update timestamp. It performs the scoped admin lookup before reading the
request and uses the existing guide invalidation and programme publisher.
Dummy guide links and removal use cache invalidation. Non-dummy links require
the selected worker publisher. That publisher is opt-in in the current Main
package. The default package still needs an equivalent non-dummy workflow.

Main and the database operator accept an omitted PostgreSQL password in the
Original's `DISPATCHARR_ENV=modular` and `POSTGRES_SSL=true` configuration.
Explicit passwords and database URLs continue to take precedence. The local
Original entrypoint controls and SQLx parser comparison exercise this
configuration. A packaged connection to PostgreSQL using certificate-only
authentication remains pending.

Formatting and strict all-target Store and application Clippy passed. The
Store suite passed all 530 default cases, with 79 external-fixture cases
ignored. Main and the operator each passed seven configuration tests.

[Focused results](../scripts/rust-integration/milestone-batch85/focused.json)
· [Milestone ledger](rust-milestones.md)
