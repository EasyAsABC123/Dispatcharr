# Connect collection and Redis configuration

The original Dispatcharr authors and contributors retain full application,
workflow and algorithm credit.

The existing Integrations page can request its collection through Native
Main. The read uses the existing admin authorization and returns the stored
integrations and nested subscriptions, including their original
configuration JSON. The optional paired Compose suite checks the normal
JSON GET, empty and populated collections, caller roles, API keys and
authentication precedence. The scope is the Integrations collection read.

Main and the packaged operator now share one Redis configuration resolver.
An explicit URL takes precedence. When a deployment uses the discrete
`REDIS_HOST`, `REDIS_PORT`, `REDIS_DB`, `REDIS_USER` and
`REDIS_PASSWORD` variables, the same resolved address feeds Main's
components and the operator. This discrete configuration currently covers
TCP Redis. A discrete TLS request requires an explicit URL.

Ten focused Rust tests, formatting, strict all-target Clippy on the app
and Store, and the fixture syntax check passed. The exact-image Compose
checks remain open.

[Focused checks](../scripts/rust-integration/milestone-batch78/focused.json)
· [Milestone ledger](rust-milestones.md)
