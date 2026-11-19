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
and Store, and the fixture syntax check passed.

Candidate78 matched all eight scoped Integrations responses against Original.
The same controller retained an independent TS 404 body mismatch and
exited 1. The older-image control reproduced eight missing-route responses.
Both Compose projects were independently removed.

The packaged operator passed 17 cases in each Redis configuration mode.
The continuation reused the successful discrete-configuration operator
result. Authenticated Main used the selected Redis cache in both modes,
including explicit URL precedence over conflicting discrete settings.
Both projects passed cleanup. The earlier fixture setup failures remain
in the evidence.

[Connect scope and initial Redis evidence](../scripts/rust-integration/milestone-batch78/packaged-scopes.json)
· [Completed Redis comparisons](../scripts/rust-integration/milestone-batch78/redis-packaged.json)

[Focused checks](../scripts/rust-integration/milestone-batch78/focused.json)
· [Milestone ledger](rust-milestones.md)
