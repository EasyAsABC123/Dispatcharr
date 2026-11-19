# Refresh-only guide publication and PostgreSQL defaults

The original Dispatcharr authors and contributors retain full application,
workflow and algorithm credit.

With `RUST_EPG_REFRESH_ENABLED=1`, the existing React guide actions can
publish XMLTV and source-less programme work through the refresh worker’s
configured broker and result endpoint. The publisher uses the existing
frame builder and waits for the worker’s queue observation. Shutdown closes
publication before draining the transport. The packaged fixture is scoped to
cached XML and the source-less task result. Normal deployment activation and
the broader download, Schedules Direct and selected-worker workloads remain open.

Blank `POSTGRES_DB`, `POSTGRES_USER` and `POSTGRES_PORT` settings now use the
pinned Original entrypoint’s defaults. The retained shell and SQLx controls
cover empty and explicit settings. A nine-case packaged operator comparison
will check the actual target and observable command output.

Formatting and strict all-target Clippy passed for the changed crates. The
locked checks passed 530 Store, 285 job-transport and eight configuration
tests each for Main and the operator. The first lint invocation reported the
readiness test module’s position. Moving it below the implementations cleared
lint and preserved the passing readiness regression.

[Focused results](../scripts/rust-integration/milestone-batch87/focused.json)
· [Milestone ledger](rust-milestones.md)
