# e765 ordinary channel-maintenance jobs

Pinned Original and exact Rust e7651a6d each ran four authenticated admin
requests in an isolated Compose namespace. EPG name, TVG-ID and logo updates
returned HTTP 200. Bulk channel creation from a stream returned HTTP 202.
Each response's task ID matched a successful Redis backend result and the
expected Channels progress.

Both backends changed only the seeded channel's requested field for each
update. The bulk request created one channel, its logo and its ordered stream
relationship. All five roles exited zero. Both broker queues and the
unacknowledged set drained. Exact container, process-generation and Compose
inventory checks passed.

The first attempt captured the four positive Original operations and exited
1. Its cleanup controller compared binary inventory rows with text container
identities, so the Native phase never started. The retry used the same image
and business cases with corrected inventory comparison.

This qualifies four ordinary jobs on the selected runtime. Stock activation,
broader task inputs, full M6 and performance remain open. Original Dispatcharr,
Celery, Django and PostgreSQL authors and contributors retain full credit.

[Redacted receipt](../scripts/rust-integration/milestone-batch113/ordinary-channel-tasks-e765.json)
