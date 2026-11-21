# Shared Core daily jobs and backup replay

The exact Rust image at `a8ea333d922f9834afc561493fc6b104f5e2b5d3` passed paired Docker Compose
checks with `RUST_CORE_SCHEDULE_OWNER=rust`. One scheduler and one
queue receiver handled the ordinary saved account-expiration and Version
tasks alongside backup and cleanup. Account expiration updated the warning,
created the expired notice and emitted two update events. All four
scheduled Version HTTP 200 outcomes retained their expected notifications
and successful task results.

In the two owned scheduled backup interruption replays, Redis and
PostgreSQL agreed with the pinned Original traces. An interruption before
Redis accepted the task led to one successful job after restart. An
interruption after acceptance and before the SQL tick committed led to two
distinct successful jobs overall. Both recorded one durable tick and
retired all owned resources. The comparison establishes at-least-once
replay for these two measured windows.

Core startup preserves the saved scheduler time when the enabled task,
calendar and retention value already match. The earlier exact Rust run
lost the replay because startup reset that time. Those failure receipts
remain part of the evidence.

The Rust workspace passed formatting, strict Clippy and 1,553 tests,
with 814 external fixture tests ignored there. Default worker
ownership, other backup interruption points, restore and full comparative
performance remain open. [Exact receipts](../scripts/rust-integration/m6-m8-shared-daily-jobs-and-backup-replay.json)

The original Dispatcharr, Django, Celery, Kombu, Redis, PostgreSQL and
other dependency authors retain full credit.
