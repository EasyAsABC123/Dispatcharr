# Shared Core scheduling for backup and backend cleanup

Docker Compose qualified Rust `d0a80aa585516ac81d583e6c5c80a0b50a6bd5be` with
`RUST_CORE_SCHEDULE_OWNER=rust`. The Rust arm used one Core Beat loop and
one Celery receiver with the scheduled-backup and backend-cleanup handlers.
The two comparisons used the same packaged Rust image and the pinned
Original.

An administrator enabled the daily backup, repeated the same save, and
restarted the app before the due minute. Both backends published one task,
advanced the saved run count once, and recorded SUCCESS for the matching
task ID. They produced a PostgreSQL format-2 archive, expired the older
archive, and passed disable and exact cleanup. The separate UTC backend
cleanup replay observed a single saved tick, SUCCESS/null, and exact
service retirement. The independent backup check verified retirement of two
Original and five Rust-side containers across the restart.

The first Compose runs stopped before Rust reached the task phase because the
fixtures expected Selected startup jobs. The corrected runs observed Core
startup activity directly. Workspace formatting and strict Clippy passed. The
full Rust workspace suite passed 1,547 tests, with 814 fixture tests ignored.

The build manifest is `46c80360812dc4f111bd0988093feb01168bf34c3b77dcea0baf0cc0c8542002`.
The backup proof SHA-256 is `a25b8b07bf7fbcc673ba7879b867d8deb6e422dfe32c74b10aced8fb1a9ae3d0`, and the
cleanup proof SHA-256 is `fcdf753bb96af1ac331c1df69f63c3e3624d3735248ba14465654fda035b1bc1`. Default activation, abrupt
interruption between SQL reservation and Redis publication, broader backup
operations, and comparative performance remain open.
[Redacted receipts](../scripts/rust-integration/m6-m8-d0a80aa5-shared-core-schedules.json)

Original Dispatcharr, Django, django-celery-beat, Celery, Kombu, and other
dependency authors retain full credit.
