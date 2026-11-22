# Shared Core plugin refresh at startup

The ordinary Linux AMD64 image for Rust application `34860b9a` ran a paired
Original and Rust startup test. The Rust test explicitly selected
`RUST_CORE_SCHEDULE_OWNER=rust`, `RUST_CORE_PLUGIN_REFRESH_OWNER=rust`, and
`RUST_CORE_PLUGIN_STARTUP_OWNER=rust`. The startup task uses the existing
Core receiver and Redis transport.

Both backends published one delayed refresh. The broker held the same
envelope until it was due, the worker acknowledged the held delivery, and
the result stored under the same task ID was `SUCCESS` with JSON `null`.
Each backend fetched the owned manifest once and persisted it. The queues
and unacknowledged entries settled. There was no saved periodic schedule in
this startup-only pair.

A separate run on the same Rust image enabled the Core and plugin refresh
owners and left the startup selector absent. An observer connected before
startup and saw zero Celery or DVR pushes through readiness. The six-hour
saved schedule subsequently ran once on the ordinary Celery queue, recorded
one durable tick, and settled with `SUCCESS` and JSON `null`. Unrelated
repository rows stayed unchanged.

All three trial lanes verified exact retirement. Build, physical execution and
independent proof checks passed. This evidence covers plugin refresh startup
and the saved-only control. Full M6 acceptance and resource measurements
remain open. The original Dispatcharr and framework contributors retain full
credit. [Scoped receipts](../scripts/rust-integration/m6-34860-core-plugin-startup-eta.json)
