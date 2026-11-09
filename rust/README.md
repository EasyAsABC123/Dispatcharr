# Dispatcharr Rust backend port

This is an in-progress derivative port of
[Dispatcharr](https://github.com/Dispatcharr/Dispatcharr). Full credit for the
original application belongs to its original authors and contributors; see
[ATTRIBUTION.md](../ATTRIBUTION.md). The upstream history and AGPL license are retained.

The React frontend stays. This workspace contains an executable partial backend
and streaming components. Full backend replacement remains in progress. Follow the [milestone ledger](../docs/rust-milestones.md)
and [Compose verification guide](../docs/rust-local-verification.md).

* `stream-core`: bounded shared media retention, TS reassembly, and legacy-compatible discontinuity handling.
* `stream-runtime`: asynchronous bounded fanout, viewer limits, cancellation, expiry, and shutdown.
* `contracts`: playback and authorization boundaries shared by the ports.
* `coordination`: fenced Redis channel ownership and provider reservations.
* `clients`: atomic viewer admission, legacy metadata, batched telemetry, and HLS capabilities.
* `stream-http`: shared native TS ingestion, ordered failover, HTTP delivery, and controls.
* `hls-core` and `hls-http`: segmenter and bounded capability-session transport.
* `store`: PostgreSQL reads, Django-compatible authentication, and account APIs.
* `app`: HTTP composition, readiness, and optional existing React static files.

The real-schema [Compose probe](../scripts/rust-integration/README.md) exercises
implemented contracts against the original backend. Native VOD work is being
integrated separately; timeshift and the remaining backend subsystems remain
open milestone gates.

The [verification summary](../docs/rust-verification-evidence.json) distinguishes
component measurements from application performance. No whole-application CPU
or memory improvement has been established yet.
