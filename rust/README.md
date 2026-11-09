# Dispatcharr Rust backend port

This is an in-progress derivative port of
[Dispatcharr](https://github.com/Dispatcharr/Dispatcharr). Full credit for the
original application belongs to its original authors and contributors; see
[ATTRIBUTION.md](../ATTRIBUTION.md). The upstream history and AGPL license are retained.

The React frontend stays. This workspace currently contains streaming components,
not a replacement HTTP backend. Follow the [milestone ledger](../docs/rust-milestones.md)
and [Compose verification guide](../docs/rust-local-verification.md).

* `stream-core`: bounded shared media retention, TS reassembly, and legacy-compatible discontinuity handling.
* `stream-runtime`: asynchronous bounded fanout, viewer limits, cancellation, expiry, and shutdown.

The [verification summary](../docs/rust-verification-evidence.json) distinguishes
component measurements from application performance. No whole-application CPU
or memory improvement has been established yet.
