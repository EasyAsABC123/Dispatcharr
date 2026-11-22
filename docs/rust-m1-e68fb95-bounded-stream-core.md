# Bounded streaming core checks on e68fb95

The original Dispatcharr authors and contributors retain full credit for the application and media algorithms. See [Attribution](../ATTRIBUTION.md).

The exact Rust source packaged as e68fb95 passed all 33 deterministic streaming library tests with Rust 1.90 and the existing shared offline Cargo target. The runner verified all 1,662 Rust input files before and after testing.

| Crate | Passed | Scope |
| --- | ---: | --- |
| `dispatcharr-stream-core` | 17 | TS synchronization, discontinuity, cursor positioning, shared payload ownership and retention |
| `dispatcharr-stream-runtime` | 16 | Cancellation, wakeups, held-frame backpressure and 100 concurrent readers |

The [receipt](../scripts/rust-integration/m1-e68fb95-bounded-stream-core.json) binds the test logs to the exact ordinary package. Four physically measured parent f960875 fixture rows are retained as historical references. The [ten-viewer Compose benchmark](rust-m2-e68fb95-live-xmltv-performance.md) supplies independent performance evidence on e68fb95. Formal M1 acceptance still depends on full M0 contract coverage.
