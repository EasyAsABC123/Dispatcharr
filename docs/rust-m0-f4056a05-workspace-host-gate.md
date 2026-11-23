# M0 exact Rust workspace host validation

The exact `f4056a0520c39c652c1085d2527da3b7319c7017` workspace passed formatting, strict all-target Clippy and default all-target tests on Rust 1.90.0. The run verified 1,682 Rust files and five committed timeshift fixture files across all 30 workspace members before and after each phase. It used the locked offline dependencies and verified local loopback transport. Across 193 suites, 1,720 enabled tests passed and 821 fixture tests remained ignored.

Exact predecessor `7c604a6c` passed the same 1,720 enabled tests. Its formatter reported 35 hunks across seven files. Strict Clippy stopped at the VOD resolver authorization tuple’s `type_complexity` lint. The successor applies pinned Rustfmt to those seven files and names the same tuple in its existing VOD resolver signatures. This preserves the tuple representation and the authorization logic. React, the license and attribution files are byte-identical.

This verifies the full Rust host gate. The ordinary AMD64 and ARM64 Compose startup receipts qualify `7c604a6c`. Exact successor packages, the remaining application contract inventory and performance acceptance are separate M0 requirements. Original Dispatcharr, React and dependency authors retain full credit. [Machine-readable evidence](../scripts/rust-integration/m0-f4056a05-workspace-host-gate.json)
