# Exact 7c604a6c Store host validation

The exact Rust `7c604a6c77fb791c8d4b3b1e30fc58148875f04e` Store library passed strict Clippy with Rust 1.90.0, its locked dependencies and ordinary feature selection. All 720 enabled Store library tests passed. Eighty ignored tests remain subject to their separate fixture requirements. The test binary was built from the same sealed 1,682 Rust files used in the ordinary AMD64 package.

The first run passed 711 tests and encountered nine `EPERM` errors at local loopback socket binds. The rerun verified both local TCP and UDP binds, then ran the same recorded binary and verified its digest again afterward. All 720 tests passed on the rerun. Both literal results are retained.

The changed-file formatting audit remains a failure on this revision. Fourteen of 17 changed Rust files were individually clean. Two files retained one existing formatting difference each. `epg_staging.rs` introduced two line-wrap changes relative to what the pinned formatter emits. The reviewed formatting-only patch passed an exact dry run and is outside this revision. Full Rust workspace lint and tests, the exact Original backend suite, and full M0 acceptance remain open. This host run did not measure performance.

The original Dispatcharr authors and contributors retain full credit. [Machine-readable evidence](../scripts/rust-integration/m0-7c604a6c-store-host-validation.json)
