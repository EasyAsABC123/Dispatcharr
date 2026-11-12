# Current workspace verification repair

The original 20-test core Compose result remains valid historical evidence. The
current check is separately source-bound in
[`verified-current-workspace-verification-1.json`](../scripts/rust-integration/verified-current-workspace-verification-1.json).
Original Dispatcharr and dependency authors retain credit and licensing.

A frozen qualified 779-input workspace was formatted with pinned Rust 1.90.0.
Exactly 79 physical Rust files changed; independent formatting of a second copy
of all 562 original Rust files produces identical outputs. The earlier audit's
81 path strings included two noncanonical aliases of `response_headers.rs`.
All 215 non-Rust fixture inputs remained unchanged. There were no manual semantic
Rust changes. Formatting reorders modules/imports and adds optional trailing
commas; this is not described as a whitespace-only textual diff.

The verifier now copies five required timeshift golden files and the portable
register tests' exact public authorities, with matching Docker ignore rules.
The portable contract modules are explicitly inventory, register-evidence and
source-links. `test_passive_binding.py` remains a separate supported-Original
Django/DRF fixture requiring `/probe` and `/inventory` mounts. It was not imported
as a dependency-free Rust test, silently skipped, installed into the SDK, replaced
or rerun. The previous discovery/import failure remains recorded. The optional
private raw register test is independently declared and skipped by default.

The first full Compose attempt passed Rust checks but failed a stale Python
warmup test mock rejecting `physical_close`. Only the mock signature was corrected,
with an assertion that ordinary execution supplies false; `perf.py` is unchanged.
The failed attempt and sandbox-denied host loopback attempts remain retained.

Host formatting, strict all-target workspace Clippy and default tests passed.
The actual network-disabled verification container also passed formatting,
strict Clippy, 560 default Rust tests (701 external-fixture tests ignored), 42
Python tests discovered (41 passed, one optional retained-raw test skipped),
1,806 byte-exact helper cases and the original component checks. Component
microbenchmark output is not an application performance result. The exact image,
archive, all public input hashes, logs and independent empty owned-project
inventories are bound in the summary. All sealed inputs stayed unchanged.

From a checkout containing this reviewed selection, load the pinned host toolchain
and run the existing commands:

```sh
export RUSTUP_TOOLCHAIN=1.90.0
cargo fmt --manifest-path rust/Cargo.toml --all -- --check
cargo clippy --manifest-path rust/Cargo.toml --locked --workspace --all-targets -- -D warnings
cargo test --manifest-path rust/Cargo.toml --locked --workspace --all-targets
python3 scripts/verify-rust-compose.py --project dispatcharr-rust-check-unique --evidence-dir /absolute/new-empty-evidence
```

Docker must be reachable; initial pinned image/dependency fetches require network.
The verification service itself uses `network_mode: none` and runs local loopback
fixtures. Host sandbox restrictions may require authorized loopback-capable test
execution. Use a fresh project/output and inspect actual exit status, source maps,
all selected checks and independent cleanup. No ignored external fixture becomes
passed merely because default verification passed.

This repairs the existing M0 tooling criterion for the exact tested source. Root
must integrate/recheck later source changes deliberately and decides formal M0/M1
status separately. Source budget negatives, the Source2 numeric/reconciliation
packets and the historical bootstrap remain unchanged; no Source workload or new
application performance run was used for this repair.

Root independently matched all 835 public execution input hashes to the reviewed
Git selection and all 24 retained evidence hashes. Publication removes one trailing
ASCII space from the verifier shell script after the measured execution: executed
SHA-256 `312722bf6a5ceb3e4e4d3815951e7d16b2607cc0d3ef8115416fba0b00cb90fb`;
published SHA-256 `342c61c4a2704afe9bf30fbe9230f161d4ed3cb91a08b944f9228f0c0cdf68c3`.
Shell tokens are unchanged and `sh -n` passed; the new byte hash is not represented
as the image-executed input. The frozen execution proof retains its original map.
