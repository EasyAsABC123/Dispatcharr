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

The later committed scanner-child workspace `4fdfc1c4154f8f72ff5ccff5928ef02fe1617cfb`
now also passes a fresh complete Compose run, using the published normalized shell
bytes. [Exact execution and review](../scripts/rust-integration/current-workspace782/review.json)
bind image `sha256:c3745da228fcbc3f34ab80aa9e6b8538225fd145dc4f357177810a2f7874421f`
and 4,796 unchanged sealed Git files, including all 782 Rust inputs. Formatting,
strict all-target Clippy, 562 default Rust tests, 41 Python tests and 1,806 byte-exact
helper cases pass; 702 external-fixture Rust tests and one optional private Python
check are explicitly excluded. Initial build DNS retries recovered without source
or network-configuration changes. Runtime verification uses no external network.
The exact owned project has no remaining containers, networks or volumes. This
is a verification image, separate from the accepted image51 application package;
its component timing output does not establish new application performance gains.

## Combined scanner788 and Source-contract integration

Committed7e514c26f507ad1d1ed5d5df14ad6bdaa9dad695 was exported into a clean5085-file Git snapshot and passed full SDK Docker Compose verification. Formatting, strict all-target Clippy and566 default Rust tests passed, with703 fixture-dependent cases ignored. Portable Python checks ran171 cases:170 passed and one existing optional private-raw check skipped. The full154 core-contract checks include the55 partial Source assignments and exact task-reference closure. All5085 source files remained unchanged.

The immutable verification image is `sha256:47d19a1f0cca801cd22cb8b1ec61b079fbf34776258eeb39e40d37c1e923f5d3`. Root independently checked its identity and empty project containers/networks/volumes. [Verification, source hashes and cleanup](../scripts/rust-integration/current-scanner788/review.json) retain exact scope. Runtime verification used network mode none. This is SDK/component verification, with no application parity or new application performance claim. Image51 remains the separately qualified packaged application.

## Configuration-admission and95-Source integration

Committed98e447186af099dd17496539aec46312f6cf4f62 was exported into a clean5405-file Git snapshot and passed full SDK Docker Compose verification. Formatting, strict all-target Clippy and566 default Rust tests passed;703 fixture-dependent cases remained ignored in that default run. Portable Python ran259 cases:258 passed and the existing optional private-raw check skipped. Its242 core-contract cases cover the current95 partial HTTP/eight nonHTTP Source assignments. All5405 frozen files remained unchanged.

The immutable verification image is `sha256:66a7600d43d033691313a8620b32a73f2dad284587cd6028e951999b5605a020`. Root checked its identity and independently empty project resources. [Verification, hashes and cleanup](../scripts/rust-integration/current-hash788/review.json) retain the exact snapshot. Runtime verification used network mode none. The separately executed component fixtures are documented with their own source/ELF bindings. Application packaging and numerical application performance remain separate gates; no prior image gain transfers to this result.
