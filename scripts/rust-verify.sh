#!/bin/sh
set -eu
export CARGO_TERM_COLOR=never
export RUSTUP_TOOLCHAIN=1.90.0
# Precreate the target root for rootless Docker overlay filesystems.
mkdir -p "${CARGO_TARGET_DIR:-rust/target}"
cargo fmt --manifest-path rust/Cargo.toml --all -- --check
cargo clippy --manifest-path rust/Cargo.toml --locked --workspace --all-targets -- -D warnings
cargo test --manifest-path rust/Cargo.toml --locked --workspace --all-targets
python3 -m unittest discover -s scripts/rust-perf -v
python3 -m unittest discover -s scripts/rust-contracts -v
python3 -m unittest discover -s scripts/rust-parity -v
python3 scripts/rust-parity/ts_discontinuity.py
evidence_dir=${EVIDENCE_DIR:-artifacts/rust}
mkdir -p "$evidence_dir"
rustc --version > "$evidence_dir/rustc.txt"
cargo run --manifest-path rust/Cargo.toml --locked --release \
    -p dispatcharr-stream-core --example fanout_bench -- 10000 65536 \
    > "$evidence_dir/stream-core-fanout.jsonl"
python3 scripts/rust-parity/benchmark.py --output "$evidence_dir/ts-helper-comparison.json"
echo "Core verification passed. Full application parity and performance remain separate gates."
