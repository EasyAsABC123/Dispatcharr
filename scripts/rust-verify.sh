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
# These are the portable core checks. test_passive_binding.py belongs to the
# supported Original Django/DRF fixture (/probe and /inventory mounts); it is
# not executable in this dependency-free Rust verification image.
for suite in test_inventory.py test_register_evidence.py test_source_links.py; do
    python3 -m unittest discover -s scripts/rust-contracts -p "$suite" -v
done
printf '%s\n' 'Original Django/DRF passive-binding fixture is a separate Source gate; not run by core verification.'
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
