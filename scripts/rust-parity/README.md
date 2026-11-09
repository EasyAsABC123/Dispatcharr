# Streaming core differential checks

Run from the repository root with Rust/Cargo and Python 3.9 or newer:

```sh
python3 scripts/rust-parity/ts_discontinuity.py
cargo test --manifest-path rust/Cargo.toml -p dispatcharr-stream-core
```

The Python script imports the existing `ts_discontinuity.py` source directly.
It shims only the package hierarchy and `TS_PACKET_SIZE=188` constant, avoiding
Django startup. Its default seeded corpus compares 1,806 responses byte for byte:
all adaptation-control layouts, invalid/empty adaptation lengths, existing flags,
null PIDs, invalid sync bytes, malformed packet lengths, randomized packets, and
repeated PIDs across batches and source-switch resets. Override `--seed` to expand
coverage reproducibly. Any mismatch or Rust execution failure exits nonzero.

The Rust `ts_parity` example is a test protocol adapter; production transport does
not use its hex encoding. Rust's packet assembler is deliberately packet-boundary
preserving, matching the current Python buffer; it does not search for sync bytes
after arbitrary corrupt input. Discontinuity insertion on payload-only packets
retains the legacy behavior of dropping the last two payload bytes. This check
proves fidelity to that helper, not decoder correctness or full playback parity.

For a local, synchronous buffer microbenchmark:

```sh
cargo run --manifest-path rust/Cargo.toml --release \
  -p dispatcharr-stream-core --example fanout_bench -- 10000 65536
```

It emits JSONL for 1, 10, and 100 viewers, three samples per scenario following
warmup. Inputs are generated deterministically. Each write allocates a new payload;
viewers only clone shared references and read one byte for a checksum. This is
not a network-throughput test, CPU profile, full memory measurement, or comparison
against the Python backend. `retained_payload_bytes` excludes in-flight transport
references and metadata. Use the Docker Compose application harness to establish
end-to-end performance gains.
