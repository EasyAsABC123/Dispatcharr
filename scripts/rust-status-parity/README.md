# Status projection parity

Full original application credit belongs to the Dispatcharr authors and
contributors; see [ATTRIBUTION.md](../../ATTRIBUTION.md).

```sh
cargo build --manifest-path rust/Cargo.toml -p dispatcharr-stream-http --example status_parity --offline
python scripts/rust-status-parity/run.py --output artifacts/rust/status-parity.json
```

The adapter executes the unchanged upstream `ChannelStatus` methods extracted
from their AST. Only dependencies are stubbed: deterministic Redis reads, time,
profile lookup, and no local Python stream manager. Rust receives identical
metadata, clients, time, and index. The 128 comparisons check exact JSON fields,
types, values, units, optional media fields, client lists, and display formatting.
Hashes bind the report to the reference, adapter, Rust projection, and binary.

This test models Rust's memory-backed chunks with no Redis payload keys. It does
not cover Python local-manager diagnostics, arbitrary corrupt legacy metadata,
Redis outages, ghost pruning, distributed status enumeration, or the full live
HTTP/React workflow. Those remain integration gates. It establishes no application
performance gain or milestone completion.
