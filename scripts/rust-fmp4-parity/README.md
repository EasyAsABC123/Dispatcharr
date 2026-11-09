# fMP4 framing differential checks

The original Dispatcharr authors and contributors receive full application
credit; see [ATTRIBUTION.md](../../ATTRIBUTION.md).

```sh
cargo build --manifest-path rust/Cargo.toml -p dispatcharr-fmp4-core --example framing_parity --offline
python scripts/rust-fmp4-parity/run.py --output artifacts/rust/fmp4-framing.json
```

Optional `--fixture FILE.mp4` arguments exercise actual fragmented MP4 captures.
The runner executes unchanged upstream `_find_moof_offset` and
`_flush_complete_fragments` method ASTs with an output-buffer recorder. It compares
exact initialization/fragment bytes through multiple partial-read widths.
The checked-in summary records 582 successful comparisons including a captured
baseline H264/AAC stream. Oversized input and retained fragments fail closed in
Rust; those explicit bounds have separate unit tests. EOF tails remain byte-for-byte
compatible, including truncated tails, and need independent decoder validation.
These are component checks, not FFmpeg runtime or application parity evidence.
