# Original HLS component parity and decoder evidence

Full credit belongs to the original Dispatcharr authors and contributors. This
tool verifies a derivative Rust port against their unchanged algorithm and tests;
it does not claim the original work as new. See [ATTRIBUTION.md](../../ATTRIBUTION.md).

From the repository root, with the pinned Rust 1.90.0 toolchain:

```sh
python3 scripts/rust-hls-parity/verify.py --output /tmp/hls-parity.json
python3 scripts/rust-hls-parity/decode.py --output /tmp/hls-decode-new
```

`verify.py` imports the actual original dependency-free HLS segmenter and runs its
40 test methods unchanged against a Rust RPC adapter. Each operation also runs
against the original Python implementation and compares emitted bytes, duration,
discontinuity, parser output, playlist text, and current-segment bytes/state.
The original source and original test file are SHA256-pinned. Django is not
imported; only package-name stubs supply the adapter to the original tests.
Twelve generated codec/fragmentation/reset sequences plus 1,000 seeded random
packet parser cases add coverage. Use `--seed` for reproducible expansion.

`decode.py` requires Docker and a fresh empty output directory. It uses the
digest-pinned baseline FFmpeg image, generates MPEG2/H.264/HEVC/AAC fixtures, runs
the Rust `segment_file` component helper, and independently decodes every emitted
segment with `-xerror`. Video requires decoded frames; audio requires timed output.
All container traffic is disabled, and fixture files are local disposable data.
The runner records fixture/segment hashes, durations, decoder progress, and logs.
An interrupted command removes only its uniquely named test container.

Compare the generated valid fixtures to the Python reference too:

```sh
python3 scripts/rust-hls-parity/verify.py --output /tmp/hls-real-parity.json \
  --fixture /tmp/hls-decode-new/mpeg2/input.ts \
  --fixture /tmp/hls-decode-new/h264/input.ts \
  --fixture /tmp/hls-decode-new/hevc/input.ts \
  --fixture /tmp/hls-decode-new/aac/input.ts
```

[Verified component evidence](verified-component.json) records 2,145 byte-exact
RPC comparisons including those four real fixtures, and 22 independently decoded
segments. These establish component behavior, not the HLS capability URL,
session, Redis storage, rolling retention, cold-start request, decoder behavior
through live HTTP, or full-application CPU/memory gates required by M3.
