# TS HLS segmenter component

This is a derivative port of the original Dispatcharr TS HLS segmenter and
playlist renderer. Full credit for the original algorithm, application, and test
expectations belongs to the upstream Dispatcharr authors and contributors; see
[ATTRIBUTION.md](../../../ATTRIBUTION.md).

The library uses Rust's standard library only. It packet-copies TS input into
keyframe-aligned HLS segments, learns PAT/PMT/video/audio PIDs, supports H.264,
HEVC, MPEG1/2, and audio-only inputs, and preserves original PTS wrap/reset,
open-GOP duration, fast-start, force-cut, parameter-cache injection, continuity
counter, and discontinuity semantics. Playlist rendering retains the existing
frozen target, discontinuity sequence, join-offset gate, and `{seq}` URI layout.

`TSSegmenter::feed` accepts arbitrary input chunk boundaries and returns finished
`Segment { data, duration, discontinuity }` values. Call `flag_discontinuity` on
known input failover or cursor loss. Like the Python reference, it closes a
measured pre-gap segment and tags the next playable segment. The parser does not
remux, transcode, spawn FFmpeg, validate PSI CRCs, or assemble multi-packet PMTs.
Incomplete PMTs are rejected as non-authoritative, matching the reference.

Additional explicit resource bounds reject oversized input calls or an open
segment that exceeds its byte limit. Defaults are 4 MiB/input call and 32 MiB/open
segment. On segment overflow, partial media is discarded and the next keyframe
starts a discontinuous segment; callers receive an error and must handle it.
Those safety limits intentionally differ from the reference's unbounded malformed
or untimed input behavior. Allocator/runtime overhead is separate from payload
limits. Segment storage and HTTP frames need their own global byte budgets.

Verification:

```sh
cargo test --manifest-path rust/Cargo.toml -p dispatcharr-hls-core --locked
python3 scripts/rust-hls-parity/verify.py --output /tmp/hls-parity.json
python3 scripts/rust-hls-parity/decode.py --output /tmp/hls-decode-new
```

The 13 Rust tests port original expectations and add resource bounds. The adapter
runs all 40 original Python tests unchanged, comparing every parser/segment/
playlist call and open-segment state against the actual hash-pinned reference.
Generated sequences and malformed packet cases expand the comparison. Valid
MPEG2/H.264/HEVC/AAC fixtures also matched segment bytes, timing, and flags; all
22 generated segment files decoded independently with pinned Docker FFmpeg.
See [component evidence](../../../scripts/rust-hls-parity/verified-component.json).

This is not yet an HLS HTTP/capability/session implementation and does not pass
M3 by itself. No full-application performance gain is established by these tests.
