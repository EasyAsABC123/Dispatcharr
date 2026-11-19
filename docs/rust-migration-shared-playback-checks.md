# Redis write retry and shared playback checks

The original Dispatcharr, media and dependency authors retain full credit.

The M1 Compose check observed Redis reject a write before its script ran.
Redis and the local writer both stayed at index 1, and the existing reader
received no extra chunk. After ACL permission returned, the same writer
published index 2 to the same reader, who later reached EOF. The fixture
removed the exact ACL user, cache, GC and reader state. The joined H.264/AAC
case independently decoded 50 primary and 50 alternate video frames in
order and 194,560 audio samples. All physical processes and resources
retired. [M1 receipt](../scripts/rust-core-contracts/clean-write-and-switch/receipt.json)

The exact Candidate105 fMP4 fixture completed on both Original and Rust.
On each, a late viewer joined the existing origin, FFmpeg transform and
remux. The late viewer continued after the lead left, and each viewer
independently decoded H.264 and AAC. The original probe's timeout remains
recorded. Its process classifier missed the pinned Linux loader wrapper
used by both Rust media processes. The corrected fixture verifies their
actual identities and retirement. [fMP4 receipt](../scripts/rust-integration/milestone-batch105/fmp4-two-viewers.json)

The paired HLS screen collected the same 30 segments and 128 playlist
responses per backend. Both sides delivered 17,309,912 bytes with identical
segment hashes and decoded audio and video frame counts. Rust used 33.30%
less whole-fixture CPU and 47.18% less simultaneous peak working memory.
Goodput and segment first-byte latency passed the fixed bounds. This is one
ordinary profiled HLS workload and its teardown passed.
[HLS paired resource receipt](../scripts/rust-perf/candidate105-profiled-hls/paired-result.json)

The 100-viewer pilot passed on both backends. All six subsequent measured
phases passed individually, but the fixed windows captured unequal playable
work. The audio and video spreads exceeded the same one-batch tolerance,
so the controller exited 1 without an eligible resource comparison. The
reader ledger excluded reads that crossed the end of the window. A new
pilot anchors both backends to the same physical source packet.
[100-viewer failure evidence](../scripts/rust-perf/candidate105-viewing100/unequal-work.json)
