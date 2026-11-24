# Signed VOD reconnect on current ARM64

Lappy ran the isolated Original and Rust VOD reconnect cases against the
exact ordinary package at `3d8f96725ed90f5d37e406492e264e7c4d37f89a`.
Both delivered two complete HTTP 200 MP4 reads under the same signed
client and server-issued session. Each read delivered 4,299,461 bytes
and independently decoded H.264 and AAC. Each arm made two sequential
Origin requests, released its provider slot after each EOF and retired
the VOD session after the second EOF. Each project retired seven
containers, one network and two volumes.

This is same-host functional reconnect evidence. The separate VOD
resource comparator, archive-retirement gate and broader M4 workflows
still require their applicable evidence.
[Redacted reconnect proof](rust-m4-arm64-vod-reconnect-3d8f9672.json)

The original Dispatcharr, React, FFmpeg and dependency authors retain full credit.
