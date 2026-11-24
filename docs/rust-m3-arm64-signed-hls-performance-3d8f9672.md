# Two-viewer signed HLS performance on ARM64

Lappy ran the same signed two-viewer HLS workload against pinned
Original `1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430` and the audited ordinary
Rust package at `3d8f96725ed90f5d37e406492e264e7c4d37f89a`. The current
functional replay verified one upstream, one marked profile, strict mapped
audio/video decoding and both normal stops in each backend.

The measured pair used 32-second windows with 33 samples per arm and
six ordered segments per viewer. Each arm performed the same media
work and retired exactly. The reported CPU and peak working memory
include the active application and scoped support services and Origin.

| Metric | Original | Rust | Scoped result |
| --- | ---: | ---: | ---: |
| CPU core-seconds | 2.880 | 2.005 | 30.4% lower |
| Peak working memory | 985,337,856 B | 244,895,744 B | 75.1% lower |
| Media goodput | 187,151 B/s | 187,161 B/s | 100.01% of Original |
| Raw segment first-byte p95 | 18.70 ms | 6.43 ms | 65.6% lower |

All four scoped thresholds passed. Both p95 observations were below
the common 50 ms acceptance floor, giving a gated latency ratio of 1.00.
The scope covers this signed HLS pair. The broader M3 workflow, internal
receiver identity and whole-milestone performance remain open.
[Redacted measured proof](rust-m3-arm64-signed-hls-performance-3d8f9672.json)

The original Dispatcharr, React, FFmpeg, VLC and upstream authors retain full credit.
