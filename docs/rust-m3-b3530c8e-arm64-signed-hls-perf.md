# B353 ARM64 paired two-viewer HLS measurement

The exact ordinary B353 package and pinned Original completed equivalent work in one quiet two-viewer signed HLS pair. Each arm collected 33 resource samples, delivered six segments per viewer, completed strict decodes, and retired both signed sessions and all owned resources. The independent replay reproduced the measured ratios and the unchanged acceptance checks. [Typed evidence](rust-m3-b3530c8e-arm64-signed-hls-perf.json)

| Metric | Original | Rust B353 | Change |
| --- | ---: | ---: | ---: |
| CPU core-seconds | 2.739465 | 1.861676 | −32.04% |
| Peak working memory | 913,801,216 B | 242,737,152 B | −73.44% |
| Delivered bytes/s | 187,209 | 187,152 | −0.03% |
| Raw p95 segment time to first byte | 20.416 ms | 7.792 ms | −61.84% |

The latency score applied the existing floor and was equal for the two arms. All scoped thresholds passed. The separate AMD64 HLS restart and full M3 gates remain open. Original Dispatcharr, FFmpeg, and upstream dependency authors retain full credit.
