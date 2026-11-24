# AMD64 VOD resource comparison

The frozen `3d8f9672` Rust package and the pinned Original ran four
alternating matched VOD pairs on Beepboop. Each arm performed the same
authenticated 1 MiB capture and a separate strict eight-second H.264/AAC
decode with 200 video frames. The runner also checked the WebSocket
lifecycle and the five-second VOD Origin retirement gate.

| Measurement | Rust compared with Original |
| --- | ---: |
| Median pair CPU core-seconds | 6.3% lower |
| Median pair peak sampled working memory | 33.3% lower |
| Median pair capture goodput | Within 0.001% |
| First-body-byte latency p95 | 263.75 ms vs. 266.42 ms |

The latency p95 uses the nearest-rank calculation from four observations
per backend. The measurement includes the backend, decoder infrastructure
and Origin under identical role limits for both arms. All eight windows
passed the complete useful-work and resource checks. The independent
reducer reproduced the reported values and verified exact teardown.

This workload measures VOD prefix capture and the associated decode.
Sustained VOD, transformed timeshift and the separate Original archive
retirement case remain outside this benchmark.

Dispatcharr and React remain the work of the original authors and
contributors. FFmpeg and all other dependencies retain full authorship
and license credit.
[Machine-readable proof](rust-m4-amd64-vod-resource-3d8f9672.json)
