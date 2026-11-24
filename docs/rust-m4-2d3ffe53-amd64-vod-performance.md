# Current AMD64 VOD resource comparison

The ordinary Rust package at `2d3ffe53` and pinned Original `1241b3f7` passed one fresh functional pair and four alternating measured pairs on Beepboop. All ten 1 MiB captures had the same full SHA-256. Each strict eight-second decode returned 200 video frames, mapped audio and video, and produced no decoder error output. The independent check recomputed the comparison from the physical measurements and verified exact teardown for both owners. [Machine-readable evidence](../scripts/rust-integration/m4-2d3ffe53-amd64-vod-performance.json)

| Metric | Original | Rust | Rust change |
| --- | ---: | ---: | ---: |
| Median CPU core-seconds per phase | 1.276 | 1.198 | -5.91% |
| Median sampled peak working set | 767.0 MiB | 504.3 MiB | -34.29% |
| Median capture throughput | 279.624 kB/s | 279.613 kB/s | -0.0037% |
| First body byte, nearest-rank p95 | 266.46 ms | 258.62 ms | -2.94% |

CPU, working-set and throughput changes are medians of paired Rust-to-Original changes. The first-byte p95 is the largest of four observations per backend. The workload covers signed VOD prefix delivery and the associated stream/session retirement. The separate archive retirement and whole-service performance gates remain open.

Dispatcharr’s original authors and the authors of its dependencies retain full credit. [Attribution](../ATTRIBUTION.md)
