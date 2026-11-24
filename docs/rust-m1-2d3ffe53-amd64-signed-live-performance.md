# AMD64 paired performance for ordinary signed live MPEG-TS

Beepboop ran nine matched pairs on the exact Original backend at `1241b3f7` and the ordinary signed Rust backend at `2d3ffe53`. Each arm used 15 seconds of warmup and a 60-second measurement window. All 666 signed viewers stayed on one continuous request and passed strict audio and video decode. The origin had at most one active connection per case and zero after each case. The independent review verified 1,062 resource samples and exact project teardown.

| Concurrent viewers | Median paired CPU reduction | Median paired peak working-memory reduction | Lowest Rust/Original delivered-byte ratio | Median first body byte p95, Original → Rust |
| ---: | ---: | ---: | ---: | ---: |
| 1 | 38.5% | 49.9% | 98.31% | 18.33 ms → 3.85 ms |
| 10 | 57.7% | 44.6% | 98.31% | 150.02 ms → 19.10 ms |
| 100 | 87.4% | 47.6% | 99.97% | 1508.76 ms → 146.60 ms |

Resource accounting includes the active backend and common PostgreSQL, Redis, and local media origin. The latency column shows the median of three raw, nearest-rank p95 values per backend and viewer count. All nine pairs met the declared limits for CPU, peak working memory, delivered bytes, and the first-byte ratio with its 50 ms floor.

This fixture measures signed live MPEG-TS delivery. Whole-service acceptance and the separate full-scan performance comparison remain open. [Redacted aggregate and exact package binding](../scripts/rust-integration/m1-2d3ffe53-amd64-signed-live-performance.json)

Dispatcharr’s original authors and the authors of its dependencies retain full credit. [Attribution](../ATTRIBUTION.md)
