# f38 AMD64 signed live CPU, memory and first-byte results

All nine signed live MPEG-TS pairs passed. The measurement controller, owner and independent audit each exited 0, and the isolated engine was empty before and after. The run used the exact published ordinary f38 package and pinned Original Dispatcharr image. Six signed 1/10/100-viewer functional windows and three fresh Original-only qualification windows passed before paired measurement.

Each of the 18 paired windows used 15 seconds of warmup and 60 seconds of measurement. Resource figures cover the active backend, common PostgreSQL/Redis and deterministic origin. CPU is the mean percent of one logical CPU across those service groups. Memory is the peak sampled simultaneous working set across them. Delivered throughput uses measured bytes divided by 60 seconds. First byte is the unfloored nearest-rank p95, then the median of the three window p95s per backend. The audit validated 1,062 resource samples.

| Viewers | Mean CPU Original → Rust | Peak working set Original → Rust | Delivered MB/s Original → Rust | Raw first-byte p95 Original → Rust |
| ---: | ---: | ---: | ---: | ---: |
| 1 | 2.75% → 1.66% | 457.73 → 227.27 MiB | 0.2473 → 0.2473 | 18.75 → 3.85 ms |
| 10 | 4.37% → 1.74% | 482.36 → 267.42 MiB | 2.5032 → 2.4734 | 140.00 → 16.37 ms |
| 100 | 19.32% → 2.37% | 405.91 → 240.72 MiB | 25.1561 → 24.7339 | 1,338.26 → 100.19 ms |

Median CPU reductions calculated across corresponding pairs were 38.52%, 60.66% and 87.81% at 1, 10 and 100 viewers. Median paired working-set reductions were 50.10%, 44.62% and 47.55%.

| Required in every pair | Worst observed ratio | Threshold |
| --- | ---: | ---: |
| Rust CPU / Original CPU | 0.6155 | ≤ 0.90 |
| Rust working memory / Original working memory | 0.5930 | ≤ 0.95 |
| Rust delivered bytes / Original delivered bytes | 0.9831 | ≥ 0.95 |
| Rust first-byte p95 / Original first-byte p95, each floored at 50 ms | 1.0000 | ≤ 1.10 |

The delivery gate required a single continuous signed request per viewer, 225,000–275,000 bytes per second per reader, a decodable 2 MiB A/V prefix and at least 25 decoded video frames, and no reader errors or OOM events. Each window required at least 50 resource samples with bounded edge coverage and cadence. The Original backend also stayed within its CPU, throttle and working-memory headroom limits. Each pair retained the same infrastructure and origin identities and verified one active upstream and complete cleanup. The JSON records the exact thresholds, per-arm samples, nine individual pairs, and each unfloored first-byte p95.

The first-byte acceptance applies the 50 ms floor to each arm. Raw latencies remain in the table and receipts, including the below-floor one-viewer results. This experiment covers signed HTTP MPEG-TS on the controlled AMD64 fixture. Formal M1 acceptance, the Original profile process-retirement blocker and ARM64 S120/S360/S7200 remain separate.

Original Dispatcharr authors, contributors and dependencies retain full credit. See ATTRIBUTION.md and LICENSE.
