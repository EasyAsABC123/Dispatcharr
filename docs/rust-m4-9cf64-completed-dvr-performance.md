# Completed DVR delivery on Rust 9cf64

Original Dispatcharr authors and contributors retain full application, media, and UI credit. FFmpeg, PostgreSQL, Redis, and all other dependency authors retain full credit for their work. See [upstream attribution](../ATTRIBUTION.md).

The published `9cf6407` Rust image passed 18 completed-recording checks and 19 prerequisite checks in the paired Compose fixture. All six timing pairs then served the same full MKV and 196,608-byte closed seeks by status, byte count, and digest. Each phase warmed for 15 seconds and measured 60 seconds, with one full read and one seek per viewer every three seconds. Three pairs ran at each viewer count.

| Viewers | CPU, one-core percent | Peak working set, MiB | Goodput, MiB/s | Full MKV p95 total, ms | Seek p95 total, ms |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 0.720% → 0.435% (39.8% lower) | 367.3 → 138.4 (62.2% lower) | 1.07 each | 21.80 → 11.41 | 8.72 → 5.65 |
| 10 | 5.713% → 2.040% (64.2% lower) | 395.4 → 163.6 (58.7% lower) | 10.69 each | 121.88 → 68.42 | 30.09 → 25.32 |

Arrows show Original to Rust. Values are medians across three phases per backend. Percent reductions are medians of the three paired ratios. CPU and peak concurrent Docker working-set memory cover the backend, infrastructure, and origin. The JSON retains all six pairs, first-byte and total p95, per-service measurements, and the latency gate's configured floor. The functional owner, timing owner, and independent reducer recorded literal exit 0. Cleanup verified no remaining owned project resources.

This comparison covers delivery of an owned completed recording on this exact image. Active recordings, catch-up, and remaining M4 workflows have separate acceptance. [Machine-readable evidence](../scripts/rust-integration/m4-9cf64-completed-dvr-performance.json)
