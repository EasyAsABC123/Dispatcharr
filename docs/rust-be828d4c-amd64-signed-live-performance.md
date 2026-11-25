# BE82 signed Live playback performance

Nine matched Docker Compose trials passed on AMD64, with three Original/Rust pairs at each viewer count. Each timed arm ran for 60 seconds after a 15-second warm-up. Every signed viewer used one request and supplied a decodable 2 MiB prefix. Six functional windows and three fresh Original qualifiers passed before measurement.

| Viewers | CPU change | Peak working memory change | Raw first-byte p95, Original → Rust | Goodput change |
| ---: | ---: | ---: | ---: | ---: |
| 1 | -39.52% | -50.13% | 18.07 → 4.11 ms | +0.00% |
| 10 | -58.80% | -44.50% | 139.05 → 16.90 ms | -1.86% |
| 100 | -88.00% | -61.22% | 1289.95 → 106.04 ms | -1.68% |

CPU, peak working memory, and goodput changes are medians of within-pair ratios. Memory is the peak simultaneous working set summed across the measured roles. The latency column shows raw p95 medians. The gate applies its 50 ms comparison floor. All 18 arms, the independent replay, and the exact Docker inventory checks passed.

The [typed results](rust-be828d4c-amd64-signed-live-performance.json) retain per-arm workload evidence summaries, delivery rates, and raw result hashes. Full Core acceptance and release-wide performance remain open.

Original Dispatcharr, FFmpeg, and all upstream contributors retain full authorship and license credit.
