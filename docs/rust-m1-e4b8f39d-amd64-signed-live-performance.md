# AMD64 paired performance for signed live MPEG-TS

The exact Original package at `1241b3f7` and the exact Rust package at `e4b8f39d` passed nine alternating pairs on Beepboop. Each arm used 15 seconds of warmup and a 60-second measured window. All 666 signed readers remained on one continuous HTTP request and decoded successfully. Each case used one concurrent upstream connection. The owner observed zero active upstream connections after each case and verified exact teardown for 21 containers and the associated project resources.

| Viewers | Median CPU reduction | Median peak working-memory reduction | Minimum Rust/Original delivered bytes | Median raw p95 to first body byte, Original → Rust |
| ---: | ---: | ---: | ---: | ---: |
| 1 | 40.1% | 50.4% | 98.3% | 17.46 ms → 3.97 ms |
| 10 | 61.7% | 44.9% | 98.0% | 135.65 ms → 18.20 ms |
| 100 | 88.1% | 47.5% | 98.3% | 1330.21 ms → 96.28 ms |

Resource accounting includes the application and common PostgreSQL, Redis and synthetic origin. CPU and memory reductions are medians of paired ratios. The memory metric is peak sampled cgroup working memory for the group. The first-byte values in the table are raw nearest-rank p95 values, with the median taken over the three trials for each backend. The acceptance calculation uses its predeclared 50 ms floor for the latency ratio. Minimum delivered-byte ratios stayed above 95% at all viewer counts. All 18 measured phases, 1,062 resource samples and the independent recheck passed.

The [evidence receipt](../scripts/rust-integration/m1-e4b8f39d-amd64-signed-live-performance.json) contains every pair ratio, exact package, benchmark protocol, and fixed thresholds. The result covers this signed live workload. The separate full-scan comparison and broader milestone gates remain open.

Original Dispatcharr authors and contributors retain full project credit.
