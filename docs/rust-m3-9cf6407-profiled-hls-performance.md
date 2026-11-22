# Profiled HLS resource comparison on 9cf6407

Original Dispatcharr authors and contributors retain full application credit. See [upstream attribution](../ATTRIBUTION.md).

One profiled HLS client used the same controlled source in each Compose arm. Each arm polled the playlist 128 times, collected 129 cgroup samples, and read 30 complete segments. Segment hashes, durations, video frames and audio frames matched. Both arms used one upstream connection and passed exact cleanup.

| Measure | Original | Rust |
| --- | ---: | ---: |
| CPU core-seconds | 5.096553 | 3.443248 |
| Peak working memory | 1,272.6 MiB | 669.9 MiB |
| Goodput | 135,234 bytes/s | 135,234 bytes/s |
| Segment first-byte p95 | 2.36 ms | 2.85 ms |

Rust used 32.4% less CPU and 47.4% less peak working memory. Both p95 measurements fell below the 50 ms floor used by the scoped latency gate. This comparison covers a single pair on `9cf6407`. The full M3 and release performance gates remain open. [Machine-readable evidence](../scripts/rust-integration/m3-9cf6407-profiled-hls-performance.json)
