# BE82 signed 6,000-entry M3U performance

The ordinary AMD64 BE82 package and the pinned Original returned byte-identical signed M3U playlists for 6,000 named-profile entries. The paired fixtures each ran cold, warm and expired-cache requests and delivered 10,476,474 useful bytes across three requests. Functionality, quiet measurement, independent proof and exact fixture retirement all passed.

| Measure | Original | Rust | Change |
| --- | ---: | ---: | ---: |
| CPU core-seconds | 1.804 | 0.854 | 52.6% lower |
| Peak cgroup charged memory | 473.8 MiB | 339.3 MiB | 28.4% lower |
| Peak summed process RSS | 578.4 MiB | 412.2 MiB | 28.7% lower |
| First-byte p95 | 217.85 ms | 44.97 ms | 79.4% lower |
| Completion p95 | 265.80 ms | 47.20 ms | 82.2% lower |
| Goodput | 2.67 MB/s | 3.56 MB/s | 33.2% higher |

All scoped comparison bars passed. This run covers the scaled M3U recipe, with three requests per arm, and leaves the wider outputs and realtime performance gate open.

Original Dispatcharr and its upstream contributors retain full authorship credit.
