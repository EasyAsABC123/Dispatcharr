# BE82 paired HLS and fMP4 resource screens

The 32-second profiled HLS pair used two signed viewers per arm. Both viewers completed the same measured playlists, segments, and decode work. Each arm used one upstream lifetime and one video transform. The independent proof and full engine inventory checks exited 0.

| HLS measurement | Original | Rust |
| --- | ---: | ---: |
| CPU across measured roles | 1.445 core-seconds | 0.846 core-seconds |
| Peak working memory across measured roles | 1,249.2 MiB | 666.3 MiB |
| Raw segment first-byte p95 | 21.05 ms | 3.44 ms |

Rust used 41.5% less CPU and 46.7% less peak working memory. Goodput was essentially unchanged. Both first-byte measurements fell below the benchmark’s 50 ms comparison floor.

The profiled fMP4 pair failed the same timed-work verifier in both arms. Each arm returned four successful signed stats responses and cleaned up exactly. The retained stream bytes failed the measured-window boundary assertion, so the fixture did not qualify a performance comparison.

The [typed evidence](rust-be828d4c-m3-resource-screens.json) retains both outcomes. Broader fanout, failover, and whole-service performance remain open.

Original Dispatcharr, FFmpeg, and all upstream contributors retain full authorship and license credit.
