# Ten live viewers plus XMLTV on 0c5133b

The current ordinary Rust image used 54.3% less backend and infrastructure CPU and 78.2% less peak working memory than Original in one paired AMD64 run. Ten authenticated clients watched the same MPEG-TS stream while each backend completed a local XMLTV import, programme index build and series evaluation. Both arms used exactly one upstream connection and one provider slot. All ten client captures decoded H.264 video and AAC audio and progressed during the import.

| Measure | Original 1241b3f7 | Rust 0c5133b | Reduction |
| --- | ---: | ---: | ---: |
| Backend and infrastructure CPU, core-seconds | 3.106 | 1.421 | 54.3% |
| Backend and infrastructure peak working set, MiB | 872.0 | 190.1 | 78.2% |
| Whole fixture CPU, core-seconds | 4.764 | 2.493 | 47.7% |
| Whole fixture peak working set, MiB | 1,131.9 | 451.5 | 60.1% |

Each arm measured 61 fixed cgroup samples across 60 seconds after warmup. CPU was the cumulative usage delta. Peak working set used the same-tick cgroup totals after subtracting inactive file cache. The whole fixture includes the origin and observers. Both arms recorded zero swap and OOM. The controller, exact teardown and the proof each exited 0. The [aggregate receipt](../scripts/rust-integration/m2-0c5133b-live-xmltv-performance.json) includes the per-arm totals, timing bounds and digests of the physical artifacts.

The fixture used the ordinary package's explicit Selected/Legacy Beat profile. This single comparison measures that profile and workload at the exact revision. The [earlier 9f run](rust-m2-9f29dbe-live-xmltv-performance.md) keeps its own results. Broader application workloads, replication across pairs and full release performance remain open.

Original Dispatcharr, FFmpeg and dependency contributors retain full credit.
