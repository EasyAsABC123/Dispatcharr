# Shared live viewing CPU and memory on ad6babd

With 100 concurrent viewers of one VLC MPEG-TS channel, Rust used
82.0–82.4% less CPU and
70.8–71.0% less peak working memory across three fresh pairs.
Both applications used one upstream connection and one provider slot per phase.
The six measured phases passed useful decoded audio and video, first-byte and
throughput budgets, zero swap, and exact cleanup.
[Machine-readable evidence](../scripts/rust-integration/m2-ad6babd-viewing100.json)

| Trial | Original CPU cores | Rust CPU cores | Original peak MiB | Rust peak MiB |
| --- | ---: | ---: | ---: | ---: |
| 1 | 0.1413 | 0.0254 | 951.3 | 277.9 |
| 2 | 0.1409 | 0.0248 | 953.6 | 277.9 |
| 3 | 0.1387 | 0.0249 | 951.7 | 276.4 |

The verified ordinary application is `ad6babd7641067a49ba33f749b8aa39cba1cbb79`. Each phase sampled the
same anchored 60-second window. The primary scope includes the backend,
infrastructure, and origin and tracks observation tools separately. The
peak is the maximum simultaneous estimate from cgroup working sets.
The measured window covers steady live viewing with idle background consumers.
Active jobs, Auto Sync latency, and release-wide performance have separate gates.

Original Dispatcharr and dependency authors retain full credit.
[Attribution](../ATTRIBUTION.md)
