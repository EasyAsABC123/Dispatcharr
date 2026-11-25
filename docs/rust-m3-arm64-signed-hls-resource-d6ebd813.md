# ARM64 signed HLS resource comparison on D6

One quiet, paired Docker Desktop Linux ARM64 run compared the published
`d6ebd813` Rust package with pinned Original `1241b3f7`. Each backend served
two concurrent signed viewers for the same 32 polls and six segments per
viewer. The independent proof verified equal media and decodes, two normal
session stops, both playlists retired, and exact cleanup.

| Measurement | Original | Rust | Change |
| --- | ---: | ---: | ---: |
| CPU core-seconds | 2.5933 | 2.0183 | 22.2% lower |
| Peak working memory | 949.7 MiB | 236.8 MiB | 75.1% lower |
| Delivered goodput | 187,174 B/s | 187,129 B/s | 0.024% lower |
| Raw segment first-byte p95 | 15.61 ms | 10.25 ms | 34.3% lower |

The resource boundary includes the active serving backend, PostgreSQL,
Redis and the deterministic Origin. The observer recorded 33 samples in
each arm. Both raw p95 values fell below the predeclared 50 ms floor, so
the latency acceptance ratio was 1.00 and passed its 1.10 limit. CPU,
working memory and goodput also passed their unchanged limits.

Preflight, the physical owner and the independent proof each exited zero.
The owner verified an empty running engine after the exact teardown.
This measurement covers one signed HLS pair. Full M3 and release
performance remain separate gates.

Original Dispatcharr, React, FFmpeg and upstream authors retain full
authorship and license credit. [Machine-readable evidence](rust-m3-arm64-signed-hls-resource-d6ebd813.json)
