# Current ARM64 signed HLS performance

Lappy ran one quiet Original/Rust pair on pinned Original `1241b3f7` and the ordinary `2d3ffe53` ARM64 Rust package. Two signed viewers per arm shared one upstream. Both watched identical media, decoded it successfully, stopped normally and observed the retired playlists. Each 32-second arm collected 33 resource samples. All five owner phases and the independent proof exited zero. [Exact paired receipt](../scripts/rust-integration/m3-2d3ffe53-arm64-signed-hls-performance.json)

| Metric | Original | Rust | Rust change |
| --- | ---: | ---: | ---: |
| Total scoped CPU core-seconds | 2.974 | 2.285 | 23.2% lower |
| Peak sampled working memory | 946.9 MiB | 237.1 MiB | 75.0% lower |
| Delivered bytes per second | 187,180 | 187,166 | 0.007% lower |
| Raw segment first-byte p95 | 12.68 ms | 7.37 ms | 41.9% lower |

All four predeclared limits passed. The latency acceptance calculation uses a 50 ms floor, so both arms had the same floored latency value. Resource accounting covers the fixture application and support roles. This single pair supplies the scoped HLS comparison. The longer whole-service memory and soak gates remain open.

Dispatcharr’s original authors and the authors of its dependencies retain full credit. [Attribution](../ATTRIBUTION.md)
