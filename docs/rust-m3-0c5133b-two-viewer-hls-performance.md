# M3 paired AMD64 two-viewer HLS resource screen

Original Dispatcharr and ordinary Rust application `0c5133b` served two
viewers from one upstream connection and one output-profile transform per
arm. Both viewers independently decoded six matching segments, sequences
3 through 8. Both stop calls returned 204 in each arm. The retired
capabilities returned 410. Both Compose arms retired their exact process
and container generations.

| Metric | Original | Rust | Change |
| --- | ---: | ---: | ---: |
| Whole-fixture CPU | 1.455 core-seconds | 0.712 core-seconds | 51.1% lower |
| Peak concurrent working memory | 1,312,174,080 bytes | 680,796,160 bytes | 48.1% lower |
| Goodput | 187,154 bytes/s | 187,154 bytes/s | Effectively equal |
| Segment first-byte p95 | 5.17 ms | 3.36 ms | Both below the 50 ms comparison floor |

Each arm ran on the same quiet AMD64 host for 32 seconds and collected
33 resource samples. CPU and memory include the application, its
infrastructure, and the owned synthetic origin. This was one paired
screening run. Full M3 performance and release acceptance remain open.

Original Dispatcharr, its React frontend, and its dependencies retain
their existing attribution and licenses.

[Machine-readable receipt](../scripts/rust-integration/m3-0c5133b-two-viewer-hls-performance.json)
