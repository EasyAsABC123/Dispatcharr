# ARM64 two-viewer HLS resource screen at d2c5e611

The ordinary ARM64 Rust package used **23.1% less CPU** and **63.8% less peak concurrent working memory** in one quiet 32-second paired run. Both backends delivered the same 5,988,928 bytes. Each signed viewer received six complete segments. The independent H.264 and AAC decodes matched. The same two sessions shared one upstream and one FFmpeg profile generation in each arm.

| Metric | Original | Rust |
| --- | ---: | ---: |
| CPU core-seconds | 3.880652 | 2.984102 |
| Peak concurrent working memory | 1,084,125,184 bytes | 392,880,128 bytes |
| Delivered goodput | 187,144.57 bytes/s | 187,185.53 bytes/s |
| Raw segment first-byte p95 | 8.117 ms | 6.972 ms |

Both arms supplied 33 samples. CPU combines the backend, fixture infrastructure, origin and their descendants. Peak working memory sums aggregate concurrent cgroup charge minus inactive file memory. The same VM reader measured both arms and recorded its own resource cost separately. The predeclared 50 ms first-byte floor yielded a gated ratio of 1.0. The raw latency ratio was 0.859.

The owner and read-only proof each exited 0. Both application scopes and the observer retired exactly. The preceding attempt completed the Original arm and produced zero Native samples because the fixture omitted the observed ARM FFmpeg loader flag. The corrected fixture retained the same workload and thresholds.

This is one scoped HLS resource comparison. Full M3 performance acceptance and the separately deferred full-scan evaluation remain open. Original Dispatcharr and React authors and contributors retain full project credit. FFmpeg and the other dependency authors retain credit for their projects.

[Scoped performance proof](../scripts/rust-integration/m3-d2c5e611-arm64-two-viewer-profiled-hls.json)
