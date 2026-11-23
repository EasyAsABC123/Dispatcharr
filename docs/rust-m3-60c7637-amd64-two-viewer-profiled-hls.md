# Two-viewer AMD64 HLS resource screen

The ordinary Rust package used **49.9% less CPU** and **48.1% less peak concurrent working memory** in this quiet, 32-second comparison. Both backends delivered exactly 5,988,928 bytes at approximately 187,154 bytes per second. Measured segment first-byte p95 rose from 4.15 ms to 8.52 ms. Both measurements fell below the screen's predeclared 50 ms floor, so the gated latency ratio was 1.0000. The raw latency ratio was 2.05.

| Metric | Original | Rust | Rust / Original |
| --- | ---: | ---: | ---: |
| CPU core-seconds | 1.442872 | 0.722688 | 0.50087 |
| Sampled peak concurrent working memory | 1,246.63 MiB | 647.55 MiB | 0.51944 |
| Delivered goodput, bytes/s | 187,154.01 | 187,153.65 | 0.999998 |
| Segment first-byte p95 | 4.15 ms | 8.52 ms | 2.0520 raw, 1.0000 gated |

The frozen limits were CPU ≤0.90, memory ≤0.95, goodput ≥0.95 and floored segment-p95 ≤1.10. Each arm contributed 33 cgroup v2 samples around its 32-second window. CPU sums the application, PostgreSQL/Redis infrastructure, origin and their descendant processes, including FFmpeg. Sampled working memory sums concurrent `memory.current - inactive_file` across the same groups, then takes the maximum sampled total. The comparison excludes warmup, the workload driver and offline decoding. The physical screen used one paired run. [Sanitized evidence and raw receipt digests](../scripts/rust-integration/m3-60c7637-amd64-two-viewer-profiled-hls.json)

Two distinct authenticated HLS sessions shared one FFmpeg OutputProfile, one transform generation and one upstream connection per arm. Each viewer polled the playlist 32 times and downloaded six complete segments, sequences 3 through 8. Segment bytes, content hashes, advertised durations and decoded H.264/AAC frame counts matched across both viewers and backends. Each stop returned 204, and both retired playlists returned 410. The transform retired, the upstream went idle, Rust Main exited 0 and all five exact container IDs and original host PID generations disappeared. Both owned Compose project inventories were empty.

The reference was Original `1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430`. Rust application `60c7637c807671955fcb789cbc94a5763ae799a2` ran from the ordinary AMD64 image `sha256:3f5d4512e615b32cf521df05ee41445677ac4cfd642ffe345e0c156d95d1c116` with build manifest SHA-256 `9b6dcfdd2880fe3dc8f4ff423fece29308cafcb466c61c920198dc0b65ea3a3b`. Full M3 functional and per-format performance acceptance and the separate full-scan latency gate remain open.

Dispatcharr’s original authors and contributors retain full credit for the application and React UI. FFmpeg and other dependency authors retain credit for their projects.
