# ARM64 ten-viewer profiled TS startup diagnosis

The ordinary ARM64 ten-viewer fixtures opened ten signed HTTP 200 MPEG-TS responses from one shared upstream and one encoder. On `7c604a6c`, one response ended after 255,868 aligned bytes and nine stayed healthy. On ordinary `f4056a05`, two responses ended before their first media bytes and eight stayed healthy. Both fixtures verified the owned provider slot and exact teardown.

The private diagnostic revision `0c81a8ad` reproduced the two zero-byte closures. The verifier joined 30 bounded events to all ten fixture readers and their private leases. In both failures, the Rust response stream reported a payload read error before the final lease-owner drop. No earlier lease event was recorded for either viewer. The diagnostic and independent replay exited 0. The actual viewing workload exited 1 before its timed window. Four scoped containers and two networks retired exactly.

The trace’s payload error category combines several storage failures. The default profile read path pessimistically reserves 32 MiB per request from a shared 256 MiB budget, which permits eight concurrent full reservations. Profile chunks in this fixture contain 255,868 bytes. The observed failure count is consistent with that capacity limit. The follow-up will verify bounded waiting under the same byte cap and repeat the ten-viewer workload. The full soak and throughput, CPU and memory gates remain open. [Machine-readable evidence](../scripts/rust-integration/m9-0c81a8ad-arm64-profiled-ts-close-diagnostic.json)

The test used a local diagnostic image with the trace enabled only for named synthetic viewers. Original Dispatcharr, React, FFmpeg and dependency contributors retain full credit.
