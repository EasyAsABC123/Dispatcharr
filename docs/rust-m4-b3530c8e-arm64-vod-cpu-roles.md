# B353 ARM64 VOD CPU by service role

The independent file-only replay reconciled the exact VOD reconnect pair to **+114,531 µs of whole-scope CPU**. Its five role deltas sum to the measured +5.02%. Rust’s backend consumed less CPU in this pair, while PostgreSQL, Redis, and the probe and decoder together used more. [Typed evidence](rust-m4-b3530c8e-arm64-vod-cpu-roles.json)

| Role | Rust − Original CPU |
| --- | ---: |
| Backend | −16,569 µs |
| PostgreSQL | +41,052 µs |
| Redis | +71,004 µs |
| Origin | −3,247 µs |
| Signed probe and decoder | +22,291 µs |

These counters locate the CPU charge. The measurement leaves the operation counts, cost per operation, and background work unresolved. Full VOD release performance remains open. Original Dispatcharr, PostgreSQL, Redis, FFmpeg, and upstream dependency authors retain full credit.
