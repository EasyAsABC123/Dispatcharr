# B353 ARM64 VOD reconnect resource comparison

One quiet paired comparison used the exact ordinary ARM64 package and pinned Original. Each arm completed two same-session 4,299,461-byte reads with equivalent Origin work, strict audio and video verification, and exact teardown. The owner and independent proof exited zero. The measurement includes the backend, PostgreSQL, Redis, Origin, and signed probe and decoder. [Typed evidence](rust-m4-b3530c8e-arm64-vod-resource-pair.json)

| Metric | Original | Rust B353 | Change |
| --- | ---: | ---: | ---: |
| CPU core-seconds | 2.279247 | 2.393778 | +5.02% |
| Peak concurrent working memory | 373,899,264 B | 235,089,920 B | −37.12% |
| Median delivered bytes/s | 266,099 | 266,250 | +0.06% |

This pair confirms the memory reduction and reproduces the higher CPU charge observed in the earlier D6 pair. It is scoped to the signed two-read reconnect workload. Full VOD parity and the release performance gate remain open. Original Dispatcharr, PostgreSQL, Redis, FFmpeg, and upstream dependency authors retain full credit.
