# Dispatcharr Rust streaming performance evidence

These matched Compose measurements apply to the application revision, architecture, and workload in each link. CPU changes compare aggregate core-seconds for the measured fixture. Memory changes compare peak working memory.

| Workload | Exact Rust revision | CPU change | Peak working memory change |
| --- | --- | ---: | ---: |
| Shared signed live MPEG-TS, 1 viewer | `f38f6cdd` AMD64 | −38.52% | −50.10% |
| Shared signed live MPEG-TS, 10 viewers | `f38f6cdd` AMD64 | −60.66% | −44.62% |
| Shared signed live MPEG-TS, 100 viewers | `f38f6cdd` AMD64 | −87.81% | −47.55% |
| Signed HLS, one matched measured pair | `b3530c8e` ARM64 | −32.04% | −73.44% |
| Signed VOD, two full reads and reconnect | `b3530c8e` ARM64 | +5.02% | −37.12% |

The AMD64 MPEG-TS campaign ran nine matched pairs. Upstream concurrency peaked at one during each same-channel cohort. The newer B353 functional run repeated the 1-, 10-, and 100-viewer scenarios with 222 total readers and a peak of one concurrent upstream connection. Its quiet performance comparison remains pending. [Measured MPEG-TS](rust-m1-amd64-signed-live-performance-f38f6cdd.md) · [B353 functional replay](rust-m1-b3530c8e-signed-live-functional.md)

The B353 ARM64 HLS pair passed its scoped measurement checks. B353 ARM64 VOD reduced memory and used more CPU in the measured window. The role counters locate the additional VOD CPU charge predominantly in PostgreSQL and Redis. They leave the cause of the additional work unconfirmed. [HLS pair](rust-m3-b3530c8e-arm64-signed-hls-perf.md) · [VOD pair](rust-m4-b3530c8e-arm64-vod-resource-pair.md) · [VOD role counters](rust-m4-b3530c8e-arm64-vod-cpu-roles.md)

B353 ARM64 passed the 120-second soak. The formal 360-second soak was within its idle memory limit and exceeded the file-descriptor growth limit. A separate naturally dirty companion showed relation-file growth in PostgreSQL and exceeded the idle memory limit. Whole-service performance and release acceptance remain open. [Formal short soaks](rust-m9-b3530c8e-arm64-short-soaks.md) · [PostgreSQL companion](rust-m9-b3530c8e-arm64-naturally-dirty-pg-counters.md)

Original Dispatcharr and all upstream authors and contributors retain full authorship and license credit.
