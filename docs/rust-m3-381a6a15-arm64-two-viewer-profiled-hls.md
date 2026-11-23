# ARM64 two-viewer HLS resource screen at 381a6a15

In one quiet 32-second paired run on Lappy, the exact ordinary ARM64 Rust package used **27.4% less CPU** and **66.5% less peak concurrent working memory** than Original. Both arms delivered 5,988,928 bytes. Each signed viewer polled 32 times and received six complete segments. All four segment sequences matched in bytes and independent H.264/AAC decoding. The two viewers shared one upstream and one FFmpeg output-profile process per arm.

| Metric | Original | Rust |
| --- | ---: | ---: |
| CPU core-seconds | 3.034937 | 2.204405 |
| Peak concurrent working memory | 1,184,403,456 bytes | 396,476,416 bytes |
| Delivered goodput | 187,191.11 bytes/s | 187,140.86 bytes/s |
| Raw segment first-byte p95 | 20.186 ms | 10.909 ms |

Both arms supplied 33 resource samples. CPU includes the application, fixture infrastructure, origin and their descendants. Peak working memory sums concurrent cgroup working charges. The same VM reader measured both arms and accounted for its own cost separately. The physical pair passed the original CPU, memory, goodput and latency ratio gates. The 50 ms first-byte floor yielded a gated ratio of 1.0. The raw ratio was 0.540.

The actual pair and outer owner exited `0`. The SSH wrapper did not retain its post-run phase markers. A separate invocation of the sealed proof and the independent receipt audit both exited `0`. The audit verified exact retirement of both application scopes, their tracked process generations and the VM reader, with all project inventories empty. The physical report SHA-256 was `71e29c31ef07052c92b36f04d61ecc41c7cfa27f594ac7d0e8f0b278b9f37a53`. Media and credentials stayed on Lappy.

This result covers the frozen two-viewer HLS profile and one paired run. Full M3 acceptance and the separate full-scan latency gate remain open. The original Dispatcharr and React authors and contributors retain full project credit. FFmpeg and other dependency authors retain credit for their projects.
