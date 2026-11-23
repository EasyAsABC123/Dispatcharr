# M1 equivalent-work core performance on 390ad8aa

Original `1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430` and the Rust core from `390ad8aa16f07de7cbc7364a86f1eb2254faa4c8` ran three paired trials each at one, ten and 100 readers on Beepboop (AMD64). The owner built the optimized Rust peer from the exact application source and verified the functional Compose run before measurement. Nine Original-only windows qualified the frozen 250 ms producer period.

| Simultaneous readers | CPU reduction, median (range) | Peak working memory reduction, median (range) |
| ---: | ---: | ---: |
| 1 | 13.5% (12.2%–15.6%) | 29.7% (28.9%–30.2%) |
| 10 | 71.1% (69.6%–71.5%) | 31.9% (31.5%–32.0%) |
| 100 | 90.9% (90.9%–91.0%) | 43.7% (43.4%–49.0%) |

Each arm warmed up for 15 seconds and measured for 60 seconds with at least 50 samples. Arm order alternated across trials. Both peers used the same deterministic MPEG-TS payload and fast and slow reader schedule. Each measured pair met the limits of 90% of Original CPU, 95% of Original peak working memory, 98% matched timed bytes within each reader class, identical final bytes, and 95% of target producer pacing. The independent verifier checked all nine pairs, all nine qualification windows, literal owner and worker exits, and exact Compose cleanup.

A separate synthetic media helper ran for about 1.26 seconds during the final Original qualification warmup. It ended at least 18 seconds before the next timed sample. Its execution overlapped no timed benchmark window.

Accounting includes the active core and shared PostgreSQL and Redis with the web service stopped. The result applies to the tested 390ad8aa application on AMD64. Formal M1 requires M0 acceptance. M9 has a separate integrated whole-service comparison.

The original Dispatcharr authors and contributors retain full application, frontend and protocol credit. See [attribution](../ATTRIBUTION.md) and the [license](../LICENSE).

[Machine-readable evidence](../scripts/rust-integration/m1-390ad8aa-equivalent-work-core-performance.json)
