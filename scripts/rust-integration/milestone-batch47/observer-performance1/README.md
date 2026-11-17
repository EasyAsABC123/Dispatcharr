Candidate47 new-protocol scoped live resource evidence

Original Dispatcharr authors and contributors retain all application and algorithm credit. FFmpeg, VLC and dependency authors retain credit for their work.

All 12 fresh phases completed valid. 6 of 6 pairs meet every preset comparison budget; 0 fail. These are scoped comparison budgets only. **gain_accepted and live_only_gain_accepted remain FALSE**, even if all 6 budgets pass: the broader 24-phase campaign, remaining viewer levels and active-DVR/resource domains remain open. No full milestone or complete migration is accepted.

This is a new measurement-instrumentation protocol on the same Candidate47 application image, not a Rust optimization. The persistent metadata helper and Redis processing are fully charged inside infrastructure; primary sums all backend/infrastructure plus Origin at each tick. External observer/orchestrator/publication scopes remain separately reported. Two synchronous unchanged EVALs run per snapshot, with 61 fixed ticks and final metadata. Connection/AUTH churn differs from the old protocol. No overhead subtraction or isolation of application-only CPU is claimed.

All 12 measured phases are new, separate from diagnostic pilots and old 45/47 measurements. No old metric is pooled, no pair selectively retried/excluded and no threshold changed. Earlier Candidate47 old-protocol CPU-budget failure is preserved at ../performance. The passed fresh Source+Native pilot/configuration and actual observer ownership/normal teardown are hash-bound here; the earlier host/diagnostic/pilot preparation evidence is preserved at ../observer-protocol1.

Image `sha256:2136bff6593153f895b2682a7d44dc4c7da3f3ab4b468cc66b103812d88c3db1`, source `c21b9b10a2844264fe55e3dfaa5a121ef8d0b461`; pinned Source `1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430` and dependency `ghcr.io/dispatcharr/dispatcharr@sha256:34f9011514edd31ef0ff111b5f256d2e37614fcb08a6ee1ed9eccec9251a856f`. Scope: one shared H264/AAC VLC input with 1 or 2 viewers, normal loaded graph with idle workers/Beat, 15s warmup, fixed 60s measurement, bounded 5s drain. Higher viewer levels, active recordings, recovery and other domains are unmeasured.

| Viewers | Trial | CPU ratio | Working-set ratio | Bytes ratio | Useful-byte ratio | p95 ratio | Budget |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| 1 | 1 | 0.73670262 | 0.29908204 | 0.99887294 | 0.99886435 | 0.95328191 | PASS |
| 1 | 2 | 0.70810586 | 0.29814225 | 0.99887294 | 0.99886435 | 0.95268462 | PASS |
| 1 | 3 | 0.74419587 | 0.29810324 | 0.99572756 | 0.99584103 | 0.95372475 | PASS |
| 2 | 1 | 0.69496971 | 0.29572085 | 0.99651391 | 0.99669725 | 0.95272848 | PASS |
| 2 | 2 | 0.71718668 | 0.29675069 | 0.99554949 | 0.99580636 | 0.95311424 | PASS |
| 2 | 3 | 0.69424769 | 0.29596760 | 0.99610319 | 0.99615284 | 0.95369903 | PASS |

Absolute values below average the three trials per level. Memory averages simultaneous trial peaks, not lifetime per-process peaks. CPU percentage is of one core.

| Viewers | Backend | CPU seconds/~60s | Mean one-core CPU | Working-set MiB | Charged MiB |
| ---: | --- | ---: | ---: | ---: | ---: |
| 1 | Source | 1.583482 | 2.63904% | 904.37 | 1020.12 |
| 1 | Native | 1.155229 | 1.92531% | 269.90 | 385.63 |
| 2 | Source | 1.652683 | 2.75443% | 908.97 | 1024.72 |
| 2 | Native | 1.160453 | 1.93402% | 269.19 | 384.91 |

Descriptive geometric mean ratios across all 6 pairs: CPU 0.71564619, memory 0.29729189, throughput 0.99693902, encoded_useful_throughput 0.99703682, first_byte_p95 0.95320541. Individual pair budgets remain authoritative; aggregate ratios do not establish acceptance.

Working set sums per-group max(0,memory.current-inactive_file) at each tick and is an estimate, not RSS. Charged memory includes file cache. CPU uses cumulative usage_usec differences divided by actual measurement elapsed time. Complete-media reports certify independently indexed/decoded useful work and the one-batch PTS bound; the sampler false useful_work_passed flag remains preserved because off-window qualification is separate.

Useful-duration equivalence: stream 0: spread 0.440000s <= bound 2.005333s; stream 1: spread 0.426656s <= bound 2.005333s. First-byte p95 averages 7.098s Source and 6.766s Native across phases; each phase has only one or two latency observations, so this is not a production tail distribution. The declared 50ms floor is inactive.

Main configuration remains functional explicit grace 45/resource absent override with compiled default 12, proxy_settings.channel_shutdown_delay 5/2, Tokio 2/1 and fixture lifetime 1800/300. This protocol changes observation transport only.

Independent raw arithmetic and peer review pass. Root independently verifies 42 exact CIDs, 12 project inventories and 499 captured original host process generations absent. Cleanup covers the recorded identities/inventories, never inferred missing generations. All raw gzip copies preserve uncompressed SHA256. Recompute without Docker/apps/media:

```bash
python3 -B scripts/rust-integration/milestone-batch47/observer-performance1/recompute.py
```
