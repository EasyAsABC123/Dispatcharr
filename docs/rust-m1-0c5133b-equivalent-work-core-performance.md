# M1 equivalent-work core performance on 0c5133b

Original `1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430` and the Rust core
from `0c5133b62dc9f2df0900f2228e25b75baecb6754` ran three paired trials each at
one, ten and 100 readers. The fresh optimized Rust 1.90 peer came from
the exact application source. A separate functional Compose run first
verified matching bytes, cursor sequence, held-viewer behavior and cache
retention. Nine Original-only pacing windows qualified the frozen 250 ms
producer period before the coordinator released comparative measurement.

| Simultaneous readers | CPU reduction, median (range) | Peak working memory reduction, median (range) |
| ---: | ---: | ---: |
| 1 | 12.8% (11.9–15.4%) | 30.1% (29.3–30.6%) |
| 10 | 70.8% (70.5–71.1%) | 32.3% (31.3–33.6%) |
| 100 | 91.0% (90.8–91.1%) | 44.7% (43.2–49.2%) |

Each arm ran a 15-second warmup and a 60-second resource window with at
least 50 in-window samples. Order alternated across three trials. The
same deterministic MPEG-TS workload and fast/slow reader schedule drove
both peers. The accepted limits required Rust CPU at most 90% of
Original, peak working memory at most 95%, measured bytes at least 98%
matched within each reader class, identical final bytes, and producer
pacing at least 95% of target. All nine pairs passed.

Accounting includes the active core and common PostgreSQL/Redis
infrastructure with the web service stopped. The owner and worker exits,
raw samples, byte ledgers and exact project teardown were independently
verified. This qualifies the M1 core performance workload. Formal M1
acceptance still depends on M0. The M9 whole-service release comparison
has its own integrated workload.

The original Dispatcharr authors and contributors retain full
application, frontend and protocol credit. See
[attribution](../ATTRIBUTION.md) and the [license](../LICENSE).

[Machine-readable evidence](../scripts/rust-integration/m1-0c5133b-equivalent-work-core-performance.json)
