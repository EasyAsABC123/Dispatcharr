# VOD and catch-up playback resources

Full application credit belongs to the original Dispatcharr authors and contributors.

Immutable image 20 passed all 12 paired trials and 24 measured phases. These are scoped playback results; full M4 and application acceptance remain open.

| Workload | Viewers | CPU reduction | Working-memory reduction | Throughput ratio |
|---|---:|---:|---:|---:|
| VOD | 1 | 40.4% | 36.8% | 1.0021 |
| VOD | 10 | 70.5% | 33.6% | 0.9999 |
| Catch-up | 1 | 37.9% | 35.2% | 0.9957 |
| Catch-up | 10 | 67.8% | 32.8% | 0.9957 |

Reductions use the median of three paired Rust/original ratios. Absolute values below are medians of the phase metrics; their ratio need not equal the median paired ratio.

| Workload / viewers | Original CPU (% core) | Rust CPU (% core) | Original memory (MiB) | Rust memory (MiB) |
|---|---:|---:|---:|---:|
| vod / 1 | 0.378 | 0.229 | 751.60 | 474.28 |
| vod / 10 | 2.066 | 0.601 | 777.30 | 516.03 |
| timeshift / 1 | 0.424 | 0.263 | 764.30 | 496.13 |
| timeshift / 10 | 2.630 | 0.846 | 777.50 | 522.67 |

The measured group includes the backend, shared PostgreSQL/Redis infrastructure and origin. Both backends use the same observed 2-CPU/512MiB backend limits; infrastructure and origin limits, fixture hashes, all numerical samples, decoder checks and pair results are preserved in the [evidence JSON](../scripts/rust-m4-perf/verified-resources-image20-2.json).

Each phase keeps one HTTP connection per viewer through 15 seconds of warmup and 60 seconds of measurement. All phases collected 30 samples. Orders alternate across three pairs. Budgets were frozen before measurement: CPU ratio≤0.80, sampled working-memory ratio≤0.95, throughput≥0.95, first-byte ratio≤1.10 with a 50ms floor, and zero delivery errors. All passed. Memory is sampled Docker working memory; it is not an instantaneous RSS peak. The host load driver is excluded.

Correctness includes matching 1 MiB prefixes and actual strict audio/video decoder samples, plus sustained byte rates, no EOF/errors, WebSocket observations and independent provider/metadata cleanup. It does not hash every later delivered byte. Fixtures are generated before sampling; no FFmpeg transcode runs during a trial.

The initial attempt was interrupted when an audit found synthetic JWTs would expire before the matrix ended. Its available incomplete artifacts remain under `artifacts/rust/m4-resources-image20-1`; no result was accepted from it. The completed run extends only synthetic fixture tokens to 2 hours and checks their remaining lifetime before the matrix and every phase. Application JWT settings, image, workloads and budgets are unchanged.

Candidate image: `sha256:fd449c861ba84d44229004c961c92b263ac06c7fe21e6dc194b59a3a70cb4dac`.
Candidate source tree: `db7ee628fe24da0589ad19134d8886a15ed59dcb50b85d6e0c59b22ee2a1117f`.
Frozen plan: `cebad4c1409679e7f5efc6b1544c9fb40fc1b9981789554c490c26d1acfc4f42`.

Run instructions are in [the experiment README](../scripts/rust-m4-perf/README.md). Later migration edits require their own checks; these figures cannot be attached to a newer image without remeasurement.

## Candidate107 playback comparison

The exact Candidate107 image passed all 12 VOD and catch-up pairs under
the same frozen plan. The independent receipt verified four strict
audio/video decoder probes, all 24 measured phases, matched playback
work and exact teardown. The figures are medians of three paired ratios
per workload.

| Workload | Viewers | CPU reduction | Working-memory reduction | Throughput ratio |
|---|---:|---:|---:|---:|
| VOD | 1 | 39.0% | 35.9% | 0.9989 |
| VOD | 10 | 72.9% | 32.9% | 0.9999 |
| Catch-up | 1 | 39.2% | 34.1% | 0.9957 |
| Catch-up | 10 | 70.0% | 31.8% | 0.9957 |

The Rust revision was `1ea708bf321ec7348a9f04a7fa5d16668e4d072b`.
The Rust image was `sha256:7e7059895df9f87554ce31bf235a760bd47a5649c1518efc87a63b61f0926d8c`.
The receipt SHA-256 is `5a0b479f21398dc62503b54c3f806ed2c28b66a74eec1508604cfe03c16b322c`.

The experiment covers sustained XC VOD and catch-up playback at one and
ten viewers. Full M4 and application-wide acceptance still require the
remaining functional and integrated gates.
[Candidate107 comparison receipt](../scripts/rust-m4-perf/verified-resources-c107.json)
