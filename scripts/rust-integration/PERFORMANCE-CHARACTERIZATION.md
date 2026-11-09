# Native TS characterization and rejected runs

Full original Dispatcharr author/contributor credit remains with upstream.
These studies compare the native TS backend subset. Frontend viewing,
WebSocket statistics and equivalent event delivery are not exercised; no full
application performance gain is accepted from this matrix.

The fixed workload uses the pinned original source/runtime, the same actual H264
TS origin and FFmpeg decoder, one backend worker, retained input60seconds,
startup5seconds behind, and shutdown5seconds. Resource totals include origin,
PostgreSQL/Redis (inside baseline container), and the running backend. Original
web is stopped for Rust trials. Container CPU and Docker working-memory usage
are primary metrics. Per-process RSS is diagnostic because shared PostgreSQL
pages may be counted more than once; observer duration is retained.

Rejected evidence is preserved:

* `m2-native-source-compat-matrix-1`: stopped after one complete pair because
  RootlessKit could not rebind the stopped candidate's previous dynamic host
  port. The harness now recreates the container and refreshes its endpoint at
  each trial. This was an infrastructure failure, not an application failure.
* `m2-native-source-compat-matrix-2`: stopped at source10viewers/trial2 with all
  ten viewers receiving zero bytes and60request errors. The prior harness primed
  Rust with a real decoder at each trial, but primed source only once before the
  matrix. Fresh source trials could therefore begin as a simultaneous cold
  burst. The failed source run and logs remain valid failure evidence; its CPU
  values cannot establish a throughput-equivalent gain.

Corrected steady-state design: **both** implementations receive a3second actual
FFmpeg decoder run (at least50frames) for every selected channel before the same
warmup. Priming logs/frame counts and a distinct workload fixture ID are retained.
This deliberately characterizes an already-active channel. It does not repair,
cover up or pass the independent cold-burst/reopen gate. The corrected harness
passed short warmed10/100viewer delivery smokes, then the full same-channel
1/10/100viewer, three-paired-trial15second/60second matrix during an agreed quiet
window (results below). Earlier partial runs are not combined with it.

Every report retains errors, empty viewers, throughput, latency, resource sample
errors, per-container samples and repeated-trial spread. A run missing output or
samples is rejected before any gain is accepted. Many-channel and slow-reader
resource matrices remain separate required work after this first characterization.

The corrected source-only warmed load smoke passed at10and100viewers: respectively
10and100requests,38.38MB/391.22MB delivered during10second measurements, no request
errors, empty responses, missing viewers or observer errors. Each trial received
its own successful decoder priming. High-port fixture bindings were captured and
the isolated project removed. Evidence: `m2-warmed-source-smoke-1/run.json`. Other
functional work was running concurrently, so these are delivery checks only.

## Completed isolated warmed native TS matrix3

All18 measurements passed, with three paired trials at1/10/100viewers,15second
warmup and60second observation per measurement. Every trial's actual decoder
priming passed at least50frames. All viewers received data; no request errors,
empty responses or observer errors occurred. All nine declared comparisons passed
CPU/memory ratios<=1, first-byte p95 ratio<=1.1 and throughput>=95% of source. The
isolated Compose project was removed.

This is a **warmed native TS characterization**, not an accepted full application
performance gain. The measured image is immutable and predates current HEAD,
including the metadata Ready barrier correction. Source WebSocket statistics,
equivalent event broadcasts and actual frontend viewing remain outside equivalence.
The remaining many-channel and slow-reader resource studies are not covered.

Median paired Rust/source ratios (ranges across the three pairs in parentheses):

| Viewers | CPU ratio | Docker working-memory peak ratio | Throughput ratio | First-byte p95 ratio |
| --- | --- | --- | --- | --- |
| 1 | 0.2961 (0.2959–0.3067) | 0.8247 (0.8201–0.8286) | 1.0013 (1.0013–1.0013) | 0.2805 (0.2565–0.3136) |
| 10 | 0.1863 (0.1587–0.2040) | 0.8456 (0.8455–0.8479) | 0.9890 (0.9859–1.0013) | 0.0732 (0.0702–0.0746) |
| 100 | 0.0673 (0.0624–0.0794) | 0.7781 (0.7749–0.7894) | 0.9942 (0.9939–1.0021) | 0.0251 (0.0237–0.0266) |

Absolute medians across each backend's three trials, for scale. Docker CPU100%
means one fully used logical core; these are modest absolute CPU loads. Memory
totals include the identical origin fixture and PostgreSQL/Redis, not only backend
RSS. RSS sums remain diagnostic because shared pages can be counted twice.

| Viewers | Source / Rust CPU percent | Source / Rust working-memory MiB | Source / Rust first-byte p95 ms |
| --- | --- | --- | --- |
| 1 | 2.840 / 0.846 | 719.60 / 590.13 | 11.12 / 3.22 |
| 10 | 4.534 / 0.867 | 746.30 / 631.46 | 115.50 / 8.57 |
| 100 | 24.623 / 1.656 | 819.50 / 635.06 | 1744.56 / 41.34 |

Source baseline commit: `1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430`.

Rust image: `sha256:964c50cd90f5e9648f3c716ec0adc49072d2a0586fce1e2d4225029a422c2034`.

Rust source tree: `9610d95e612c3736113eadb97bb7322b6d0a91d7064d13133334b57f21344dcc`.

Durable full data, per-container CPU/working-memory/RSS, observer durations and
individual paired-trial ratios are in `verified-native-source-compat-matrix-3.json`.
Raw run: `artifacts/rust/m2-native-source-compat-matrix-3/run.json`; summary and
18 measurement hashes are alongside it. Exact measured harness bytes were copied
under that artifact's `tools/` with their captured SHA256 manifest before further
harness edits. This enables auditing the earlier measured tool version.

## Ten-channel functional smoke

`m2-many-channel-smoke-1` passed source plus frozen image1 with one viewer on each
of10channels. Bounded parallel decoder priming passed20/20channels (at least50
frames each), completion spread0.481seconds/source and0.00655seconds/Rust, below
the5second shutdown delay. All ten owners and active metadata states were checked
before warmup. Both load phases had10requests, no errors or missing viewers, ten
upstreams and no extra upstream connects. All ten upstreams closed at cleanup.
No additional holder readers were used.

This5second/10second concurrent functional smoke is not an isolated comparison
and does not fill60second retention. The possible memory duplication of Rust's
local ring plus Redis input versus source Redis storage must be measured with a
longer many-channel matrix before architectural changes. Its observed resource
values cannot establish many-channel gains or rule out a full-retention regression.
Durable evidence: `verified-many-channel-smoke-1.json`.
