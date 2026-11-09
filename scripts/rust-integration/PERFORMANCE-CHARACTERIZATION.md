# Native TS characterization and rejected runs

Full original Dispatcharr author/contributor credit remains with upstream.
This ledger retains native TS studies and a separate read-only API study.
Frontend viewing, WebSocket statistics and equivalent event delivery are not
accepted by these partial workloads; no full application gain is claimed.

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

## Failed full-retention ten-channel diagnostic

A single isolated source/Rust pair at10channels/10viewers used15second warmup and
60second measurement, with20successful parallel decoder primes and all owners
active before load. Both phases delivered data to every viewer without errors,
and cleanup removed the isolated project. This is n=1 diagnostic evidence.

The declared memory gate **failed**: full-fixture Docker working-memory peak was
517.68MiB/source versus522.58MiB/Rust, ratio1.009465 (+4.90MiB). CPU was23.7345%
versus5.3055%; observed ten-channel throughput was2.763/2.776MB/s total
(source/Rust), reflecting2Mbps per channel.
First-byte p95 was116.84/8.06ms. No many-channel performance gain is accepted.

Independent per-component peaks (do not sum these non-coincident maxima):

| Container role | Source working-memory MiB | Rust phase working-memory MiB |
| --- | --- | --- |
| Baseline web/PostgreSQL/Redis; web stopped for Rust |431.40|277.70|
| Shared origin fixture |86.59|87.01|
| Rust native process |—|159.40|

Process RSS is diagnostic and can double-count shared pages. Redis RSS peaked at
165.18/162.39MiB; source uWSGI RSS sum235.37MiB, Rust process166.46MiB. PostgreSQL
RSS sum was135.95/253.93MiB, with9/14postgres processes observed (including server
and background workers). Actual pg_stat_activity states and private/PSS memory
were not collected. The fixed Rust pool8 may contribute, alongside its local
media ring plus Redis storage; neither explanation is established causally.

Image1 did not export direct local retained-byte or full Redis-key memory
telemetry. Its configured local ring is16MiB/channel (160MiB across ten), in
addition to the Redis input copy. The next work is direct admin-only retained-byte
telemetry and database connection/private/PSS diagnostics, followed by a bounded
hot-cache/Redis-history design if warranted. Source60second/16MiB history,
behind-zero live positioning, slow consumers and follower behavior must remain
intact. No retention reduction or repeated heavy matrix substitutes for that
functional proof.

Raw evidence: `artifacts/rust/m2-many-channel-retention-diagnostic-1/run.json`;
component breakdown: `verified-many-retention-diagnostic-1.json`.

## Retention and database observer validation

The versioned admin-only `/internal/proxy/ts/v1/memory` endpoint now separates
local retained payload, shared Redis logical payload and Redis allocation for
known tracked input keys. It does not read media payloads, scan arbitrary keys,
refresh TTLs or change the source-compatible status response. A real HTTP/Redis
test independently observed24,064bytes in each storage layer with the current
owner fence. Allocation exceeded payload, as expected. This validates the
accounting; it does not establish full-ring occupancy or explain the failed
many-channel memory gate. In-flight frames and allocator overhead are excluded;
shared Redis totals must not be summed across replica reports.

`collect_fixture_memory.py` reads `pg_stat_activity` and PostgreSQL
`smaps_rollup` as the fixture's database OS user. It excludes its SQL observer,
records no SQL text/credentials/client addresses, and reports private memory and
PSS separately from RSS. One concurrent management-fixture smoke read all17
PostgreSQL processes without permission failures:11client backends,127.26MiB
PSS,96.39MiB private and432.54MiB RSS sum. These values illustrate why RSS sums
cannot represent database working-memory. They are not a paired source/Rust
comparison or evidence of a pool-size cause. Snapshots are not atomic.

The previous memory failure remains open. No retention or pool behavior changed.
Durable component evidence: `verified-retention-telemetry-1.json`.

Frozen image10 subsequently passed a candidate-only75second functional capture
at ten channels/readers. At the last snapshot its local rings retained143.97MiB
of payload and shared Redis retained a second143.97MiB; tracked Redis keys
allocated147.57MiB. All owner fences matched, all twenty prime/capture decodes
returned75frames with video/audio, and cleanup removed the project. PostgreSQL
had eight client backends,50.43MiB PSS and23.93MiB private memory. This proves
duplicate full-history payload storage in that image. It does not establish its
causal share of image1's earlier memory failure or measure an optimization gain.
The run was concurrent functional work, with observer snapshots excluded from
any performance gate. Full binding, samples and limitations:
`verified-retention-image10-1.json`; proposed next gates:
`docs/rust-native-retention-investigation.md`.

Frozen image11 passes the smaller-cache functional gates without reducing the
authoritative history. Ten channels retain39.04MiB locally plus143.97MiB of
Redis history after75seconds. Native H264 owner/follower/shared-stop/crash gates
and source/Rust HLS/fMP4 decoders pass. A separate100-viewer45second late-seek
gate matches every opening prefix to old Redis media outside the cache and
decodes ten representative video/audio captures. These concurrent functional
runs prove retained-byte behavior; they do not establish a Docker working-memory
or CPU gain. A separate isolated paired study follows below. Evidence:
`verified-hot-cache-image11-1.json` and
`verified-hot-cache-late-history-image11-1.json`.

## Three paired ten-channel trials with the bounded cache

Frozen image11 now passes six isolated measurements: three source/Rust pairs,
alternating backend order, one viewer on each of ten channels,15second warmup
and60second measurement. Both backends receive equal parallel per-channel
decoder priming; all60primes decode at least74frames and all selected owners
remain active before warmup. Every load has zero errors, empty responses,
missing viewers or observer errors. Each phase closes all origins and final
cleanup removes the project. The fixture remains300seconds/H264, matching the
older failed diagnostic. Fresh trials restart upstreams from byte zero, so they
need one trial's fixture lifetime rather than the entire matrix wall time.

| Metric | Median paired Rust/source ratio | Range across three pairs |
| --- | --- | --- |
| Full-fixture CPU |0.22128|0.21767–0.24541|
| Full-fixture Docker working memory |0.82405|0.81707–0.83216|
| First-byte p95 |0.06490|0.06407–0.08029|
| Delivered bytes/second |0.98898|0.98594–1.00134|

Absolute backend medians are23.5855%CPU/source versus5.232%/Rust,
515.93MiB/source versus421.74MiB/Rust working memory, and129.80ms/source versus
9.12ms/Rust first-byte p95. Absolute backend medians and medians of paired ratios
are different statistics. CPU100% means one fully used logical core. Primary
memory is the simultaneous full-group Docker working-memory peak, including
PostgreSQL, Redis and the identical origin. Independent container maxima are
provided separately and must not be added as if simultaneous. RSS remains a
diagnostic because PostgreSQL shared pages can be counted repeatedly.

All declared native-slice budgets pass: CPU and memory ratios at most1,
p95 at most1.1, throughput at least95%. The older image1 failure remains in its
ledger. This new result establishes warmed native Proxy playback resource
behavior for image11, not whole-application gains. React/browser execution,
live WebSocket/event side effects, other profiles, cold reopen/burst behavior
and HLS/fMP4 resource acceptance remain outside this experiment.

Rust image: `sha256:363679ce182bea55be97b873c1faffb3024e5abdae0886f655c6678b00943f33`.

Rust source: `d135d0f8e8b21bde6ee102ff77cf0e73f36b84079480590eb4aa08a7f0aed3e0`.

Durable evidence, exact paired values and component/RSS diagnostics:
`verified-hot-cache-many-channel-image11-1.json`. Raw artifacts and immutable
measured tool copies: `artifacts/rust/m2-hot-cache-many-channel-matrix-1/`.

## Image14 read-only API characterization

The separate [API report](verified-api-resources-image14-1.json) passes all three
independent pairs at ten synthetic users and40successfulrequests/second. Each
phase has15seconds of equal warmup and60seconds of observation; backend order
alternates. All14,400 measured responses match the original canonical JSON, with
zero delivery or observer errors. These are repeated channel/settings/user-agent
reads used by React initialization, rather than an actual browser session.

| Metric | Median paired Rust/source ratio | Three-pair range |
| --- | --- | --- |
| Service-group CPU |0.10483|0.10329–0.10748|
| Sampled Docker working-memory peak |0.62949|0.62839–0.63011|
| First-byte p95 |0.06490|0.06301–0.06508|
| Complete-response p95 |0.06605|0.06416–0.06609|
| Successful requests/second |1.00000|1.00000–1.00000|

This is about89.5%lessCPU and37.1%lesssampled working memory for this API subset.
The absolute backend medians are30.372%CPU/source versus3.1825%/Rust and
357.60MiB/source versus225.10MiB/Rust working memory. CPU100% means one logical
core. Identical successful request rates also normalize the CPU comparison by
completed requests. These medians and medians of paired ratios are distinct
statistics.

Each trial restarts the selected web service, stops the other implementation,
and clears the fixture's isolated Redis. The same seeded PostgreSQL dataset and
identical idle origin remain. Totals include backend, PostgreSQL, Redis and
origin; `docker stats` working memory is a sampled peak, not an unsampled kernel
peak or process RSS. All fixed CPU/memory/latency/throughput gates in the captured
harness pass in every pair. The shorter smoke is retained separately and is not
combined with this isolated matrix.

The measured Rust image is
`sha256:1ecdec13810a0442f9721197f64c657cc780fb15775b89a19ce0c0247895454a`, with
source tree`3dee6610ccae2626a800e46dc8797170f25e919dde5cef07f4c501d1cbf833aa`.
Actual browser execution, authentication/mutation workloads, WebSocket events,
background jobs, long-run growth and full M5/application acceptance remain open.
Later shared-auth and gateway changes must receive their own package/resource
gates. Exact tools, raw samples, isolation record and cleanup are retained in
`artifacts/rust/m5-api-resource-matrix-1/`.

Image20 also has a completed [VOD/catch-up playback matrix](../../docs/rust-m4-performance.md): four workloads, three isolated paired trials each, with actual decoder/WebSocket checks and full selected-service resource samples. Those results apply only to that immutable image and synthetic playback subset; no completed M4 or full-application acceptance is implied.
