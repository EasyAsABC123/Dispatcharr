# Read-only API resource comparison

All original application credit remains with Dispatcharr authors and
contributors. This characterizes a selected backend API workload and does not
accept browser behavior or a complete milestone.

Build a uniquely tagged native image and retain its immutable manifest first.
Then run, from the repository root:

```sh
python3 scripts/rust-integration/run.py \
  --output artifacts/rust/api-resource-local-1 \
  --candidate-image dispatcharr-rust-candidate:m5-auth-management-image14-1 \
  --candidate-build-manifest artifacts/rust/candidate-build-m5-auth-management-image14-1/build.json \
  --contract-suites core --api-resource-matrix
```

Use the tag and manifest of the actual build being tested. The example names
identify the recorded image14 study. A new output directory is required. The
runner snapshots its tools, starts disposable Compose PostgreSQL/Redis/origin
services, verifies API/schema/media foundations, and runs three independent
pairs with alternating backend order. It restarts the selected web service and
clears only owned fixture Redis before each trial; seeded database data is the
same. Ten synthetic users each make four requests per second, rotating through
channel page1/page_size200, Core settings and user-agent lists. Every response
must match canonical original JSON, including boolean/numeric types.

The default15secondwarmup/60secondmeasurement and three pairs qualify as repeated
characterization. `--api-repetitions 1 --api-warmup 1 --api-duration 10` is only a
load smoke. `--api-start-signal /tmp/unique-new-file` waits after fixture readiness
so competing workloads can finish; creating that file starts measurement.

Each pair requires zero errors, coverage of every endpoint, at least three
resource samples, at least95%of declared pacing and native throughput, CPU and
working memory at most source, and first-byte/complete-response p95 at most1.1x.
All pairs must pass. Containers are removed after the run. Tokens and response
bodies are not written to performance reports. Four-MiB response caps, at most100
workers and bounded phase duration protect the test driver; the recorded study
uses ten workers and ordinary fixture responses.

Inspect `integration.json`, `api-resources/matrix.json` and each trial JSON, not
only the console summary. Memory is simultaneous sampled Docker working memory
for the full selected service group, rather than summed process RSS or an
unsampled kernel peak. CPU100% means one logical core. The API study excludes
login/API-key/mutation work, actual browsers, active media and WebSockets,
background workers, soak and complete M5/application acceptance.

Recorded results and exact hashes:
[image14 report](verified-api-resources-image14-1.json),
[performance ledger](PERFORMANCE-CHARACTERIZATION.md).
