# Actual Python playback baseline

This is benchmark infrastructure for the original Dispatcharr backend. Full credit
for that application belongs to its original authors and contributors. The Rust
port remains incomplete; this baseline does not establish any Rust gain or a
completed migration milestone.

The runner archives immutable source revision
`1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430` and uses the digest-pinned Linux/amd64
CI image in `docker/compose.rust-playback.yml`. Direct dependencies must match;
no source or dependency patching occurs. PostgreSQL17/Redis, migrated fixture DB,
media, network, and generated source are disposable. The source archive stays
alive through exact-project cleanup in `finally`.

Requirements: Python 3.9+, Git, tar, Docker with Compose v2 and permission to access
the daemon. Docker must be on PATH or passed via `--docker /absolute/path`;
`DOCKER_HOST` and `DOCKER_CONFIG` are honored. Use a new empty output directory.

The seed uses the migrated custom M3U account and active unlimited default profile,
locked native Proxy profile, and ten deterministic channel IDs, each with a primary
and alternate origin path. It loads no production credentials or provider URLs.
Only private/local request networks are allowed by the disposable application DB.
The fixture bridge publishes ephemeral ports on127.0.0.1; outbound egress is not
enforced. No Celery, Daphne, nginx, or scheduled provider imports are started.
One uWSGI gevent worker/400 slots serves the actual unchanged WSGI application,
including input/Redis buffers, per-viewer delivery, and lifecycle logic.

Before any load, image FFmpeg must decode three seconds of actual proxied TS into
video frames. The origin fixture is MPEG2 video at 320x180 and 25 fps, MP2 audio at 48 kHz,
2 Mbps mux, packet-aligned and paced at250,000 bytes/sec. Measurements select both
backend and origin; PostgreSQL/Redis/media children inside the backend count in
its samples. Inspect component samples instead of treating the fixture as app RAM.

## Historical initial probe

`verified-playback.json` records the earlier short 1/10 viewer probe: one 10-second
trial per scenario after 2 seconds warmup, valid 75 frames decoded, no load errors,
and one shared upstream. It used an 180-second source that wrapped timestamps;
that configuration is not suitable for longer runs. Its source hashes identify
that exact earlier probe. The expanded runner below fixes duration handling and
has separate evidence; do not apply old results to changed infrastructure.

## Expanded matrix and lifecycle controls

The runner now defaults to 15 seconds warmup, 60 seconds measurement, and three
trials for each of 1/10/100 viewers. `--warmup`, `--duration`, `--timeout`,
`--trials`, and `--scenarios` make each trial explicit. A run records the exact
configuration, fixture hash, upstream path counters, source/tool hashes, decoder
progress, per-container resource samples, and upstream cleanup after each trial.
The old initial probe above remains historical evidence, not evidence for the
expanded source revision.

```sh
python3 scripts/rust-playback-baseline/run.py \
  --output artifacts/rust/baseline-matrix-new \
  --viewers 1 10 100 --trials 3 --warmup 15 --duration 60 \
  --fresh-web-per-trial --measurement-quality isolated-characterization
```

Use `--scenarios many-channel` for ten distinct real channels, or `slow-readers`
for ten intentionally slowed readers. The latter is **all slow readers**, not a
mixed slow/fast independence check; that remains a separate required gate.
`--start-signal /tmp/new-signal-file` waits after actual proxy decoder readiness
for a CPU-quiet measurement window. Do not reuse an existing signal file. Fresh
output directories are mandatory. `--measurement-quality load-smoke` is the
conservative default; concurrent development smoke data cannot establish gains.

The origin now generates a 1,800-second deterministic fixture by default, streams
its SHA256 computation without loading the entire file into Python memory, and
**closes at EOF instead of wrapping timestamps**. `--fixture-seconds` configures
60..3,600 seconds; insufficient duration for the matrix is rejected before Docker
starts. The fixture uses up to approximately 450 MB of origin tmpfs at the default
length. Therefore inspect per-container samples: changing fixture length changes
origin memory. Compare identical fixture length/hash and selected service
footprints; do not present fixture-memory differences as application savings.

The benchmark-only supervisor keeps PostgreSQL and Redis alive while the web
process can be stopped for paired Rust trials:

```sh
docker compose -p PROJECT -f docker/compose.rust-playback.yml exec -T baseline \
  /dispatcharrpy/bin/python /verification/supervisor.py control status
# Replace status with stop or start for that uniquely owned web process group.
```

Control is only through a container-local mode-0600 Unix socket. No application
code changes or production services are involved. SIGTERM closes web/PG/Redis.
`--fresh-web-per-trial` explicitly records web stop/start and the key count before
and after clearing **only that isolated benchmark container's Redis**. This
measures independent fresh trials; it does not fix or hide lifecycle failures.

Cold concurrent request bursts have produced zero-byte runs in the immutable
Python baseline, including after a fresh web restart/Redis reset. See
`cold-reopen-evidence.json` and raw artifacts; their validity gates failed.
The exact cause remains under investigation. The per-worker DB pool has eight
connections, but pool starvation is a hypothesis rather than an established
cause. `--warmup-ramp 4` records a different, explicit warmed-fanout scenario:
one viewer starts first and stays connected while other warmup viewers join four
seconds later. The measured phase still starts all viewers concurrently; there
is no extra unmeasured anchor viewer. A warmed-fanout success cannot satisfy the
separate cold-burst/reopen gate. Baseline and candidate must have identical ramp
settings, which the common comparison tool checks.

The origin accepts test-only POST `/control` JSON with `path`, `action`, and
optional `seconds`: `stall`, `drop`, `fail` (503 plus existing-connection drop),
and `recover`. Controls are per origin path; normal runs use no injected faults.
These controls have loopback regression tests but do not themselves prove backend
failover correctness. Normal matrix runs reject fixture EOF and require upstream
connections to disappear within `--cleanup-timeout` (default 60 seconds).

```sh
python3 -m unittest discover -s scripts/rust-playback-baseline -v
```

## Paired Python/Rust runner and source provenance

Add `--candidate-overlay docker/compose.rust-candidate.yml` to alternate Python
and Rust phases on the same fixture database/origin. The inactive web process is
stopped; PostgreSQL/Redis remain inside the baseline container. Python selects
`baseline origin`; Rust selects `baseline origin candidate`. Only fixture DB,
Redis and the supervisor shell remain in the baseline container during Rust runs.
Each phase records its service footprint, actual image ID, reset, decoder output,
and origin counters. Trial order alternates Python/Rust then Rust/Python.
The runner does not rebuild images during measurements.

For accepted isolated characterization, supply `--candidate-build-manifest` with
`build.json` from `scripts/rust-integration/build.py`. That manifest binds an
immutable Rust source snapshot to the image ID and source-tree label. The runner
checks the running container against both, hashes and records the manifest, and
fails on a mismatch. It also fails if benchmark tools or Compose files change
during the run. This permits unrelated code work while testing an immutable image,
but no CPU-heavy concurrent work can be part of an isolated performance run.

Current controlled benchmark settings explicitly use five seconds shutdown delay,
five seconds behind live, and60seconds retained media on **both** implementations.
They are ordinary database proxy settings, not edits to baseline code. The actual
settings are included in `seed.json` and the fixture identifier; compare only
identical values. The shutdown delay keeps warmup retention alive across measured
viewer reconnection. It cannot establish parity for default zero-delay lifecycle
behavior. Historical `paired-smoke-evidence.json` records a served-media smoke
whose throughput gate failed with the zero-delay configuration; no CPU/RAM gain
is claimed for those runs.

Redis listens only on the disposable test bridge, requires the public fixture
password `playback-fixture-only`, and has no host-published Redis port. Both
implementations use those credentials. `REDISCLI_AUTH` supplies credentials to
scoped reset/shutdown commands without embedding a production secret in arguments.

After all pairs the runner writes `comparisons.json`, applying the common errors,
resource-sample, equal-workload and95percent-throughput checks plus declared
CPU/memory/startup p95 ratio ceilings. `--max-cpu-ratio` and `--max-memory-ratio`
default to 1 (non-regression); freeze any improvement targets before measurements.
`--require-comparison` additionally requires at least three trials and 15/60 second
warmup/measurement, isolated quality, and a bound build manifest, and returns
nonzero if any pair fails. This is a native TS **subset** resource gate, not full
application acceptance. First-byte p95 for a single request is just that sample;
startup distribution gates require separate reconnect/churn workloads.
