# Local Rust migration verification

The React UI stays. The Rust workspace currently verifies streaming components;
it is not a runnable replacement for the Dispatcharr backend. Follow the
[milestone acceptance ledger](rust-milestones.md) and
[architecture inventory](rust-migration.md) when assessing progress. M0 and M1
remain **in progress**, even when the developer checks below pass.

## Prerequisites and scope

Run commands from the repository root. Use Python 3.9 or newer, Git, Docker Engine
with a working daemon and Compose v2, and enough space to build images. The
baseline image is pinned for Linux/amd64; other architectures need compatible
emulation or an explicitly qualified baseline image. Local host Rust checks need
Rust 1.90.0 with rustfmt and Clippy, as pinned by `rust/rust-toolchain.toml`.
The Docker validation image supplies Rust/Python for container checks, so host
Rust is optional for that path. Image pulls/build dependencies initially require
network access; the core verification container itself runs without networking.
Docker requires daemon/socket permission, and the harness selftest opens only a
local loopback fixture port (19292 by default).

The development session installed disposable tools under `/tmp`, without changing
system package configuration. To use those tools while they remain present:

```sh
export CARGO_HOME=/tmp/dispatcharr-cargo
export RUSTUP_HOME=/tmp/dispatcharr-rustup
export RUSTUP_TOOLCHAIN=1.90.0
export DOCKER_CONFIG=/tmp/dispatcharr-docker-config
export DOCKER_HOST=unix:///tmp/dispatcharr-docker.sock
export PATH="/tmp/dispatcharr-cargo/bin:/tmp/dispatcharr-docker:$PATH"
docker version
docker compose version
```

These paths and the isolated Docker daemon are session-local, not a permanent
installation or daemon startup procedure. After `/tmp` cleanup/reboot, install
and start Docker/Compose through the host's supported tooling and install the
pinned Rust toolchain, or use the container validation path with normal Docker.
Do not export the session socket path when using a different Docker daemon.

## Repeatable developer verification

The preferred runner writes metadata, source hashes including untracked migration
files, image IDs, logs, commands, exit codes, and component evidence. It uses a
unique Compose project and removes that project's resources after the run.

```sh
python3 scripts/verify-rust-compose.py
```

The printed evidence directory defaults to `artifacts/rust/<UTC timestamp>/`.
Inspect `verification.json` and `compose.log`, not only the final console line.
A nonzero exit is a failed verification. `--evidence-dir /absolute/path` changes
where evidence is written, `--timeout` bounds each Compose step, and `--keep`
retains the project for debugging. Use a unique empty evidence directory per run
so later runs do not mix old results into new evidence.

The equivalent low-level Compose commands are:

```sh
docker compose -f docker/compose.rust.yml config --quiet
docker compose -f docker/compose.rust.yml up --build --abort-on-container-exit --exit-code-from verify
docker compose -f docker/compose.rust.yml down --volumes --remove-orphans
```

The low-level commands use the file's default project and do not collect the
runner's full metadata. The verification image runs formatting, Clippy, Rust
workspace tests, Python harness/inventory unit tests, TS helper differential
checks, a buffer microbenchmark, and the paired Python/Rust helper benchmark.
Observed verified component checks include the core Compose runner and synthetic
load-harness selftest; subsequent code changes require a fresh run and evidence
source-hash match. Consult `verification.json` and the test log for the exact
workspace test counts and source version covered by each run. The latest
[verification summary](rust-verification-evidence.json) records 20 passing Rust
tests (ten core, ten runtime), 20 Python tool tests, and 1,806 differential cases
in Compose, with successful cleanup. The runtime includes a four-worker stress
test in which 100 readers each receive all 1,000 published sequences.

The Compose service is a finite test job rather than an HTTP application, so
there is no application health endpoint or React playback smoke test yet.

For faster host iteration, explicitly select the pinned toolchain. Cargo invoked
from the repository root does not discover a toolchain file nested in `rust/`
merely through `--manifest-path`:

```sh
export RUSTUP_TOOLCHAIN=1.90.0
cargo fmt --manifest-path rust/Cargo.toml --all -- --check
cargo clippy --manifest-path rust/Cargo.toml --locked --workspace --all-targets -- -D warnings
cargo test --manifest-path rust/Cargo.toml --locked --workspace --all-targets
python3 -m unittest discover -s scripts/rust-contracts -v
python3 -m unittest discover -s scripts/rust-perf -v
python3 scripts/rust-parity/ts_discontinuity.py
```

## Contract inventory and Python baseline

```sh
python3 scripts/rust-contracts/inventory.py --output docs/rust-contract-inventory.json --summary
```

The [checked-in static candidate inventory](rust-contract-inventory.json) lists
source locations for routes, includes, routers/actions, models/migrations,
tasks, commands, serializers/viewsets, WebSockets, and tests. The
[inventory README](../scripts/rust-contracts/README.md) explains classification
and deterministic regeneration. It does not import Django or resolve routes.

The baseline runner archives the exact Python source revision, runs existing
Django tests in a digest-pinned application image with ephemeral PostgreSQL and
Redis, and collects dependencies and runtime contracts. Source is mounted read
only; the app data, media, and test database are disposable. It requires a new
empty output directory for each run:

```sh
python3 scripts/rust-baseline/run.py --output /tmp/dispatcharr-baseline-new-run
```

Default labels cover existing TS discontinuity, buffer discontinuity, and HLS
segmenter tests. Pass explicit Django labels after the options to broaden the
suite. `tests.log` holds the actual output; `run.json` records the source/image
and exit code. `dependencies.json` records the baseline runtime. The final
`runtime-contracts.json` is emitted only if tests and extraction succeed. One clean
focused run completed all 48 default tests and runtime extraction successfully,
resolving 592 Django routes and 246 OpenAPI paths/362 operations. The schema
reported 58 warnings and 66 graceful-fallback error diagnostics for views whose
serializers could not be inferred; those views are omitted from the generated
schema. The [runtime snapshot metadata](rust-runtime-contracts.json) records
these limitations. A broader run of the live proxy, proxy, and VOD proxy suites
also passed 526 tests; this is a separate run, not 526 additional tests beyond
the focused 48.
Do not infer success from the existence of a directory or a partially written
log. The source revision is pinned, but the image's dependency versions are its
own recorded environment and must be checked for compatibility with that source.

That baseline runner covers behavior tests. A separate
[actual playback characterization](../scripts/rust-playback-baseline/README.md)
now runs the immutable Python source through one gevent uWSGI worker with internal
PostgreSQL/Redis and a valid local TS origin:

```sh
python3 scripts/rust-playback-baseline/run.py --output /tmp/playback-new-run --viewers 1 10 --trials 1
```

The [recorded probe](../scripts/rust-playback-baseline/verified-playback.json)
decoded 75 frames over three seconds. One-viewer aggregate CPU averaged 3.24%
of one core with 328.5 MB sampled peak memory; ten viewers averaged 6.44% and
334.8 MB. Each used one shared upstream connection with no load errors.
These are one ten-second trial per scenario after two seconds of warmup, covering
the backend and origin containers. Celery, Daphne, nginx, the React client,
transcoding, and other production background work are excluded. They establish
an initial playback characterization, not steady-state or full-application
performance, a completed M0 gate, or any measured Rust improvement. The full
scenario matrix, repeated trials, longer warmup/measurement, and candidate
comparisons remain required. This test bridge publishes only loopback ports;
it does not enforce outbound-network isolation.

## Performance checks and interpretation

Validate the Docker load harness independently:

```sh
python3 scripts/rust-perf/selftest.py --output-dir /tmp/dispatcharr-perf-selftest-new
```

This runs two synthetic, paced byte-stream workloads, verifies container
selection and cleanup counters, accepts an identity comparison, and rejects an
injected load error. [Recorded selftest evidence](../scripts/rust-perf/selftest-evidence.json)
reports a successful isolated run. It measures the fixture, not video correctness
or a Python-to-Rust backend gain.

Measure the actual existing Python discontinuity helper against its Rust port:

```sh
python3 scripts/rust-parity/benchmark.py --output /tmp/ts-helper-comparison.json
```

The paired benchmark performs warmup and at least three samples in alternating
backend order, validates checksum/PID counts, and reports wall time/CPU and
process peak RSS where available. [Differential checks](../scripts/rust-parity/README.md)
compare helper behavior over a seeded malformed/valid TS corpus. One verified core Compose run measured a median Rust/Python wall-time ratio
of 0.0327251 (about 30.6 times faster for that workload). These results
apply to that helper only: interpreter startup/imports influence process RSS,
and real playback has many other costs. The fanout microbenchmark similarly
characterizes retention and reference sharing rather than network delivery.

When both real playback stacks exist, use the [playback harness documentation](../scripts/rust-perf/README.md)
to run `perf.py run` against each configured local stack sequentially, selecting
all API/worker/Redis/database/media containers. Compare identical media/profile,
viewer/channel counts, timing, and resource limits, with at least three paired
trials per required scenario. The current harness reads continuous response
bodies; it neither decodes media nor follows HLS playlists. Decoder, HLS,
permission, ownership, cleanup, and fault checks are separate required gates.

[Full milestone budgets](rust-performance-budgets.example.json) deliberately
have `frozen: false` and null unknowns. They include media-validation and cleanup
requirements beyond the HTTP harness's current capabilities and cannot pass a
milestone gate. Freeze measured baseline provenance and numerical budgets before
evaluating candidate results.

The stricter [HTTP harness budget template](rust-harness-budgets.example.json)
uses a separate scope supported by `scripts/rust-perf/milestone.py`. It also
intentionally fails until populated and frozen. Provide at least three distinct
baseline/candidate pairs for every frozen scenario; each pair must pass. The
aggregator checks explicit image/revision labels, complete selected-service lists,
host/Docker identity, workload parameters, throughput, first-byte p95/sample count,
resource ratios, and report validity. It rejects missing/null/unknown budget
fields, unfrozen budgets, duplicated evidence, and incomplete scenario matrices.
The first-byte metric is HTTP response latency, not validated video startup.

```sh
python3 scripts/rust-perf/milestone.py \
  --budget /tmp/frozen-harness-budgets.json \
  --evidence /tmp/paired-report-manifest.json \
  --output /tmp/harness-gate-result.json
```

The evidence manifest references report paths relative to itself:

```json
{
  "scenarios": [{
    "id": "same-channel-10-viewers-ts-copy",
    "pairs": [
      {"baseline": "baseline-1.json", "candidate": "candidate-1.json"},
      {"baseline": "baseline-2.json", "candidate": "candidate-2.json"},
      {"baseline": "baseline-3.json", "candidate": "candidate-3.json"}
    ]
  }]
}
```

`perf.py compare` remains a single-pair diagnostic. Even a passing repeated-pair
HTTP resource gate reports `application_performance_verified: false`: it cannot
establish decoder correctness, HLS behavior, source equivalence, or post-disconnect
cleanup. Those additional checks and the full scenario matrix remain milestone
requirements. No complete playback performance gain has been established by the
component or fixture checks.

## Required contract register entries

The static inventory supplies investigation candidates. The executable contract
register still needs an entry per concrete behavior, including generated router
actions, jobs and non-HTTP interfaces. Minimum entry shape:

```json
{
  "id": "live.ts.viewer-disconnect.release-once",
  "milestone": "M2",
  "source": "apps/proxy/live_proxy/tests/test_ts_generator_release.py",
  "interface": "live TS viewer disconnect",
  "role": "baseline-authorized viewer",
  "fixture": "seeded-local-channel-and-profile",
  "assertions": ["viewer removed", "slot released once", "other viewers continue"],
  "test_command": null,
  "coverage_state": "unimplemented",
  "baseline_result": null,
  "candidate_result": null,
  "evidence_path": null
}
```

Null tests/results and unresolved required entries block coverage acceptance.
A source candidate count is not a passing-test count. Keep report artifacts and
the tested source/image identity with each result; add compact sanitized
summaries to the ledger only after inspection.
