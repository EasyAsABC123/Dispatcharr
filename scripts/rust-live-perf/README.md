# Authenticated continuous live playback preflight

Original Dispatcharr authors retain full application/algorithm credit and
licensing. Django, DRF, FFmpeg and dependency authors retain their notices.

[Reviewed image47 evidence](verified-live-functional-image47-1.json) passes six
actual Source/native phases:1,10,100 viewers on each backend,222 continuous
connections in total. Every reader uses a real fixture user's Bearer token, and
Redis client metadata must identify that user. The token comes from pinned
SimpleJWT's factory with a two-hour fixture expiry; this fixture does not test
login. Tokens remain in memory. Anonymous decoder-primer clients must retire
before the authenticated readers start.

Each request stays open across five seconds of warmup and five seconds of
observation, with no retry, HTTP failure or premature EOF. Readers wait at a
post-window barrier before connection closure. Each captures a bounded2MiB raw
TS prefix; every captured prefix passes a strict bounded five-second audio/video
decode with at least25 video frames. All222 hashes and decodes were independently
reviewed. The upstream returns to zero active connections and owned Compose
resources are removed. The Source uWSGI/Daphne and native backend each have
2CPU/512MiB limits, sharing identical separate PostgreSQL/Redis and origin services.

This is functional evidence, **not a resource comparison or M2 completion**.
No resource sampler runs. The short window does not accept sustained throughput
or CPU/memory/p95 budgets. Provider/user slot growth, full event/fault/churn and
owner-loss behavior, and full service accounting remain separate gates.

## Reproduction

From the repository root with Docker and an immutable passing app build manifest:

```sh
python3 scripts/rust-live-perf/run.py --repo . \
  --build-manifest artifacts/rust/YOUR_IMMUTABLE_BUILD/build.json \
  --output artifacts/rust/live-functional-NEW
python3 -m unittest discover -s scripts/rust-live-perf -p test_workload.py -v
```

The [published-tree replay](verified-runner-public-replay-1.json) verifies the
snapshot-copy correction with two fresh authenticated Source/native phases. The
runner now merges its exact current tools into the committed archive's existing
tool directory. The earlier222-capture evidence keeps its original binding; the
correction changes no Rust application code.

The output must be new. Default viewer cohorts are1,10,100. `--viewers 1` runs a
small real preflight. The runner archives the pinned Original revision, freezes
committed support plus its own exact tools, checks native image labels/source
provenance and retains per-reader media hashes and execution evidence. Its
Docker Compose fixture uses local published loopback ports and synthetic owned
data; no real provider configuration is loaded. It never builds an implicit
mutable Rust candidate.

Four loopback controls cover bounded prefix capture, actual Authorization header,
one continuous request, observer failure/physical closure and rejection/EOF with
no hidden retry. A disposable owned database and temporary fixture users are used
only by the actual Source/native runner.

## Capture boundaries and decoder diagnostics

An arbitrary1MiB cutoff failed strict full-tail decoding on the Original itself.
It can begin between H264 SPS/PPS/IDR units and end inside a PES/TS packet. That
failed capture is retained. Before examining Native playback, Source-only
qualification selected a fixed2MiB capture and five-second output decode, leaving
room for the fixture's two-second GOP resynchronization and excluding an arbitrary
truncated tail. The minimum25-frame requirement was preserved.

Successful input probing still emits startup diagnostics on all111 Original
prefixes in this run; they are retained separately. The111 Native prefixes emitted
none. This does not establish a clean-every-byte raw-prefix result or byte equality
between timed Source/native responses. Bounded playback decoding and whole-media
byte equality are different assertions.

To repeat all decodes while retaining successful stdout/stderr, use the pinned
Original image. Mount a complete runner output at `/captures/all`, this tools
folder at `/tools`, and a new writable audit directory at `/audit`:

```sh
mkdir artifacts/rust/live-prefix-audit-NEW
docker run --rm --network none --read-only --cap-drop ALL \
  --security-opt no-new-privileges:true --cpus 2 --memory 512m \
  --tmpfs /tmp:rw,size=16777216 \
  -v "$PWD/artifacts/rust/live-functional-NEW:/captures/all:ro" \
  -v "$PWD/scripts/rust-live-perf:/tools:ro" \
  -v "$PWD/artifacts/rust/live-prefix-audit-NEW:/audit" \
  --entrypoint /dispatcharrpy/bin/python \
  ghcr.io/dispatcharr/dispatcharr@sha256:34f9011514edd31ef0ff111b5f256d2e37614fcb08a6ee1ed9eccec9251a856f \
  /tools/audit_prefixes.py --captures /captures/all --output /audit
```

The auditor requires the complete six-phase1/10/100 matrix. `--captures` accepts
multiple run directories when cohorts were executed separately. It rejects
changed prefix hashes, fewer frames, nonzero decoder exits or an incomplete
matrix. It starts no resource sampler. The retained reports/222prefixes are local
evidence; their hashes and reviewed observations are published, not444MiB of media.

The protocol lists prospective resource budgets, but this runner exposes only
functional execution. Source-only isolated qualification, sustained paired
measurement, full lifecycle/accounting and formal M2 acceptance remain pending.
Prior cold100-viewer failures and older performance bindings remain unchanged.
