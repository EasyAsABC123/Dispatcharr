# Real-schema Rust compatibility probe

The original Dispatcharr authors and contributors receive full credit for the
application and contracts being ported. See [ATTRIBUTION.md](../../ATTRIBUTION.md).
This harness verifies a partial derivative backend; it does not establish full
backend parity or application performance savings.

Build a source-bound immutable candidate, then run the disposable Compose probe:

```sh
python scripts/rust-integration/build.py --output artifacts/rust/candidate-build-1 --tag dispatcharr-rust-candidate:integration-1
python scripts/rust-integration/run.py --output artifacts/rust/integration-1 \
  --candidate-image dispatcharr-rust-candidate:integration-1 \
  --candidate-build-manifest artifacts/rust/candidate-build-1/build.json
```

The runtime bundles a production build of the original React frontend. Validate
its unchanged test suite independently with the same immutable build mechanism:

```sh
python scripts/rust-integration/build.py --target frontend-tests \
  --output artifacts/rust/frontend-tests-1 --tag dispatcharr-rust-frontend-tests:local
```

The first pinned-Docker run passed all 6,567 frontend tests; its sanitized
[evidence](verified-unchanged-react-tests-1.json) records every frontend input
hash. This is separate from browser-to-Rust workflow parity, which remains open.
Unimplemented backend paths return a JSON 404 instead of the React navigation
fallback. Normal React navigation paths receive the built application.

After a successful `run.py --keep` trial, verify production assets, navigation,
HEAD/authentication guards and bounded process shutdown on that retained fixture:

```sh
python scripts/rust-integration/runtime_smoke.py \
  --integration-evidence artifacts/rust/integration-1 \
  --output artifacts/rust/runtime-1
```

This intentionally sends SIGTERM to that project's candidate while a native TS
body and an incomplete HTTP request remain open. It checks successful exit under
14 seconds and actual Redis channel cleanup. It does not remove the retained
project; the original report records the exact Compose files and fixture path
for cleanup. The [first combined image](verified-runtime-shutdown-1.json) passed.

Use fresh output directories. Docker Engine, Docker Compose, and Buildx must be
available. The build runner records the normalized source archive hash, every
included file hash, image ID, worktree status, and build log. The integration
runner archives the pinned original Python revision and uses its actual migrated
PostgreSQL schema, Redis, Django password hashes, and SimpleJWT implementation.
It starts only disposable services with loopback host ports and fixture-only
credentials. It removes that Compose project's containers and volumes on exit;
`--keep` deliberately retains the fixture and its source directory for diagnosis.
If Compose cleanup fails, the source directory is also retained so the recorded
project can still be inspected and removed. The report records retained paths.
The runner verifies the container image ID and source label against the build
manifest; an old image cannot silently supply evidence for a newer source tree.
Probe modules, integration helpers, baseline fixture scripts and Compose files
are copied into the report's `tool-source` directory before startup. Later
imports and container mounts use those captured bytes, whose hashes are recorded,
so parallel repository edits cannot change an active trial's test code.
The default runs every contract suite. For a new isolated oracle, use
`--contract-suites group_cleanup` (or other comma-separated suite names); schema,
authentication and decoded-media checks still run, and the report lists the
selected suites explicitly. Unknown or duplicate names fail before startup.

Assertions cover the currently implemented account contracts and native media
slice. The report records status/body digests, role checks, token interoperability,
and real FFmpeg decoding. Account probes include owner updates, API-key rotation
and revocation, administrator user/group CRUD, and app-data isolation, JSON
representation, quotas, and concurrent writes. Fixtures include administrator,
standard, level-zero, and inactive users. JSON comparisons use canonical digests
so Python's equality between booleans, integers, and floats cannot hide a mismatch.
Fixture credentials stay in the runner pipe and are
excluded from the checked-in evidence summaries. Raw local evidence is ignored
by Git. Consult [milestones](../../docs/rust-milestones.md) for outstanding work.

An integration pass is functional evidence. Performance comparison additionally
requires equivalent background work, valid delivered media, repeated paired
trials, resource accounting for all services, and cleanup checks. The earlier
prototype matrix was halted because its candidate lacked per-viewer telemetry;
its partial results must not be presented as verified savings.
