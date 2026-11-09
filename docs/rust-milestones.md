# Rust migration milestones and acceptance gates

The target is the complete backend behavior at Python revision
`1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430`, with the existing React frontend.
[Architecture and compatibility inventory](rust-migration.md) explains the scope.
A milestone passes only when its implementation, contract tests, container run,
and performance evidence all pass. A scaffold, unit test, or fast synthetic buffer
alone does not establish application parity or an application performance gain.

## Progress ledger

Statuses: **planned**, **in progress**, **blocked**, **passed**. A blocked runtime
check remains unverified; it is never converted into a pass by documentation.
Update this ledger with report paths and tested commit IDs as evidence arrives.

| ID | Deliverable | Depends on | Current status | Evidence required to pass |
| --- | --- | --- | --- | --- |
| M0 | Reproducible baseline, toolchain, fixtures, and measurement | None | In progress | Rust checks; Compose readiness; Python baseline contracts and performance reports |
| M1 | Shared bounded streaming core | M0 | In progress | Buffer, TS, discontinuity, wakeup, shutdown tests; comparative core benchmark |
| M2 | Authenticated live TS vertical slice | M1 | Planned | Existing-client playback against both backends; lifecycle, limits, and resource comparison |
| M3 | Complete live playback formats and profiles | M2 | Planned | HLS/fMP4/profile parity, decoder checks, failover and fanout performance |
| M4 | VOD and timeshift parity | M2 | Planned | Range/seek/catchup and session parity; throughput, CPU, and memory comparison |
| M5 | Accounts, settings, and channel control APIs | M2 | Planned | API/permission/database/React workflow parity and API benchmarks |
| M6 | Provider ingestion, guide, matching, and scheduled jobs | M5 | Planned | Import/job parity; failure recovery; import and concurrent-playback benchmarks |
| M7 | Output protocols and realtime/integration behavior | M3, M4, M5, M6 | Planned | Xtream/HDHR/XMLTV/M3U/WebSocket/integration suites and performance reports |
| M8 | Backup, admin, plugins, and operational compatibility | M5, M6, M7 | Planned | Restore/rollback/plugin/admin/config tests; full-service resource accounting |
| M9 | Full parity, soak, packaging, and cutover rehearsal | M0–M8 | Planned | All contracts covered; Compose end-to-end, soak and performance acceptance; rollback rehearsal |

Current evidence is described in the [local verification guide](rust-local-verification.md).
The core Compose run has passed, the load-harness Compose selftest has passed,
and Python baseline runs have passed (48 focused tests and a separate broader
526-test proxy suite). Static source discovery and
runtime discovery are available; the runtime resolves 592 Django routes and 246
OpenAPI paths/362 operations, with 58 schema warnings and 66 graceful-fallback
error diagnostics that omit views with uninferred serializers. None of those discoveries is complete executable
contract coverage. A measured TS helper comparison is component-only. Full-stack
playback baselines, frozen scenario budgets, decoder checks, complete M1 gates,
and the required application integration remain outstanding.

M3/M4/M5 can proceed concurrently after M2, using separate owned modules and
contract fixtures. M6 can proceed once its M5 models and API contracts stabilize.
Independent contract discovery, fault-fixture development, and test porting can
run throughout. Integration stays sequential: each change lands only after its
shared contracts and prerequisite gates pass.

## Evidence and performance rules for every milestone

For each run preserve: tested Git commit and dirty patch, Python baseline commit,
image IDs/digests, Rust/Python/tool versions, Docker/Compose versions, kernel and
architecture, CPU/RAM and container limits, scenario/configuration, fixture
hashes, warmup/measurement duration, raw samples, summarized results, logs,
exit codes, and declared normalizations. Store reports in an ignored local
artifact directory and keep small sanitized result summaries in version control.
Never record provider passwords, tokens, or real subscriber traffic in fixtures.

Compare a pinned Python build and a Rust build using the same deterministic
fixture source, equivalent seeded datasets, profile, output format, viewer load,
and resource limits. Run sequentially on the same machine; simultaneous backends
compete for resources and invalidate a clean comparison. Use isolated databases,
Redis namespaces, and volumes to prevent one implementation affecting the other.
Record at least three measured repetitions after warmup; alternate backend order.
Treat noisy or incomplete runs as inconclusive and rerun after finding the cause.

Measure the total service group (API/proxy, job workers, Redis, database, FFmpeg,
and compatibility subprocesses) and each component. Include CPU seconds per
wall-clock second, memory current/peak, memory after disconnect/cleanup, startup
p50/p95/p99, successful output bytes and bitrate, errors, upstream connection
count, and media process count. Record cgroup memory and CPU where available;
`docker stats` samples alone may miss short-lived peaks and omit nested child
attribution. Retain metrics-collection capability and failures in the report.

Report relative change as `(Rust - Python) / Python * 100%`; lower CPU/RAM/latency
is better. Normalize CPU by successfully delivered media GiB for playback and by
completed objects/requests for jobs/APIs. Do not claim savings when Rust delivers
less media, drops viewers, skips validation, or performs less background work.
A Rust-only measurement is a useful characterization, not a measured gain.

[The unfrozen budget template](rust-performance-budgets.example.json) captures
required fields and intentionally cannot pass acceptance. The separate
[HTTP harness budget template](rust-harness-budgets.example.json) is consumed by
`scripts/rust-perf/milestone.py` for strict repeated-pair resource checks; that
subset does not validate media or cleanup and cannot pass a full milestone alone.
M0 freezes numeric budgets in a scenario manifest before implementation results
are used to choose them. Required fields: minimum valid media throughput,
maximum error count, startup p95 ceiling, CPU ratio ceiling, memory ratio ceiling,
cleanup deadline, and allowed post-cleanup growth. For functionality-only
milestones set CPU/memory non-regression budgets; for streaming milestones set
an explicit improvement target based on the observed baseline and noise floor.
The report must fail if required fields, baseline samples, or sufficient successful
work are absent. Until budgets are frozen and measured, performance is pending.
The final goal requires a material total-service improvement under viewing load,
not just improvement in one function or process.

Minimum scenario matrix:

| Scenario | Purpose |
| --- | --- |
| Idle after startup and after previous playback | Base footprint and leak detection |
| 1, 10, and 100 viewers on one channel | Shared ingestion and per-viewer scaling |
| 10 distinct channels, then configured capacity | Per-channel memory/CPU and slot limits |
| Slow reader plus fast readers | Backpressure, bounded memory, unaffected fast viewers |
| Connect/disconnect and reconnect churn | Slot accounting, sockets, tasks/process cleanup |
| Upstream stall, failure, source change, and recovery | Timeouts, failover, discontinuity, retry behavior |
| Import/EPG refresh during playback | Background contention and viewer latency |
| Sustained playback followed by disconnect | Retention bound, long-run growth, cleanup |

## Local Docker Compose contract

The migration stack lives at `docker/compose.rust.yml`, the Rust validation image
at `rust/Dockerfile`, and performance tooling under `scripts/rust-perf/`. Invoke
Compose with `docker compose -f docker/compose.rust.yml ...`; consult that file and
the harness documentation for implemented profiles and commands. The complete
stack described below is an acceptance requirement, not a claim that all its
services already exist.


Use a migration-specific Compose stack alongside the production definitions;
production deployment defaults must remain usable throughout the migration.
The final harness needs services/profiles for a fixture origin, PostgreSQL,
Redis, pinned Python baseline, Rust candidate, and a test/load runner. M0 may
start with a smaller Rust-only developer stack, but that does not pass baseline
or parity acceptance. Add only locally generated media and mocked providers;
never use a paid provider as the benchmark source.

A single documented runner must validate `docker compose config`, build images,
start dependencies, wait on health checks with a finite deadline, seed isolated
data, execute assertions, collect reports/logs, and propagate test failures as a
nonzero exit. Use unique project names and dedicated disposable volumes. Cleanup
must address only that test project's resources. Support retaining failed runs
for inspection. Bind exposed test ports to loopback. Container health must test
readiness for the milestone rather than merely that a process exists.

Execute the same suite from a fresh checkout/clean volumes and an existing-data
upgrade. Test graceful SIGTERM and forced worker death. Restart tests must verify
no orphan media subprocess, stale ownership lease, leaked profile slot, or
unreachable channel survives the documented recovery deadline. Do not route
active real channels to Rust until M2's ownership and rollback gate passes.

## M0 — baseline and executable contracts

* Install Rust/Cargo, pin a reproducible toolchain, and run workspace tests,
  formatting, and lint checks locally and in the Rust image.
* Resolve Django URLs including DRF router-generated actions, not only literal
  `path` calls. Snapshot OpenAPI and inventory non-OpenAPI URLs from
  `dispatcharr/urls.py`, `apps/output/urls.py`, `apps/hdhr/urls.py`, and proxy URLs.
* Build a contract register: unique ID, source handler/test, method/path or event,
  role, setup fixture, assertions, owning milestone, coverage state, and result.
  Inventory environment options, management commands, schedules, schema,
  plugin context APIs, and admin workflows separately from HTTP routes.
* Generate deterministic TS, fMP4, HLS, and seekable VOD fixtures plus malformed,
  stalled, redirected, ranged, and failure-injected upstream responses.
* Run the pinned Python tests using the existing backend test-discovery/bootstrap
  workflow (`scripts/ci_backend_test_labels.py`, `scripts/ci_bootstrap_backend.sh`)
  in its supported image. Preserve pre-existing failures separately; do not
  silently accept failures in contracts targeted by the rewrite.
* Capture baseline scenarios above and freeze per-scenario performance budgets.

Pass: another developer can reproduce contracts and measurements using Compose;
no baseline or tool unavailability remains hidden behind a passing status.

## M1 — streaming core

Add TS alignment across arbitrary input chunk boundaries, truncated-packet
handling, time-based startup positioning, lag recovery, discontinuity stamping,
bounded retention, independent viewer cursors, notifications without lost
wakeups, cancellation, and shutdown. Define the adapter from Python's
last-consumed cursor to Rust's next-to-read cursor explicitly.

Port expectations from `test_ts_discontinuity.py`,
`test_buffer_discontinuity.py`, and relevant segmenter/buffer tests under
`apps/proxy/live_proxy/tests`. Add concurrent slow/fast reader and idle expiry
cases. Bound retained media, queued transport references, channel count, and
in-flight client data; a ring byte cap alone is insufficient.

Pass: deterministic core tests and container execution pass; memory remains
bounded under slow readers; core benchmark compares equivalent Python and Rust
work. Label these as core results, distinct from full playback gains.

## M2 — authenticated TS playback vertical slice

Implement route/channel resolution, existing authentication and authorization
semantics, pool/profile limits, upstream HTTP delivery, channel ownership/leases,
viewer registration, stats, stop, failure/keepalive deadlines, and release-once
cleanup. Preserve anonymous capability behavior where the baseline permits it;
do not globally add or remove authentication.

Use existing `test_stream_ts_client_registration.py`, `test_start_race.py`,
`test_ts_generator_release.py`, `test_metadata_slot_release.py`,
`test_slot_release_helpers.py`, `test_process_tree_cleanup.py`,
`test_health_reconnect.py`, and `apps/proxy/tests/test_stream_limits.py` as
contract seeds. Test two workers contending for the same channel, source death,
client cancellation, and Rust rollback. The unchanged frontend must play a seeded
channel and show correct viewer stats.

Pass: Compose delivers decoder-valid TS with expected auth and ownership;
repeated connection/disconnection returns resource counts to baseline; measured
single-channel fanout meets the frozen performance budgets.

## M3 — live formats, profiles, and failover

Implement profile transforms and output sharing, fMP4 init/fragment delivery,
native HLS segmentation/playlist/segment/capability semantics, session stop,
TTL refresh, and source switching. Preserve supported FFmpeg/VLC/profile
arguments and accounting. Count external process costs in reports.

Port `test_hls_segmenter.py`, `test_hls_views.py`,
`test_hls_playlist_waiters.py`, `test_hls_cold_start_lua.py`,
`test_hls_manager_discontinuity.py`, `test_hls_session_stop_api.py`,
`test_output_ttl_refresh.py`, `test_fmp4_remux_interleave.py`,
`test_failover_retry_window.py`, and profile/stream-switch tests.

Pass: player/decoder checks cover each format and switch; many viewers share the
expected pipelines, with no new process per equivalent viewer; Compose fault
runs and per-format performance budgets pass.

## M4 — VOD and timeshift

Preserve HTTP Range, Content-Range, HEAD if supported, seeking, extension and
content-type behavior, redirect validation, alternate provider selection,
connection limits, profile restrictions, XC movie/series IDs, timeshift URL
formats, and cleanup. Cover missing IDs and upstream errors exactly.

Port `apps/proxy/vod_proxy/tests`, `apps/timeshift/tests`, and corresponding
output tests. Pass: compare headers/statuses/body ranges, decoder-seek behavior,
concurrent limits, failure recovery, and post-session resource counts in Compose;
VOD/catchup throughput and memory/CPU meet budgets.

## M5 — accounts, settings, and channel APIs

Port account setup/login/proxy-login/logout, API keys, JWT refresh, user/group
permissions, per-user app data, settings, profiles, channels, groups, logos,
filters, sorting, pagination, validation, and defaults. Preserve existing hashes,
IDs, constraints, timezone handling, trusted-proxy rules, and PostgreSQL/SQLite
behavior. Enforce the same role-specific visibility and errors.

Pass: contract register covers each router action and role; existing DB fixtures
work without resetting passwords or IDs; unchanged React workflows pass against
Rust; API load and idle-footprint budgets pass. Keep schema migration ownership
single and changes additive during coexistence.

## M6 — imports, guide, matching, and jobs

Port M3U/Xtream/VOD imports, XMLTV/EPG, scheduled refresh, progress updates,
matching/search, filters, deduplication, partial-failure preservation, retry and
cancellation semantics. Inventory Celery/beat tasks before replacing them;
in-memory timers do not preserve durable scheduling and retry behavior.

Seed contracts from `apps/m3u/tests`, `apps/epg/tests`, `apps/vod/tests`,
`apps/m3u/tasks.py`, `apps/epg/tasks.py`, `apps/epg/sd_tasks.py`, and
`apps/vod/tasks.py`. Include compressed input, empty/incomplete upstream data,
restart during import, schedule timezone boundaries, and DB recovery.

Pass: imported objects and retained details match baseline fixtures; durable
jobs survive interruption without duplication; guide/import throughput, peak
memory, and playback-during-refresh budgets pass in Compose.

## M7 — protocols, realtime, and integrations

Port M3U/XMLTV/Xtream outputs, HDHomeRun endpoints/discovery, stats payloads,
WebSocket messages and connection auth, events, notifications, and `apps/connect`
behavior. Preserve response shape and wire-format details used by real clients.
`dispatcharr/consumers.py` restricts connection telemetry and admin notifications;
cover unauthorized, standard, and admin sockets rather than only happy paths.

Pass: `apps/output/tests`, HDHR tests, connection/event tests, and WebSocket
contracts pass through actual container networking. M3U/XMLTV generation,
WebSocket fanout, and integration delivery performance meet budgets.

## M8 — operations and remaining compatibility

Implement compatible backup/restore, supported admin workflows, logs, static/media
handling, CLI/management operations, environment and TLS options, upgrades,
observability, and Python plugin/provider/matching dependency strategy.
Existing Python plugins may import Django APIs and invoke Celery. An isolated
interpreter without those APIs is not compatible. Keep a tested compatibility
runtime until equivalent behavior exists; account for its memory and CPU.
No plugin or admin behavior may disappear under a full-parity claim.

Pass: fixture plugins exercise documented settings/actions/context/imports;
backup round trips preserve IDs/hashes/media references; restored data works
against both implementations; Compose covers supported configurations, SQLite
and PostgreSQL, startup and graceful shutdown. Report full-service idle and
active resource totals including compatibility workers.

## M9 — release and cutover rehearsal

All contract IDs must be covered by passing executable tests or documented as
unresolved; unresolved required behavior blocks full parity. Run all backend
contracts, unchanged frontend workflows, fault scenarios, fresh install and
upgrade, and a sustained soak (at least two hours, including refresh activity)
with post-soak cleanup. Inspect resource growth and decoder errors.

Pass: every preceding gate passes on the release images; CPU/RAM improvement
holds for the complete service group under viewing load; no functionality is
removed to obtain it. Rehearse rollback on copied data and publish exact image,
configuration, migration, restore, and rollback steps. Production deployment is
a separate action; local acceptance does not itself alter the live service.
