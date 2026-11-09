# Rust backend migration

Baseline: `1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430`.
The React frontend stays. Backend behavior, persisted data, playback URLs,
permissions, and integrations are the compatibility target. This is an incremental
migration; the Rust workspace is not yet an application replacement.

## Source findings and performance baseline

The source tree contains 612 Python files across apps, core, and dispatcharr
(including tests and migrations). The backend includes Django/DRF, Celery,
Channels, Redis, and external media processes.

* `apps/proxy/live_proxy/input/buffer.py` copies incoming data into aligned TS
  chunks, writes media into Redis, and retrieves batches for individual viewers.
* `apps/proxy/live_proxy/output/ts/generator.py` maintains per-viewer cursors,
  reads the buffer, checks state, updates stats, and sleeps between empty reads.
* `apps/proxy/live_proxy/output/buffer.py` retains additional format-specific
  media in Redis. Its reads can retrieve the full range from a cursor to head.
* `apps/proxy/live_proxy/input/manager.py` launches external stream tools;
  `output/fmp4/manager.py` invokes FFmpeg. Native HLS segmentation already uses
  packet copy without spawning another encoder.

These are investigation targets, not measured causes of the reported CPU/RAM
usage. No production runtime was profiled. Rewriting orchestration does not
remove encoding costs. Measure web/worker processes, Redis, FFmpeg, and the
browser separately before attributing savings to Rust.

Use deterministic local TS and VOD fixtures, fixed encoding profiles, and identical
hardware for old/new comparisons. Record idle, one viewer, 10 and 100 viewers on
one channel, many distinct channels, slow readers, reconnect churn, and prolonged
upstream failure. Record total service memory (including Redis and media tools),
per-process RSS/PSS, CPU seconds, Redis operations/bytes, active media processes,
startup latency, output bitrate, dropped/discontinuous packets, and cleanup time.
Include sustained runs after warmup to detect leaks. Establish numerical
acceptance thresholds from that baseline, rather than promising an unmeasured gain.

## Target architecture

Use an asynchronous HTTP service with separate modules for authentication,
database access, provider ingestion, jobs, streaming, and API compatibility.
Select and pin runtime/HTTP/database dependencies when adding the service;
the initial stream-core crate uses only the standard library.

One channel owner handles upstream input and one output pipeline per required
profile/format. Viewers share immutable payloads with independent cursors. Bound
retention by both bytes and age; bound transport queues and in-flight chunks too.
A slow viewer must neither block ingestion nor retain an unlimited queue.
Use notifications with a sequence recheck around waiting to avoid lost wakeups.
Keep network I/O outside buffer locks. Batch stats updates outside the media path.

`rust/crates/stream-core` implements opaque chunk retention, shared payload
references, time positioning, explicit lag detection, TS packet-boundary assembly,
and legacy discontinuity stamping. `rust/crates/stream-runtime` adds bounded
Tokio fanout, independent viewer cursors, cancellation, shutdown, and idle expiry.
Neither crate serves HTTP or implements authentication, channel resolution,
Redis ownership, or application lifecycle behavior. Byte caps cover retained
payloads; in-flight references, global channel count, and future transport buffers
need their own limits. An owned HTTP response-body adapter must preserve explicit
in-flight accounting. Rust's next-to-read cursor convention differs from Python's
last-consumed cursor and requires an explicit adapter.

Redis remains available for coordination during coexistence. Removing Redis
from media delivery requires owner-aware routing or an inter-process media
transport: process-local buffers alone do not preserve multi-worker behavior.
Preserve existing ownership, lease, pool-slot, and crash-recovery contracts until
their replacements are tested. Never allow both backends to own the same channel.

## Compatibility inventory

| Surface | Source of truth | Required coverage |
| --- | --- | --- |
| REST APIs | `apps/api/urls.py`, app routers, serializers, views | Methods, URLs, JSON types/defaults, errors, filtering, pagination, permissions |
| Auth and accounts | `apps/accounts`, `core`, Django settings | Password hashes, JWT/session semantics, setup restrictions, reverse-proxy trust |
| Live streaming | `apps/proxy/live_proxy` | TS, fMP4, HLS, startup positioning, keepalives, failover, discontinuity, stop/cleanup |
| VOD and timeshift | `apps/proxy/vod_proxy`, `apps/timeshift` | Range requests, seeking, redirects, catchup formats, connection limits |
| Playlist and device outputs | `apps/output`, `apps/hdhr`, `dispatcharr/urls.py` | M3U, XMLTV, Xtream endpoints, HDHomeRun discovery and lineup |
| WebSocket | `dispatcharr/routing.py`, consumers | `/ws/`, auth, event names/payloads, reconnect behavior |
| Imports and jobs | `apps/m3u`, `apps/epg`, `apps/vod`, task modules | Scheduling, retries, deduplication, cancellation, progress, matching |
| Integrations | `apps/connect`, `apps/plugins`, `Plugins.md` | Connection behavior and Python plugin settings/actions/context APIs |
| Persistence | models and migrations, `apps/backups` | IDs, relationships, constraints, PostgreSQL and SQLite, backup/restore compatibility |
| Operations | Docker files, settings, management commands | Environment options, volumes, TLS, startup, health, static/media assets, shutdown |
| Admin | Django admin registrations | Replacement for supported admin workflows |

OpenAPI alone is insufficient: it excludes substantial streaming, output,
WebSocket, operational, and plugin behavior. Inventory each concrete route and
job into an executable compatibility suite before declaring feature parity.
Record the baseline schema and fixture DB from the pinned Python revision.
Compare old/new responses, normalizing only documented volatile values.

Python plugins can import application APIs and schedule Celery work. An isolated
Python runner needs compatibility implementations for those APIs; launching
`plugin.py` alone is not parity. Keep the existing plugin subsystem during the
transition and inventory real plugin requirements before replacing it. A fully
Python-free deployment requires porting plugins or an explicitly approved break
in compatibility. Apply the same scrutiny to Python-only provider and matching
dependencies (streamlink, yt-dlp, sentence-transformers).

## Delivery sequence and gates

The [milestone ledger and acceptance specification](rust-milestones.md) defines
M0–M9, dependencies, concrete test sources, local Docker Compose acceptance,
and performance evidence required at every step. The user has authorized
progress through milestones and parallel agent work. Keep the ledger honest:
implementation progress and verified acceptance are separate states.

Use additive schema changes while both implementations operate; appoint a single
migration owner. Test existing password hashes and IDs without resetting users.
Do not silently replace Celery scheduling/retry semantics with in-memory tasks.
No milestone passes on unit tests alone: the corresponding container workflow,
contract coverage, and performance gate must also pass.

## Current verification

The [local verification guide](rust-local-verification.md) documents Rust checks,
Docker Compose execution, pinned Python baseline tests, inventory generation,
and performance tools. Rust 1.90.0/Cargo and an isolated Docker environment were
installed under session-local `/tmp` paths. The core Compose runner has passed,
as have Python baseline suites (48 focused tests and a separate broader 526-test
proxy suite) and the harness selftest. These are component
and test-infrastructure results; M0/M1 remain in progress.

The [static inventory](rust-contract-inventory.json) records source candidates;
the baseline runtime additionally resolved 592 Django routes and 246 OpenAPI
paths/362 operations. The schema has 58 warnings and 66 graceful-fallback error
diagnostics; views with uninferred serializers are omitted. Discovery does not
establish contract coverage. Runtime-generated routes differ from literal source
candidate counts by design.

The actual legacy Python TS discontinuity helper and Rust port were compared in
paired component trials; one verified run measured a Rust/Python median wall-time
ratio of about 0.03273. This excludes HTTP delivery, Redis, database work,
transcoding, and most application behavior. It is not a whole-application CPU or
memory improvement. Use the [milestone gates](rust-milestones.md) to establish
full behavior and service-level performance evidence before cutover.

The [real-media Python playback probe](../scripts/rust-playback-baseline/README.md)
also passed decoder and initial one-/ten-viewer measurements using a single shared
upstream. Its recorded totals cover only the specified benchmark services and
short trials; it supplies initial baseline evidence, not a Rust comparison.

Full credit for the original application belongs to the original Dispatcharr
authors and contributors; see [ATTRIBUTION.md](../ATTRIBUTION.md).
