# Playback realtime producer follow-up

Original application credit belongs to Dispatcharr's authors/contributors. This
is a source audit and test plan, not accepted native producer parity. Transport
and live-client statistics belong to the separate realtime workstream.

## VOD

The source manager's `_send_vod_event` emits to group `updates` with outer
`type="update"` and `data` containing `type="vod_started"` or `"vod_stopped"`,
`content_name`, `content_uuid`, `client_ip`, and string `user_id`. There is no
`success` field and no session ID in that WebSocket data, even though the helper
receives a session ID. It then logs `vod_start`/`vod_stop` with content name/UUID,
client IP and username, and triggers a full `vod_stats` snapshot. The statistics
message also omits `success`; `stats` is a JSON string, not a nested object.

A new session's start event occurs only when its response generator first runs.
Creating provider HTTP/session state alone must not announce playback. Reusing a
session does not announce another start. Normal completion and disconnect
release provider capacity immediately; after one second without active streams,
the source sends stop and performs smart cleanup. Reconnection during the grace
window suppresses that stop. An unstarted newly created response sends neither
start nor stop; an unstarted reused response can send stop after grace if it
leaves no active streams. These rules require actual body-demand/close evidence,
not only testing a metadata helper or emitting on response creation.

`build_vod_stats_data` projects global `vod_persistent_connection:*` hashes and
adds actual Movie/Episode/Series/logo/profile/account/user metadata. Source
fallbacks include movie duration estimation when seek metadata permits it and a
40-minute episode fallback. Its complete field conversions, rates, timestamps,
filtering and malformed-row behavior need a captured oracle before porting.
Do not fabricate a reduced stats object merely to make event delivery pass.

## Timeshift

`_trigger_timeshift_stats_update` emits outer `type="update"` and `data` with
`success=true`, `type="timeshift_stats"`, and a JSON-string full statistics
payload. The existing admin-stop management hook is already wired; broader
playback lifecycle hooks remain open.

Registration publishes only when both caller `emit_stats_update` and computed
`notify_stats_update` are true. Notification begins true for a new connected-at
record and also becomes true when the programme virtual channel changes. Normal
reopens/near-EOF duration probes must not accidentally publish as new viewers.
Programme-hop displacement publishes after unregistering displaced statistics.
Grace finalization publishes after cleanup and capability finalization. Preserve
the temporary grace-only raw client hash versus visible-statistics distinction
already captured by M4 admin-stop tests.

## Implementation and evidence boundaries

Reuse the existing `UpdateEventSink` and keep complete original envelopes.
A producer holds Store/Redis/sink references, not an owning media-engine clone:
otherwise installing it in a session pool can create a reference cycle. Queue
and snapshot work must be bounded, cancellation-safe and included in shutdown.
Explicit start/stop transitions cannot be silently coalesced into generic stats.
M8 Connect/plugin dispatch remains a separate bridge from database audit writes.

Capture original actual Redis/Channels messages and system-event rows for new,
reused, reconnecting, stopped, dropped/unpolled and failed-provider bodies. Compare
native messages semantically (including JSON-string stats parsed separately),
all stable fields and relational time bounds. Prove no low-level subscriber leak,
no new provider admission on probes, and no event on an unstarted new body. Then
run an immutable app-image comparison and replay the prior M4 HTTP/decoder gates.
No full application resource gain follows from transport or envelope tests alone.

## Actual source component oracle

`verified-vod-producer-source-component.json` records19 actual original
ORM/Redis statistics projection cases, including movie/episode duration
fallbacks, elapsed/capped/invalid/future positions, missing content/profile
fallbacks, malformed rows skipped, and client-ID duration fallback. A bounded
controlled clock applies only to that projection call. The same original
fixture invokes the unmodified VOD helper and receives four actual Channels
messages plus two DB audit rows: `vod_started`, `vod_stats`, `vod_stopped`,
`vod_stats`. This confirms original envelopes and audit details; it does not
establish native implementation or HTTP/body lifecycle timing parity.
Reproduce with `scripts/rust-vod-contracts/run.py --output <new-artifact-dir>
--vod-producer-source`. The helper runs only inside the disposable fixture,
uses the actual ORM/Redis/Channels and restores modified content durations.

The existing native VOD body pump prefetches before downstream demand. The
producer integration must therefore gate the pump's first body read on an
explicit first-body-poll signal, watching session shutdown while it waits.
Emitting when response headers/provider setup finish would announce an
unpolled response. A small one-frame upstream could otherwise finish and
remove its metadata before the consumer ever polls the body. Holding an owning
Ticket in an unpolled response is also insufficient: shutdown must release
provider/session admission without requiring that response to be polled.

Stop eligibility belongs to each closing request: Source suppresses an
unstarted **new** response, while an unstarted **reused** response can schedule
stop after grace. A global 'session ever announced' boolean would change this
quirk. Preserve request-created/body-started flags through fenced cleanup and
recheck active count/epoch after grace so reconnection cancels the old stop.
Source schedules the final statistics snapshot in another thread before smart
cleanup, so the stopping snapshot races metadata removal; tests must distinguish
this observed timing from the stable start/stop envelope and eventual empty
snapshot. Native statistics projection has its own deterministic-clock SQL
oracle rather than relying on a convenient empty-only event fixture.

Timeshift admission currently writes pending connected-at metadata before
registration. Simply inspecting connected-at in the subsequent START script
would incorrectly suppress a new-viewer update. Preserve the pre-admission
registration decision separately; use the original existing-connected-at and
existing-programme rules, including the legacy case where connected-at exists
but programme_vid is missing. Reopens/probes must preserve that prior decision.

## Native host milestones

`verified-vod-producer-host.json` records the actual six Source/native response
body cases, eight equal audit rows, nineteen SQL/global-Redis statistics
projections and fifteen pre-existing HTTP/Redis regressions. A stalled sink
cannot produce a false drained shutdown; provider/session/actor work cleans up.
The first run exposed an actual native telemetry mismatch: the old timer wrote
prefetched bytes. Native now records acknowledged per-request data at the
Source100×8192-byte milestones, including the captured4,096,000-byte full-file
sample. The normal-completion fixture pauses both implementations after their
first chunk until the start snapshot arrives, preventing unlike observation
points from being confused with telemetry differences. Earlier failures remain.

`verified-timeshift-producer-host.json` adds original conditional publication
and immediate old-position displacement evidence, native full-snapshot actor
registration/grace/hop cases, stalled publication cleanup, eleven archive-pool
regressions and the original-derived shared admission suite. The first owned
registration is distinguished from its private pending admission; repeats and
legacy connected-at-without-programme suppress notification. Previous-position
removal publishes before replacement provider selection, including a later
limit refusal. Only the private admission result gained a hop count on LIMIT;
existing policy/counter semantics retain their Source tests.

Both actors hold Store/Redis/sink context rather than an owning playback-engine
reference. Their bounded queues, single snapshot workers and supervised transport
guards have no per-viewer publication polling. Explicit VOD transitions stay in
order and are not merged. VOD omits success; catch-up snapshots retain success.
The app attaches both publishers before playback. IMAGE18 packaged transport
and complete earlier M4 contract replay are the next separate acceptance gates.

The actual HTTP/WebSocket Source probe also captured a remaining legacy race:
a reconnect that closes again before the first one-second cleanup expires can
produce duplicate vod_stopped/statistics pairs, because both original daemon
threads see idle state. Native epoch-fenced cleanup currently emits once.
This rapid-close variant remains an explicit parity gap; the accepted
reconnect scenario keeps the replacement active beyond the old grace window.
The packaged comparison uses an optional262144-byte/s identical MP4 origin
and1.2-second active hold so that scenario is reproducible through HTTP socket
buffers. Default VOD fixtures remain unpaced and all file/header bytes stay
unchanged. No performance gain follows from these functional probes.

## Post-IMAGE18 known-length progress correction

`verified-vod-progress-host.json` preserves an actual immutable18 HTTP witness:
819199/819200/819201-byte206 range payloads match Source hashes exactly, while
published Redis byte progress remains0 instead of819199/819200/819200. A direct
body collector had hidden part of this behavior: Hyper can finish a declared
Content-Length without polling the body again for EOF. The correction tracks
the provider response length and acknowledges its final emitted frame, while
retaining EOF handling for unknown-length bodies. It also preserves Source's
actual byte count when its100th8192-byte generator block is a short final block.

The working correction passes all three cases through an actual Axum/Hyper
HTTP socket, plus the fifteen HTTP/Redis, six lifecycle, nineteen full-statistics
and stalled-publication regressions. This is post18 host evidence; repeat the
same witness on immutable19 before accepting its packaged behavior. Interrupted
partial-block variants, the rapid-reclose legacy race and the broader remaining
M4/resource gates remain explicit.

## Immutable IMAGE19 packaged regression and rapid reclose witness

`verified-vod-progress-image19.json` now repeats the failing IMAGE18 witness on
immutable19 through actual HTTP: all three206 payload hashes, EOF metadata and
819199/819200/819200 Source progress values match. The original IMAGE18 failure
remains preserved. `verified-vod-producer-image19.json` and
`verified-timeshift-producer-image19.json` repeat real Source Daphne/native
WebSocket transport, stable full statistics DTOs and audio/video decode gates.
These are functional results, not resource measurements or full M4 acceptance.

`verified-vod-rapid-reclose-source.json` makes the remaining race reproducible:
reconnect and reclose within4.75ms produce one start, two stop events and
start/stop/stop DB audits in the original backend. Its snapshots race from one
connection to zero, then metadata and provider counters clean up. Native fenced
cleanup currently emits one stop. Porting the observed event/audit multiplicity
without letting stale cleanup remove active ownership remains a separate gate.
