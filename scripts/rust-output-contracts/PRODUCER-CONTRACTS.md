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
