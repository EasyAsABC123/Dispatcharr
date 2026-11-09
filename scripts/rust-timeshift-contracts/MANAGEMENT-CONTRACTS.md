# Timeshift management follow-up

Original application behavior belongs to the Dispatcharr authors and contributors.
The source references below describe the pinned original implementation, not
accepted Rust parity. These routes remain open after the playback/XC slices.

| Route | Original method | Permission | Result |
| --- | --- | --- | --- |
| `/proxy/catchup/stats/` | GET | Active UI-authenticated admin (level >= 10), UI network policy | Redis/SQL session statistics |
| `/proxy/catchup/programs/` | POST | Same | Batch guide entries and playhead advancement |
| `/proxy/catchup/stop_client/` | POST | Same | Administrative per-session stop and cleanup |

All three are DRF function views in `apps/timeshift/stats_views.py`; method,
authentication, malformed JSON, content negotiation and OPTIONS behavior must be
captured from the original server. API-key and JWT behavior must follow shared
REST authentication, independently of the XC playback authenticator. Inactive
XC playback compatibility does not apply to these management endpoints.

`stats/` returns `timeshift_sessions`, `total_connections`, and a wall-clock
`timestamp`. The builder scans `timeshift:channel:*:metadata`, skips unparseable
channel identifiers, empty client sets, absent client hashes and missing
programme starts. Redis fields supply connection times, bytes, average bitrate,
paused state and playhead anchors. Actual Channel name/logo and M3U profile/account
rows override or augment the Redis data. Sessions group by parsed session ID;
each includes its connections. A builder exception returns an empty payload;
an unavailable Redis client at view entry instead returns JSON500
`{"error":"Redis not available"}`. Ordering follows Redis SCAN/set iteration and
must not be presumed stable. Dynamic time fields require a controlled clock or
explicit relational assertions, never wholesale omission from acceptance.

`programs/` distinguishes absent `sessions` (400 `sessions is required`) from a
non-list value (400 `sessions must be an array`). It filters entries missing
channel UUID/programme start/session ID, then takes at most 50 valid entries.
Each result retains these three identifiers even if no guide programme exists.
It resolves the programme containing the URL timestamp, optionally advances
through a supplied numeric position, or estimates position from Redis anchors,
byte-range base and paused state. A playhead past the original programme may
advance into the next programme. Programme bounds are start-inclusive and
end-exclusive. Malformed element and UUID behavior must be measured rather
than silently sanitized into a newly invented response.

`stop_client/` parses the raw JSON body, returns400 for malformed JSON or a
missing/false session ID, and404 when its Redis client-key scan finds no match.
It signals `admin` stop keys with TTL60 for the stored programme virtual channel
plus every matching stream generation, closes owned local upstreams, cancels
disconnect grace, unregisters statistics, removes all generations, finalizes
playback-session authentication and deletes the superseded-pool key. It then
triggers the shared Timeshift statistics update. The success body is
`{"message":"Timeshift session stop signal sent","session_id":...}`. Porting
only the JSON response would not satisfy its cleanup or cross-worker contract.

Required next evidence is an original-source golden for authorization, method,
JSON validation, empty/populated data, guide advancement and stop side effects;
then native tests on the actual Django PostgreSQL/Redis fixture; then an immutable
Compose comparison including an actively streaming and an idle session. Assert
held-body cancellation, provider release, API-capability revocation and unrelated
session preservation. Pubsub/WebSocket delivery remains an additional gate until
wired to the native shared event transport. No resource gain is established by
management or playback-only functional tests.

Current narrow native proof:31 original HTTP cases plus real active/idle media
admin stops match on actual Django PostgreSQL/Redis; see verified-management.json.
The first host mismatch (malformed-JSON EOF location) and initial active-stop
grace mismatch remain in ignored evidence. The port preserves the original
transient grace-only client hash after an active admin stop and its eventual
removal, while retaining the idle pool fingerprint. Two real Redis regressions
check this branch and reopen cancellation; all11 pool tests pass. The full31
case test also injects a failing event sink and checks three complete stats
envelopes without changing successful stop responses. This establishes a
component hook, not actual shared Redis/WebSocket delivery. App router and
parallel shutdown wiring compile; immutable image14 now matches all31 HTTP and
real active/idle controls in Compose, with final cleanup. The full run also
replays the prior XC/direct/API/media/idle/busy/limit/VOD scopes and strict
audio/video decoders. Its249 fixture/probe files are captured and hashed before
setup and used by all later imports/container script mounts. See verified-compose.json.

Run `scripts/rust-vod-contracts/run.py --output <empty-directory>
--timeshift-management --timeshift-management-active --candidate-overlay
<overlay.yml> --candidate-build-manifest <build.json>` after a new immutable
application image captures this implementation. The harness compares all31
HTTP cases plus immediate/post-grace provider/API/client/pool state for real
active and idle viewers. Its capture-complete flag rejects incomplete media
setup, an unclosed active body, provider leakage and API/stat cleanup failures.
Top-level timestamps are normalized only after checking request time bounds.
More malformed input, Unicode/numeric conversion, guide/cross-worker and
all playback lifecycle event cases remain outside this accepted host slice.

Next source-oracle matrix (audit targets, not accepted behavior):

- Programme channel UUIDs: canonical, uppercase, unhyphenated, malformed and
  non-string inputs. The original batch helper builds a dictionary keyed by
  canonical `str(channel.uuid)` and looks it up with the presented string;
  treating every parseable UUID as equivalent needs an actual compatibility
  check before acceptance.
- Playheads: null, booleans, numeric strings, negative values, nonfinite strings,
  very large finite values and exact programme boundaries. The original helper
  catches datetime overflow inside `get_programme_info` and keeps the initial
  guide result when advancement returns no programme.
- JSON parsing: incomplete objects/arrays/strings, misplaced delimiters,
  multiline and non-ASCII input, top-level scalars and non-object session
  elements. The current accepted malformed-body case is only the incomplete
  object captured by the31-case slice; it does not prove all Python JSON error
  messages or renderer/exception responses.
- Redis projections: missing/empty init/byte fields, invalid numeric metadata,
  missing DB channels/profiles, blank codec fields, multiple connections and
  multiple sessions. Keep request-clock bounds and all stable display fields in
  comparisons. Enrich unique channels/profiles with bounded batch SQL, matching
  the original builder's batch lookup rather than introducing a query per row.
- Stop races: repeated admin stop during the temporary grace-only hash, HTTP
  cancellation around a delayed Redis reply, idle cache reopening after stop,
  multiple workers and the actual shared stats-event transport. Verify raw
  Redis state and visible statistics separately because the original temporary
  hash has no programme fields or set membership and is omitted by the builder.
