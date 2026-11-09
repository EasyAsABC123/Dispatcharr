# Timeshift / catch-up migration contracts (inventory, implementation pending)

All application authorship and credit belong to the original Dispatcharr authors
and contributors. This inventory describes the original application source;
it does not claim Rust support or milestone completion.

The source has three playback entry points: native GET
`/proxy/catchup/{channel_uuid}`, XC PATH
`/timeshift/{username}/{password}/{duration}/{timestamp}/{channel_id}`, and XC
QUERY `/streaming/timeshift.php`. Native catch-up authenticates JWT/API key/query
JWT or a previously authorized opaque playback session. Network STREAMS rules
apply. Missing authentication is 401; an expired session without direct auth is
401; mismatched authenticated/session users are 403; direct-auth inaccessible
channels are 404. A valid bound API session loads its stored channel without
rechecking channel profile visibility, but refreshes the User row and current
active/catch-up/user-limit controls. Native HEAD is not an exposed playback API.

`POST /api/catchup/sessions/` requires a standard user and authorized channel.
It stores a channel/user/start/duration binding in `timeshift:session:{id}`.
Unused handshake TTL is 60 seconds; playback and active heartbeats extend a
600-second sliding idle window. Session DELETE and position-report endpoints
must preserve ownership, validation and cleanup behavior. JWTs do not appear in
the headerless player's returned playback URL.

Provider setup reserves actual account-profile/server-group slots, converts
UTC programme starts to provider IANA time zones, and chooses archive depth
appropriate to programme age. A positive client duration receives a five-minute
buffer capped at 480 minutes; otherwise EPG/default duration applies. Timestamp
normalization accepts ISO UTC/offset, epoch seconds/milliseconds and several XC
wall-clock layouts. Provider URL credentials are percent encoded. Proxy mode
tries seven ordered PATH/QUERY timestamp formats and caches the successful
format. Redirect mode mirrors the inbound PATH/QUERY layout without probing.

Timeshift differs from native VOD: its URL-candidate loop tries further formats
on 400/404/500 and PHP/HTML error bodies, but short circuits 403/302. Real
upstream 416 and client partial206 byte positions are preserved. Byte ranges,
content-length inference, MPEG-TS sync preambles and busy near-EOF probes have
specific existing regression tests and must be captured before implementation.

State is under `timeshift:`. Provider pools are `timeshift:pool:{session}`;
stats client hashes/sets are `timeshift:channel:{channel}_{session}:clients...`.
Separate stream generations and stop keys prevent an old request from closing
its replacement. Idle/busy pool ownership, fingerprint matching, profile
release-once, programme changes, reconnect/disconnect grace, session auth
finalization, position metadata and UI/WebSocket statistics are required
contracts. These must never create phantom live channels. Shared user limits
include live, VOD and timeshift sessions.

The source regression suites are `apps/timeshift/tests/test_helpers.py`,
`test_views.py`, `test_sessions.py`, `test_stats.py`, and
`test_catchup_redirect.py`. The original full backend baseline already passes
all 3,362 tests; that does not establish Rust timeshift parity. Next evidence is
pure-helper vectors, authenticated session/redirect HTTP goldens, fault/seek
fixtures, native Rust implementation, Compose decode and resource comparisons.

Evidence now captured: `python-helper-goldens.json` contains 207 pure helper
vectors from a pinned, read-only, network-disabled Docker execution of the
immutable source. `python-session-goldens.json` contains 24 actual native session
and playback-error responses from the isolated PostgreSQL/Redis Compose fixture;
opaque capabilities and handshake deadlines are normalized, and ephemeral JWTs
are never persisted. The Rust pure helper crate matches all 207 vectors; the
session prototype's real Redis/loopback suite matches 22 API cases. This does not
claim Rust playback parity. Reproduce session capture through
`scripts/rust-vod-contracts/run.py --timeshift-sessions --output <new-directory>`;
add `--timeshift-rust-test` for the actual Store/Auth/Redis coupling test.
