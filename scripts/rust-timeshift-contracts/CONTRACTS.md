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

Additional completed source evidence: `python-range-goldens.json` records 246
pure Range/representation/header vectors matched by the Rust core;
`python-media-goldens.json` records six actual native media/provider scenarios.
Three successful paced Range cases returned identical bytes and passed strict
H264/AAC decoding (74 video frames). They released capacity, marked the pool
idle, then removed native API/pool/stats after grace. 403/404/500 provider modes
released capacity without removing API authorization. Global format caching
changes subsequent URL-shape order; source bare-profile VOD React fallback is
tracked separately in the VOD evidence. The additive native API-capability media/pool code now compiles and passes
eight targeted HTTP/Redis/cancellation checks plus the actual Store/Auth/JWT
native Range/hash/grace and strict A/V decoder gate (74 frames). See
`verified-native-media.json`; app wiring/frozen image and the broader source
contracts remain open.

Positive-admission source ordering (code preparation, runtime oracle pending):
`_terminate_previous_timeshift_sessions` runs before cap checks for unlimited
users too. It compares actual stats-key media IDs (`channelPK_opaqueSession`)
with programme media IDs (`channelPK_safeTimestamp`), so ordinary same-user
same-channel opaque sessions are displaced as `hop`, even for the same show.
This literal identity mismatch must not be silently corrected. Hard-cap tests
therefore need another channel/live/VOD; changing User-Agent alone only avoids
fingerprint adoption. Source limit-stop values/TTLs are live `true`/30seconds,
VOD `true`/60seconds, Timeshift `limit`/60seconds; hop is `hop`/60seconds.
Complete pubsub/WebSocket and generation effects remain separate gates.

The frozen image 11 comparison includes established capabilities on a
Redirect-profile channel whose user's allowed M3U profiles are empty. The
original still proxies that bound capability; the allowed-profile filter belongs
to the bare redirect selection path. Both apps matched positive-cap refusal and
same-capability retry, the 24 API cases, six provider cases, exact media bytes,
strict A/V decoding and cleanup. See `verified-compose.json` for immutable image
and source hashes. General direct-auth/XC and lifecycle parity remain open.

`python-idle-reopen-goldens.json` captures the original same-capability sequence
of a 256-byte startup Range, a paced 1,880,000-byte reopen and a 256-byte middle
seek. All return 206 and release the provider after the body. The pool retains
the cached final URL, connection/channel start times and cumulative 1,880,512
bytes. The last seek anchors position to 263.16778826271667 seconds. Its short
transfer cancels the prior established disconnect grace, so pool/API/stats remain
nine seconds later under startup-idle retention. Reproduce with
`run.py --timeshift-idle-reopen --output <new-directory>` in the VOD contract
runner. Observation excludes capability identifiers, JWTs and provider URLs.
Add a frozen candidate overlay/manifest to compare this slice; comparisons keep
byte/status/header/order equality and timestamp preservation relationships.
Rust local idle reuse has passed actual-schema/Auth/JWT HTTP, exact three-range
hashes and seek/retention metadata checks, 21 targeted HTTP/Redis tests and strict
Clippy. `verified-idle.json` records host and immutable image 11 proof. The frozen app
comparison matches all three responses and metadata/lifecycle observations with
strict A/V (74-frame) decoding; every previously captured image 7 control also
passes in the same isolated run. Cross-worker/fingerprint/programme/profile adoption and broader stop/failure
contracts remain unfinished.

Busy source characterization is now in `python-busy-seek-goldens.json`. A middle
seek cuts off the prior response and serves the requested 256 bytes from the same
pool. A plain reconnect returns a new full 200 response and later removes the
session after disconnect grace. A near-EOF 256-byte probe leaves the original
transfer busy, with the existing single provider counter unchanged in before/
during snapshots; the source's dedicated cached-CDN path has no pool/stat/profile
side effects. These snapshots do not prove absence of a transient quota gap.
`capture.py --busy-controls` records 448 immutable pure control vectors matched
by Rust; the actual-schema/Auth/JWT host test and frozen image 11 now match the
three busy response/status/header/hash, before/during/eventual quota/lifecycle
and old-body termination controls. Source and candidate hold established bodies
at least 2.5 seconds after initial bytes, so startup retention cannot be confused
with established disconnect grace. Queued old bytes, immediate TCP-close
scheduling and busy byte-total timing are captured but not equated. Full
telemetry and cross-worker behavior remain open.


Native direct-auth source behavior is captured in `python-direct-auth-goldens.json`
(15 cases): error ordering, direct caller-supplied SID playback, private 21600-second
session-mint 301, retained query token, no API capability for that 301 SID, bare
provider 302 with original timestamp shape, empty allowed-profile refusal and
global network 403 JSON. Query tokens and provider credentials are redacted in
Locations. Reproduce with `run.py --timeshift-direct-auth` in the VOD contract
runner. This capture does not establish a Rust direct-auth implementation.
