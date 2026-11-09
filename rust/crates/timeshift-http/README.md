# Native catch-up slice (M4, in progress)

All original application authorship and credit belong entirely to the original
Dispatcharr authors and contributors. See the retained AGPL LICENSE and
ATTRIBUTION.md. Full Timeshift parity and performance acceptance remain open.

`CatchupSessions` implements the native authenticated session handshake,
ownership/revocation, bound identity and position metadata using Store/Auth and
bounded supervised Redis transport. Handshake TTL is 60 seconds; authorized
playback/position extends the source's 600-second sliding window. Bound playback
loads the stored channel without repeating mint-time profile filtering, refreshes
active-user state and checks visibility if the pool names a different user.

`SqlCatchupResolver` reads actual Django provider descriptors: client duration
precedence, real EPG fallback, default-profile timezone, active XC identity
credentials and seven ordered URL shapes. Unsupported account transforms/server
groups fail closed. `ArchiveClient` handles cached-format priority, cached-CDN
redirect suppression, portal fallback, PHP/JSON rejection, verbatim partial
bytes, terminal 416 and source error classes. Its request permit remains attached
to the incremental body; it does not buffer the archive file.

The app-wired `CatchupMedia` route at `/proxy/catchup/{uuid}` supports established
API capabilities. It checks authentication/global network rules before resolving
the capability, rejects owner mismatches and streams through bounded supervised
body tasks. `ArchivePool` uses scoped provider leases, token-fenced pool/stat
hashes, source format caching, startup-versus-grace cleanup and bounded shutdown.
Cancellation rollback retains ownership through actual Redis mutation replies.
A cumulative remote byte watermark prevents double-counting a committed
heartbeat whose reply is cancelled.

Positive user admission uses an independent `clients::timeshift` module;
existing live and VOD Lua is unchanged. It preserves literal hop-before-cap
ordering, counts actual legacy live/VOD/catch-up records, applies source policies
and publishes a pending reservation before provider quota. Pending TTL refresh
continues through slow setup; cleanup cancels/joins its refresh task and drains
admitted mutations. START verifies the admission fence before publishing media.
Source-literal stop values/TTLs and generation stop targets are retained.

Completed verification:

- 207 timestamp/timezone/provider-URL and 246 Range/header source vectors match
  the Rust core, with explicit overflow rejection.
- 24 captured native API/error cases match a frozen Docker app against original
  source. Four Redis/session tests also cover ownership, active-user/visibility
  and position TTLs; four HTTP transport tests cover media selection/byte rules.
- Three shared-admission tests and six original-helper differential cases use
  actual Redis, including cross-protocol races, hop ordering, source stop keys,
  same-session exemption and corruption guards. These helper fixtures use owned
  synthetic existing connection records and explicit policy inputs.
- Five native HTTP, nine pool/order/shutdown and two delayed actual Redis tests
  cover positive limits, provider-full ordering, fences, missing-hash cleanup,
  unread bodies and ADMIT/CREATE/START/HEARTBEAT cancellation.
- Actual Django Store/Auth/JWT native HTTP passed positive-limit 403 refusal,
  retry of the same capability 206 with exact 1,880,000 source bytes, grace cleanup
  and strict A/V decoding 74 frames. Refusal uses an explicitly synthetic owned
  live accounting record, not a fake authorization/provider resolver.
- Frozen image 11 matches all 24 API/error cases, six actual media/provider/lifecycle
  scenarios and actual-JWT positive-limit refusal/retry on an established Redirect
  capability with an empty allowed-profile list. All four candidate
  catch-up captures strictly decode audio/video 74 frames. Image/source binding
  and Compose cleanup passed. Native VOD 7 cases and both profile/React aliases
  also match in that run.
- Compile and strict Clippy for all targets pass. Evidence is in
  `scripts/rust-timeshift-contracts/verified-{contracts,admission,native-media,compose}.json`.

Remaining gates: additional direct-auth/fingerprint/control corners; cross-worker/programme/profile
pool adoption and broader generation/stop faults; broader
positive-cap/preemption/stop reasons; complete range position, stream statistics,
pubsub/WebSocket effects; transforms/server groups and failure recovery;
serializer/OPTIONS/path and unusual timestamp/integer corners; full resource
comparison. Unsupported programme/profile adoption and cross-worker reuse fail explicitly
until their remaining contracts are ported. See the contract scripts' MILESTONES.md. No M4 completion,
application parity or performance gain is claimed.

Current follow-up: local same-capability idle Range reopen retains its scoped
owner, re-reserves provider capacity, checks fresh user limits, reuses the cached
final URL and fences old cleanup with a request epoch. It preserves cumulative
bytes and connection/channel start times. Source characterization, 21 targeted HTTP/Redis regressions, the delayed provider
reply cancellation case, strict Clippy and actual Django Store/Auth/JWT native
HTTP proof have passed. Three Range responses match source hashes, preserve
1,880,512 cumulative bytes and start times, retain the source middle-seek base,
and cancel prior grace through startup retention. See `verified-idle.json`. Frozen image 11 also matches the original three Range/status/header/hash
responses, metadata relationships, cleanup cancellation and retention; its
established reopen strictly decodes audio/video (74 frames). See immutable
provenance in `verified-compose.json`. This establishes only this local idle
slice; fingerprint/cross-worker/programme/profile behavior remains open.

Busy-control preparation is separate: 448 pure decision vectors captured from
immutable original views match `timeshift-core`, including full restart versus
bounded startup Range, near-EOF thresholds and capped open-ended tail ranges.
Original busy middle-seek, plain-reconnect and tail-probe HTTP/Redis snapshots
are recorded in `python-busy-seek-goldens.json`; the frozen image 11 app now matches their three-case wire/quota/lifecycle
comparison. The runtime drains old bodies while retaining provider quota,
increments and fences stream generations, and handles tail probes through
a separately bounded raw cached-URL path without pool/stat/profile mutations.
Held probe bodies participate in shutdown. Both the actual-schema/Auth/JWT host
proof and immutable Compose proof passed; see `verified-busy.json`. Continuous
quota sampling, full statistics and cross-worker adoption remain separate gates.

Native direct-auth entry now matches 15 original wire cases in immutable image 12,
including caller-supplied session byte ranges, first-hop redirect minting and
provider Redirect formats. See `verified-direct.json`. XC PATH and
`/streaming/timeshift.php` adapters additionally match 23 actual-source cases
against native Rust and real original PostgreSQL/Redis: required-query ordering,
wrong credentials, channel visibility, inactive XC users, ignored Bearer headers,
XC network precedence, HEAD, supplied sessions and both Redirect layouts.
The XC adapter calls the shared Store XC authenticator directly and retains the
original authors' behavior. See `verified-xc-routes.json`; immutable image 13 now matches all 23 XC cases and repeats the 15 native
direct cases, with fixture cleanup and media decode passing. Strict all-target Clippy, four library tests and
25 HTTP/Redis/runtime regressions pass. No full M4 completion or performance
acceptance follows from these slices.

The `management*` modules match31 original admin HTTP cases on actual Django
PostgreSQL/Redis, including populated statistics, guide advancement, first50
valid sessions, permission/JSON/method handling and synthetic stop cleanup.
Active and idle real-media stops also match body cancellation, provider release,
API revocation and raw grace/pool lifecycle. In particular, original active-stop
finalization briefly re-creates a grace-only client hash; both implementations
remove it after grace and retain the idle fingerprint. Eleven real Redis pool
regressions cover cancellation/reopen fences and that compatibility branch.
A deliberately failing test event sink verifies three full stats-envelope
attempts without changing stop success. Immutable image14 now passes the full31 management cases plus real active/idle
control comparison, all23 XC and15 direct cases, and all prior M4 subset
comparisons/strict decoders, with cleanup. Its249 fixture/probe files were
copied before setup and executed from that captured tree. Actual shared event
transport and the broader remaining contracts remain open. See
MANAGEMENT-CONTRACTS.md and verified-management.json.
