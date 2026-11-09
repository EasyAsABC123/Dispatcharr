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
- Four native HTTP, five pool/order/shutdown and one delayed actual Redis test
  cover positive limits, provider-full ordering, fences, missing-hash cleanup,
  unread bodies and ADMIT/CREATE/START/HEARTBEAT cancellation.
- Actual Django Store/Auth/JWT native HTTP passed positive-limit 403 refusal,
  retry of the same capability 206 with exact 1,880,000 source bytes, grace cleanup
  and strict A/V decoding 74 frames. Refusal uses an explicitly synthetic owned
  live accounting record, not a fake authorization/provider resolver.
- Frozen image 5 matches all 24 API/error cases, six actual media/provider/lifecycle
  scenarios and actual-JWT positive-limit refusal/retry. All four candidate
  catch-up captures strictly decode audio/video 74 frames. Image/source binding
  and Compose cleanup passed. Native VOD 7 cases and both profile/React aliases
  also match in that run.
- Compile and strict Clippy for all targets pass. Evidence is in
  `scripts/rust-timeshift-contracts/verified-{contracts,admission,native-media,compose}.json`.

Remaining gates: direct-auth mint/adoption and XC playback; idle/busy/cross-worker
pool adoption, cached final URL reuse and seeks/generation handoff; broader
positive-cap/preemption/stop reasons; complete range position, stream statistics,
pubsub/WebSocket effects; transforms/server groups and failure recovery;
serializer/OPTIONS/path and unusual timestamp/integer corners; full resource
comparison. Existing media/admission reuse fails explicitly until its remaining
contracts are ported. See the contract scripts' MILESTONES.md. No M4 completion,
application parity or performance gain is claimed.
