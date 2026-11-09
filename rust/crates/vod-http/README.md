# Native VOD migration slice (M4, in progress)

All application authorship and credit belong to the original Dispatcharr authors
and contributors. This derivative port retains AGPL-3.0-only; see the repository
LICENSE and ATTRIBUTION.md.

This crate is a migration prototype, not an accepted M4 implementation and not
wired into the application router yet. It resolves the existing Django
PostgreSQL movie/episode/series and XC account/profile records. The initial scope
is identity XC credentials and native HTTP file playback. Non-XC accounts do not
have a native direct-file URL in the original application.

The HTTP path ports the original Range validation and response headers and
streams the body without collecting the file. Request and session semaphores bound concurrency. A body
owns its session ticket; disconnect releases the provider reservation after the
last active request, and idle metadata has a short cleanup grace. Redis VOD
owners use their own namespace while sharing provider counters with live leases.
Stop keys and token-fenced metadata avoid resurrecting deleted sessions.

The wired Django route accepts GET and OPTIONS, not HEAD. Rust explicitly
returns HEAD 405 instead of Axum's automatic GET-derived HEAD. Byte-range
handling preserves the measured source quirks: with a known cached file length,
suffix bytes=-N is rewritten to bytes=0-N; If-Range is ignored; invalid cached
ranges return a plain 416 without Content-Range; a Range request forces 206.
These compatibility quirks are documented and tested rather than silently fixed.
The first cold suffix request can have different wire behavior because the
source learns file size from the response. That case remains an explicit gate.

Verification so far:

- `cargo check -p dispatcharr-vod-http --offline` passed.
- `cargo test -p dispatcharr-vod-http --no-run --offline` passed.
- Actual disposable Redis test
  `vod_owner_is_scoped_but_provider_capacity_is_shared_with_live` passed. It
  checks distinct VOD ownership, absence of live phantom ownership, and shared
  provider capacity in both directions.
- Loopback HTTP/Redis contracts passed **15/15** against actual loopback HTTP and
  disposable Redis: cached Python 200/206/suffix/416 compatibility bytes and headers; concurrent session
  ranges share one slot while a second session is blocked; disconnect/stop
  cleanup; native HEAD 405; initial provider error cleanup; no metadata
  resurrection; stop cleanup while the body is never polled; exact interrupted
  full/bounded Range resume and exhausted retries; positive user-cap rejection;
  provider-capacity fallback that preserves another viewer's primary slot. Run with explicit
  disposable `REDIS_TEST_URL` and
  `cargo test -p dispatcharr-vod-http -- --ignored`.

- Shared live/VOD admission passed **2/2** real Redis contracts: mixed admission
  race has one winner, bidirectional preemption uses the correct legacy keys,
  VOD does not receive a VOD-same-content exemption, and corrupt stop keys cannot
  cause partial preemption. Existing live REGISTER Lua is unchanged.
- Actual seeded Django PostgreSQL resolver contract passed: movie, episode,
  series, XC URLs/profile limits, stale UUID fallback, flags and offsets. Source
  native seven-case wire capture completed under Compose with cleanup passing.

Remaining acceptance gates include complete cold/warm HTTP range/status/header
contracts, actual ORM/Compose source-versus-Rust probes, authenticated session
handoff, user flags/network rules, complete shared live-plus-VOD user-cap/preemption differential coverage,
provider selection/fallback variants, retry-budget/reset variants, arbitrary profile
transforms, server-group credential rotation, legacy/cross-worker session
adoption/fencing, cross-worker direct-redirect session adoption, complete metadata/stat
compatibility, setup cancellation/failure races and resource measurements.
Unsupported transforms/server groups and legacy cross-worker session adoption
fail closed with explicit unsupported responses. The source's provider failover
selects among accounts/profiles with capacity; it does not probe alternate
providers after a selected provider's HTTP failure. Cached-final-URL recovery
and same-provider midstream Range reopen preserve that distinction.
No M4 parity or performance gain is claimed.

`VodEngine::shutdown()` closes admission, signals supervised bodies and waits for
persistent-session/provider cleanup under `VodConfig.shutdown_timeout` (default
five seconds). It skips idle grace, tracks pending metadata creation cleanup and
waits for the shared coordinator's admitted operations to finish. Call it along
with the other service shutdowns so global coordinator work can quiesce. It
returns false on the global deadline; Redis-outage recovery still needs ledger
expiry/a healthy reaper. The real Redis regression retains eight unread bodies
until shutdown has returned, verifies metadata/counter cleanup and new-request
rejection, then verifies cancellation reaches every retained body.

User admission precedes provider selection, matching source preemption order.
A pending hash has a separate metadata fence and a ten-second startup TTL;
successful provider setup promotes it to the source's 3,600-second TTL. Failed
provider selection rolls back the pending admission. Direct Redirect performs
read-only capacity checks and never reserves a slot, including concurrent calls.
Connection/read timeouts are ten seconds and provider redirect limit is 30,
matching the source Requests defaults/configuration. Shutdown reports false for
observed Redis/quota cleanup errors as well as its deadline. Remaining source
profile-alias/encoding/error-body corner cases are not labeled exact parity.

The frozen Rust application now passes the actual Compose seven-case native
VOD comparison against the original source, using the same ORM database and
origin: zero status/header/byte differences, strict audio/video decoding of
both native responses (75 frames each), validated image/source binding and
successful cleanup. See `scripts/rust-vod-contracts/verified-compose.json`.
This closes that narrow functional gate; the remaining M4 gates above stay open.
