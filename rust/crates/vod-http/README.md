# Native VOD migration slice (M4, in progress)

All application authorship and credit belong to the original Dispatcharr authors
and contributors. This derivative port retains AGPL-3.0-only; see the repository
LICENSE and ATTRIBUTION.md.

This crate is a migration prototype, not an accepted M4 implementation and not
wired into the application router yet. It resolves the existing Django
PostgreSQL movie/episode/series and XC account/profile records. The initial scope
is identity XC credentials and native HTTP file playback. Non-XC accounts do not
have a native direct-file URL in the original application.

The HTTP path forwards Range and conditional headers to the provider, preserves
actual provider statuses and content headers, and streams the body without
collecting the file. Request and session semaphores bound concurrency. A body
owns its session ticket; disconnect releases the provider reservation after the
last active request, and idle metadata has a short cleanup grace. Redis VOD
owners use their own namespace while sharing provider counters with live leases.
Stop keys and token-fenced metadata avoid resurrecting deleted sessions.

The wired Django route accepts GET and OPTIONS, not HEAD. Rust explicitly
returns HEAD 405 instead of Axum's automatic GET-derived HEAD. Byte-range
handling deliberately preserves real upstream 206/416 and suffix bytes; the
source's forced-206/suffix defects must be documented in differential evidence,
not labeled exact parity.

Verification so far:

- `cargo check -p dispatcharr-vod-http --offline` passed.
- `cargo test -p dispatcharr-vod-http --no-run --offline` passed.
- Actual disposable Redis test
  `vod_owner_is_scoped_but_provider_capacity_is_shared_with_live` passed. It
  checks distinct VOD ownership, absence of live phantom ownership, and shared
  provider capacity in both directions.
- Loopback HTTP/Redis contracts are implemented but have not yet been executed
  in this checkpoint. Run with explicit disposable `REDIS_TEST_URL` and
  `cargo test -p dispatcharr-vod-http -- --ignored`.

Remaining acceptance gates include actual HTTP range/status/header and HEAD
contracts, actual ORM/Compose source-versus-Rust probes, authenticated session
handoff, user flags/network rules, shared live-plus-VOD user-cap admission,
provider HTTP failover, transparent midstream Range resume, arbitrary profile
transforms, server-group credential rotation, legacy/cross-worker session
adoption/fencing, idle direct-redirect session adoption, complete metadata/stat
compatibility, disconnect/cancellation races and resource measurements. Positive
user stream limits, unsupported transforms/server groups, and legacy
cross-worker session adoption fail closed with explicit unsupported responses.
No M4 parity or performance gain is claimed.
