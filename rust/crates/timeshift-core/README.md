# Timeshift pure helpers (M4, in progress)

Original application credit belongs entirely to upstream Dispatcharr's authors
and contributors; retained AGPL-3.0-only licensing is in LICENSE/ATTRIBUTION.md.

This additive crate ports pure functions from `apps/timeshift/helpers.py`:
timestamp normalization, provider timezone conversion, client duration windows,
archive age, percent-encoded credentials and ordered provider URL candidates.
The original-source Docker capture contains 207 vectors, including DST changes
and source quirks such as shape normalization before calendar validation.

This crate is not a Timeshift HTTP backend. Session APIs, authorization, provider
reservation/rotation, URL-format fallback, busy seek/reconnect/generation
semantics, MPEG-TS peek validation, metadata/statistics/WebSocket side effects,
Compose browser playback and performance comparisons remain open. Complete
arbitrary Unicode/ISO timestamp compatibility also requires broader vectors.
No application parity or performance gain is claimed.

Verification: `cargo test -p dispatcharr-timeshift-core --offline` passes three
tests, including all 207 original Python helper vectors. Strict Clippy with all
targets and warnings denied passes. These are pure helper contracts, not a
claim that Timeshift streaming is implemented.
