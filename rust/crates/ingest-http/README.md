# Bounded playlist HTTP downloads

Full application and algorithm credit belongs to the original Dispatcharr
authors and contributors. This component ports the network-to-cache portion of
`fetch_m3u_lines`; refresh orchestration and account/progress events are separate.

Downloads use isolated source-compatible HTTP attempts, connect/read deadlines
of 30/60 seconds and the source's complete 2xx success range. Decoded frames feed
one blocking disk worker through a bounded queue of copied 8-KiB buffers. The
worker retains admission until actual cleanup finishes, including when its
request future is aborted. Shutdown stops admission and producers, then waits
for disk workers. A queued `Finish` is the validated-cache commit point.

The disk component retains only a 32-KiB validation prefix and atomically replaces
the old cache after successful EOF/validation. Invalid, empty, oversized, aborted
and interrupted transfers preserve the old cache. An existing unowned staging
file requires account-lock recovery and is not overwritten or removed.

The original pinned Requests runtime and Rust passed thirteen real HTTP/cache
comparisons, including 206/299 success, gzip/ZLIB/raw/stacked decoding, redirect
cookies, fresh-attempt isolation and truncated transport. Four native lifecycle
tests additionally verify capacity, cancellation, queued admission, stalled
bodies and staging ownership. See
`scripts/rust-ingest-parity/verified-http-fetch-original-16.json`.

HTTP error bodies remain bounded raw bytes with their content type; exact
source charset/error-message mapping belongs to the future job adapter. Account
statuses, progress/WebSocket events, source-cache reuse, scheduled execution and
paired import resource measurements remain open. No public refresh route or
completed ingestion milestone is claimed by this crate.
