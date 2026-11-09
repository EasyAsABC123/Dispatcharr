# Native input HTTP compatibility audit

The source reference is `apps/proxy/live_proxy/input/http_streamer.py` from the
pinned original Dispatcharr revision. Original authors retain full credit.
This audit supplements the TS byte, buffering and cold-wire comparisons.

The original creates a new `requests.Session` for each connection attempt,
disables adapter retries, permits only HTTP200 input, and uses connect/read
timeouts of 5/30 seconds. Session redirect behavior comes from the installed
Requests version, normally a 30-redirect budget. The manager performs the
separate source retry/failover policy. Its unused `_create_session` helper has
different settings and must not be used as the behavioral reference.

The working tree now sets application timeouts to 5/30, rejects unsolicited206,
and permits30 redirects. These changes and queued15-/31-hop tests were written
during the load-measurement quiet window; they are not included in the frozen
image1 resource characterization or attributed to its earlier proof. The frozen
cold-wire proof establishes the measured status/body/timing cases, not complete
upstream request, retry-timing or header equivalence.

The readiness barrier now awaits committed canonical channel state before the
first native response. Host real-Redis tests cover owner and follower readiness
before media and after a timer tick. Frozen image5 also passed the full two-replica
Compose gate: metadata was `active` at the first observed owner/follower media in
all three healthy/shared-stop/crash cases; all six captures decoded74video frames
with audio. Crash provider/client/metadata/input cleanup completed in26.619seconds
against a35second deadline. Exact image/source binding and sanitized evidence:
`scripts/rust-integration/verified-image5-readiness-replica-1.json`. The older
image1 resource study retains its documented metadata-ordering gap; this later
proof is not attached retroactively to that image.

The unchanged source reader was loaded by AST inside the pinned source image
and exercised against loopback transport fixtures. All41 source cases passed:
only200; redirect301/302/303/307/308 and30-hop success/31-hop failure; default
headers; Latin-1 User-Agent and invalid header boundaries; same/cross-host URL
credentials; cookie redirects with fresh-attempt isolation; gzip, ZLIB and raw
deflate, and stacked gzip/deflate decoding. Exact source/tool/runtime hashes are
in `scripts/rust-integration/verified-upstream-http-source-1.json`.

The working tree now uses an isolated client/cookie jar for every provider
attempt, HTTP/1 and matching request defaults. Streaming codecs emit at most8192
bytes/frame, match the source five-link decoding limit and raw-deflate fallback,
and retain a bounded64KiB first-output probe. Cookie/header resource budgets are
explicit safety caps. Six codec tests, six native HTTP tests (including real
Redis cancellation/failure provider rollback), and nineteen existing HTTP
regressions pass. Frozen image5 predates these cookie/codec changes.

Frozen image7 passed the full Compose encoded H264 gate: cookie-gzip redirect,
clean-cookie identity, ZLIB deflate, raw deflate and stacked gzip/deflate on both
backends. All ten captures decoded74video frames with audio; every770048-byte
prefix matched the original fixture, including all five source/Rust pairs.
Provider limit503 rejected an additional stream without another origin; every
case cleared owner/provider/client/metadata/input state. Rust reused the same
process across all five modes, proving cookies did not leak into later channels.
Source used a fresh worker per mode after a separately preserved source reopen
failure (cookie-gzip passed, subsequent identity stalled with no input index).
This functional isolation is explicit and cannot establish performance gains.
Exact image7/source binding and comparisons:
`scripts/rust-integration/verified-upstream-media-image7-1.json`.

Environment proxies/custom CA bundles, all cookie-policy edge cases and complete
malformed-encoding partial-output equivalence remain separate gaps. Do not enable
a global cookie jar: source sessions are scoped to one connection attempt.

These cases belong in M2/M3 acceptance and final operational verification. A
successful steady-state load trial cannot close them; each needs matching source
responses and provider/metadata cleanup checks before performance acceptance.
