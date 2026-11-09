# Source-compatible upstream HTTP transport

This is an extraction of the native playback transport, preserving its tested
request, cookie, redirect and streaming-codec implementation. Full application
and algorithm credit belongs to the original Dispatcharr authors; see
`ATTRIBUTION.md` and the retained AGPL license.

`Attempt` is isolated to one provider connection attempt. Callers choose their
source-specific connect/read deadlines and accepted status codes; the shared
transport does not assume that ingestion and playback accept the same statuses.
`body` yields bounded decoded frames with the pinned Requests/urllib3 behavior.
Cookie/header and decoding-probe bounds remain explicit errors. Existing native
source comparisons and codec regressions must pass after extraction; this crate
alone does not implement ingestion jobs or establish performance savings.
