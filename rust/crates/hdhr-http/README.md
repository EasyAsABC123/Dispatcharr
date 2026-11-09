# HDHomeRun HTTP migration

The original Dispatcharr authors and contributors retain full application credit.
This is a port of `apps/hdhr/api_views.py`, serializers, URL declarations, and the
shared tuner calculation. See `ATTRIBUTION.md` and the AGPL-3.0 license.

The adapter implements the JSON discovery, lineup, status, fixed device XML,
device management, profile variants, API namespace aliases, and JSON device
suffix routes. Discovery authenticates presented credentials and uses the global
M3U/EPG network policy; device management applies the original Standard/Admin
roles and UI policy. It preserves effective channel overrides, hidden flags,
named-profile membership, validated output-profile defaults, and calculated
rather than stored tuner counts. There is no UDP responder in the original
HDHR module or its startup configuration; this port does not add one.

`m7-hdhr-native-7` compares a frozen native component with 127 real original
HTTP requests against disposable original-schema PostgreSQL and Redis. Status,
parsed JSON including array order, selected response headers, and XML bytes
match. JSON serialization is compact, so JSON byte counts need not match the
original encoder. HEAD bodies are empty. Three additional real PostgreSQL tests
prove that caller cancellation retains admission through a blocked query,
a final response frame retains its byte lease across shutdown, and long Host
prefixes/unused allocation capacity cannot bypass the response budget.

Requests are admitted before authentication/database work. URI and header bytes are capped at 64 KiB
before cloning, and lists serialize one row at a time into a capped buffer. An owned worker
continues if its HTTP caller disappears. Defaults bound requests to 32, channel
rows to 50,000, device rows to 4,096, request bodies to 64 KiB, metadata to 8 MiB,
responses to 16 MiB, and shared build/response reservations to 128 MiB. Work has
a 15-second deadline and shutdown waits at most five seconds. Owned response
bytes charge the allocated Vec capacity and retain their memory reservation
until the last clone is dropped. These
are explicit native resource guards, not claims of unlimited-input parity or
complete process RSS accounting. PostgreSQL connections remain bounded by the
shared Store pool. Capacity failures return 503.

The same 127 HTTP cases also pass in frozen image18 (`verified-hdhr-image18-1.json`).
A generated lineup URL returns real H264/audio TS; a complete prefix decodes 74
video frames with zero decoder stderr. The app exits 0 without OOM or a drain
warning, and the isolated Compose project is removed. Full-sample ffprobe stderr is retained separately from the complete-prefix
decoder proof; both are empty in this run.

HTML browsable rendering,
multipart input, exhaustive malformed JSON/Unicode numeric settings, unusual
unknown-route middleware behavior and suffix/query-format precedence, and concurrent duplicate-ID error timing
remain outside the current 127-case acceptance scope. No full M7, full app, or
performance acceptance follows from these component tests.
