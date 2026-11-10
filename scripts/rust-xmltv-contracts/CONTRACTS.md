# XMLTV output migration contracts

Full application and algorithm credit belongs to the original Dispatcharr
contributors. Reference commit: `1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430`.
This is a Source-first component plan, not native or full-M7 acceptance.

Normal `/output/epg` accepts optional profile and trailing slash. It applies
M3U_EPG policy without using presented JWT/API-key credentials or user filters.
HEAD applies that network policy and returns an empty XML attachment without
profile lookup or cache work. Other safe methods generate XML. Unsafe requests
pass through Django CSRF. `/xmltv.php` is an ordinary Django query-credential
view using XC_API, then the same generator with the resolved XC user. It does
not add M3U_EPG or DRF presented-auth checks. Its HEAD/cache behavior must be
measured separately, rather than borrowing normal HEAD semantics.

The body/header generator, channel/profile visibility, effective values, number
assignment, logo and TVG-ID sources, real-guide ordering, duplicated EPG links,
UTC timestamps, metadata tags, and standard dummy text are original contracts.
URL day values win over saved preferences, then Core history-policy defaults.
A present invalid/negative URL value becomes0. Forward0 means all stored future
rows, plus default3-day dummy output. URL forward days are capped at365; past
window is capped by the shared history policy. Explicit saved0 is not unset.

Real-programme inclusion uses end>=lookback and start<cutoff; within each EPG,
rows are rendered by start/id after the original end/id page scan. Native can
preserve that visible order with a bounded SQL row stream rather than retaining
an entire EPG's XML pending list. For shared EPG links, each programme emits all
mapped channel IDs before the next programme. Default dummy generation begins
at the current UTC hour and advances in4-hour steps; its end<=lookback exclusion
differs from real programmes. It does not invent historical dummy rows.

XML formatting includes exact whitespace, html.escape quote/apostrophe rules,
rich programme attributes, SD poster proxy paths/hashes, and dummy descriptions.
Original Source inline AST and helper outputs are retained as differential
vectors; these are not a second Python implementation. Custom dummy name/stream
selection, template/regex matching, time zones and fallback behavior require the
third-party Python regex compatibility gate. Native must explicitly reject an
unsupported fresh export rather than silently dropping those programmes.

Source uses unprefixed Redis generation and chunk keys. Ready/chunks/status
expire300s, build lease120s, error60s, follower poll50ms and wait600s. Cache key
includes profile-or-all, username-or-anonymous, resolved windows, logo flag,
TVG-ID source, origin and generation. Cache hits replay the exact stored chunks
without new SQL/render/event work. Generation changes retire entries without
interrupting old builders/readers. Download events occur only after a complete
build and use the original2-second client/user/profile deduplication key.

An unknown normal profile GET actually returns HTTP200 plus the142-byte XML
preamble, then records cache error60s. It does not become an initial404. Unknown
profile HEAD remains200empty. This wire behavior is captured in Source46cases.

Native design requires pre-admission and cancellation-safe physical Redis
commands, byte/count/TTL limits, one bounded SQL builder per cache entry,
owner-fenced writes/cleanup, generation isolation, bounded follower requests and
held-frame accounting, and globally bounded shutdown. Internal orphan/stale
leader recovery changes must be labeled resource/consistency improvements and
must not be described as identical legacy failure behavior. A persistent bounded
catalog must account for retired generations until their data expires.

Current evidence: `artifacts/rust/m7-xmltv-source-3/xmltv-source.json` has47actual
HTTP cases, raw XML, cache/TTL/digest/event observations,378programme-renderer
cases (261XML/117Sourceerrors),144standard-dummy cases and808pinned Source
isdigit codepoints. The Rust pure renderer matches those cases; a metadata
depth/node/byte bound regression and strict all-target Clippy also pass. This
does not establish native HTTP/cache or packaged XMLTV behavior. See
`verified-xmltv-source-render-1.json`.

The Source cache helper's actual real-Redis fault corpus confirms eight readers
invoke one builder,300s ready TTL, error60s cleanup/rebuild, and generation
isolation. It also measures internal defects: closing a leader leaves chunks
and building status120s without a lock; a resumed old leader after lock loss
appends old bytes to its successor's ready cache. Cold XC HEAD can likewise
leave building120s after emitting no HTTP body. Native fenced writes/cleanup
will intentionally fix these internal failure cases with Root approval, while
preserving the captured normal wire/HEAD/preamble contracts. These failures
must not be represented as identical legacy behavior.

Native cache design must retain legacy chunk-key compatibility with valid UTF8
per stored list member (the Python reader decodes each chunk independently).
It must count ready and retired generations against a persistent bounded cache
catalog, preflight reply lengths before host deserialization, retain physical
read/write guards after cancellation, and keep a slow HTTP reader to one held
frame. Catalogue cleanup must fence the actual entry owner and never delete a
successor or an unowned legacy cache. Rows and output bytes need separate
budgets; a SQL ordering plan may replace the Source's unbounded per-EPG pending
XML list while preserving visible programme/channel order.
