# Current programmes

All original application/view/serializer credit belongs to the Dispatcharr
maintainers and contributors. The React frontend and original licence remain.

The [current-programme host gate](verified-current-http-component-15.json)
passes64 exact HTTP entity/type/status comparisons,47 parser/database cases
and3 retained-response/SQL ownership scenarios. Nine invalid-input500 bodies
were observed through the actual original WSGI server, rather than inferred
from APIRequestFactory exceptions. A fixed clock is injected into the native
host test; the app uses UTC now. The packaged replay instead moves owned test
intervals near real time and compares actual original/native HTTP within a
five-minute window. Exact time-boundary behavior stays in the fixed-clock gate.

Compatibility details established by the original runtime:

* POST is admin-only. Permissions run before malformed-body parsing. GET/HEAD
  are authorized as reads and then return405; OPTIONS retains original metadata.
* Omitted/null selectors mean the channel branch; an empty array means no rows.
  Two non-null selector families return400, including two empty arrays.
* EPG IDs use Python integer conversion, including booleans/Unicode decimal
  strings and truncated floats. Every element is validated before the first50
  input positions are retained. Out-of-database-range integers do not match rows.
* Channel current programmes use lowest matching programme primary key; direct
  EPG requests use the first encountered unordered row. These are different when
  overlapping rows were inserted out of primary-key order.
* Channel overrides win, shared EPG rows remain shared, and the original current
  channel query does not apply the grid's hidden/profile/adult filters.

The Rust selector preserves those captured results and caches repeated channel
lookups by EPG identity. Its cache and response hold capped serialized bytes,
not growing collections of JSON objects. SQL rejects rows above1MiB before
transfer. Current selection has a transaction-local2s statement timeout and an
aggregate deadline checked between rows/queries; rollback drains before its
worker completes. Request admission is shared with ordinary programme reads,
and final response byte clones retain that admission. Caller abort and logical
observer timeout do not release an outstanding SQL worker's capacity.

Current DB selection is bounded at50,000 channels/results and at the configured
programme response-byte capacity (16MiB default). The ordinary programme read
observer timeout/concurrency settings also apply; the current DB component's
local2s limit is independent. HTTP request bodies have a2MiB cap. These limits
are deliberate resource policy, not claims about original over-limit behavior.
Large guides, concurrent refresh and whole-service CPU/memory acceptance remain
separate workloads.

Real EPG rows without stored programmes require the unfinished byte-offset
lookup/index path; they return an explicit501 guard instead of being silently
omitted. Full UUID/JSON error/media negotiation grammar, multipart bodies,
browsable HTML and the separate streamed guide-grid API remain open. No full M5
or whole-application performance claim follows from these component checks.
