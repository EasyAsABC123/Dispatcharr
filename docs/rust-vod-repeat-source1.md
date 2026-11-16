# Source VOD repeat and cleanup observations

Original Dispatcharr, Django, Celery, Requests and dependency authors retain all
application and algorithm credit. React and licensing remain unchanged.

One Source-only Docker Compose context used the existing owned HTTPS provider
and production HTTP refresh action. Fresh ingestion was a causal warmup before
five refresh phases: unchanged repeat; update/reassignment/global Movie dedup
with category replacement; stale relation pruning with another account's real
reference; empty-category preservation; and empty Movie cleanup.

The [actual controller](../scripts/rust-integration/vod-repeat-source1/execution.json)
exited **1 in 113.410 seconds**, with both output pipes closed and its process
group settled. The [original report](../scripts/rust-integration/vod-repeat-source1/report.json)
contains four passing cases and one failed case. This failure remains literal.
The final case completed its real root, provider, SQL and WebSocket assertions,
then failed the observer's final catalogue-count assertion.

Actual cleanup retained one global Movie and the other account's relation, while
removing every stale relation belonging to the refreshed account. The other
account was deliberately inactive. Source's Movie catalogue filters for active
account relations, so the observed HTTP 200 with zero visible results was
correct; the helper incorrectly expected one result. The retained SQL and HTTP
capture supports the [offline corrected observation](../scripts/rust-integration/vod-repeat-source1/offline-correction.json).
No application was replayed and no failed result was rewritten. Capture
qualification is separate from whole-workflow acceptance.

The [semantic projection](../scripts/rust-integration/vod-repeat-source1/semantic-projection.json)
retains each phase's before/after catalogue, category and relation state, exact
HTTP bodies, ordered provider requests, backend result and ordered WebSocket
payloads. Generated database IDs, UUIDs, timestamps and elapsed durations are
abstracted for comparison; the original workflow separately checks stored
identity/timestamp stability and scan-time progression. Raw observations and
hashes remain in the [evidence index](../scripts/rust-integration/vod-repeat-source1/index.json).

Independent cleanup confirmed both exact fixture containers and original host
process generations, the Compose project and the readability probe container
were absent. No Native repeat/update acceptance or application performance gain
is claimed. The next Native batch must use the corrected zero-result observer
and compare its actual observations with this retained Source capture. Metadata,
languages, nonempty Series and wider catalogue/concurrency domains remain open.
