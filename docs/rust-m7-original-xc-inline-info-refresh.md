# Original Xtream metadata refresh within the HTTP request

Pinned Original returned HTTP 200 in four cases with stale or missing
movie or series metadata. Each request made two provider GETs, one
authentication request and one selected metadata request. The response
and committed SQL identified the same initially selected provider relation.
The queue and Redis task-result census stayed identical.

Both movie requests updated the movie and its provider relation. The stale
series kept its stored episode. The series with missing metadata and episodes
created exactly one episode and one episode relation. The checker compared
causal response fields with the committed SQL. Unrelated table and sequence
state was preserved across all four cases. Both Compose roles retired exactly.

The first controller exited 1 after blocking the inherited readiness
User-Agent. The second reached the HTTP service and exited 1 during fixture
setup. Its seed expected a Django task-result model absent from pinned
Original. Both cleaned up exactly before the corrected run. The successful
fixture uses Original’s configured Redis task-result backend.

Native stale and missing behavior and comparative performance remain open.
Original Dispatcharr and dependency authors retain full credit.

[Redacted reference evidence](../scripts/rust-integration/m7-original-xc-inline-info-refresh.json)
