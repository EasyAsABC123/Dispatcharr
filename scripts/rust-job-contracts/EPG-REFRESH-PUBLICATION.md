# Native refresh publication and normal result reading

Original Dispatcharr authors and contributors retain full application/algorithm
credit and AGPL licensing. Celery/Kombu, Redis/redis-py, Django/PostgreSQL and
framework/dependency authors retain credit and licenses.

The [frozen publication gate](verified-epg-refresh-publication-source-native-2.json)
passes124 checks,11 native initial publications and one native ETA republication
through the actual Original worker. Three actual TCP reply resource tests,
eight frozen pure tests and strict all-target Clippy also pass. The
[synthetic dataset](source-native-epg-refresh-publication-2.json) records actual
messages, Source results, native reader audits and controlled child context.

The test-only helper mirrors bounded existing PUBLISH/lease/error behavior. It
parses its encoded message again, rejects production index-only connections and
cannot choose arbitrary task names. Default, positional and keyword forms,
inactive/dummy/missing/busy/exhausted and local imported cases execute through
Source. The native reader observes absent results and bounded normal terminal
records, preserving null/missing-source string, date and child geometry.
Missing-source orphan cleanup is performed by Source here, not by native code.

The ETA extension removes only the owned queued Source child and its owned file
blocker. It republishes the same task ID, root, parent and actual15-second ETA
through native code. Source executes it and publishes index/SeriesRules
grandchildren with the original root and immediate child as parent. The original
parent result stays unchanged. Non-refresh children are routed to an explicitly
owned unconsumed queue; no unrelated family executes.

Reply tests hold actual Redis responses after confirming server-side LPUSH.
A logical publication timeout, canceled publication waiter and canceled result
reader retain caller/admission ownership through the physical response.
Shutdown truthfully reports incomplete until the response settles. Underlying
reconnect/no-replay acceptance remains separately scoped. The helper itself does
not retry publication. Native compact JSON whitespace differs from Source;
actual consumption and decoded fields are verified, not raw byte identity.

ETA clock arithmetic floors nanoseconds to Source's microseconds before adding
the delay. The codec rejects unsupported precision/year, omits the fractional
part at whole seconds and preserves six digits otherwise. Actual pinned Source
ISO values and boundary units cover that rule. The earlier113-check publication
fixture is retained; it tested Source-generated deferral but not native ETA
republication.

All fixtures removed their projects and left the default database unchanged.
The helper and peer are cfg(test), with no native refresh consumer or result
writer, generic task enum expansion, main startup, HTTP202 or scheduler hook.
Immediate native execution and its settled cleanup/failure boundaries are the
next distinct gate. Native ETA intake/recovery, hard-limit enforcement, ordinary
child execution and full M6 parity remain open.
