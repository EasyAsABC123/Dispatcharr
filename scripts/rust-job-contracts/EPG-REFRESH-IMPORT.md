# EPG refresh arguments and imported child publication

Original Dispatcharr authors and contributors retain full application and
algorithm credit and AGPL licensing. Celery/Kombu, Redis/redis-py, Django and
PostgreSQL authors retain framework credit and licenses.

The [actual Original capture](verified-original-epg-refresh-import-2.json) passes
91 checks across nine cases in a copied nonempty PostgreSQL database. It extends
the independent seven-case refresh wrapper reference without replacing the task
or its helpers. Initial parent calls cover default arguments, force as keyword
or positional value, positional retry, and all-keyword arguments. Actual bodies,
Python argument representations, hard limit14400 and result metadata are retained
in the synthetic [dataset](source-epg-refresh-import-2.json).

Busy default refresh publishes a new task with `force=False`, retry1 and a
15-second ETA; positional forceTrue/retry7 publishes forceTrue/retry8. The child
retains the original root and current parent, with the Celery retries header0.
This is child rescheduling rather than the Celery RETRY state. The result's
children field contains the actual published child identity.

Two real local-guide imports update channel and programme rows. Each publishes
`apps.epg.tasks.build_programme_index_task(source_id)` followed by
`apps.channels.tasks.evaluate_series_rules()`, with parent/root lineage and both
child identities in the parent result. The index remains invalidated because
these child tasks are deliberately not executed. All parent results are
SUCCESS/null; refresh locks release and result TTL is within3590–3600 seconds.

One fixture control is explicit: a router loaded only by the isolated observer
sends all non-refresh tasks to `owned-refresh-children`; the worker consumes only
`celery`. This allows real publications to be observed without running unrelated
families. It does not establish ordinary child routing or index/SeriesRules body
execution. No external network or production database is used. The copied
fixture and owned services are removed, with the default database unchanged.

The first attempt is retained. An observer initialization accidentally became
conditional and raised UnboundLocalError during busy-child observation; five
inactive inputs had already passed. The retry corrects only that observer path
and adds exact input-body assertions. Neither application behavior nor acceptance
criteria were relaxed.

Native work remains component-only: a bounded test-only protocol handles the
observed typed shapes and validates common envelope options; it rejects duplicate
arguments, unsupported types/options/families and oversized frames. Native JSON
body whitespace is compact, so comparison checks decoded JSON structure and
exact other envelope fields, not base64 byte equality. The additive
`FileJobRunner::run_with_dispatcher` captures a local immutable dispatcher while
`run` retains its configured default. The [frozen native component](verified-epg-refresh-dispatch-source-native-2.json)
passes the actual configured-default/concurrent trace, imported callback and
delayed-reply ownership gates. A caller abort keeps refresh ownership and
admission alive through a real Redis reply; short shutdown reports incomplete,
then all jobs drain. Foreign file-lock owners are preserved. The test publisher
is a callback recorder with an actual blocking Redis operation, not a native
Celery broker publisher. The child wire adapter has pure captured-envelope
coverage, not an end-to-end native refresh execution claim. No application worker, public202, scheduler,
remote refresh, normal child execution or complete M6 parity is claimed.


The successful final native fixture reruns all91 Source checks and adds four
native audit checks (95 total). It uses the prior immutable runtime plus only a
hash-bound owned test overlay to exercise imported callbacks. The full resulting
archive and selected tools are retained. Unknown late-mapped child families fail
explicitly rather than silently omitting publication. Typed argument support is
limited to positive signed64 source IDs, boolean force and integer retry0–480;
Python's arbitrary truthy/float/coercion input shapes are not claimed.

Next gates are actual typed broker publication into the Original worker, refresh
early-ACK/restart behavior, ordinary child routing and result-null/string handling.
The native FileJob component does not yet establish orphan PeriodicTask/interval
cleanup for the real missing-source worker case. Remote XMLTV, Schedules Direct,
newly mapped per-channel child families and body/finally exception precedence
also remain separate. No app/main, worker intake, public202, scheduler or general
Celery consumption is enabled by this slice.

The separate
[actual deferred-descendant capture](verified-original-epg-refresh-descendant-1.json)
passes18 checks. After the busy parent returns, only the owned blocking file-lock
value is removed. The Original worker actually executes the15-second refresh
child, commits local programme data and publishes index/SeriesRules grandchildren
without executing them. Child result `parent_id` refers to the original task;
grandchildren retain its root while referring to the immediate child as parent.
The parent's raw result stays unchanged and result TTLs expire independently.
This is Source execution evidence, not a native worker or visibility-expiry gate.
The copied runner's historical scope text retained the earlier no-child-execution
phrase; the summary explicitly corrects that label to distinguish executed
refresh children from unconsumed non-refresh grandchildren without rewriting raw
evidence.

The [normal-result decoder component](verified-epg-refresh-result-component-1.json)
adds two bounded tests over18 exact Source metadata records; the frozen combined
refresh suite has six passing tests and strict all-target Clippy. It accepts only
SUCCESS/null or the exact missing-source cleanup string tied to the expected
source ID, with matching task UUID, known fields, UTC date and child tuple
geometry. The actual deferred-child record supplies `parent_id`; initial task
records omit it. This is a test-only parser, not a result writer or evidence that
native orphan cleanup occurs. Unknown result shapes, errors and oversized input
remain unsupported.
