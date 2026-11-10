# Original EPG refresh acknowledgement and process loss

Original Dispatcharr authors and contributors retain full application/algorithm
credit and AGPL licensing. Celery/Kombu, Redis/redis-py and framework/dependency
authors retain credit and licenses.

The [Source-only gate](verified-original-epg-refresh-ack-2.json) passes17 checks
in a copied database, using the actual task, parser and local FIFO input.
The [synthetic dataset](source-epg-refresh-ack-2.json) records actual task
attributes, producer envelopes, normal result and hard-loss observations.
A fixture-only router keeps non-refresh children in an unconsumed owned queue;
no task/helper implementation is replaced and no unrelated family executes.

The task has `acks_late=False`, `track_started=False`,
`reject_on_worker_lost=None`, `ignore_result=False`, max_retries3, hard limit14400
and no soft limit. While real parser I/O is blocked, its exact task is already
absent from the queue and reservations. The backend has no result/STARTED record,
and the source index has been invalidated. Both refresh/file locks are held.

SIGTERM to the main worker PID waits for the body. Supplying owned XML finishes
SUCCESS/null, releases both locks and produces the configured3600-second result
TTL. SIGKILL to the whole owned process group leaves no terminal record or
postrun, with both `locked` keys retaining near300-second TTL. A replacement
Original worker processes a fresh inactive task, while the lost task remains
unreplayed during a one-second observation. This does not measure actual
visibility expiry or establish every worker-loss exception path.

The first attempt remains failed: process-group SIGTERM killed the prefork child
as well as signaling its parent, producing WorkerLostError in the real worker log
and no subsequent FIFO reader. The retry corrects the graceful signal target;
criteria are unchanged. That failed run did not retain the raw WorkerLost result
payload, so no generic exception-payload mapping is inferred from it.

Both attempts removed their projects and left the default database unchanged.
The gate does not implement native publication/claiming/worker/HTTP202, scheduler,
body/finally failure classification, actual4-hour hard-limit enforcement or
ordinary child execution. The separate
[argument/imported-child reference](EPG-REFRESH-IMPORT.md) and native per-execution
resource component remain independently scoped.
