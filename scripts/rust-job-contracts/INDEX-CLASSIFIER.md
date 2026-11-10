# Initial classified IndexService adapter

Original Dispatcharr authors retain full application/algorithm credit and AGPL
licensing. PostgreSQL, Django, psycopg, Celery and Kombu authors retain framework
credit and licenses.

`index_classifier.rs` is test-only. Its three unit tests and strict all-target
Clippy pass. Actual classified-worker execution now passes through both immediate
and scheduled workers; see [terminal result evidence](verified-index-terminal-results-9.json).
The
separately accepted detailed service/resource APIs are the observation source,
and the bounded P0001 formatter has its own35case/140check live Source proof.

The adapter's legacy `execute` delegates to the existing ServiceExecutor. Its
explicit `execute_terminal` calls `build_report_with_dispatcher` using an immutable
per-execution ChildPublisher context. It maps only:

- Written with committed state and actually Released lease to null success.
- MissingSource/MissingFile with NotWritten and actually Released to null success.
- Deferred/Exhausted with NotWritten/NotAcquired to null success.
- FileOpen IO kind IsADirectory, raw errno21, NotWritten and Released to the
  captured builtins.IsADirectoryError arguments `[21,"Is a directory"]`.
- Write-stage PostgreSQL P0001 without positions, NotWritten and Released to
  ProgrammingError with the independently verified bounded rendered argument.

Every other cause, inconsistent state, admission/cancellation/capacity, failed or
uncertain release, CAS-not-owner, ambiguous commit and unsupported formatter
shape remains an operational error. The adapter does not roll back or delete an
already committed index. Cached network errors need safe endpoint information
before an exact message mapping; no errno111 or Source138 startup/producer error
is invented from a coarse Worker error.

Root's typed terminal DTO produces a clearly marked native diagnostic string in
the opaque traceback field. It does not recreate executed Python frames or claim
literal Source traceback equality. IDs, parent/children/date and backend writing
remain worker-owned. The classifier itself writes no result or application202.

The ignored `native_classified_service_peer` reads a bounded stdin JSON request
with action(immediate/scheduled),task_id,source_id,broker_url,result_url,
database_url,locks_url,source_cwd,media_root. It requires the owned copied database,
fixture credentials/loopback endpoints and exact `/app`/`/app/media` context. It
checks the queued identity and source before execution and inside the executor,
uses the real Store pool1/TaskLocks/IndexService and actual worker, then drains
worker/service/shared transport and prints `NATIVE_INDEX_TERMINAL_JSON=` with
caller/lease/job audits. It creates no triggers or source rows. The fixture must
have one intended consumer and queue; this test peer is not an application intake
API or a general claim-selection policy.
