# Native index outcome classification before failure writing

This is a code-review plan, not a native failure-writer acceptance claim. Original Dispatcharr authors retain application/algorithm credit. Source136's42 observations and exact synthetic results are authoritative for the captured cases.

## Preserve normal outcomes

`Written`, `MissingSource`, `MissingFile`, busy `Deferred`, and retry `Exhausted` remain normal null SUCCESS outcomes. Malformed XML, invalid UTF-8 and empty files in136 also return null SUCCESS: the original indexer scans bytes rather than parsing a document. The accepted raw scanner already preserves replacement decoding and opening-tag offsets; do not add strict XML validation or turn its normal output into a task failure. Conversely, never blanket-map `ImportError::Parser` to SUCCESS: the captured cases actually store their exact partial/empty indexes before returning null. The native raw scanner must independently produce and commit those same JSON indexes before its success is accepted.

## Known information loss

- `ServiceExecutor` maps every `build_with_dispatcher` error to `Error::Execution`. That loses file errno, database error and lock stage before a writer could choose an original exception.
- `IndexService` classifies every non-regular path as capacity. A directory is a captured original `IsADirectoryError` failure, while FIFO/device limits remain explicit bounded-resource policy. Distinguish directories without opening/blocking arbitrary special files.
- TaskLocks maps network, authentication and other Redis failures to `RedisUnavailable`; IndexService further maps them to `Worker`. Add an owned classified path before mapping a task error. Keep existing APIs as delegates with their current coarse errors.
- `RetainedLease::Released` stores only success/failure. False conflates negative owner-checked deletion, Redis errors, timeout and missing runtime. Its physical completion must retain detailed release information without releasing caller/file-job guards before the actual Redis reply.
- `ServiceDispatcher` maps child publication errors to `ImportError::Worker` too. A busy-defer broker error is a different phase with no acquired file lease; it needs an actual original child-publication failure gate before any source exception mapping.
- `StoreError::Database(sqlx::Error)` already retains structured SQL errors. Read that variant directly; its display text deliberately hides database details and cannot serve as a source exception classifier.

## Additive interface proposal

Keep existing `IndexService::build` and `build_with_dispatcher` behavior/API for accepted callers. Add a detailed per-execution path returning a bounded report with normal `IndexOutcome` or a typed stage failure, plus whether the index commit happened and the physical release outcome. Stages should distinguish lock acquisition, source read, cache directory creation, file metadata/open/read, scan/resource guard, index transaction and lock release. Do not retain whole files, SQL rows, driver buffers or arbitrary error strings in the report.

Classified failures need bounded source exception module/type/args only for actual proven mappings. Preserve errno for the directory case. Preserve PostgreSQL SQLSTATE/message/context until the copied-trigger ProgrammingError mapping is gated; do not generalize that class to all database errors. Retain safe endpoint components and Redis error class through acquisition/release; do not format credential-bearing URLs. Source136 faults an already cached lock client. It does not prove original RedisClient initialization failure: original _init_client can exhaust retries and return None, making the later .set raise a different AttributeError. Do not map native connector/startup failures to the captured cached-client AuthenticationError/ConnectionError without that separate original gate. Resource limits, cancellation, admission, shutdown, lost ownership and ambiguous physical replies remain operational outcomes rather than invented Python exceptions.

Coordinate a separate task terminal-result type with the job owner. Existing copyable worker-control `Error` should not silently become a string-bearing result payload. An additive classified executor method can default existing executors to normal success or explicit operational error; ServiceExecutor overrides it only after its detailed service gate. A terminal result can distinguish null success, a captured raised exception, and an operational/unsupported outcome. No default adapter may fabricate a Source failure from `Error::Execution`.

A concrete additive signature can be `IndexService::build_report_with_dispatcher(request, caller, dispatcher) -> Result<IndexExecutionReport, ImportError>`. Its outer error covers admission/setup; a report retains operation outcome, stage/cause, commit observation (`NotWritten`, `Written`, `Ambiguous`) and physical release observation (`NotAcquired`, `Released`, `NotOwner`, `Failed`, `Ambiguous`). Both operation and release must be preserved, rather than losing the first error in cleanup. Exact precedence when both fail needs the queued original double-failure gate. A task-lock `Ok(false)` from native CAS is `NotOwner`, not evidence of a Python Redis exception; preserve ownership fencing and keep its policy explicit.

Result publication must use the observed SUCCESS guard: the later same-UUID execution can fail while backend bytes/date/TTL stay unchanged after prior SUCCESS. Source136's release failure is still FAILURE after a committed index, with its lock remaining; a new writer must not roll back/delete a committed index or claim release succeeded. Keep native owner fencing. A timeout is not proof of a connection error or absent side effect.

Exact original result fields/exception args and traceback null-versus-string behavior are captured in136. Native tracebacks must not be represented as executed Python frames without an explicit reviewed compatibility policy. Structured error and traceback-content parity require separate gates; a generic exception string is not enough.

## Gates after the performance quiet window

1. Replay the12 observed execution/result cases through a detailed IndexService adapter without writing any failure result. Assert normal outcomes, directory errno, SQL error identity, Redis acquisition/auth/release class, index commit state and owned lock effects.
2. Hold real SQL/file/Redis replies, cancel callers, and prove detailed errors/guards survive until actual completion. Verify release failure after commit and retain existing CAS replacement-owner controls. No local timeout may manufacture a settled failure.
3. Capture additional original parented/ETA failure metadata, SQL-plus-release double-failure precedence and result-backend outage/restart ambiguity.136 has initial tasks and a repeated initial UUID; it does not prove these cases.
4. Only then add the bounded writer/worker terminal-result path with the job owner. Compare actual original/native backend result fields/TTL and preserve existing SUCCESS bytes, then repeat actual Current HTTP background flow134 and independent family drains.
5. Application attachment, mixed queues, automatic visibility recovery and deployment volume cutover remain independent requirements.
