# Original durable-job contract capture

Original Dispatcharr authors and contributors retain full application credit and AGPL licensing. This directory records original behavior; it does not provide a native broker, worker, scheduler, job202 endpoint or production restore authority.

`verified-original-celery-contracts-4.json` binds the pinned original image/revision, captured tools, actual Source report and55 HTTP/transport assertions/observations. `source-goldens.json` preserves the observed status entities and protocol shape without JWTs, HMAC task tokens, raw task IDs, archive data or password hashes. The disposable application database contained26 nonempty tables; the default fixture database manifest remained unchanged. All containers/volumes were removed. Failed attempts remain separately bound and explained.

## Observed transport

The installed original versions are Celery5.6.3, Kombu5.6.2, redis-py6.4.0 and Django6.0.9. A real malformed-JSON POST to backup/create succeeds202 because the handler never reads request data. Before a worker runs, the task is durably present on Redis list `celery`, with JSON protocol2 headers/properties and a base64-encoded JSON three-element body: positional arguments, keyword arguments, embedded callback/chain/chord/errback metadata. Delivery mode2, priority0 and routing keycelery were observed. No native publication is inferred.

Broker DB8 and result DB9 were explicitly separated in this fixture. Actual result keys have prefix `celery-tasks:celery-task-meta-`; JSON fields are children/date_done/result/status/task_id/traceback. Original result expiry3600seconds was measured on successful tasks. A separate fixture-only one-second EXPIRE showed that a fresh status query becomesPENDING after result deletion; the result3600second expiry itself was not waited out. Producer web restart leaves the queued task intact.

The exact production `/dispatcharrpy/bin/celery` executable matters. `python -m celery` sets a different argv basename and misses the original application's initialization skip, potentially enqueuing an unrelated plugin refresh. The successful capture uses the real production entrypoint. Original web startup also queues plugin refresh; its name/envelope hash were captured and the message quarantined in the owned broker before worker consumption. Original DVR/version worker-ready one-shots were suppressed through their own Redis dispatch locks. No unrelated plugin/provider/version work was intentionally executed.

## Observed status and authorization

Unknown or active untracked tasks expose HTTP200 `{state: pending}`. Real neutral fixture tasks with the explicitly documented `track_started=True` override produced backend STARTED and RETRY, observed as lowercased states. This does not claim original backup tasks emit STARTED: their actual default is `task_track_started=False`.

A real original backup task created a ZIP on the copied database and produced CelerySUCCESS / HTTP200 completed. An actual invalid ZIP restore failed before schema destruction, was caught by the original task and returned a failure dictionary: CelerySUCCESS / HTTP200 failed. An uncaught ValueError is CeleryFAILURE but becomes HTTP500 from the status endpoint: `ready()` is true, and the following `get()` propagates before the handler reaches `failed()`. SUCCESS strings, lists and null similarly cause Source attribute-error500 responses rather than a successful job state. An empty result dictionary yields failed/Unknown error; an explicit null error remainsJSONnull.

DRF credential authentication runs before the token branch. Invalid Bearer and inactive JWT return401 even with valid task tokens. A truthy invalid token returns403 even for an authenticated admin; empty token uses the manual admin fallback. A valid token admits anonymous or viewer status reads. Without token, a viewer receives401 Authentication required; a blocked-UI admin still receives status200 because this AllowAny status view does not apply the list/create UI-network permission. NonASCII task tokens trigger the observed SourceHTML500 outside the inner status try block. Do not substitute the general admin/UI helper for these status rules.

## Acknowledgments and unproved recovery

Defaults observed: acks_lateFalse, track_startedFalse, prefetch multiplier4, broker pool10, visibility timeout3600seconds, acks_on_failure_or_timeoutTrue. A blocked neutral task with those defaults was already absent from its own unacked reservation while active, and its result remainedPENDING. Hard-stopping the worker process group then starting a replacement did not immediately requeue that acknowledged active task; fresh work succeeded on the replacement. This demonstrates an early-ACK loss boundary, not durable native recovery.

The one-hour visibility-timeout expiration was not observed. Prefetched unacked messages, warm-drain behavior, full long-delay redelivery, duplicate delivery/idempotency, durable native claims, restoration quiescence, scheduler coupling and successful restore jobs remain separate gates. The neutral tasks are transport probes, never application task implementations. Actual backup tasks retain their original code and parameters.

## Reproduction and next component

Source-only fixture:

```sh
. artifacts/rust/tooling/environment.sh
python3 scripts/rust-job-contracts/run_source.py --output artifacts/rust/m8-original-celery-contracts-NEW
```

It creates only an explicitly guarded `rust_probe_backup_*` database, owned Redis DB8/9 and files under its evidence directory. There is no production restoration. A bounded pure native status mapper may be tested against these Source vectors next; it must remain disconnected from production routes and job202 until the durable transport, handler and restart gates in `docs/rust-jobs-acceptance.md` pass.

A test-only native projection now passes11 Source entity/assertion vectors in3 tests, plus strict all-target Store Clippy; see `verified-backup-status-component-1.json`. It accepts already-decoded typed snapshots and preserves returned-failure versus propagated exception behavior. Result bytes/nodes/depth/messages are bounded. This is not a Celery wire decoder, HTTP authorization implementation, durable transport or executable worker. Source-derived completed echo and primitive type controls are explicitly separate from actual captured vectors. Production Store routes, app wiring, Cargo and backup202 behavior are unchanged.

The complete original producer-envelope extension now passes26 checks/four publications with no worker or restore: `verified-original-job-envelope-2.json` and `source-envelope-goldens.json`. Actual deterministic headers/properties/body, private raw bytes, routing binding, retry policy and hashed ID/origin/reply/delivery relations are captured across two webproducer epochs. Internal network plus container-local readiness prevented external egress. A fixed per-process reply_to must not be assumed: this two-thread producer returned different reply targets within one epoch. This is Source-only evidence, not a native publisher/result decoder or consumer-order gate.

The status component was additionally retested from a precise480-file Rust/license snapshot with its own Cargo target, with3 tests and all-target Store strict Clippy passing. `verified-backup-status-frozen-1.json` binds archive, module/dataset and both actual logs; it does not extend production runtime scope.

Raw Source result/exception extension2 passes67 actual observations and24 complete bounded metadata captures; `verified-original-job-results-2.json` binds private raw JSON and public `source-result-goldens.json`. Application backup/invalidZIP handlers remain distinct from neutral builtin exception/Kombu date/datetime/Decimal/bytes/base64/UUID probes.

Native publication/result interoperability1 passes65 observations with the real Sourceworker on copiedDB (`verified-native-source-interop-1.json`). The separate physical gate passes2 actualRedis TCP reply-proxy tests (`verified-native-job-resources-1.json`): accepted LPUSH before withheld reply, ambiguous caller timeout, retained caller/admission leases, rejection/recovery and shutdown waiting through canceled result requests. Both were run from exact frozen source/binaries with defaultDB unchanged and project removed. No nativeworker, public202, productionconstructor, scheduler or restore quiescence is enabled. Earlier reports retain their original narrower remaining-gate statements; these later proofs are independently bound.

The final index component closure is
[verified-final-index-worker-closure-3.json](verified-final-index-worker-closure-3.json).
It binds retained compiler inputs, selected successful tools and coupled ingest
all-target test inputs. Constructor19/ETA20 and14 physical tests were replayed on
one tree; the final checked-count guard has11 unit tests and strict Clippy. An
actual24-check Source/native repeated-ID witness verifies real SQL reexecution
with preserved existing SUCCESS metadata/TTL for immediate and ETA handlers.
`AlreadySucceeded` is a result-write outcome, not an execution skip. The standalone
16-check Original authority is
[verified-original-index-duplicate-2.json](verified-original-index-duplicate-2.json).
Earlier failed fixture assumptions and resource-isolation attempts remain bound
in the manifests. App attachment, autonomous visibility restoration, exception
writing, public202, successful restore and periodic scheduling remain open.

The test-only bounded visibility component has3 actual Redis gates,12 library
tests and strict Clippy in
[verified-index-visibility-resources-1.json](verified-index-visibility-resources-1.json).
It restores one batch of at most10 index reservations with score/tuple/mutex
fences, preserves foreign/fresh reservations, and retains owners through an
actual delayed restore reply after its server-side effect. It has no production
registration, polling loop or application hook. The captured Original worker
passes `unacked_restore_limit=None` (full eligible sweep), unlike the helper's
num10 default; see
[verified-original-visibility-static-1.json](verified-original-visibility-static-1.json).
Elapsed polling cadence, visibility expiration and full-sweep equivalence remain
unproved.

The separate [failure codec draft](INDEX-FAILURE-CODEC-DRAFT.md) was written during
the performance quiet window. It is unregistered and unexecuted. No exception
classifier, writer or traceback generator is promoted by that draft.

The test-only failure payload component now passes36 actual Original-reader
checks,2 physical Redis tests,14 library tests and strict Clippy; see
[verified-index-failure-payload-writer-2.json](verified-index-failure-payload-writer-2.json).
It relays the known Source136 exception data and opaque supplied traceback text,
preserves existing SUCCESS bytes/TTL and verifies actual Source exception
reconstruction. It does not classify or execute native errors, generate Python
frames or attach a worker/API. The earlier quiet-window draft note is superseded
by this bounded component evidence; native failure behavior remains pending.

The later [native terminal result checkpoint](INDEX-TERMINAL-RESULTS.md) adds typed
terminal handling to both worker paths, actual Directory/P0001 IndexService
classification through a test-only adapter, and physical failure-result resource
gates. It supersedes the earlier pending statement for those named cases. Native
diagnostic text explicitly differs from Python traceback frames; general failure
classification, recovery and application attachment remain separate gates.
