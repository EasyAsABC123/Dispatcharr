# SeriesRules child boundary

Original Dispatcharr authors retain full application/algorithm credit and AGPL licensing. Celery/Kombu/Django retain their framework credit. The SeriesRules consumer remains unimplemented in this prerequisite scope; successful imports must not silently discard its FIFO message.

`verified-series-rules-prereq-1.json` binds seven actual Original workers (79 checks), then a repeated Original corpus plus native cache/lock tests (110 checks). Absent/empty DVR settings, empty/non-list `series_rules`, and arrays containing no dictionary rules return exactly `{"scheduled":0,"details":[]}`. A held `task_lock_evaluate_series_rules_all` returns the exact skipped/concurrent detail without reading the DVR settings group. No recording row changed, child task was published, or unrelated family executed.

The absent-group case uses a bounded observer task_prerun timing gate, then blocks the real application's settings SELECT with a copied-database table lock. The actual task owns its global lock, has no result, and is already absent from queue/unacked storage. Application functions remain unchanged. Parented options are separately labeled fixture controls; they are not a fabricated real refresh parent execution.

The group is shared Django protocol4 data with 300-second TTL. Missing/empty DB groups fill all thirteen actual defaults; a stored nonempty dictionary is not merged with defaults. Native cache hits and DB-fill results match Original Django in ten rounds, including Original loading native fills. The typed native global entry reuses physical acquisition/cleanup/fencing, accepts no arbitrary key suffix, and deliberately starts no renewer, matching this Source function. A short-TTL actual Redis test confirms no renewal and successor-token preservation. Existing numeric-ID acquisition still renews.

The pure, test-gated no-argument Series wire parser reuses the observed common protocol2 validator, retains exact parent/root relationships, rejects other task families/options, and recognizes only the two captured zero-scheduled returns. Its dataset normalizes volatile UUIDs/origin only. Configured dictionary rules, recording scheduling and its additional child families remain separate Source-first work; no coarse success or dequeue authority follows from the parser or cache tests.

## Native no-recording execution component

`verified-series-worker-2.json` binds 147 checks: the Original79 reference plus ten actual Source-producer/native-consumer/Original-AsyncResult cases. Seven known no-rule/busy/parented cases and one held-SQL case write only their exact observed zero-scheduled results. Configured dictionary rules and a foreign backup task remain byte-identical in the FIFO with PENDING/no backend record. The fixture never executes either unsupported branch.

This native consumer performs an explicit configuration support check before its compare/pop. That preflight is **not** Source early-ACK timing parity. Known accepted no-rule work retains its global lease and settings/command ownership. An actual copied-database SQL lock plus aborted outer waiter keeps the slot/lease/caller alive; shutdown reports incomplete until rollback and actual replies settle. The final result is independently read by Original AsyncResult; the aborted waiter has no fabricated return. The first139-check artifact remains unchanged; the second adds a test-only physical control against the same runtime.

Intake remains fixture-gated. Production constructor/polling, default app attachment, configured rules/recording scheduling and refresh15-second ETA work are separate. The complete import→Index/Series→second import→guide workflow must pass before enabling the incomplete application lane by default.

## Explicit production authority and prepared handle

`verified-series-production-2.json` binds 155 checks/eleven native cases to explicit production Series authority, the same no-recording executor and a prepared service. Preparing leaves the actual queued envelope unchanged for100ms and starts no intake. `start()` begins bounded polling, a second start fails, and shutdown drains the family and its independently owned transport under one deadline. The Series client rejects backup, refresh and programme-index publication; the original index authority remains unchanged.

Integration uses `SeriesApplication::prepare(store, Options { broker, backend, cache, poll_interval }).await`, then `start()`, then `shutdown(grace).await`. No event sink is needed by the two captured no-rule/busy return branches because Original returns before its recordings notification. Configured dictionary rules remain visible as unsupported; no recording/artwork task is consumed. Main/default enablement, the full two-import/guide workflow and refresh ETA handling are not accepted by this constructor proof.

Production attempt1 is retained: the copied driver referenced `WorkerPanic` instead of the Series `Panic` variant. Compilation failed before any fixture containers/runtime. Corrected input2 passed strict Clippy and the actual155 gate. The earlier139/147 artifacts retain their exact scopes.
