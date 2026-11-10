# Native programme-index worker contract and bridge gates

Original Dispatcharr authors and contributors retain all application and algorithm credit. This is a code/source/evidence plan prepared during the image28 measurement quiet window. It adds no runtime, Store, Cargo, application route, queue consumer or production publisher. No fixtures were executed for this plan.

## Evidence boundary

| Evidence | What it establishes | What it does not establish |
| --- | --- | --- |
| `verified-programme-index-task-source-native-90.json` | 24 original task/function outcomes and three native physical resource/rollback/replacement scenarios; correct five-second index deferral; stored map/clock/file-lease behavior | Actual Celery message, task attributes, worker acknowledgement, result envelope, restart or durable dispatch |
| `verified-current-order-source-native-106.json` | Original programme-first/full-metadata ordering, deferred stored serialization, actual relation-lock/cancellation and pool-one callback checks | An application-attached file callback or durable missing-index publication |
| `verified-current-real-bridge-source-native-107.json` | Real Store→real CurrentLookup, pool1, 19 literal Source outcomes plus six explicit unsupported malformed shapes, actual blocked-index cancellation and final callback byte ownership | Production Parsing: the publisher is a component spy; relative paths, outer error HTML and larger preexisting-index/server memory remain separate |
| `../rust-job-contracts/verified-original-celery-contracts-4.json` | 55 original observations using real backup tasks and neutral transport probes; measured global defaults and selected ACK/result/restart behavior | Index-task-specific task attributes, ETA child lineage, ignore-result or crash behavior |
| `../rust-job-contracts/verified-native-source-interop-1.json` | Typed backup publisher/result reader against actual original worker, app-unwired | Native consumer/worker; arbitrary task publication; EPG task or defer options |

Source90 intercepted `apply_async` to compare its arguments/countdown. It did not capture the broker envelope. Neutral Source55 `self.retry()` is not a substitute for the index wrapper's new `apply_async()` publication.

## Pinned Source task contract

Authoritative locations are `apps/epg/tasks.py:build_programme_index_task`, `build_programme_index`, `find_current_program_for_tvg_id`; `dispatcharr/celery.py`; `dispatcharr/settings.py`; and `core/utils.py:TaskLockRenewer` at revision `1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430`.

- The wrapper is `@shared_task` with `build_programme_index_task(source_id, _defer_retry=0)`. No decorator override supplies `acks_late`, `ignore_result`, `track_started`, autoretry, queue or max retries. The expected registered name is `apps.epg.tasks.build_programme_index_task`, also present literally in Source's memory-cleanup task list; record actual `task.name` before treating runtime naming as measured.
- The only explicit application task route is recording→`dvr`. Index routing is not explicitly overridden. Source55 measured default queue `celery`; exact resolved index exchange/routing/properties still need capture.
- Acquire `task_lock_epg_source_file_<raw source_id>` before looking up the source/file. The original NX lock uses value `locked`, TTL300seconds; its renewer defaults to120seconds. Source starts the renewer only after acquisition and stops/releases in `finally` after the build call.
- If the file key is held, `_defer_retry>=480` logs and returns. Otherwise publish a new invocation with `args=[source_id]`, `kwargs={'_defer_retry':_defer_retry+1}`, `countdown=5`, then return. This maximum applies only to the busy branch: an acquired lock still permits a build with a larger supplied retry value.
- This is manual rescheduling, not `Task.retry`. Do not replace the kwargs counter with protocol `headers.retries`, apply Celery's ordinary `max_retries`, or infer a `RETRY` result state. A new child UUID, parent/root lineage, parent result/children metadata and exact ETA representation must be measured in an actual worker.
- Missing source or missing selected file returns normally. A successful index write also returns None. Busy-deferred and exhausted wrappers return None. Actual success/null result storage depends on the task's effective ignore-result setting, which has not been measured specifically for this task.
- The builder reads inactive/dummy source rows too; it does not skip a fresh/existing index. It always updates/creates the stored index and its timestamp on a completed build. Do not add an existence shortcut or pre-check source existence before the file-lease branch.
- File/SQL errors propagate out of the wrapper after its `finally`; they are not converted into a successful returned-error dictionary. Exact worker exception/result encoding needs a Source capture.
- Source's global task prerun/postrun hooks call `close_old_connections`; this task is explicitly in the postrun memory-cleanup list (`force_collection`, heap trim). These are cleanup/resource observations to measure, not evidence that native RSS automatically returns to its initial value.

Source55 measured `track_started=False`, `acks_late=False`, prefetch4, broker pool10, visibility3600seconds, result TTL3600seconds, JSON serialization and result prefix `celery-tasks:celery-task-meta-`. `task_reject_on_worker_lost` was null in that captured configuration. Do not silently turn null into a measured False attribute. Index decorator/global inheritance suggests defaults, but actual task-bound attributes and index-worker ACK/result behavior remain unmeasured. In particular, do not claim index `ignore_result=False` solely because a neutral or backup task stored a result.

## Source-only probe needed after the full matrix

Use a new isolated original Compose fixture, synthetic source rows, owned files, separately configured broker/result Redis namespaces and an explicit fixture guard. Capture runnable tools, pinned source/image, task/app versions and actual effective task attributes before publication. Preserve failed attempts and return nonzero for incomplete observations.

1. Capture ordinary `build_programme_index_task.delay(source.id)` while no worker consumes it. Read its actual Redis/Kombu envelope without modifying payload. Record task name, args/kwargs and repr fields, protocol/content encoding, effective route/exchange/priority/delivery mode, retry/ignore-result fields and task identity. Do not infer fields from the existing backup writer.
2. Hold the exact source-file key using an owned marker; run the original task in a real original Celery worker. Observe the original busy branch's actual new publication, ETA/time bounds, `_defer_retry`, root/parent IDs, new ID and caller/result children. A synchronous `after_task_publish` observer can capture the actual queued envelope before the same single/solo consumer resumes; prove the observer read that exact child ID. Do not replace this with manually calling `apply_async` outside a worker.
3. Prevent unrelated plugin/version/DVR/network work from executing. Reuse the accepted fixture technique: quarantine only unrelated messages in this owned broker with task type/hash recorded, and use Source's own startup dispatch NX locks. Use the real production Celery executable/argv path; earlier Source55 setup showed that `python -m celery` changes startup behavior. Bound worker startup, observation, SIGTERM and exact-process-group escalation/cleanup.
4. Capture the task's effective `acks_late`, `ignore_result`, `track_started`, result serializer, routing and limits. For an actual in-progress ACK witness, an owned FIFO can gate the original raw-file reader without replacing the task body: connect an owned writer, keep it open without bytes, observe the specific task's unacked entry/status and file key, then send a tiny guide and EOF. Treat FIFO as Source-only observation apparatus; native regular-file limits do not thereby gain FIFO support.
5. Observe successful/null, missing source/file, held-lock defer and retry480 exhaustion results, timestamps/TTL and parent/child metadata. Do not apply the backup status endpoint's non-dictionary behavior to a nonexistent index status API; CurrentPrograms returns parsing and polls the index.
6. If testing hard loss, stop only the owned worker with its exact identity; record early ACK loss, file-key TTL and result state. Do not wait or shorten3600-second visibility/300-second lease durations and then call that an unmodified expiry proof. Graceful ETA reservation restore and hard-loss recovery require separate observations.

No runnable command is advertised yet: the EPG envelope/worker controller is not implemented. After quiet, add a new owned probe rather than modifying accepted Source90/55 controllers. A successful Source probe is still not a native worker acceptance gate.

## Native handler/dispatch bridge

The existing `dispatcharr-job-transport` typed `Task` supports only captured create/restore backup publications. It has no EPG variant or generic ETA/parent options, and its connection authority is fixture-only. `Jobs::publish` holds caller/transport credits through the physical broker reply; `AmbiguousPublication(id)` is not an acknowledgement. Never turn ambiguity or a process-local queue into a production Parsing response.

After the Source envelope gate, add only the measured typed index argument/lineage/ETA family. Ordinary missing-index publication comes from the HTTP lookup and should preserve its measured no-worker-parent form. File-refresh dispatch and busy self-deferral occur inside worker contexts and need their actual root/parent behavior. Do not serialize caller guards, user credentials or opaque execution contexts.

Use one typed per-execution worker trace containing task ID, root/parent IDs and separately named manual-defer/protocol-retry fields. Acknowledged child IDs may need to appear in the original parent's result children. The current `IndexServices` contains one fixed dispatcher returning `Result<()>`; mutating a global "current parent" would mix concurrent jobs. Prefer an additive per-build dispatcher override (old `build` behavior delegates unchanged), whose instance owns that job's trace and acknowledged child list. Do not use unsafe thread/global context across supervised tasks.

The handler invokes existing IndexService with source ID, manual retry, cancellation flag and an owning worker context. Map Written/MissingSource/MissingFile/Deferred/Exhausted to the measured original normal return/result behavior; do not invent failures for normal missing/exhausted outcomes. Transport, parser, SQL, limit and ambiguous-publication errors require separately measured/mapped outcomes. Keep unsupported raw argument families explicit rather than silently coercing bool/string/float task IDs into different lock names.

Worker intake, pending ETA records, active jobs, file state, SQL/driver buffers, result/publication bytes and queued physical commands need independent finite admission. ETA reservation is not execution: early ACK before handler execution must not be confused with ACK immediately when a future-ETA message is first received. The existing transport does not implement consumer reservation/ACK/restore; that is a separate owner/gate, not an LPUSH adapter.

## Physical ownership and clocks

- Keep worker admission/trace/caller credits through actual claim/ACK/result/publication replies, not merely their logical timeout. After source-compatible early ACK, handler/file/SQL resources still remain owned until their physical work completes.
- Reuse IndexService's owned supervisor, memory reservation, parser task, private SQL writer, owner-checked file release and cancellation/rollback behavior. Hold the execution context through actual file read, commit/rollback, connection close and Redis release reply. A canceled waiter or expired HTTP request is not physical completion.
- Keep ETA publication clock, Redis lease/visibility clocks and index `updated_at` clock separate. Source index timestamp samples after its database row lock; do not precompute it at message receipt or before a blocked SQL writer proceeds.
- Source90 covers observed replacement/cancellation boundaries, not an atomic Redis/PostgreSQL fence. The native commit cancellation check is a defined boundary; do not claim impossible cross-system atomicity.
- A Redis renewal call can remain pending beyond the file TTL. Existing source90 explicitly leaves a local renewal-outage deadline unverified. Before production worker attachment, prove that local expiry stops stale work while physical renewal/release command guards remain held; delayed replies must not reopen admission or authorize a stale write.
- Stop intake on shutdown, signal owned work, and wait a global deadline. Report incomplete drain if physical jobs/commands still exist. Forced process exit is not successful resource cleanup.

## Source CWD/media/volume context

Use the detailed `rust/crates/ingest-epg/RELATIVE_PATHS_PLAN.md` gates. Source web and Celery operate from `/app`; MEDIA_ROOT is `/app/media`, created as an image-layer application directory, not a proven `/data` symlink. CurrentLookup rejects relative paths; IndexService currently resolves selected relative paths against its process CWD, which is also not a Source-compatible context guarantee.

Introduce a shared explicit Source working-directory/media-root context only after its owned-file oracle passes. Preserve first-truthy extracted/file/cache choice, missing-preferred-path behavior, literal stored strings, OS symlink/dot semantics and independent lookup forward opens. Default fallback is `MEDIA_ROOT/cached_epg/<id>.tmp`. Package/rehearse any compatibility mount or image-layer cache migration, UID permissions and restart/rollback before app attachment. Do not silently change the entire native process CWD or reinterpret file_path as a URL.

## Native acceptance sequence

1. Actual index-specific Source task/envelope/worker/ACK/result evidence above.
2. Typed native index publication accepted by the original worker, with caller cancellation/delayed replies and exact lineage/ETA support.
3. App-unwired native handler invokes IndexService on real disposable PostgreSQL/Redis/files and matches maps, timestamps, normal/error/defer outcomes. Mirror Source worker crash/ACK policy explicitly; stronger recovery is not an automatic compatibility pass.
4. Prove concurrent per-job parent traces, owner replacement, delayed renewal beyond TTL, canceled parser/SQL, result-write failure, ambiguous child publish, bounded ETA/backpressure and shutdown. Keep Source and native failure artifacts.
5. Real CurrentLookup→acknowledged job→native handler→stored index→subsequent real Store/file response, including pool1, restart and actual caller byte ownership. No test spy in this gate.
6. Only then attach application callbacks/dispatch/worker topology. Run unchanged React parsing→current-programme behavior, existing-data/path migration and workload/resource comparisons with full PostgreSQL/Redis/worker footprint. Functional gates do not establish a material total-service improvement or a completed milestone.
