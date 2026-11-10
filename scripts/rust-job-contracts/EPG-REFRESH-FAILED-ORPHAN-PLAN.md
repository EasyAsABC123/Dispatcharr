# Post-quiet refresh Failed/orphan review — unexecuted design

Original Dispatcharr authors and contributors retain full application/algorithm
credit and AGPL licensing. Celery/Kombu, django-celery-beat, Redis/redis-py,
Django/PostgreSQL and dependency authors retain credit and licenses.

This is static review and a proposed gate/patch, separate from accepted frozen
worker76 and publication124. No implementation or execution of this plan occurred
during the category performance quiet window.

## Every Failed constructor

There are six constructors in `ingest-epg/src/file_job.rs` (not counting the enum
variant). A successful public return `Ok(Failed)` currently implies terminal
cleanup returned Ok and refresh release observed **Released**. If a file lease
was acquired, that lease also observed Released before the local outcome was
returned. `file_lease::Released::wait()` does not treat NotOwner, Failed,
RuntimeUnavailable or timeout as release success. However that proves release,
not the original operation's Source equivalence.

| Constructor | Source path/reference | Is blanket normal-null mapping safe? |
| --- | --- | --- |
| Invalid parser counter after bounded GET/int parse | `_source_tvg_parse_count` raises ValueError; outer refresh except updates error, then finally returns null unless cleanup fails. Ordinary bad-counter was in local-file17 reference. | No blanket string/cause inference. Native reporter must settle; Source int/repr edge cases still need exact vectors. |
| Missing ordinary local file with no URL | `fetch_xmltv` reports missing URL/file and returns False; wrapper returns null. Captured local-file17 case. | Candidate known reason after reporter+release succeed; not generic IO failure. |
| Incomplete local XML tail | `_xmltv_file_is_complete` returns False for stat/open errors/empty/bad tail; `_reject_incomplete_xmltv` reports and returns False. Captured ordinary incomplete cases. | Candidate known reason; distinguish missing path and unverified compressed fallback/mutation boundaries. |
| Any non-cancel channel error | ChannelError::Import, Scan, Final, LogoProperties, NoUrl or NeedsFetch; only Import(Cancelled) escapes as Err. | **Unsafe catch-all.** Includes Busy/Stopping/Capacity/Worker, unclassified SQL/runtime errors and partial effects; Source has no identical overload/runtime concept. Reporter failure here is also discarded by ChannelImporter. Preserve typed cause/stage before promotion. |
| Programme import returns ImportError::Incomplete | Source programmes tail check rejects before swap; native can reach this after earlier channel changes. | Not yet independently gated for mutation between channel/programme stages. Keep operational until exact partial-commit/effect proof. |
| Refresh defer exhaustion >=480 | `_defer_refresh_epg_data`: error status, no new child, normal return; actual Original worker50 and native producer/Source worker124 observed. | Best first promotion: record an explicit per-execution FileBusy/ParserBusy exhaustion reason only after physical reporting, then require successful final cleanup/release. Do not infer from request retry/status text alone. |

Source `_set_epg_source_status` and `_ensure_epg_refresh_terminal_status` swallow
and log failures inside their guarded query/update/report blocks; each calls the
DB-connection reset before that try block, so its failures must not be assumed
swallowed. Native reporter/terminal errors can propagate. Thus
not every native reporting failure is a Source terminal FAILURE either. No generic
exception or normal-success inference should be made from coarse Failed.

## Minimal exhaustion patch proposal

Add an immutable per-run report wrapper, preserving existing `run` and
`run_with_dispatcher` return types/defaults. The supervised internal execution
can retain an optional bounded per-run observation (no service-global reason).
After `failure(...).await?` in the exhaustion constructor, record typed
`RefreshBlockedExhausted { FileBusy | ParserBusy }`. Return that reason alongside
the legacy outcome **only after** existing terminal cleanup and physical release
succeed. Other constructors initially have no accepted normal-failure reason.

The worker maps only that explicit settled exhaustion reason to normal null.
Coarse Failed remains operational. Reporter/terminal failure, CAS NotOwner,
release error/timeout, cancellation and unsupported conditions cannot inherit a
reason from stale DB text or another task. Tests must include concurrent exhausted
and imported jobs, canceled/delayed physical reporting/release, and reporter or
release faults before the normal writer. Re-run old local-file17/physical/gzip
references and the worker76 closure. Actual Original parser-busy exhaustion
should be captured alongside the already observed file-busy case.

## Exact orphan helper design

`apps/epg/tasks.py::delete_epg_refresh_task_by_id` looks up the unique name
`epg_source-refresh-{id}`. No task returns False with no delete. It snapshots the
interval ID and **total** reference count before deleting the task. It then
deletes the interval only if that earlier count was1; shared intervals stay.
There is no explicit crontab/solar/clocked deletion. Interval DoesNotExist is
caught specially; other exceptions return False. The enclosing missing-source
refresh still returns its cleanup string regardless of that boolean.

Do not turn this into one all-or-nothing transaction: task deletion can commit
before later interval deletion fails. Conversely do not assume a single SQL
DELETE reproduces Django's collector/signals. Both EPGSource.refresh_task and
M3UAccount.refresh_task use ORM SET_NULL; captured PostgreSQL FKs have no database
ON DELETE SET NULL, so these updates must be observed and ported. Beat's
PeriodicTasks changed-marker updates and exact transaction order also need the
pinned package/source+SQL capture before native implementation.

Proposed bounded helper: target only the exact task-name row, record pre-delete
interval reference count, perform observed FK/marker/task effects in Source's
first deletion boundary, then the separately observed sole-interval boundary.
Retain caller/admission through actual SQL/commit/rollback. Return a detailed
cleanup observation even when Source's wrapper returns the misleading cleanup
string; never claim orphan removal solely from that string.

No concurrency strengthening is silently added. Source counts before deletion
and does not re-count immediately before deleting the interval. Concurrent task
creation/deletion may change effects; bounded initial implementation should
explicitly decline unsupported races rather than claim general race parity.
No cron/solar/clocked garbage collection, schedule reconciliation or scheduler
startup belongs to this helper.

## Post-quiet Source/native gates

1. Read/capture exact pinned PeriodicTask.delete, PeriodicTasks.changed and
   update_changed, interval deletion signal registration, and SQL statements.
   Do not infer marker order from table names.
2. Copied-DB actual Source helper/worker cases: no task; sole interval; shared
   interval; non-interval task; another EPG/M3U row referencing the task;
   nonexistent/changed interval where valid; old changed-marker presence/absence.
3. Isolated PostgreSQL triggers at task-delete and interval-delete boundaries:
   assert helper bool, surviving rows/FKs/marker, partial commits and actual
   worker result string. Preserve raw failures and default-DB audit.
4. Native bounded helper on equivalent rows, pool1, count/byte admission,
   delayed physical commit/rollback/caller abort, error classification and
   shutdown drain. Only then wire MissingSource cleanup into FileJob/worker.
5. Repeat Sourceproducer→nativeworker→SourceAsyncResult for orphan and
   exhaustion, then final frozen workspace/constructor/index/ETA regressions.

The existing worker76 intentionally leaves orphan rows and coarse Failed without
fabricated normal metadata. Its acknowledged unsupported bodies are deployment
gaps; no production intake/HTTP202 is enabled until those boundaries are solved.
