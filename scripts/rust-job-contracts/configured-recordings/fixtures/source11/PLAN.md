# Next M6: configured Series and enabled recurring recording authority

PRIVATE, UNEXECUTED preparation. No fixture slot is reserved; Root currently holds heavy work for package solo gates. Original Dispatcharr/Celery/Django authors retain application/algorithm credit and licenses. No new Native runtime, Main/default graph, transport authority or recording/media consumer is enabled.

Pinned Original commit1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430 must be archived explicitly. Existing `scripts/rust-job-contracts/SERIES-RULES-CONTRACT.md` and Source79/production155 prove only no-rule/busy; `DEFAULT-TASKS-PLAN.md` Source93 proves disabled recurring0, not enabled rules. This work captures actual framework workers, not direct implementation-function calls or a Factory substitute.

## Concrete dependency discovered from pinned code

`apps/channels/tasks.py::_evaluate_series_rules_locked` creates Recording then explicitly queues `prefetch_recording_artwork(countdown=1)`. Recording.post_save in `apps/channels/signals.py::schedule_task_on_save` ALSO queues artwork and calls schedule_recording_task. For a future start, schedule_recording_task creates ClockedSchedule plus enabled one-off PeriodicTask named `dvr-recording-<recording-id>` and returns that name; it does NOT immediately publish run_recording. Therefore the first positive Series case should expose two artwork children plus one clocked Beat row, rather than silently discard a duplicate or invent an immediate recording frame. Recurring creation uses the same signal and should expose one artwork child plus one Beat row per new Recording.

Creating the owned Channel with EPGData also publishes a real `apps.epg.tasks.parse_programs_for_tvg_id` setup child via refresh_epg_programs. That setup frame must remain intact and be captured before the first case; five artwork frames therefore coexist with this additional setup frame. No programme helper task is executed or removed. The inactive EPGSource creation itself does not queue refresh_epg_data (pinned is_active guard).

The first capture intentionally routes ONLY the owned Series/recurring parent executions to `rust-owned-configured-recordings`. All descendants keep their actual default queue/options/IDs and remain intact/unconsumed; they are captured before every assertion and survive until the owned disposable project is removed. This is a declared initial Source component control, not a complete recording/media/artwork workflow. Unknown task families are never removed/quarantined to obtain a pass. No Beat or default worker is started; the host must prove any Original supervisor worker/Beat is stopped before beginning, then retain actual process/role inventories. The bounded host wrapper/gate is implemented in prep2 and remains subject to Root final seal/input/slot review before dispatch.

## Fixed eleven-case corpus before results

A unique owned Channel/EPGSource/EPGData and exact title/tvg/episode identity isolate all programme matching. Real UTC anchor is captured at fixture start, with Series programmes starting tomorrow at anchor+24h/+26h/+28h and one-hour durations, within Source's seven-day horizon. No replacement of timezone.now/task/helper is allowed. UTC system timezone, zero pre/post offsets and explicit full dictionary rules are fixture settings, retained in raw output. Native replay must use the retained absolute programme times and reject an expired reference; it cannot shift dates after a difference.

1. dictionary `{}` invalid_rule, no Recording.
2. one exact-title/tvg/source/channel rule and future episode1, one Recording.
3. repeat identical rule/programme, no extra Recording or child.
4. add separate episode2 and repeat, one new Recording (first remains deduped).
5. pass explicit unmatched tvg filter, no_rule, no Recording or frontend event.
6. real foreign global Series lock, skipped result, unchanged token, no Recording/settings work.
7. add episode3, actual owned Recording BEFORE INSERT P0001 trigger, observe caught per-programme error and normal task result; no pretend global FAILURE.
8. enabled UTC recurring rule for exactly one tomorrow date/time, one Recording.
9. repeat, no extra Recording/child.
10. disable that rule, maintenance returns0; existing Recording/Clocked/Periodic rows remain.
11. separate enabled recurring rule with actual Recording INSERT P0001, observe caught warning/normal integer0 and partial/no event effects exactly as Source returns.

Expected code-derived created counts are0,1,0,1,0,0,0,1,0,0,0. The hypothesis is three total new Recording rows, three one-off Clocked/Periodic rows and five retained artwork frames (2+2+1). These expectations are predeclared and must fail literally on actual divergence; no outcome-driven removal, reordering, extra case or retry. Complete raw frame ledger and SQL/results are saved BEFORE assertions, including failures. P0001 triggers/functions are disposable copied-DB only, removed after their observed case; no real provider/network/files or media execution.

## Collector and authority projection

Record all SQL Recording/Recurring/Periodic/Clocked rows, complete custom_properties, every UTC/date/time field and Beat change sentinel; full actual Channels messages with raw bytes; Django DVR cache pickle bytes/TTL and decoded bounded fixture dictionary; raw Redis lock tokens/TTL; actual broker frames/unacked and backend through app.backend.get_key_for_task; task publication/prerun/postrun/SQL attempts including exception class/SQLSTATE, all IDs/args/options/root/parent/children, own process UID/cwd and real task attributes. Actual Source AsyncResult returns typed normal dict/int plus full metadata/TTL. No blanket timestamp stripping. Parent windows are real UTC; previously seeded times stay literal. Cache read-only diagnostics are small/racy fixture observations, not production transfer bounds.

Before any publication or invariant check, retain a durable complete snapshot/transport ledger. Write-all/fsync/dirsync helper is reused from the qualified ledger; capacity refusal emits a small explicit receipt before stopping. On host interruption, attempt-all owned worker stop/actual waits, defaultDB11 before/after audit and independent exactCID/all-project containers(--all)/networks/volumes are mandatory. Process0 and semantic pass are separate. Never flush/delete legitimate queue/results during cases. Source task execution code is unchanged.

## Native implementation boundaries after Source review

Promote the existing regular Series service with a bounded one-channel/one-rule exact-title/all-mode/future explicit-EPG-source subset first, reuse existing typed Store/TaskLocks/cache/event/transport rather than second services. SQL queries need server-side bounded projections and explicit unsupported flags before ACK; control all Recording/model-signal/Clocked/Periodic/Beat-marker and duplicate artwork publication effects. No early ACK authority follows from conservative admission. Preserve old no-rule/busy API behavior. Unknown filter/title grammar, multi-channel precedence, custom/dummy/offset/date/DST/active-recording/network paths remain explicit unsupported. Positive recurring should reuse the SAME Recording scheduling primitive after its actual Source authority, not duplicate SQL.

Physical QA must include held settings/Recording/Clocked/Periodic SELECT/INSERT/UPDATE Ready, caught errors and Source partial commits/order, Channels and both child publications, result oldSUCCESS/TTL/lost/denied replies, cancellation/caller retention/admission/shutdown through actual I/O, foreign lock replacement and no stale writes. Unknown completed/unsettled errors remain operational unless the precise Source caught boundary is proven. Actual Native worker → Original decoder and future complete application graph must consume both duplicate artwork children/recording families or explicitly refuse before enabling intake; no 202/import workflow completion claim with a blocked FIFO head.

## Remaining preparation before any Source launch

Final Root review/admission of the implemented host pinned support/archive/image/role/default-audit/cleanup wrapper and closure/finite once-plan is still required. The draft AST and41 socket-free negative controls passed after Root released pure work; this is not Source authority or an actual fixture pass. Root reserves a global slot and approves an exact seal first. No Source rerun of old79/93/Source4 is needed for planning labels.
