# Next known-interval heap milestone — predeclared plan

Original Dispatcharr/Celery/django-celery-beat authors retain credit and licenses. This plan follows the exact framework files retained by `m6-source-native-scheduler-final-1`. It changes no milestone ledger, main configuration or default intake.

The six eager defaults are distinct from the two supported periodic task families:

| Default row | Actual registered task | Schedule | Current native execution authority |
| --- | --- | --- | --- |
| `celery.backend_cleanup` | `celery.backend_cleanup` | UTC crontab 04:00, expiry 43200 | Unsupported |
| `fetch-channel-statuses` | `apps.proxy.tasks.fetch_channel_stats` | 2 seconds, explicitly disabled | Unsupported |
| `scan-files` | `core.tasks.scan_and_process_files` | 20 seconds | Unsupported |
| `maintain-recurring-recordings` | `apps.channels.tasks.maintain_recurring_recordings` | 3600 seconds | Unsupported; not an alias for Series evaluation |
| `check-version-updates` | `core.tasks.check_for_version_update` | 86400 seconds | Unsupported |
| `check-account-expirations` | `apps.m3u.tasks.check_account_expirations` | 86400 seconds | Unsupported |

The known families are `apps.epg.tasks.refresh_epg_data` and `apps.channels.tasks.evaluate_series_rules` with no configured recording rules. They are independently created periodic rows, not replacements for the six defaults. Installing defaults does not authorize running them.

## Concrete runtime blockers

The current bounded loader stores entries in a name-ordered BTreeMap, losing database iteration order. Tick rejects more than one enabled entry, has no actual heap, and sync rejects more than one dirty row. A boolean only records whether the single entry was initialized. Generalizing this requires retained heap state, Original reservation/reinsertion semantics, sentinel-triggered rebuilds, and multiple dirty saves with original per-row commit/error boundaries. Existing count/field/aggregate limits, two owned admissions, physical command lifetime and one-deadline driver shutdown must remain unchanged.

Original's SQL queryset has no declared ORDER BY for PeriodicTask. The fixture must capture query text, actual loaded order and heap events; it must not assume lexical name order or silently sort outputs. `_when` reads each entry's clock separately, heap priority is always 5, and a true equal `(time, priority)` tie uses `id(entry)` in `ScheduleEntry.__lt__`. Source explicitly calls that final ordering arbitrary. Equal last-run timestamps are not necessarily equal heap times. Capture these separately rather than claiming a stable cross-process label order for an identity tie.

Original's dirty collection is a Python set. Its database-error partial-commit order is not an ordered-name contract. The first multi-row implementation must prove successful multi-dirty sync and separately fault a single dirty row in a multi-entry cohort. Multi-dirty server-error ordering remains open until its actual Source behavior is captured and a predeclared faithful contract is agreed; do not substitute name sorting after a diff.

## Predeclared Source cohort

All data is owned copied PostgreSQL; broker8/result9, inactive refresh and Series0 only. Defaults are installed by the actual eager constructor, then disabled with the real sentinel. Do not consume unrelated tasks. Record chronological publication observations and raw queue list/RPOP order separately; LPUSH list order is not delivery order.

1. Create `z-created-first-refresh` then `a-created-second-series`, both 3600-second intervals, equal last-run timestamps two hours in the past. Predeclare creation-order publication for this physical fixture, conditioned on capturing the unchanged queryset's actual load order; fail and preserve any contradictory observation instead of sorting it away.
2. Reverse the task families while keeping reversed-lexical labels and creation order. Capture heap events before each pop and the exact two ordered frames.
3. Future refresh first, due Series second: only Series publishes. Capture the remaining heap and returned delay.
4. Due one-off refresh and due regular Series: capture all mutations, count/reset/marker changes and four consecutive ticks, including when completed one-off disable becomes observable.
5. Disable one entry using an ordinary model save and sentinel after the first tick. Capture dirty sync, cache reload, heap rebuild and subsequent publications.
6. Delete one owned row and publish the sentinel. Capture missing-row sync disposition and the remaining entry without recreating the deleted task.
7. Real LPUSH-denied producer: two known due entries, two bounded tick attempts, exact attempt order/counts, zero durable publications, then explicit SQL sync. No fabricated broker error.
8. Controlled true equal heap times: inject clock dependencies into the unmodified Original entry/schedule clocks, record entry identities and exact heap tuple order before execution. Assert Source's own identity comparator, keeping this distinct from ordinary equal-due real-clock ordering. Do not impose a post-hoc cross-runtime alphabetical order.
9. Graceful stop/caller cancellation and SIGKILL windows for a multi-entry cohort: capture queued frames, dirty/persisted rows and fresh restart publication order before implementing a native counterpart. A before-sync duplicate is allowed only where the actual Source shows it.

No corpus or acceptance rule is retuned after observing a native diff. Unknown tasks/options and calendar schedules remain explicit unsupported; no main/default scheduler enablement is part of this milestone.
