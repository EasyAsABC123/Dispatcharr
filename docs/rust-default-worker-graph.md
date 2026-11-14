# Actual default Main graph audit after batch3

Original Dispatcharr/Celery/django-celery-beat and dependency authors retain all
application, algorithm and protocol credit. This is a source audit, not new
runtime acceptance. Current runtime is bfb4086; public evidence is 3143f9a.
Do not edit the protected legacy scheduler/EPG/DVR work in progress.

This is the selected14-family integration graph, not the complete Source task
registry. The [Source task coverage audit](rust-source-task-coverage.md) finds47
static application task declarations and requires a producer/callback crosswalk.
Core rehash and recording-offset rescheduling are already observed natural
producers outside this selected graph. Its completion cannot alone close M6.

| Family / canonical task | Actual Main route | Reusable implementation and remaining composition |
|---|---|---|
| Scanner / core.tasks.scan_and_process_files | absent | scanner_files::Worker, logo_scanner/LogoApplication and scanner_index_helper exist. Their mapped-file and logo-only domains refuse mixed populated roots; combine Original scan ordering, first-scan state, actual child provenance and totals into one normal service. Native image/data-init currently creates only logos, while Original watches /data/m3us, /data/epgs and /data/logos. |
| M3U groups / apps.m3u.tasks.refresh_m3u_groups | absent | scanner_m3u::Service + scanner_m3u_worker + M3uPorts exist. Scope is scanner-parented; derive it from actual scanner report/backend/publication rather than fixture parent input. Direct model/API producers and remote/Xtream domains still need their actual normal adapters. Service requires its own shared Store/Auth pool with max_connections=1; Main currently uses a larger pool. |
| M3U single / apps.m3u.tasks.refresh_single_m3u_account | absent | Same retained local-M3U service/ports; actual parent capabilities, direct/periodic producer shapes and normal API publication remain necessary. |
| Refresh / apps.epg.tasks.refresh_epg_data | enabled EPG graph | refresh_application + configured Refresh→Series handoff work for captured direct/API initial scope. Scanner-parent descendants exist only in scanner_refresh::Worker; add dynamic actual scanner provenance to production composition. Deferred/retry/restart/other HTTP variants remain required. |
| Programme index / apps.epg.tasks.build_programme_index_task | enabled EPG graph | index_application owns dispatcher, CurrentLookup, service and driver. Native API chain actually passed. Keep current-file path context and exact retained child publication. |
| Per-channel programmes / apps.epg.tasks.parse_programs_for_tvg_id | enabled EPG graph | tvg_programme_application actually passed success/no-op/P0001 and next-task continuation. Unknown physical failures remain operational; held-Ready is unmeasured. |
| Series / apps.channels.tasks.evaluate_series_rules | enabled EPG graph | configured_series_application actually passed one exact configured rule and normal artwork. Default empty rules currently fault its configured-only preparation: the four-path additive normal no-rules overlay fixes that without changing legacy preparation. More rules/modes/offsets remain unsupported. |
| Recurring / apps.channels.tasks.maintain_recurring_recordings | absent | configured_recording_worker Family::Recurring + retained configured service exist; KnownDomains also covers disabled-only rules. Assemble the generic actual rule service, not a disabled-only success substitute. Timezone/day/offset domains beyond current subset remain required. |
| Artwork / apps.channels.tasks.prefetch_recording_artwork | enabled EPG graph | actual normal SeriesReport→backend→exact publication/ETA registry and recording_existing_artwork_application passed. Direct-Series artwork, manual/provider/image precedence, restart/uncertain results and other branches remain grouped future work. |
| Recording / apps.channels.tasks.run_recording | absent; DVR queue | recording_dvr owns real SQL/cache/media/event/cleanup components, but currently admits only captured 30/61-second manual/local windows and the captured local poster path. Do not register that as a generic production recorder. Derive general normal scope from Original algorithm/actual row/profile plus owned internal TS endpoint; retain real process/file/drain behavior. |
| Backend cleanup / celery.backend_cleanup | absent | DefaultWorker + KnownDomains already implement pinned RedisBackend's no-op cleanup, with expiry/revocation/restoration scopes. Needs actual normal Main registration; do not scan/delete unrelated result keys. |
| Proxy stats / apps.proxy.tasks.fetch_channel_stats | absent | normal StatsJobs + StatsWorker exist; Source trait must use actual combined statistics/Redis/channel scan, not the QA Python helper/Empty source. Original periodic default is disabled, but actual manual task remains a family. |
| Version / core.tasks.check_for_version_update | separately enabled | version_application actually passed four owned HTTP200 cases, real notices/events and physical HTTP runtime drain. Original normal startup schedules it; current Main flag is default-off. Development/non-numeric/error branches remain outside proven stable subset. |
| Account expiry / apps.m3u.tasks.check_account_expirations | absent | DefaultWorker + KnownDomains::with_active_notifications and actual retained notification service exist. Register with the real Main realtime sink; preserve caught task failures versus unsettled operations and full account/profile query semantics. |

## Generic prerequisites, rather than fixture requirements

`SourcePathContext` only resolves stored relative file paths and cache locations.
It does not read Python code. Dockerfile.app already creates empty /app and
/app/media → /data. The M6 fixture mounts Original for its separate Python
ORM/Result helper; Main itself needs only application data. The overlay corrects
Main's misleading source-tree-mount error/comment. A production data directory
must still exist and be readable/writable under its real ownership policy.

`RUST_EPG_REFRESH_ENABLED` and `RUST_VERSION_TASKS_ENABLED` both default off.
The EPG flag gates both the workers and its HTTP import router. Consequently,
a default clean install's working React/health route does not prove Original's
normal import/background behavior. Enable the completed normal graph through a
reviewed production configuration only after required consumers/producers are
assembled; simply flipping the flags cannot create the eight absent families.

`milestone_dispatch::Prepared` classifies fourteen families and requires all
fourteen handlers; `milestone_application` can run one bounded poller. They have
no normal concrete Head implementation and are not constructed in Main. Current
Main runs six independent polling lanes. The actual default dispatcher must own
one read-only bounded raw-head transport, dispatch to real retained handlers,
leave unsupported FIFO heads unchanged and report missing/faulted families.
Do not wrap captured-DVR/empty-statistics fixtures merely to satisfy the count.

DatabaseSchedule setup/reload/calendar/heap/sync and its Driver are compiled,
but absent from Main. The current KnownPublisher admits only Refresh/Series.
Its setup installs scanner, recurring, version, account-expiry and cleanup rows;
therefore its own normal default schedule is rejected by populate_locked before
a tick. Extend typed normal task publication/options and actual same-process
clocked/DVR provenance before enabling the timer. Preserve older scheduler drafts.

`/health/ready` checks database/schema and returns parity_complete:false. It does
not observe stopped/faulted background lanes. Production readiness must include
the actual required worker/producer states and their drain/ownership status;
missing families must remain visible rather than being mistaken for a full port.

## One affected finite Main cohort for the next candidate

Reuse the existing M6 cloned database, files, envelopes, real API producer chain,
failure/continuation, version Origin and cleanup in one run. Remove only Native's
Original /app bind: mount its data at /data and use packaged /app/media mapping.
Keep the separate Python management fixture isolated. Omit all EPG path overrides
to prove packaged defaults work without Source code mounted in Main.

Run the proven configured-rule API→artwork chain, then the same actual Native
import workflow with Source-filtered no-rules DVR settings. Require real parent
and child backends, exact no-rules result, no recording/artwork/change event,
lease cleanup, continued following work, raw FIFO order/ETA and normal drain.
Treat missing/empty/non-list/all-nondictionary forms as one Source filtering
branch, not separate admissions or Source application launches. Retain current
14-success/P0001 evidence and capture the additional affected workflow honestly.
Combine the Core51 normal-routing regression in this same coherent image/batch.

A later single defaultgraph milestone cohort covers scheduler setup/change/
publish/sync, natural scanner→M3U/Refresh→Index/Series→Artwork and clocked DVR,
actual statistics/cleanup/accounts, mixed supported/unsupported FIFO heads,
normal caught failures followed by successful work, actual readiness and final
producer-before-consumer drains. Reuse existing Source37/11/6 and normal DVR
Source authorities; group genuinely missing Source domains once, without replaying
qualified tasks per field/value. Benchmark only equivalent successful complete
workloads under an exclusive resource lease. Full M6 remains incomplete.
