# Selected worker graph and remaining acceptance

Original Dispatcharr/Celery/django-celery-beat and dependency authors retain all
application, algorithm and protocol credit.

[SelectedJobs](../rust/crates/app/src/selected_jobs.rs) assembles the
selected Main worker graph. The Candidate41 mapping covered 36 of the 47
static application declarations, plus framework backend cleanup and dynamic
plugin registration. The
[Source-task reconciliation](rust-source-task-coverage.md) separates those
registrations from actual natural producers, executable evidence and remaining
unmapped workflows.

Four recent paired Compose scopes exercise the selected Main workers with
`RUST_EXPERIMENTAL_SELECTED_JOBS=1` and
`RUST_EXPERIMENTAL_SELECTED_BEAT=1`. On `c2a35f9d`, the Core
compatibility Beat task produced its Stats event, scanner state and linked
M3U group result. On `82414407`, the enabled hourly recurring rule created
one future recording and completed its linked artwork job. The authenticated
Series import completed Refresh, Index, Series and two artwork tasks with
one artwork update. Daily account expiration matched warning and expired
notification rows, dismissals and four Channels effects. All four verified
teardown.
[Compatibility Beat](rust-m6-c2a3-compat-beat.md)
[Recurring rule](rust-m6-c2a3-enabled-recurring.md)
[Series artwork](rust-m6-82414407-series-artwork.md)
[Daily account expiration](rust-m6-82414407-account-expiration.md)

| Family group | Current Main composition | Actual acceptance and remaining scope |
| --- | --- | --- |
| Station discovery and all-account M3U refresh | Existing SD family/ETA consumer and shared M3U/XC aggregate handler/HTTP producer | [Candidate40](rust-milestone-batch40.md) passes all four aggregate and four real station HTTP/ETA cases. Native56 remains a historical failed attempt. Other source CRUD/provider/schedule and concurrency domains remain open. |
| VOD catalogue ingestion | Natural refresh-vod HTTP producer and shared XC/normal worker; real inline Movie/category processing | [Candidate40](rust-milestone-batch40.md) passes fresh basic Movie/empty-Series cases. Candidate41 adds repeat/update implementation; actual Native comparison, metadata and nonempty Series remain open. |
| Manual commercial detection | Real admin recording Comskip HTTP producer, shared normal worker and owned pinned media processes | [Source manual reference](rust-comskip-manual-source1.md) passes three actual cells, including exit-one/no-commercials processing. Candidate41 compiles; Native comparison is pending. Nonempty mark/FFprobe, automatic DVR publication and cut remain unqualified. |
| Channel matching (all, selected, single) | Shared Main queue, matching HTTP producers, existing Auth/cache/events/SD child capability | [Native52](rust-channel-matching-native.md) passes all 14 Source51-derived cases in the no-download/absent-model/empty-plugin domain. Model inference, broader roles and concurrency remain open. |
| Refresh, index, TVG, SD guide, Series, artwork, version | Real handlers and SD/ingestion plugin binding | Later guide workflows execute actual Connect and downloads; their recorded Source/image scopes remain separate. Candidate37-wide acceptance is not inferred. |
| Recording and startup recovery | Normal DVR, real HTTP controls, claimed-task Connect binding and recovery capability guard | Native46 first STOP200 retains the claimed task through End/SUCCESS and decoding; the full event context fails observer capacity. Connect recovery/direct DELETE refusal controls pass. Nonempty restart retains strict decoder failure. |
| Scanner, M3U groups/single, auto-sync and XC account info/profiles | Real scanner/M3U/XC handlers and shared provenance | Native46 grouped scanner passes; older XC/auto-sync evidence retains its own domain and image. Candidate40 qualifies the bounded aggregate and fresh VOD workflows above; broader remote/account domains remain open. |
| Recurring, expiration, cleanup, proxy stats | Real default services and live statistics source | Native46 grouped M6 passes all five cells (including scanner); broader settings, calendar and fault branches remain separate. |
| Core rehash/offsets and channel bulk/name/logo/TVG tasks | Real Core and Channel handlers, not absent registrations | Older production batches contain scoped successes and failures; current implementation mapping is not fresh acceptance of every effect/domain. Populated plugin/offset guards remain explicit. |
| Backup create/restore/scheduled and plugins | Shared normal backup ports, restore binding, plugin runtime and registered jobs | Native46 actions/strict negatives and ordinary restore/partial admission pass; general enabled-plugin/history restore and full M8 remain open. |
| Normal Beat | Assembled shared-pool scheduler implements clocked, interval and crontab rows | Implementation exists; full Source schedule/timezone/default-task acceptance is not established by registration or the selected batches. |

The [Native46 whole run](../scripts/rust-integration/milestone-batch37/Native46/qualification.json)
exits 1 with five passing and two failed contexts, settled pipes/process group and
exact cleanup. Source44's fresh event-observer qualification remains Source-only.
Native47 also retains five passing and two failed contexts; Native48 qualifies
its affected recording-event slice. Their exact image scopes do not transfer
to newer candidates. Preserve failed whole runs even when independent cells pass.

`RUST_EXPERIMENTAL_SELECTED_JOBS` and `RUST_EXPERIMENTAL_SELECTED_BEAT` default
off; Beat requires selected jobs and exactly one configured Beat instance. Main
uses the normal scheduler, rather than the historical clocked-only adapter.
Ordinary admission allows six concurrent tasks and normal DVR allows twenty;
individual-family limits and unsupported domains still preclude a Source
concurrency-parity claim. The readiness response observes actual startup,
control, worker and scheduler states and still reports `parity_complete:false`.
Full task/route coverage, SQLite, physical fault/cancellation branches, strict
recording media quality and equivalent-work resource acceptance remain open.

## Historical batch3 audit

The table below records the earlier `bfb4086` implementation, before the
experimental assembly. Its absent routes, pool-one-only adapters and fixed
recording-domain restrictions are historical findings, not current Main status.
Public evidence at that checkpoint was `3143f9a`. Protected legacy scheduler/
EPG/DVR work in progress remains separate.

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

## Historical batch3 prerequisites and proposed cohort

The remaining text preserves the batch3 planning checkpoint. Its absent-family,
clocked-only scheduler and readiness limitations describe that earlier code;
the current composition and unresolved acceptance are summarized above. It is
not the launch plan for Candidate37 or a claim that those registrations are
still missing.

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

### Historical proposed next-candidate cohort

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
