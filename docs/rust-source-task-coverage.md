# Source task coverage beyond the selected default graph

Original Dispatcharr and Celery authors retain application and protocol credit.
The pinned Source revision is
`1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430`.

The [reproducible static inventory](../scripts/rust-job-contracts/source-task-declarations.json)
finds 47 top-level task-decorated functions outside test files. It records exact
Source paths, lines, file hashes and literal decorator options. This is not the
runtime registry, an implementation count or a passing coverage gate: aliases,
dynamic registration, direct calls and required framework workflows still need
an executable crosswalk. Its `runtime_registration_verified` and
`source_execution` fields describe that static capture and remain unchanged.

## Current implementation mapping

The `fdede54c` static classifier maps 38 of the 47 pinned Original declarations, plus framework `celery.backend_cleanup`. It recognizes `core.tasks.beat_periodic_task` through the Scanner family and `apps.epg.tasks.refresh_all_epg_data` through the Refresh family. Each has earlier scoped Docker Compose evidence under the selected jobs and Beat configuration. Current default-worker acceptance remains open. [Compatibility Beat](rust-m6-c2a3-compat-beat.md) · [Aggregate XMLTV](rust-m6-d482-aggregate-xmltv.md)

Candidate41 source `c0031d640d61cd21ff9b2a12fbc74cc0ab6aadf1` has moved beyond
the historical selected14 proposal. The literal classifier in
[milestone_dispatch.rs](../rust/crates/job-transport/src/milestone_dispatch.rs)
recognizes 36 of the 47 application names, plus framework
`celery.backend_cleanup`. [SelectedJobs](../rust/crates/app/src/selected_jobs.rs)
assembles their real family handlers, registered plugin tasks and the normal
Beat scheduler. Some names share a family; these are not counts of independent
passing workflows. Registration does not establish that every producer, domain,
result, child publication or shutdown path works.

| Mapped application declarations | Main composition | Executed evidence and limits |
| --- | --- | --- |
| Station discovery and all-account refresh (2) | Shared SD/ETA and M3U/XC aggregate handlers | [Candidate40](rust-milestone-batch40.md) passes all four aggregate phases and four real station HTTP/ETA phases against retained Source55/53. Historical Native56 remains failed; broader provider, source CRUD, schedule and concurrency domains remain open. |
| VOD catalogue refresh (1) | Natural refresh-vod HTTP root, shared XC slots and real default-queue worker; inline Movie/category helpers | [Candidate40](rust-milestone-batch40.md) passes the three fresh basic Movie/empty-Series cases against Source58. Repeat/update Source capture is qualified by an explicit offline observer correction; its original controller exit1 remains literal. Candidate41 repeat execution, metadata and nonempty Series remain open. |
| Manual Comskip (1) | Real admin HTTP producer, shared normal queue/CAS/backend and owned pinned Comskip/FFprobe process service | [Source manual reference](rust-comskip-manual-source1.md) passes no-file, actual exit-one/no-commercials and already-processed cases. Candidate41 is compiled but Native comparison is pending. Nonempty commercial marking/FFprobe, automatic DVR publication and cut mode remain unqualified. |
| Channel matching all/selected/single (3) | Matching HTTP roots and shared normal worker | [Native52](rust-channel-matching-native.md) passes all 14 actual matching cases against retained Source51 in the explicit no-download domain; real model inference, wider roles and faults remain open. |
| Backup create/restore/scheduled (3) | Normal backup jobs and restore plugin binding | Candidate37 Native46 ordinary restore six cells and partial-admission two cells pass; general enabled-plugin/history restore remains open. |
| Channel bulk creation, names/logos/TVG maintenance, Series, recurring, artwork, recording, startup recovery and offset rescheduling (10) | Channel handlers, Series/artwork, normal DVR/recovery and Core handlers | Native46 grouped recurring and explicit Connect/recovery refusal controls pass. Its ordinary event workflow fails observation and nonempty restart fails strict decoding. Older channel/artwork successes retain their own image/input scopes. |
| Refresh, programme index, per-channel programmes and two SD guide task names (5) | Refresh/index/TVG handlers and SD guide wrapper | Fresh Source36 guide authority and later Native guide batches cover declared cases; they do not establish every EPG producer or automatically pass Candidate37. |
| Account info/profiles, expiry, groups, single-account refresh and auto-channel sync (6) | XC, scanner/M3U, default expiry and auto-sync handlers | Native46 grouped scanner/expiry pass for that cohort. Older XC/auto-sync cases remain historical references with their original domains and images. |
| Plugin repository refresh (1), proxy stats (1), scanner/version/rehash (3) | Plugin/default/scanner/Core handlers | Native46 grouped stats/scanner pass. Rehash is implemented and has older production execution evidence; current registration is not fresh rehash acceptance. |

The [Candidate37 whole-run record](rust-milestone-batch37.md) and
[Native46 qualification](../scripts/rust-integration/milestone-batch37/Native46/qualification.json)
record five passing contexts and two failed contexts, actual exit 1 and exact
cleanup. [Source44](../scripts/rust-integration/milestone-batch37/Source44/qualification.json)
qualifies the extra event observer and Source stopped/completed behavior only.
Native47 retains five passing and two failed contexts; Native48 separately
qualifies the affected recording-event slice on that earlier image. These links establish their stated workflow scopes, not
case bindings for every generated register row.

Core99's natural `core.tasks.rehash_streams` and
`apps.channels.tasks.reschedule_upcoming_recordings_for_offset_change` heads are
no longer absent from the classifier. [normal_core_tasks.rs](../rust/crates/job-transport/src/normal_core_tasks.rs)
registers both and performs actual envelope admission/claim before execution.
The earlier [Core99 capture](rust-batch5-ops-acceptance.md) qualified publications,
while [Candidate12](rust-batch12-functional-observations.md) separately passed
rehash and preserved its offset failure. Later capability guards and negative
controls do not turn all offset/artwork/plugin domains into successful execution.

## Producer reconciliation beyond the earlier 29-name map

The earlier Candidate37 review found 18 declarations without literal entries.
Candidate38 maps the three matching declarations; Candidate39 additionally maps
station discovery and aggregate refresh. Candidate40 adds VOD refresh, and Candidate41 adds manual Comskip, leaving 11 without literal entries. The table retains all original rows to preserve the producer crosswalk;
it is not a list of 18 proven missing algorithms. Call sites below refer to the pinned Source files;
all nine inventoried Source file hashes still match the committed inventory.
A task name appearing in `dispatcharr/celery.py`'s memory-cleanup list is not
itself a producer. A natural queued workflow needs its actual HTTP/signal/Beat
producer mapped to the intended Rust consumer or equivalent service, followed
by Source and production execution evidence.

| Declaration(s) | Pinned Source authority | Remaining executable mapping |
| --- | --- | --- |
| `match_epg_channels`, `match_selected_channels_epg`, `match_single_channel_epg` | `apps/channels/api_views.py:2601–2620` queues all/selected/single matching from real actions | [Source51](rust-channel-matching-source51.md) now qualifies 14 natural HTTP cases in its explicit no-download domain and supplies pure decision vectors. [Native52](rust-channel-matching-native.md) now qualifies the integrated HTTP/worker/SQL slice in the same domain. Broader roles/concurrency and real model inference remain open; maintenance name/logo/TVG tasks are different workflows. |
| `comskip_process_recording` | `apps/channels/tasks.py:3094` and `apps/channels/api_views.py:3752` queue processing | Candidate41 now maps the real standalone admin producer and shared normal consumer. [Source manual reference](rust-comskip-manual-source1.md) passes three cells in the actual exit-one/no-commercials domain; Native comparison remains pending. Normal recording policy still refuses automatic enabled Comskip (`recording_normal_policy.rs`); nonempty mark/FFprobe and cut paths remain unqualified. |
| `fetch_schedules_direct_stations` | `apps/epg/signals.py:22` and `apps/epg/sd_api.py:274` queue station discovery | [Source53](../scripts/rust-integration/channel-stations-source53/README.md) passes four actual creation/countdown, lineup-add, partial-detail and idle cases with real task/SQL/channel-layer evidence. [Candidate40](rust-milestone-batch40.md) passes four actual Native source-create/lineup HTTP and worker/ETA cases. Other CRUD/provider/schedule domains remain open. Guide-fetch success alone cannot close this workflow. |
| `refresh_m3u_accounts` | `apps/m3u/api_views.py:765` queues the all-active-accounts task; task then queues one child per active account (`tasks.py:725`) | [Source55](rust-m3u-aggregate-source55.md) passes four natural API phases, including the active custom account's caught-error child contract. [Native56](rust-milestone-batch39.md) retains the failed local-child receipt behavior. [Candidate40](rust-milestone-batch40.md) corrects the authenticated broker scope and passes all four phases, including real child claims and active-default-custom handling. Wider accounts, provider and concurrency domains remain open. |
| `refresh_vod_content` | M3U API create/update and `apps/m3u/tasks.py:4064` queue VOD ingestion | [Candidate40](rust-milestone-batch40.md) passes natural refresh-vod HTTP/worker/SQL/provider/event behavior for fresh basic Movies and empty Series. Candidate41 repeat/update work remains pending Native comparison. Automatic M3U-to-VOD publication, metadata and nonempty Series remain separate gates. |
| `_safe_name` | Direct calls in `apps/channels/tasks.py:1226,1393–1414` | Preserve naming through the calling channel workflows; no queued producer identified in this static review. |
| `process_groups` | Direct call in `apps/m3u/tasks.py:1971` | Map inline M3U group processing; do not require a separate worker solely because it has a decorator. |
| `cleanup_orphaned_vod_content`, `process_movie_batch`, `process_series_batch`, `refresh_movie_advanced_data` | Direct calls in `apps/vod/tasks.py:164,287,342`, `apps/vod/api_views.py:229` and `apps/output/views.py:1704` | Preserve effects through actual import/metadata/output callers; queued registration is not proved necessary by these direct calls. |
| `purge_recurring_rule`, `rebuild_recurring_rule` | Decorated wrappers in `apps/channels/tasks.py`; API/helper paths call the purge implementation directly | Map direct rule mutation/rebuild effects and inspect any persisted schedule producer; no natural queued wrapper producer identified here. |
| `refresh_all_epg_data` and `beat_periodic_task` | Definitions in `apps/epg/tasks.py:483` and `core/tasks.py:56` | Exact task names resolve to Refresh and Scanner. Scoped Docker Compose checks exercised custom aggregate XMLTV scheduling and the compatibility Beat task on the historical images linked above. |
| `batch_refresh_series_episodes` | Definition in `apps/vod/tasks.py:1846`, direct test calls, and the cleanup-name list in `dispatcharr/celery.py` | Inspect persisted Beat or integration registrations for an actual producer. Runtime proof for a queued invocation remains open. |

Dynamic plugin registrations and framework tasks remain additional workflow
contracts outside the 47-name inventory. Unsupported FIFO heads must remain
visible and unchanged, rather than being counted as executed or silently dropped.

## Next acceptance work

Finish matching's remaining domains, broaden the scoped station/aggregate/VOD acceptance,
and validate Candidate41 VOD repeat and manual Comskip behavior. For each,
map the real Source producer, arguments/options, parent/children, result and
SQL/cache/media/event effects to its current Rust route/service. Reuse qualified
Source evidence only where its state and inputs actually apply; characterize
missing branches in the existing milestone cohort. Then add reviewed exact
case bindings to the [contract register](rust-contract-register.md). Do not
manually promote static inventory fields or generated register rows to passes.

Keep M0 executable coverage, M6 and final parity open until the required
workflows pass. The 784 SDK51-ignored tests are not a count of missing workflows.

```sh
python3 -B scripts/rust-job-contracts/source_task_inventory.py \
  --output /absolute/path/to/fresh-inventory.json
```
