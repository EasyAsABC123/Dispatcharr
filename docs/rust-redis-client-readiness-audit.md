# Redis first-command readiness audit — SDK97

Original Dispatcharr, Celery, redis-py, redis-rs and dependency authors retain all
application/client/protocol credit. This is a static ownership and coverage audit
of frozen SDK97, not a claim that every Redis path is enumerated or faulty. No
runtime, timeout, reconnect policy or active/frozen input was changed for it.

Pinned Source Redis client `_execute_command` obtains a pool connection before
ordinary commands. Its pool connects, checks `can_read`, and disconnects/reconnects
an already closed or unready socket before returning it. The pinned Rust manager
returns the failed command error and arranges a replacement for later commands.
This matters when the first command after idle is a mutation. It does not permit
replaying a mutation whose reply was lost. The Source Redis idle default is300
seconds; actual fixture CONFIG was independently checked during Candidate56.

## Qualified fixes and explicit limits

| Path | Actual proof | Remaining limit |
| --- | --- | --- |
| All Jobs result writes through `write_metadata` | SDK95 six physical controls; actual long automatic MARK parent/child terminal results pass on Candidate57 | Does not cover independent broker, cache, lock or event managers |
| Root-artwork publication | SDK97 nine real-Redis controls pass, including exact socket closure, reply loss without replay and pending PONG ownership | Candidate59 rebuilt Main workflow pending; original CUT503 physical error kind was not captured |
| Other generic `command_classified`, `command`, compiled scripts and dependency pipelines | Existing no-replay/ownership controls and path-specific functional references | No general Source-style first-command readiness guarantee follows from those controls |

The producer-specific preflights use the same admitted physical task, permit,
caller guard and deadline. A settled dropped read-only PING can repeat once;
mutations never repeat. There is no blanket PING on hot data-plane commands.
Source catches/logs recording scheduling/artwork exceptions; Rust's unsettled
publication503 remains a separate, unqualified caught-fault response difference.

## Prioritized persistent-manager review

| Owner and construction | Commands and activity | First-idle coverage / next bounded check |
| --- | --- | --- |
| [NormalRecordingPublisher](../rust/crates/job-transport/src/normal_recording_publication.rs#L180), separate `Jobs::connect_recording_dvr` | `publish_due`, immediate and recovery producer paths; independent of NormalDvr and shared queue pollers | **Highest immediate risk:** after MARK's due publication, producer can idle >520s before CUT's next due root. Generic `publish_normal_recording` has no preflight. This is a static prediction, not an observed59 failure. Qualify a producer-only closed-socket case and actual second due publication; never retry uncertain LPUSH. |
| [LocalArtwork Application](../rust/crates/job-transport/src/recording_existing_artwork_application.rs#L90) shares Jobs with booking artwork publisher | Selected-head queue shell supplies frames, so its own broker does not poll while empty; claimed artwork uses same manager | Actual MARK client's last use precedes CUT by≥540.654s. Candidate59 preflight covers root-artwork publication only, not every local-artwork consumer operation. |
| [Jobs constructor](../rust/crates/job-transport/src/transport.rs#L274) creates separate broker/result transports for each prepared family | Ordinary publish/refresh/index/configured-artwork, claim, peek and result operations | Shared result fix is not broker coverage. Trace each concrete prepared family's clone ownership before deciding a broker is kept hot by polling. Rare API/model-signal publishers need independent idle-first tests. |
| Auth-bound provider/profile/source/EPG producer services using Jobs | API settings, profile saves and programme refresh may be separated by hours | Audit actual callback-to-Jobs owner, then one unchanged API after an owned broker idle-close; retain committed SQL, exact broker mutation and backend/event outcomes. No general fix is inferred from mock-only callback tests. |
| [NotificationCache](../rust/crates/store/src/notifications.rs#L26), retained raw ConnectionManager | Notification reads, group/config invalidation and plugin pipelines; idle between UI operations possible | Dedicated cache manager is outside Jobs preflight. First request after idle may differ from a hot request; characterize Source fallback/error behavior before changing it. |
| [CachedSwapPolicy](../rust/crates/ingest-epg/src/cache_policy.rs#L345) retains generation-tagged transport | Config/cache reads and fills; invalidation on backend failure | Has its own fallback/invalidation behavior. Determine whether stale first-read fallback already matches Source; do not equate an error with an absent value or retry a write. |
| [RealtimeBus](../rust/crates/realtime/src/bus.rs#L100) and [EPG Reporter](../rust/crates/ingest-epg/src/reporter.rs#L209) | Channel-layer send/group/receive and cache/report operations | Subscriptions/polling and command manager can be different connections. An open websocket alone does not prove writer readiness. Qualify a first event after idle with actual delivery and owner settlement. |
| [Task locks](../rust/crates/coordination/src/task_locks.rs#L152) and coordination registry | SET/NX, token-checked renewal/release, registry Lua; active leases renew, idle pools do not | Preserve no-replay lock authority. Test closed socket before acquisition separately from reply loss after SET; do not synthesize lock success or release physical ownership early. |
| [VOD sessions](../rust/crates/vod-http/src/sessions.rs#L220), recovery and active-recording coordination managers | Admission/generation/session operations; active loops may refresh some sockets | Direct managers bypass shared transport preflight. Existing session/recovery proofs do not automatically qualify a new first request after300s with no viewers. |

## Hot paths and freshly created connections

Input/output buffer stores, output gateway, VOD/timeshift statistics/events,
XC counts, XMLTV cache, timeshift management/media pool and combined statistics
also construct transports. Their active-viewing operations can be frequent;
blind per-command health probes would add CPU/network work to the very workload
being optimized. Treat cold admission after idle, active-buffer I/O and recovery
as distinct states, and map which manager each state actually uses.

Some resources are connected per admitted operation, rather than retained across
idle. For example [operator commands](../rust/crates/store/src/operator_commands.rs#L242)
create their connection in `connection()`, and normal-control response writers
connect when handling a response. These are lower priority for stale retained
socket diagnosis, but they still need physical reply-loss/cancellation semantics.
Logo/scanner file/cache resources must be classified by concrete lifetime before
being counted as persistent; constructor grep alone does not establish lifetime.

Next implementation should cover a coherent producer domain with real ownership
and no-replay tests, then its actual affected workflow. Keep actual errors and
committed side effects, publish failed attempts, and distinguish closed-before-send
from uncertain-after-send. A successful diagnostic or component test does not
accept full M6, all Redis clients, application performance or the entire migration.
