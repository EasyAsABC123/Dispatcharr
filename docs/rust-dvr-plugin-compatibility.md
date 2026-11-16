# DVR plugin compatibility: remaining interfaces

SDK47 established idle Main startup and early refusal of unsupported DVR work in a stable owned namespace. SDK49 subsequently implemented a narrower claimed normal-recording event slice with an empty general-plugin domain. Neither change closes M8 or establishes general DVR plugin compatibility. Source42 and fresh Source44 have measured the event contract; Native event application acceptance remains separate from implementation and package checks.

Original Dispatcharr, plugin, Celery, Django and other dependency authors retain full application, library and algorithm credit.

## Pinned Source authority

Authority is Original commit `1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430`, original archive SHA256 `f2323ac705be2af5d1c264f26b1d297cb2bede53874e6bceccee4bf6603fa7ff`. The six Source files cited below were checked byte-for-byte against their retained entries in [the compatibility manifest](../rust/runtime/plugin_compat_source/manifest.json). The namespace packaging correction does not modify these Source bodies.

There are two distinct extension paths:

- **System-event actions.** Recording start and end call the generic event framework. [Recording end emission](../apps/channels/tasks.py#L1737) reaches [log_system_event](../core/utils.py#L934), which inserts the audit row and invokes [integration dispatch](../core/utils.py#L898). [Event payload enrichment](../core/utils.py#L812) resolves channel, current stream/provider/profile and removes empty fields. [trigger_event](../apps/connect/utils.py#L17) executes Connect subscriptions first; [its plugin phase](../apps/connect/utils.py#L95) then performs cached discovery, obtains event actions, checks actual enabled PluginConfig rows and catches each action exception. Celery prefork uses the synchronous branch. These observable ordering and payload semantics are part of DVR compatibility.
- **Import and constructor side effects.** [Ordinary app initialization](../apps/plugins/apps.py#L12) discovers enabled plugins in each process. [Discovery](../apps/plugins/loader.py#L229) imports enabled plugin modules; [loading](../apps/plugins/loader.py#L599) executes module code and constructs Plugin(). Such user code can replace recording helpers or task methods. There is no finite named DVR monkeypatch API in this Source loader. Event-action support alone cannot establish arbitrary monkeypatch parity.

[Recording model signals](../apps/channels/signals.py#L353) also matter: revoke prior scheduled work on relevant updates, publish immediate work on transaction commit or create Beat schedules, save task identity, dispatch artwork and revoke on delete. Arbitrary plugin ORM writes can enter these paths; native producer guards are not proof of signal compatibility.

## Implemented claimed-task slice and remaining interfaces

The existing [PluginRuntime hook session](../rust/crates/store/src/plugin_runtime.rs#L610) already retains a task actor, Invocation, actual caller, task identity/trail and caller-provided TaskSqlSession. NativePorts provides existing Auth SQL/cache, events, jobs and guarded processes. Its finish/abort path and physical Host EOF/wait settlement are reusable; a separate pool or detached task identity is unnecessary.

The [SDK49/Candidate37 evidence](rust-milestone-batch37.md) records all four SDK gates and installed Docker/package controls. Its concrete integration now includes:

1. [Hook task context](../rust/crates/store/src/plugin_hook_context.rs) admits `apps.channels.tasks.run_recording` only with a private witness derived from the actual broker frame after successful claim. [Python event context](../rust/runtime/plugin_bridge/dispatcharr_native/plugin_events.py) includes recording_start/recording_end. Arbitrary request metadata, HTTP callers and recovery cannot fabricate this witness.
2. [NormalDvr](../rust/crates/job-transport/src/recording_dvr_normal.rs) and [recording Connect composition](../rust/crates/store/src/recording_connect_events.rs) use the existing Main Auth pool, writer budget and PluginHookSession. Short per-event TaskSqlSessions end before the next media phase; uncertain Host/SQL settlement retains the original owner. No new pool or long media-duration event session is introduced.
3. [CaptureStore](../rust/crates/store/src/recording_capture.rs) composes audit insertion, [current-phase enrichment](../rust/crates/store/src/recording_event_enrichment.rs), Connect/template dispatch and history trimming. General PluginConfig rows and relevant script integrations remain rejected. The implemented event slice requires the closed empty native-child trail; it does not admit arbitrary event-action child publication.

Remaining interfaces include actual recovery event ownership, direct HTTP cancellation event context, enabled general-plugin constructors/actions, arbitrary ORM/signal effects, and constructor-installed recording helper wrappers. [Builtin guards](../rust/runtime/plugin_bridge/dispatcharr_native/builtin_tasks.py) prohibit executing Source backend task bodies. Widening a task-name list or invoking Source run_recording cannot implement these interfaces. Recording model signals and on_commit publication require their actual SQL/publication ordering rather than caller-supplied success metadata.

## Measured Source event authority and HTTP distinction

Source42 captured actual stopped/completed Start/End payloads, task identities and plugin actions. The [public contract shapes/hashes](../scripts/rust-integration/milestone-batch37/components/Source42-event-shapes.json) retain that authority without publishing owned URLs or payload values. The cold start omits stream fields; a later warm start includes stream/provider/profile fields.

Fresh Source44 independently passed the unchanged stopped/completed recording callback, four templated HTTP200 deliveries, four actual plugin actions and four added no-template HTTP400 payload captures. The caught400 responses did not abort recording completion or decoding. Parsed DeliveryLog payloads equal actual Source action payloads and match the Source42 common contract. With configured max_system_events=3, the exact Source cutoff retains four rows. See [qualification](../scripts/rust-integration/milestone-batch37/Source44/qualification.json), [payload shapes](../scripts/rust-integration/milestone-batch37/Source44/payload-shapes.json) and [independent cleanup](../scripts/rust-integration/milestone-batch37/Source44/independent-cleanup.json). These are actual Original results, not Native acceptance.

Source [stop](../apps/channels/api_views.py#L3929) writes stopped status and delegates teardown/revoke; it does not directly emit End. The running recording task later emits End under the same actual task identity observed at Start. Candidate37 retains that claimed-task path, whose actual stopped-event behavior must be measured separately from natural completion. A missing HTTP event context alone does not justify rejecting stop.

Source [DELETE](../apps/channels/api_views.py#L4194) directly emits cancelled End when status is recording, or stopped with the remux_success key absent. A present false/null value differs from an absent key. Native direct cancellation with relevant active End Connect remains guarded before effects because that HTTP caller lacks the claimed normal-task event context. Pending stop/delete do not automatically require that End hook. Recovery with active End Connect is also deferred before dispatch-lock/root publication; two successful startup roots are not Source three-root parity.

## Smallest coherent next slice

Source event characterization and the SDK49 claimed-task integration above are complete within their recorded scopes. The next acceptance step is the coherent Native event/template/caught-failure/history/media/closure batch on the exact installed image, including actual stopped and completed tasks, direct cancellation and recovery refusals, and legacy regressions. Until its actual terminal receipts are reviewed, implementation and package success do not establish Native event parity. Keep arbitrary helper replacement explicitly guarded until characterized.

For the separate monkeypatch slice, an enabled Source plugin can ordinarily wrap `_dvr_recording_end_payload` ([definition](../apps/channels/tasks.py#L1644)), call its original pure helper and record before/after observations. Observe constructor/cache behavior and normal/stop/recovery calls without patching backend task bodies, private cache clearing or calling the helper from the test controller. That establishes one concrete helper seam; it cannot qualify all possible user monkeypatches.

The historical [batch36](rust-milestone-batch36.md) idle/refusal scope remains narrower than the SDK49 event implementation. Neither early-refusal controls nor the measured Source event contract qualify arbitrary plugin helpers, constructors or signals. Full Source three-root startup with enabled End Connect, general plugin DVR behavior and equivalent-work CPU/memory gains remain open.
