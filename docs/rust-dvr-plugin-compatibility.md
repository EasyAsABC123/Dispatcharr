# DVR plugin compatibility: remaining interfaces

SDK47 permits idle Main startup and early refusal of unsupported DVR work in a stable owned namespace. It does not implement DVR plugin compatibility or close M8. This is a static source analysis; the proposed characterization and runtime interfaces below have not been exercised.

Original Dispatcharr, plugin, Celery, Django and other dependency authors retain full application, library and algorithm credit.

## Pinned Source authority

Authority is Original commit `1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430`, original archive SHA256 `f2323ac705be2af5d1c264f26b1d297cb2bede53874e6bceccee4bf6603fa7ff`. The six Source files cited below were checked byte-for-byte against their retained entries in [the compatibility manifest](../rust/runtime/plugin_compat_source/manifest.json). The namespace packaging correction does not modify these Source bodies.

There are two distinct extension paths:

- **System-event actions.** Recording start and end call the generic event framework. [Recording end emission](../apps/channels/tasks.py#L1737) reaches [log_system_event](../core/utils.py#L934), which inserts the audit row and invokes [integration dispatch](../core/utils.py#L898). [Event payload enrichment](../core/utils.py#L812) resolves channel, current stream/provider/profile and removes empty fields. [trigger_event](../apps/connect/utils.py#L17) executes Connect subscriptions first; [its plugin phase](../apps/connect/utils.py#L95) then performs cached discovery, obtains event actions, checks actual enabled PluginConfig rows and catches each action exception. Celery prefork uses the synchronous branch. These observable ordering and payload semantics are part of DVR compatibility.
- **Import and constructor side effects.** [Ordinary app initialization](../apps/plugins/apps.py#L12) discovers enabled plugins in each process. [Discovery](../apps/plugins/loader.py#L229) imports enabled plugin modules; [loading](../apps/plugins/loader.py#L599) executes module code and constructs Plugin(). Such user code can replace recording helpers or task methods. There is no finite named DVR monkeypatch API in this Source loader. Event-action support alone cannot establish arbitrary monkeypatch parity.

[Recording model signals](../apps/channels/signals.py#L353) also matter: revoke prior scheduled work on relevant updates, publish immediate work on transaction commit or create Beat schedules, save task identity, dispatch artwork and revoke on delete. Arbitrary plugin ORM writes can enter these paths; native producer guards are not proof of signal compatibility.

## Reusable owner and missing interfaces

The existing [PluginRuntime hook session](../rust/crates/store/src/plugin_runtime.rs#L610) already retains a task actor, Invocation, actual caller, task identity/trail and caller-provided TaskSqlSession. NativePorts provides existing Auth SQL/cache, events, jobs and guarded processes. Its finish/abort path and physical Host EOF/wait settlement are reusable; a separate pool or detached task identity is unnecessary.

The remaining integration points are concrete:

1. [Hook task context](../rust/crates/store/src/plugin_hook_context.rs#L42) accepts only five EPG task names; [Python event context](../rust/runtime/plugin_bridge/dispatcharr_native/plugin_events.py#L38) accepts only epg_error/epg_refresh. DVR requires actual claimed recording/recovery context and real settled publisher trails, not caller-supplied metadata.
2. [NormalDvr](../rust/crates/job-transport/src/recording_dvr_normal.rs#L99) has its own actual caller/slot, CaptureStore and uncertain-scope retention, but no PluginRuntime/session integration. Its task must borrow the same configured Auth-pool session at defined idle SQL boundaries and retain its physical owner through hook completion, cancellation and child retirement.
3. [CaptureStore audit insertion](../rust/crates/store/src/recording_capture.rs#L928) explicitly leaves integration ports uncomposed; its context checks require empty Connect/plugin tables. The real audit-insert → enriched Connect delivery → actual plugin action → history-trim sequence must be composed before those guards can change.
4. Arbitrary constructor-installed helper wrappers need a distinct native continuation/helper interface. [Builtin guards](../rust/runtime/plugin_bridge/dispatcharr_native/builtin_tasks.py#L1) prohibit executing Source backend task bodies; the current inline builtin bridge is unavailable. Widening a task-name list or invoking Source run_recording would not implement this interface. Recording model signals and on_commit publication need their own actual SQL/publication ordering integration.

## Smallest coherent next slice

First characterize one actual Source recording_start/recording_end event subscription and plugin action, using an owned short stream and the existing M8 operations fixture. Record audit/payload/Connect/action order, current_task identity, actual SQL/child publications and stop/recovery behavior. Then reuse the existing EPG Connect and PluginHookSession ports at the native recording audit boundary. Keep arbitrary helper replacement explicitly guarded until characterized.

For the separate monkeypatch slice, an enabled Source plugin can ordinarily wrap `_dvr_recording_end_payload` ([definition](../apps/channels/tasks.py#L1644)), call its original pure helper and record before/after observations. Observe constructor/cache behavior and normal/stop/recovery calls without patching backend task bodies, private cache clearing or calling the helper from the test controller. That establishes one concrete helper seam; it cannot qualify all possible user monkeypatches.

The current [batch 36](rust-milestone-batch36.md) negative DVR controls establish none of these event or monkeypatch results until actually run, and their early refusal scope remains narrower even after they pass. Full Source three-root startup, enabled plugin DVR behavior and equivalent-work CPU/memory gains remain open.
