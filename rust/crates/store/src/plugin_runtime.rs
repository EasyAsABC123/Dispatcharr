//! Lazy actual user-plugin action/task interpreters, sharing normal Main ports.
//! No source HTTP listener/worker or fallback backend task body. Execution is
//! still guarded by actual prepared Jobs capabilities; full parity is unproved.
//! Original Dispatcharr/CPython/Celery authors retain full algorithm credit.
use super::{
    plugin_execution_host::{Config, Error as HostError, Host, Invocation, Mode},
    plugin_execution_ports::{NativePluginJobs, NativePorts},
    plugin_process_guard::GuardedPluginProcesses,
    plugin_publication_owners::PluginPublicationOwners,
    plugin_python_registry::{HostRole, PluginTaskCapability, PluginTaskRegistry},
    plugin_trace_claims::PluginTraceClaims,
};
use crate::{AuthService, StoreError, TaskSqlSession};
use dispatcharr_contracts::UpdateEventSink;
use serde_json::{json, Value};
use std::{
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};
use uuid::Uuid;
type Owner = Arc<dyn Send + Sync>;
struct TaskActor {
    host: Host,
    ports: Arc<NativePorts>,
    busy: AtomicBool,
    loaded: AtomicBool,
}
struct Busy {
    actor: Arc<TaskActor>,
}
impl Drop for Busy {
    fn drop(&mut self) {
        self.actor.busy.store(false, Ordering::Release);
    }
}
struct Inner {
    auth: AuthService,
    actions: Host,
    tasks: Vec<Arc<TaskActor>>,
    registry: PluginTaskRegistry,
    publications: PluginPublicationOwners,
    claims: PluginTraceClaims,
    closed: AtomicBool,
    unsettled_sql: AtomicBool,
}
#[derive(Clone)]
pub struct PluginRuntime {
    inner: Arc<Inner>,
}
/// Opaque real actor/SQL admission. No task IDs/parent lists can construct it.
pub struct PluginTaskAdmission {
    actor: Arc<TaskActor>,
    scope: Invocation,
    capability: PluginTaskCapability,
    id: Uuid,
    name: String,
    frame: Value,
}
fn unavailable() -> StoreError {
    StoreError::Unsupported("Prepared native plugin runtime unavailable")
}
fn host_error(_: HostError) -> StoreError {
    unavailable()
}
impl PluginRuntime {
    pub(crate) fn shares_recording_auth(&self, auth: &AuthService) -> bool {
        std::ptr::eq(
            self.inner.auth.store.pool().options(),
            auth.store.pool().options(),
        ) && Arc::ptr_eq(
            &self.inner.auth.user_agent_workers,
            &auth.user_agent_workers,
        )
    }
    // Explicit existing Auth/events/Jobs/lifetime plus actual runtime paths and
    // bounded provenance; construction starts no interpreter or extra service.
    #[allow(clippy::too_many_arguments)]
    pub fn prepare(
        auth: AuthService,
        executable: PathBuf,
        source: PathBuf,
        plugins: PathBuf,
        events: Arc<dyn UpdateEventSink>,
        jobs: Arc<dyn NativePluginJobs>,
        lifetime: Owner,
        provenance_bound: usize,
    ) -> Result<Self, StoreError> {
        let registry = PluginTaskRegistry::new(plugins.clone());
        let publications = PluginPublicationOwners::prepare(provenance_bound)?;
        let claims = PluginTraceClaims::prepare(6)?;
        let actor = |role| {
            let ports = Arc::new(NativePorts::new(
                auth.clone(),
                role,
                registry.clone(),
                publications.clone(),
                claims.clone(),
                events.clone(),
                jobs.clone(),
                Arc::new(GuardedPluginProcesses::default()),
            ));
            let host = Host::prepare(
                Config {
                    mode: Mode::Plugin,
                    role,
                    executable: executable.clone(),
                    source: source.clone(),
                    plugins: plugins.clone(),
                },
                ports.clone(),
                lifetime.clone(),
            );
            (host, ports)
        };
        let (actions, _) = actor(HostRole::Actions);
        // Existing global Celery6 selects one physical interpreter per active task.
        // These are inert actors; prepare starts ZERO Python children/SQL clients.
        let tasks = (0..6)
            .map(|_| {
                let (host, ports) = actor(HostRole::Tasks);
                Arc::new(TaskActor {
                    host,
                    ports,
                    busy: AtomicBool::new(false),
                    loaded: AtomicBool::new(false),
                })
            })
            .collect();
        Ok(Self {
            inner: Arc::new(Inner {
                auth,
                actions,
                tasks,
                registry,
                publications,
                claims,
                closed: AtomicBool::new(false),
                unsettled_sql: AtomicBool::new(false),
            }),
        })
    }
    pub fn envelope_capability(&self, raw: &[u8]) -> Result<PluginTaskCapability, StoreError> {
        if raw.len() > 65536 || self.inner.closed.load(Ordering::Acquire) {
            return Err(unavailable());
        }
        let frame: Value = serde_json::from_slice(raw).map_err(|_| unavailable())?;
        self.inner
            .registry
            .envelope_capability(frame["headers"]["task"].as_str().ok_or_else(unavailable)?)
    }
    pub fn registered_head(&self, raw: &[u8]) -> bool {
        if self.inner.closed.load(Ordering::Acquire) || raw.len() > 65536 {
            return false;
        }
        let Ok(frame) = serde_json::from_slice::<Value>(raw) else {
            return false;
        };
        frame["headers"]["task"].as_str().is_some_and(|name| {
            self.inner
                .registry
                .has_registered_name(name)
                .unwrap_or(false)
        })
    }
    pub async fn discover_configured(&self, owner: Owner) -> Result<(), StoreError> {
        let session = self
            .inner
            .auth
            .borrow_task_sql_session(owner.clone())
            .await?;
        let result = async {
            let enabled = session
                .run(|c| {
                    Box::pin(async move {
                        sqlx::query_scalar::<_, bool>(
                            "SELECT EXISTS(SELECT 1 FROM plugins_pluginconfig WHERE enabled)",
                        )
                        .fetch_one(c)
                        .await
                    })
                })
                .await??;
            if enabled {
                self.discover(session.clone(), owner).await?;
            }
            Ok(())
        }
        .await;
        session.close().await?;
        result
    }
    pub async fn discover(
        &self,
        session: TaskSqlSession,
        owner: Owner,
    ) -> Result<Value, StoreError> {
        self.inner
            .actions
            .call(
                "discover".into(),
                json!({"sync_db":false,"force_reload":false}),
                Invocation {
                    owner,
                    sql: Some(session),
                },
            )
            .await
            .map_err(host_error)
    }
    pub async fn run_action(
        &self,
        key: String,
        action: Value,
        params: Value,
        session: TaskSqlSession,
        owner: Owner,
    ) -> Result<Value, StoreError> {
        if self.inner.closed.load(Ordering::Acquire) {
            return Err(unavailable());
        }
        self.inner
            .actions
            .call(
                "run_action".into(),
                json!({"key":key,"action":action,"params":params}),
                Invocation {
                    owner,
                    sql: Some(session),
                },
            )
            .await
            .map_err(host_error)
    }
    /// Same admitted SQL session and physical request owner as plugin actions.
    pub async fn logo_descriptor(
        &self,
        key: String,
        session: TaskSqlSession,
        owner: Owner,
    ) -> Result<Value, StoreError> {
        self.inner
            .actions
            .call(
                "plugin_logo".into(),
                json!({"key":key}),
                Invocation {
                    owner,
                    sql: Some(session),
                },
            )
            .await
            .map_err(host_error)
    }
    pub async fn list_plugins(
        &self,
        origin: String,
        session: TaskSqlSession,
        owner: Owner,
    ) -> Result<Value, StoreError> {
        self.inner
            .actions
            .call(
                "list_plugins".into(),
                json!({"origin":origin}),
                Invocation {
                    owner,
                    sql: Some(session),
                },
            )
            .await
            .map_err(host_error)
    }
    pub async fn reload_plugins(
        &self,
        session: TaskSqlSession,
        owner: Owner,
    ) -> Result<Value, StoreError> {
        let scope = Invocation {
            owner,
            sql: Some(session),
        };
        self.inner
            .actions
            .call(
                "stop_all_plugins".into(),
                json!({"reason":"reload"}),
                scope.clone(),
            )
            .await
            .map_err(host_error)?;
        let plugins = self
            .inner
            .actions
            .call(
                "discover".into(),
                json!({"sync_db":true,"force_reload":true}),
                scope,
            )
            .await
            .map_err(host_error)?;
        Ok(json!({"success":true,"count":plugins.as_array().ok_or_else(unavailable)?.len()}))
    }
    pub async fn change_enabled(
        &self,
        key: String,
        enabled: bool,
        origin: String,
        session: TaskSqlSession,
        owner: Owner,
    ) -> Result<Value, StoreError> {
        self.inner
            .actions
            .call(
                "change_enabled".into(),
                json!({"key":key,"enabled":enabled,"origin":origin}),
                Invocation {
                    owner,
                    sql: Some(session),
                },
            )
            .await
            .map_err(host_error)
    }
    pub async fn prepare_task(
        &self,
        raw: Arc<[u8]>,
        caller: Owner,
    ) -> Result<PluginTaskAdmission, StoreError> {
        let runtime = self.clone();
        tokio::spawn(async move { runtime.prepare_task_owned(raw, caller).await })
            .await
            .map_err(|_| unavailable())?
    }
    async fn prepare_task_owned(
        &self,
        raw: Arc<[u8]>,
        caller: Owner,
    ) -> Result<PluginTaskAdmission, StoreError> {
        if !self.registered_head(&raw) {
            return Err(unavailable());
        }
        let frame: Value = serde_json::from_slice(&raw).map_err(|_| unavailable())?;
        let id = frame["headers"]["id"]
            .as_str()
            .and_then(|s| Uuid::parse_str(s).ok())
            .ok_or_else(unavailable)?;
        let name = frame["headers"]["task"]
            .as_str()
            .ok_or_else(unavailable)?
            .to_owned();
        let actor = self
            .inner
            .tasks
            .iter()
            .find(|a| {
                a.busy
                    .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                    .is_ok()
            })
            .cloned()
            .ok_or_else(|| {
                StoreError::StatusBody(503, json!({"detail":"Native plugin task actors busy."}))
            })?;
        let owner: Owner = Arc::new((
            caller,
            Busy {
                actor: actor.clone(),
            },
        ));
        let session = self
            .inner
            .publications
            .admit(id, &raw, self.inner.auth.clone(), owner.clone())
            .await?;
        let scope = Invocation {
            owner,
            sql: Some(session.clone()),
        };
        let result = async {
            if !actor.loaded.load(Ordering::Acquire) {
                actor
                    .host
                    .call(
                        "discover".into(),
                        json!({"sync_db":false,"force_reload":false}),
                        scope.clone(),
                    )
                    .await
                    .map_err(host_error)?;
                actor.loaded.store(true, Ordering::Release);
            }
            let identity = actor.ports.identity().map_err(host_error)?;
            let capability = self.inner.registry.task_capability(&identity, &name)?;
            Ok(PluginTaskAdmission {
                actor,
                scope,
                capability,
                id,
                name,
                frame,
            })
        }
        .await;
        if result.is_err() {
            session.close().await?;
        }
        result
    }
    /// Called after actual owner-checked durable reservation and pool admission.
    /// Early/late ACK is owned by the selected handler, never a Python callback.
    pub async fn execute_after_actual_admission(
        &self,
        admission: PluginTaskAdmission,
    ) -> Result<Value, StoreError> {
        let runtime = self.clone();
        tokio::spawn(async move{
   let session=admission.scope.sql.as_ref().ok_or_else(unavailable)?.clone();
   let epoch=runtime.inner.claims.after_actual_admission(admission.id,&admission.capability)?;
   let args=json!({"id":admission.id.to_string(),"name":admission.name,"body_base64":admission.frame["body"],"content_type":admission.frame["content-type"],"content_encoding":admission.frame["content-encoding"],"request":request(&admission.frame),"invocation":epoch.token.to_string()});
   let result=admission.actor.host.call("execute_plugin_task".into(),args,admission.scope.clone()).await.map_err(host_error);
   let closed=session.close().await;drop(epoch);closed?;result
  }).await.map_err(|_|unavailable())?
    }
    /// Source Request._announce_revoked stores its actual backend metadata
    /// before ACK, with consumed Context children; this never executes the body.
    pub async fn announce_revoked(
        &self,
        admission: &PluginTaskAdmission,
        expired: bool,
    ) -> Result<Value, StoreError> {
        let runtime = self.clone();
        let actor = admission.actor.clone();
        let scope = admission.scope.clone();
        let capability = admission.capability.clone();
        let id = admission.id;
        let name = admission.name.clone();
        let frame = admission.frame.clone();
        tokio::spawn(async move {
            let epoch=runtime.inner.claims.after_actual_admission(id,&capability)?;
            let result=actor.host.call("announce_plugin_revoked".into(),json!({"id":id.to_string(),"name":name,"request":request(&frame),"body_base64":frame["body"],"invocation":epoch.token.to_string(),"expired":expired}),scope).await.map_err(host_error);
            drop(epoch);result
        }).await.map_err(|_|unavailable())?
    }
    pub async fn finish_revocation(
        &self,
        admission: PluginTaskAdmission,
        expired: bool,
    ) -> Result<Value, StoreError> {
        let runtime = self.clone();
        tokio::spawn(async move {
            let session=admission.scope.sql.as_ref().ok_or_else(unavailable)?.clone();
            let epoch=runtime.inner.claims.after_actual_admission(admission.id,&admission.capability)?;
            let result=admission.actor.host.call("signal_plugin_revoked".into(),json!({"id":admission.id.to_string(),"name":admission.name,"request":request(&admission.frame),"body_base64":admission.frame["body"],"invocation":epoch.token.to_string(),"expired":expired}),admission.scope.clone()).await.map_err(host_error);
            let closed=session.close().await;drop(epoch);closed?;result
        }).await.map_err(|_|unavailable())?
    }
    pub async fn release_task(&self, admission: PluginTaskAdmission) -> Result<(), StoreError> {
        let session = admission
            .scope
            .sql
            .as_ref()
            .ok_or_else(unavailable)?
            .clone();
        session.close().await?;
        drop(admission);
        Ok(())
    }
    pub fn actual_task_acknowledged(&self, id: Uuid, raw: &[u8]) -> Result<(), StoreError> {
        self.inner.publications.consumed(id, raw)
    }
    pub fn actual_task_restored(
        &self,
        id: Uuid,
        raw: &[u8],
        restored: &[u8],
    ) -> Result<(), StoreError> {
        self.inner.publications.restored(id, raw, restored)
    }
    pub fn accepting(&self) -> bool {
        !self.inner.closed.load(Ordering::Acquire)
            && !self.inner.unsettled_sql.load(Ordering::Acquire)
    }
    pub fn publication_owners(&self) -> PluginPublicationOwners {
        self.inner.publications.clone()
    }
    pub fn stop_admission(&self) {
        self.inner.closed.store(true, Ordering::Release);
        self.inner.actions.stop_admission();
        for actor in &self.inner.tasks {
            actor.host.stop_admission();
        }
    }
    pub fn interrupt_at(&self, deadline: tokio::time::Instant) {
        self.inner.actions.interrupt_at(deadline);
        for actor in &self.inner.tasks {
            actor.host.interrupt_at(deadline);
        }
    }
    pub async fn shutdown(&self, budget: Duration) -> Result<(), StoreError> {
        self.stop_admission();
        let deadline = tokio::time::Instant::now() + budget;
        let initial_unsettled_sql = self.inner.unsettled_sql.load(Ordering::Acquire);
        runtime_shutdown_diagnostic(
            "initial_sql",
            "runtime",
            0,
            "observed",
            Some(initial_unsettled_sql),
            None,
            budget,
        );
        let mut failure = initial_unsettled_sql.then(unavailable);
        for (index, host) in std::iter::once(&self.inner.actions)
            .chain(self.inner.tasks.iter().map(|a| &a.host))
            .enumerate()
        {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            let result = host.shutdown(remaining).await;
            if runtime_shutdown_diagnostics_enabled() {
                let receipt = host.last_receipt().await;
                runtime_shutdown_diagnostic(
                    "host_result",
                    if index == 0 { "actions" } else { "task" },
                    index.saturating_sub(1),
                    match &result {
                        Ok(_) => "Ok",
                        Err(error) => shutdown_host_error_kind(error),
                    },
                    None,
                    receipt.as_ref(),
                    deadline.saturating_duration_since(tokio::time::Instant::now()),
                );
            }
            if result.is_err() {
                failure = Some(unavailable());
            }
        }
        let final_unsettled_sql = self.inner.unsettled_sql.load(Ordering::Acquire);
        runtime_shutdown_diagnostic(
            "final_sql",
            "runtime",
            0,
            "observed",
            Some(final_unsettled_sql),
            None,
            deadline.saturating_duration_since(tokio::time::Instant::now()),
        );
        if final_unsettled_sql {
            failure = Some(unavailable());
        }
        failure.map_or(Ok(()), Err)
    }
}
// Diagnostic only: SAME selected shutdown flag, fixed vocabulary, max32 records
// per process. Never print HostError payloads, SQL, IDs, paths or credentials.
fn runtime_shutdown_diagnostics_enabled() -> bool {
    static ENABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ENABLED
        .get_or_init(|| std::env::var("RUST_SELECTED_SHUTDOWN_DIAGNOSTICS").is_ok_and(|v| v == "1"))
}
fn shutdown_host_error_kind(value: &HostError) -> &'static str {
    match value {
        HostError::Closed => "Closed",
        HostError::Capacity => "Capacity",
        HostError::Protocol => "Protocol",
        HostError::Child => "Child",
        HostError::Callback => "Callback",
        HostError::Drain => "Drain",
        HostError::Unavailable => "Unavailable",
        HostError::Remote(_) => "Remote",
    }
}
fn runtime_shutdown_diagnostic(
    stage: &'static str,
    host_kind: &'static str,
    host_index: usize,
    outcome: &'static str,
    unsettled_sql: Option<bool>,
    receipt: Option<&super::plugin_execution_host::Receipt>,
    remaining: Duration,
) {
    use std::sync::atomic::AtomicUsize;
    static RECORDS: AtomicUsize = AtomicUsize::new(0);
    if !runtime_shutdown_diagnostics_enabled()
        || RECORDS
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| {
                (n < 32).then_some(n + 1)
            })
            .is_err()
    {
        return;
    }
    let flags = receipt.map(|r| {
        json!({
            "actual_exit": r.actual_exit,
            "wait_observed": r.wait_observed,
            "stdout_eof": r.stdout_eof,
            "stderr_eof": r.stderr_eof,
            "forced": r.forced,
            "descendants_absent": r.descendants_absent,
            "callbacks_settled": r.callbacks_settled,
        })
    });
    eprintln!(
        "plugin_shutdown {}",
        json!({
            "stage": stage,
            "host_kind": host_kind,
            "host_index": host_index,
            "outcome": outcome,
            "unsettled_sql": unsettled_sql,
            "receipt": flags,
            "remaining_millis": remaining.as_millis().min(u128::from(u64::MAX)) as u64,
        })
    );
}

impl PluginTaskAdmission {
    pub fn capability(&self) -> PluginTaskCapability {
        self.capability.clone()
    }
    pub fn id(&self) -> Uuid {
        self.id
    }
}
fn request(frame: &Value) -> Value {
    let mut request = frame["headers"].clone();
    request["properties"] = frame["properties"].clone();
    request["delivery_info"] = json!({"exchange":frame["properties"]["delivery_info"]["exchange"],"routing_key":frame["properties"]["delivery_info"]["routing_key"],"priority":frame["properties"]["priority"],"redelivered":frame["properties"]["delivery_info"].get("redelivered").cloned().unwrap_or(json!(false))});
    request["reply_to"] = frame["properties"]["reply_to"].clone();
    request["correlation_id"] = frame["properties"]["correlation_id"].clone();
    request
}

/// One actually claimed native task's hook context. Retains its existing task
/// actor/global caller and provided SQL session across repeated event calls.
pub struct PluginHookSession {
    inner: Arc<HookInner>,
}
struct HookInner {
    runtime: std::sync::Weak<Inner>,
    actor: Arc<TaskActor>,
    scope: Invocation,
    trail: Arc<dyn crate::plugin_hook_context::NativeTaskTrail>,
    cursor: tokio::sync::Mutex<u64>,
    serial: tokio::sync::Mutex<()>,
    registration: super::plugin_hook_publications::HookRegistration,
    finished: AtomicBool,
    task_id: String,
}
impl PluginRuntime {
    pub async fn begin_plugin_hooks(
        &self,
        context: crate::PluginHookTaskContext,
        trail: Arc<dyn crate::NativeTaskTrail>,
        session: TaskSqlSession,
        caller: Owner,
    ) -> Result<PluginHookSession, StoreError> {
        let runtime = self.clone();
        tokio::spawn(async move {
            if runtime.inner.closed.load(Ordering::Acquire)
                || context.task_id() != trail.claimed_task_id()
            {
                return Err(unavailable());
            }
            let idle = session
                .run(|c| Box::pin(async move { c.protocol_transaction_is_idle().await }))
                .await??;
            if !idle {
                return Err(StoreError::Unsupported(
                    "Native hook context requires an actual idle/autocommit SQL boundary",
                ));
            }
            let actor = runtime
                .inner
                .tasks
                .iter()
                .find(|a| {
                    a.busy
                        .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                        .is_ok()
                })
                .cloned()
                .ok_or_else(|| {
                    StoreError::StatusBody(503, json!({"detail":"Native plugin task actors busy."}))
                })?;
            let owner: Owner = Arc::new((
                caller,
                Busy {
                    actor: actor.clone(),
                },
            ));
            let scope = Invocation {
                owner,
                sql: Some(session),
            };
            if !actor.loaded.load(Ordering::Acquire) {
                actor
                    .host
                    .call(
                        "discover".into(),
                        json!({"sync_db":false,"force_reload":false}),
                        scope.clone(),
                    )
                    .await
                    .map_err(host_error)?;
                actor.loaded.store(true, Ordering::Release);
            }
            let registration = actor
                .ports
                .hook_publications
                .begin(context.task_id().into(), &scope.owner)
                .map_err(host_error)?;
            let began = actor
                .host
                .call(
                    "begin_native_task".into(),
                    json!({"frame":context.frame}),
                    scope.clone(),
                )
                .await
                .map_err(host_error)?;
            if began["task_id"] != context.task_id || began["task_name"] != context.task_name {
                return Err(unavailable());
            }
            Ok(PluginHookSession {
                inner: Arc::new(HookInner {
                    runtime: Arc::downgrade(&runtime.inner),
                    actor,
                    scope,
                    trail,
                    cursor: tokio::sync::Mutex::new(0),
                    serial: tokio::sync::Mutex::new(()),
                    registration,
                    finished: AtomicBool::new(false),
                    task_id: context.task_id,
                }),
            })
        })
        .await
        .map_err(|_| unavailable())?
    }
}
impl HookInner {
    async fn interrupt_unknown_sql(&self) {
        self.actor.host.interrupt_at(tokio::time::Instant::now());
        if let Some(session) = &self.scope.sql {
            if let Some(runtime) = self.runtime.upgrade() {
                runtime.unsettled_sql.store(true, Ordering::Release);
                PluginRuntime { inner: runtime }.stop_admission();
            }
            let _ = session.abort_physical().await;
        }
    }
    async fn abort_serialized(&self) -> Result<(), StoreError> {
        let _serial = match self.serial.try_lock() {
            Ok(serial) => serial,
            Err(_) => {
                // A callback holding serial may itself be blocked on this SQL
                // session. Signal physical retirement BEFORE waiting for serial.
                self.interrupt_unknown_sql().await;
                self.serial.lock().await
            }
        };
        self.abort_owned().await
    }
    async fn abort_owned(&self) -> Result<(), StoreError> {
        if self.finished.load(Ordering::Acquire) {
            return Ok(());
        }
        let result = self
            .actor
            .host
            .call(
                "abort_native_task".into(),
                json!({"task_id":self.task_id}),
                self.scope.clone(),
            )
            .await;
        if result.as_ref().is_ok_and(|v| v["aborted"] == true) {
            self.finished.store(true, Ordering::Release);
            return Ok(());
        }
        self.interrupt_unknown_sql().await;
        // Keep caller/SQL/slot ownership through actual retirement, including
        // an observer disappearing. Missing receipts stay physically unsettled.
        loop {
            if self.actor.host.last_receipt().await.is_some_and(|r| {
                r.wait_observed && r.stdout_eof && r.stderr_eof && r.descendants_absent
            }) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        self.finished.store(true, Ordering::Release);
        Err(unavailable())
    }
    async fn sync_native(&self, require_closed: bool) -> Result<(), StoreError> {
        let mut cursor = self.cursor.lock().await;
        loop {
            let delta = self.trail.settled_after(*cursor)?;
            if delta.state == crate::NativeTaskTrailState::Unsettled {
                return Err(unavailable());
            }
            if delta.entries.len() > 65536 {
                return Err(unavailable());
            }
            if delta.entries.is_empty() {
                if delta.next_cursor != *cursor
                    || require_closed && delta.state != crate::NativeTaskTrailState::Closed
                {
                    return Err(unavailable());
                }
                return Ok(());
            }
            let mut ordinal = *cursor;
            let mut children = Vec::new();
            for entry in delta.entries {
                ordinal = ordinal.checked_add(1).ok_or_else(unavailable)?;
                if entry.ordinal != ordinal {
                    return Err(unavailable());
                }
                children.push(entry.source_result_tuple);
            }
            if ordinal != delta.next_cursor || ordinal > 65536 {
                return Err(unavailable());
            }
            let bytes = serde_json::to_vec(&children).map_err(|_| unavailable())?;
            if bytes.len() > 4 * 1024 * 1024 {
                return Err(unavailable());
            }
            self.actor
                .host
                .call(
                    "append_native_trail".into(),
                    json!({"children":children}),
                    self.scope.clone(),
                )
                .await
                .map_err(host_error)?;
            // Cursor moves only after the actual same-thread append reply settled.
            *cursor = ordinal;
        }
    }
    fn report(&self, actual: Value, counts: Value) -> Result<crate::PluginHookReport, StoreError> {
        if actual["task_id"] != self.task_id || !actual["children"].is_array() {
            return Err(unavailable());
        }
        Ok(crate::PluginHookReport {
            actual_published_children: self.registration.receipts().map_err(host_error)?,
            observed_request_children: actual["children"].clone(),
            attempted: counts["attempted"].as_u64().ok_or_else(unavailable)?,
            completed: counts["completed"].as_u64().ok_or_else(unavailable)?,
            caught: counts["caught"].as_u64().ok_or_else(unavailable)?,
        })
    }
}
impl PluginHookSession {
    pub async fn begin_event(&self, event: String, payload: Value) -> Result<(), StoreError> {
        if !matches!(
            event.as_str(),
            "epg_error" | "epg_refresh" | "recording_start" | "recording_end"
        ) {
            return Err(unavailable());
        }
        let inner = self.inner.clone();
        tokio::spawn(async move {
            let _serial = inner.serial.lock().await;
            if inner.finished.load(Ordering::Acquire) {
                return Err(unavailable());
            }
            inner.sync_native(false).await?;
            inner
                .actor
                .host
                .call(
                    "begin_plugin_event".into(),
                    json!({"event":event,"payload":payload}),
                    inner.scope.clone(),
                )
                .await
                .map_err(host_error)?;
            Ok(())
        })
        .await
        .map_err(|_| unavailable())?
    }
    pub async fn current_event_payload(&self) -> Result<Value, StoreError> {
        let inner = self.inner.clone();
        tokio::spawn(async move {
            let _serial = inner.serial.lock().await;
            inner
                .actor
                .host
                .call(
                    "plugin_event_payload".into(),
                    json!({}),
                    inner.scope.clone(),
                )
                .await
                .map_err(host_error)
        })
        .await
        .map_err(|_| unavailable())?
    }
    pub async fn end_event(&self) -> Result<(), StoreError> {
        let inner = self.inner.clone();
        tokio::spawn(async move {
            let _serial = inner.serial.lock().await;
            inner
                .actor
                .host
                .call("end_plugin_event".into(), json!({}), inner.scope.clone())
                .await
                .map_err(host_error)?;
            Ok(())
        })
        .await
        .map_err(|_| unavailable())?
    }
    pub async fn dispatch_plugin_actions(&self) -> Result<crate::PluginHookReport, StoreError> {
        let inner = self.inner.clone();
        tokio::spawn(async move {
            let _serial = inner.serial.lock().await;
            if inner.finished.load(Ordering::Acquire) {
                return Err(unavailable());
            }
            inner.sync_native(false).await?;
            let counts = inner
                .actor
                .host
                .call(
                    "dispatch_plugin_event".into(),
                    json!({}),
                    inner.scope.clone(),
                )
                .await
                .map_err(host_error)?;
            let actual = inner
                .actor
                .host
                .call(
                    "snapshot_native_task".into(),
                    json!({}),
                    inner.scope.clone(),
                )
                .await
                .map_err(host_error)?;
            inner.report(actual, counts)
        })
        .await
        .map_err(|_| unavailable())?
    }
    pub async fn render_connect_template(
        &self,
        template: String,
    ) -> Result<crate::PluginTemplateOutcome, StoreError> {
        if template.len() > 65536 {
            return Err(unavailable());
        }
        let inner = self.inner.clone();
        tokio::spawn(async move {
            let _serial = inner.serial.lock().await;
            if inner.finished.load(Ordering::Acquire) {
                return Err(unavailable());
            }
            inner.sync_native(false).await?;
            let result = inner
                .actor
                .host
                .call(
                    "render_connect_template".into(),
                    json!({"template":template}),
                    inner.scope.clone(),
                )
                .await
                .map_err(host_error)?;
            let render = match result["actual_render_succeeded"].as_bool() {
                Some(true) => Ok(crate::PluginTemplateRender::Rendered(
                    result["rendered"]
                        .as_str()
                        .ok_or_else(unavailable)?
                        .to_owned(),
                )),
                Some(false) => Ok(crate::PluginTemplateRender::Caught {
                    actual_class: result["actual_exception_class"]
                        .as_str()
                        .ok_or_else(unavailable)?
                        .to_owned(),
                    message: result["message"]
                        .as_str()
                        .ok_or_else(unavailable)?
                        .to_owned(),
                }),
                None => Err(unavailable()),
            }?;
            Ok(crate::PluginTemplateOutcome {
                render,
                current_payload: result["current_payload"].clone(),
                source_json_loads_succeeded: result["source_json_loads_succeeded"].as_bool(),
            })
        })
        .await
        .map_err(|_| unavailable())?
    }
    /// Error-path context retirement; no normal trail/terminal report is minted.
    /// Await this BEFORE the caller closes its provided TaskSqlSession.
    pub async fn abort(&self) -> Result<(), StoreError> {
        let inner = self.inner.clone();
        tokio::spawn(async move { inner.abort_serialized().await })
            .await
            .map_err(|_| unavailable())?
    }
    pub async fn finish(&self) -> Result<crate::PluginHookReport, StoreError> {
        let inner = self.inner.clone();
        tokio::spawn(async move {
            let _serial = inner.serial.lock().await;
            if inner.finished.load(Ordering::Acquire) {
                return Err(unavailable());
            }
            inner.sync_native(true).await?;
            let actual = inner
                .actor
                .host
                .call("finish_native_task".into(), json!({}), inner.scope.clone())
                .await
                .map_err(host_error)?;
            inner.finished.store(true, Ordering::Release);
            inner.report(actual, json!({"attempted":0,"completed":0,"caught":0}))
        })
        .await
        .map_err(|_| unavailable())?
    }
}
impl Drop for PluginHookSession {
    fn drop(&mut self) {
        if self.inner.finished.load(Ordering::Acquire) {
            return;
        }
        let inner = self.inner.clone();
        tokio::spawn(async move {
            let _ = inner.abort_serialized().await;
        });
    }
}
