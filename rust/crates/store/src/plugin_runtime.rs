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
    raw: Arc<[u8]>,
    frame: Value,
}
fn unavailable() -> StoreError {
    StoreError::Unsupported("Prepared native plugin runtime unavailable")
}
fn host_error(_: HostError) -> StoreError {
    unavailable()
}
impl PluginRuntime {
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
            }),
        })
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
                raw,
                frame,
            })
        }
        .await;
        if result.is_err() {
            session.close().await?;
        }
        result
    }
    /// Called by the actual common selected handler only AFTER its exact queue
    /// compare/pop or owner-checked ETA ACK settled. No callback exposes this API.
    pub async fn execute_after_actual_ack(
        &self,
        admission: PluginTaskAdmission,
    ) -> Result<Value, StoreError> {
        let runtime = self.clone();
        tokio::spawn(async move{
   let session=admission.scope.sql.as_ref().ok_or_else(unavailable)?.clone();
   let epoch=runtime.inner.claims.after_actual_ack(admission.id,&admission.capability)?;
   runtime.inner.publications.consumed(admission.id,&admission.raw)?;
   let args=json!({"id":admission.id.to_string(),"name":admission.name,"body_base64":admission.frame["body"],"content_type":admission.frame["content-type"],"content_encoding":admission.frame["content-encoding"],"request":request(&admission.frame),"invocation":epoch.token.to_string()});
   let result=admission.actor.host.call("execute_plugin_task".into(),args,admission.scope.clone()).await.map_err(host_error);
   let closed=session.close().await;drop(epoch);closed?;result
  }).await.map_err(|_|unavailable())?
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
        let mut failure = None;
        for host in
            std::iter::once(&self.inner.actions).chain(self.inner.tasks.iter().map(|a| &a.host))
        {
            if host
                .shutdown(deadline.saturating_duration_since(tokio::time::Instant::now()))
                .await
                .is_err()
            {
                failure = Some(unavailable());
            }
        }
        failure.map_or(Ok(()), Err)
    }
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
    request["delivery_info"] = frame["properties"]["delivery_info"].clone();
    request["correlation_id"] = frame["properties"]["correlation_id"].clone();
    request
}
