//! Actual Auth4/session/cache/Channels/registry callback composition. Jobs and
//! process adapters are supplied by normal Main's EXISTING owned services.
//! Missing executable interfaces cannot advertise ready or execute Source task
//! bodies as a fallback. Original Dispatcharr authors retain full credit.
use super::plugin_execution_host::{Error, Identity, Invocation, Ports};
use super::plugin_publication_owners::PluginPublicationOwners;
use super::plugin_python_registry::{HostRole, PluginTaskCapability, PluginTaskRegistry};
use super::plugin_trace_claims::PluginTraceClaims;
use super::{
    plugin_python_cache::PythonCachePort, plugin_python_events::PythonEventPort,
    plugin_python_sql_registry::PythonSqlRegistry,
};
use crate::AuthService;
use base64::{engine::general_purpose::STANDARD, Engine};
use dispatcharr_contracts::{BoxFuture, UpdateEventSink};
use serde_json::{json, Value};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};
use uuid::Uuid;
type Owner = Arc<dyn Send + Sync>;
/// Main implements this using the SAME Jobs/broker/result/global dispatcher.
/// Capability is verified against actual child/module bytes before publication.
/// No JSON caller can supply this typed argument or forge child identity.
pub trait NativePluginJobs: Send + Sync {
    fn ready(&self) -> bool;
    fn capabilities(&self) -> Vec<&'static str>;
    fn callback(
        &self,
        identity: Identity,
        method: String,
        params: Value,
        capability: Option<PluginTaskCapability>,
        scope: Invocation,
    ) -> BoxFuture<'_, Result<Value, Error>>;
    fn backend_callback(
        &self,
        identity: Identity,
        method: String,
        params: Value,
        scope: Invocation,
    ) -> BoxFuture<'_, Result<Value, Error>>;
    fn close_host(&self, identity: Identity, owner: Owner) -> BoxFuture<'_, Result<(), Error>>;
    fn settled(&self, identity: Identity, owner: Owner) -> BoxFuture<'_, Result<bool, Error>>;
}
pub trait NativePluginProcesses: Send + Sync {
    fn ready(&self) -> bool;
    fn child_started(&self, identity: Identity, owner: Owner) -> BoxFuture<'_, Result<(), Error>>;
    fn callback(
        &self,
        identity: Identity,
        method: String,
        params: Value,
        scope: Invocation,
    ) -> BoxFuture<'_, Result<Value, Error>>;
    fn settle(
        &self,
        identity: Identity,
        forced: bool,
        owner: Owner,
    ) -> BoxFuture<'_, Result<bool, Error>>;
}
pub(crate) struct NativePorts {
    identity: Mutex<Option<Identity>>,
    role: HostRole,
    registry: PluginTaskRegistry,
    publications: PluginPublicationOwners,
    claims: PluginTraceClaims,
    sql: PythonSqlRegistry,
    cache: PythonCachePort,
    events: PythonEventPort,
    jobs: Arc<dyn NativePluginJobs>,
    processes: Arc<dyn NativePluginProcesses>,
    closed: AtomicBool,
    pub(crate) hook_publications: super::plugin_hook_publications::HookPublications,
}
impl NativePorts {
    // Each argument is an existing owned production port or sealed registry;
    // keeping explicit injection prevents ambient services or extra clients.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        auth: AuthService,
        role: HostRole,
        registry: PluginTaskRegistry,
        publications: PluginPublicationOwners,
        claims: PluginTraceClaims,
        events: Arc<dyn UpdateEventSink>,
        jobs: Arc<dyn NativePluginJobs>,
        processes: Arc<dyn NativePluginProcesses>,
    ) -> Self {
        Self {
            identity: Mutex::new(None),
            role,
            registry,
            publications,
            claims,
            sql: PythonSqlRegistry::new(auth.clone()),
            cache: PythonCachePort::new(auth),
            events: PythonEventPort::new(events),
            jobs,
            processes,
            closed: AtomicBool::new(false),
            hook_publications: Default::default(),
        }
    }
    pub(crate) fn identity(&self) -> Result<Identity, Error> {
        self.identity
            .lock()
            .map_err(|_| Error::Protocol)?
            .clone()
            .ok_or(Error::Closed)
    }
}
impl Ports for NativePorts {
    fn ready_for_execution(&self) -> bool {
        !self.closed.load(Ordering::Acquire) && self.jobs.ready() && self.processes.ready()
    }
    fn child_started(&self, identity: Identity, owner: Owner) -> BoxFuture<'_, Result<(), Error>> {
        Box::pin(async move {
            if self.closed.load(Ordering::Acquire) {
                return Err(Error::Closed);
            }
            self.registry
                .bind_host(identity.clone(), self.role)
                .map_err(|_| Error::Protocol)?;
            self.processes
                .child_started(identity.clone(), owner)
                .await?;
            *self.identity.lock().map_err(|_| Error::Protocol)? = Some(identity);
            Ok(())
        })
    }
    fn callback(
        &self,
        port: String,
        method: String,
        params: Value,
        scope: Invocation,
    ) -> BoxFuture<'_, Result<Value, Error>> {
        Box::pin(async move {
            if self.closed.load(Ordering::Acquire) {
                return Err(Error::Closed);
            }
            let identity = self.identity()?;
            match port.as_str() {
                "sql" => self
                    .sql
                    .callback(&method, params, scope)
                    .await
                    .map_err(|_| Error::Callback),
                "cache" => self
                    .cache
                    .callback(&method, params, scope)
                    .await
                    .map_err(|_| Error::Callback),
                "events" if method == "group_send" => {
                    let group = params["group"]
                        .as_str()
                        .filter(|s| s.len() <= 100)
                        .ok_or(Error::Protocol)?
                        .to_owned();
                    self.events
                        .group_send(group, params["message"].clone(), scope.owner)
                        .await
                        .map_err(|_| Error::Callback)
                }
                "process" => {
                    self.processes
                        .callback(identity, method, params, scope)
                        .await
                }
                "jobs_backend" => {
                    self.jobs
                        .backend_callback(identity, method, params, scope)
                        .await
                }
                "jobs" => {
                    if method == "register_loaded_tasks" {
                        let witness = self
                            .registry
                            .register(identity, params["tasks"].clone(), scope.owner)
                            .await
                            .map_err(|_| Error::Protocol)?;
                        return Ok(json!({"witness":witness.to_string()}));
                    }
                    if method == "require_capabilities" {
                        let wanted = params["required"].as_array().ok_or(Error::Protocol)?;
                        let actual = self.jobs.capabilities();
                        if wanted
                            .iter()
                            .any(|v| v.as_str().is_none_or(|v| !actual.contains(&v)))
                        {
                            return Err(Error::Unavailable);
                        }
                        return Ok(params["required"].clone());
                    }
                    if method == "validate_trace_invocation" && self.role != HostRole::Tasks {
                        return Err(Error::Protocol);
                    }
                    let capability = if matches!(
                        method.as_str(),
                        "publish_registered" | "validate_trace_invocation"
                    ) {
                        let witness = params["witness"]
                            .as_str()
                            .and_then(|s| Uuid::parse_str(s).ok())
                            .ok_or(Error::Protocol)?;
                        let name = params["name"].as_str().ok_or(Error::Protocol)?;
                        Some(
                            self.registry
                                .capability(&identity, witness, name)
                                .map_err(|_| Error::Protocol)?,
                        )
                    } else {
                        None
                    };
                    if method == "validate_trace_invocation" {
                        let token = params["invocation"]
                            .as_str()
                            .and_then(|s| Uuid::parse_str(s).ok())
                            .ok_or(Error::Protocol)?;
                        let id = params["task_id"]
                            .as_str()
                            .and_then(|s| Uuid::parse_str(s).ok())
                            .ok_or(Error::Protocol)?;
                        let capability = capability.as_ref().ok_or(Error::Protocol)?;
                        return self
                            .claims
                            .validate(token, id, &identity, capability)
                            .map(Value::Bool)
                            .map_err(|_| Error::Protocol);
                    }
                    let hook_intent = if method == "publish_registered" {
                        let raw = STANDARD
                            .decode(params["raw_base64"].as_str().ok_or(Error::Protocol)?)
                            .map_err(|_| Error::Protocol)?;
                        self.hook_publications.before(&raw, &scope.owner)?
                    } else {
                        None
                    };
                    let intent = if method == "publish_registered" {
                        let encoded = params["raw_base64"]
                            .as_str()
                            .filter(|s| s.len() <= 90000)
                            .ok_or(Error::Capacity)?;
                        let raw = STANDARD.decode(encoded).map_err(|_| Error::Protocol)?;
                        let value: Value =
                            serde_json::from_slice(&raw).map_err(|_| Error::Protocol)?;
                        let id = value["headers"]["id"]
                            .as_str()
                            .and_then(|s| Uuid::parse_str(s).ok())
                            .ok_or(Error::Protocol)?;
                        self.publications
                            .before_publish(id, &raw, &scope)
                            .map_err(|_| Error::Capacity)?
                    } else {
                        None
                    };
                    let result = self
                        .jobs
                        .callback(identity, method, params, capability, scope)
                        .await?;
                    if let Some(intent) = intent {
                        if result["settled"] != true {
                            return Err(Error::Unavailable);
                        }
                        let actual_id = result["task_id"]
                            .as_str()
                            .and_then(|s| Uuid::parse_str(s).ok())
                            .ok_or(Error::Protocol)?;
                        self.publications
                            .settled(intent, actual_id)
                            .map_err(|_| Error::Protocol)?;
                    }
                    if let Some(intent) = hook_intent {
                        if result["settled"] != true {
                            return Err(Error::Unavailable);
                        }
                        intent.settled(result["task_id"].as_str().ok_or(Error::Protocol)?)?;
                    }
                    Ok(result)
                }
                _ => Err(Error::Unavailable),
            }
        })
    }
    fn callback_reentrant(
        &self,
        port: String,
        method: String,
        params: Value,
        scope: Invocation,
        reentry: Arc<dyn super::plugin_execution_host::Reentry>,
    ) -> BoxFuture<'_, Result<Value, Error>> {
        if port == "plugin_events" {
            return Box::pin(async move {
                if self.closed.load(Ordering::Acquire)
                    || self.role != HostRole::Tasks
                    || scope.sql.is_none()
                    || method != "dispatch"
                {
                    return Err(Error::Unavailable);
                }
                super::plugin_event_control::dispatch(scope, reentry).await
            });
        }
        if port != "plugin_control" {
            return self.callback(port, method, params, scope);
        }
        Box::pin(async move {
            if self.closed.load(Ordering::Acquire)
                || self.role != HostRole::Actions
                || scope.sql.is_none()
            {
                return Err(Error::Unavailable);
            }
            match method.as_str() {
                "enabled" => super::plugin_enabled_control::change(params, scope, reentry).await,
                _ => Err(Error::Unavailable),
            }
        })
    }
    fn close_callback_admission(
        &self,
        identity: Identity,
        owner: Owner,
    ) -> BoxFuture<'_, Result<(), Error>> {
        Box::pin(async move {
            self.closed.store(true, Ordering::Release);
            self.sql.begin_shutdown();
            // Wake infinite logical reads before the host joins its callback JoinSet.
            self.jobs.close_host(identity, owner).await
        })
    }
    fn settle_callbacks(
        &self,
        identity: Identity,
        owner: Owner,
    ) -> BoxFuture<'_, Result<bool, Error>> {
        Box::pin(async move {
            let sql = self.sql.shutdown().await.is_ok();
            let jobs = self.jobs.settled(identity.clone(), owner).await?;
            self.registry
                .remove_host(&identity)
                .map_err(|_| Error::Protocol)?;
            Ok(sql && jobs)
        })
    }
    fn settle_descendants(
        &self,
        identity: Identity,
        forced: bool,
        owner: Owner,
    ) -> BoxFuture<'_, Result<bool, Error>> {
        self.processes.settle(identity, forced, owner)
    }
}
