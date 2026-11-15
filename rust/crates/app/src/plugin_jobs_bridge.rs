//! Real Main Jobs/broker/result bridge. No source worker or private extra pool.
//! Original Dispatcharr/Celery/Kombu authors retain full protocol credit.
use base64::{engine::general_purpose::STANDARD, Engine};
use dispatcharr_contracts::BoxFuture;
use dispatcharr_job_transport::{normal_queue, transport::Jobs, PluginResults};
use dispatcharr_store::{
    NativePluginJobs, PluginHostError, PluginHostIdentity, PluginInvocation, PluginTaskCapability,
};
use serde_json::{json, Value};
use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};
type Owner = Arc<dyn Send + Sync>;
type Key = (u32, String);
#[derive(Clone)]
pub(crate) struct PluginJobsBridge {
    jobs: Jobs,
    lifetime: Owner,
    prepared: Arc<AtomicBool>,
    results: Arc<tokio::sync::Mutex<BTreeMap<Key, PluginResults>>>,
}
impl PluginJobsBridge {
    pub(crate) fn prepare(jobs: Jobs, lifetime: Owner) -> Result<Self, PluginHostError> {
        jobs.plugin_queue_capacity()
            .map_err(|_| PluginHostError::Unavailable)?;
        Ok(Self {
            jobs,
            lifetime,
            prepared: Arc::new(AtomicBool::new(false)),
            results: Arc::new(tokio::sync::Mutex::new(BTreeMap::new())),
        })
    }
    /// Called only with the ACTUAL successfully constructed extended common
    /// dispatcher. A wrapper/header flag cannot announce runtime capabilities.
    pub(crate) fn attach_prepared(
        &self,
        application: &normal_queue::Application,
    ) -> Result<(), PluginHostError> {
        if !application.registered_plugin_prepared() {
            return Err(PluginHostError::Unavailable);
        }
        self.prepared.store(true, Ordering::Release);
        Ok(())
    }
    pub(crate) async fn shutdown(&self, budget: Duration) -> Result<(), PluginHostError> {
        self.prepared.store(false, Ordering::Release);
        let ports = std::mem::take(&mut *self.results.lock().await);
        let deadline = tokio::time::Instant::now() + budget;
        let mut error = false;
        for port in ports.into_values() {
            if port.close_host().await.is_err() {
                error = true;
            }
        }
        if self
            .jobs
            .shutdown_plugin_subscriptions(
                deadline.saturating_duration_since(tokio::time::Instant::now()),
            )
            .await
            .is_err()
        {
            error = true;
        }
        if error {
            Err(PluginHostError::Drain)
        } else {
            Ok(())
        }
    }
}
impl NativePluginJobs for PluginJobsBridge {
    fn ready(&self) -> bool {
        self.prepared.load(Ordering::Acquire)
    }
    fn capabilities(&self) -> Vec<&'static str> {
        if self.ready() {
            vec![
                "sealed_registry",
                "source_frame_publish",
                "source_binding",
                "queue_size",
                "claimed_trace",
                "backend_commands",
                "result_subscription",
            ]
        } else {
            Vec::new()
        }
    }
    fn callback(
        &self,
        _identity: PluginHostIdentity,
        method: String,
        params: Value,
        capability: Option<PluginTaskCapability>,
        scope: PluginInvocation,
    ) -> BoxFuture<'_, Result<Value, PluginHostError>> {
        Box::pin(async move {
            if !self.ready() {
                return Err(PluginHostError::Unavailable);
            }
            match method.as_str() {
                "validate_broker" => {
                    self.jobs
                        .validate_plugin_broker(&params)
                        .map_err(|_| PluginHostError::Unavailable)?;
                    Ok(Value::Null)
                }
                "validate_queue" if params["queue"] == "celery" => Ok(Value::Null),
                "queue_size" if params["queue"] == "celery" => self
                    .jobs
                    .registered_plugin_queue_length(scope.owner)
                    .await
                    .map(Value::from)
                    .map_err(|_| PluginHostError::Callback),
                "bind_queue" => {
                    self.jobs
                        .bind_registered_plugin_queue(
                            params["exchange"]
                                .as_str()
                                .ok_or(PluginHostError::Protocol)?,
                            params["routing_key"]
                                .as_str()
                                .ok_or(PluginHostError::Protocol)?,
                            params["pattern"].as_str(),
                            params["queue"].as_str().ok_or(PluginHostError::Protocol)?,
                            scope.owner,
                        )
                        .await
                        .map_err(|_| PluginHostError::Callback)?;
                    Ok(Value::Null)
                }
                "publish_registered" if params["queue"] == "celery" => {
                    let text = params["raw_base64"]
                        .as_str()
                        .filter(|s| s.len() <= 90000)
                        .ok_or(PluginHostError::Capacity)?;
                    let raw = STANDARD
                        .decode(text)
                        .map_err(|_| PluginHostError::Protocol)?;
                    let receipt = self
                        .jobs
                        .publish_registered_plugin(
                            capability.ok_or(PluginHostError::Protocol)?,
                            raw,
                            scope.owner,
                        )
                        .await
                        .map_err(|_| PluginHostError::Callback)?;
                    Ok(
                        json!({"settled":true,"task_id":receipt.task_id,"queue_length":receipt.queue_length}),
                    )
                }
                "publish_native_refresh"
                    if params["queue"] == "celery"
                        && params["name"] == "apps.plugins.tasks.refresh_plugin_repos" =>
                {
                    let text = params["raw_base64"]
                        .as_str()
                        .filter(|s| s.len() <= 90000)
                        .ok_or(PluginHostError::Capacity)?;
                    let raw = STANDARD
                        .decode(text)
                        .map_err(|_| PluginHostError::Protocol)?;
                    let receipt = self
                        .jobs
                        .publish_builtin_plugin_refresh(raw, scope.owner)
                        .await
                        .map_err(|_| PluginHostError::Callback)?;
                    Ok(
                        json!({"settled":true,"task_id":receipt.task_id,"queue_length":receipt.queue_length}),
                    )
                }
                // Direct builtin .run is guarded BEFORE the Original backend body. Actual
                // same-session family adapters are still required; never evaluate Python
                // backend task code merely because an enabled plugin requested it.
                "inline_builtin" => Err(PluginHostError::Unavailable),
                _ => Err(PluginHostError::Unavailable),
            }
        })
    }
    fn backend_callback(
        &self,
        identity: PluginHostIdentity,
        method: String,
        params: Value,
        scope: PluginInvocation,
    ) -> BoxFuture<'_, Result<Value, PluginHostError>> {
        Box::pin(async move {
            if !self.ready() {
                return Err(PluginHostError::Unavailable);
            }
            let key = (identity.pid, identity.start_generation);
            let port = {
                let mut ports = self.results.lock().await;
                if !ports.contains_key(&key) {
                    if ports.len() >= 8 {
                        return Err(PluginHostError::Capacity);
                    }
                    ports.insert(
                        key.clone(),
                        PluginResults::prepare(self.jobs.clone(), self.lifetime.clone())
                            .map_err(|_| PluginHostError::Unavailable)?,
                    );
                }
                ports
                    .get(&key)
                    .expect("actual inserted host result port")
                    .clone()
            };
            port.callback(&method, params, scope.owner)
                .await
                .map_err(|_| PluginHostError::Callback)
        })
    }
    fn close_host(
        &self,
        identity: PluginHostIdentity,
        _owner: Owner,
    ) -> BoxFuture<'_, Result<(), PluginHostError>> {
        Box::pin(async move {
            let port = self
                .results
                .lock()
                .await
                .remove(&(identity.pid, identity.start_generation));
            if let Some(port) = port {
                port.close_host()
                    .await
                    .map_err(|_| PluginHostError::Drain)?;
            }
            Ok(())
        })
    }
    fn settled(
        &self,
        identity: PluginHostIdentity,
        _owner: Owner,
    ) -> BoxFuture<'_, Result<bool, PluginHostError>> {
        Box::pin(async move {
            Ok(!self
                .results
                .lock()
                .await
                .contains_key(&(identity.pid, identity.start_generation)))
        })
    }
}
