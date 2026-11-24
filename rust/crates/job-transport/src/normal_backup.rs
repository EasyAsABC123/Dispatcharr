//! Normal Main backup HTTP and shared-family create/schedule/current-schema restore.
//! Original Dispatcharr/Celery authors retain all application/protocol credit.
use crate::{
    milestone_dispatch::{Disposition, Handler, Lifecycle},
    normal_backup_archive::Executor,
    normal_backup_protocol::Kind,
    transport::{Error as TransportError, Jobs},
};
#[cfg(target_os = "linux")]
pub use dispatcharr_backups::VerifiedGuardedHelper;
pub use dispatcharr_backups::{PgConfig, Runtime};
use dispatcharr_contracts::BoxFuture;
use dispatcharr_store::{
    AuthService, BackupJobApi, PluginActions, ProducerIdentitySource, Store, StoreError,
};
use serde_json::{json, Value};
use std::{
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore};
use uuid::Uuid;
#[path = "normal_backup_failure_status.rs"]
mod failure_status;
#[path = "normal_backup_manual.rs"]
mod manual;
pub use manual::ManualApplication;
#[cfg(target_os = "linux")]
#[path = "normal_backup_selected_taskless.rs"]
mod selected_taskless;
#[cfg(target_os = "linux")]
pub(super) struct SelectedTaskless {
    authority: crate::normal_queue::TasklessBuiltins,
    helper: VerifiedGuardedHelper,
}

#[derive(Debug)]
pub enum Error {
    Configuration,
}
struct Owner {
    _permit: OwnedSemaphorePermit,
    _caller: Arc<dyn Send + Sync>,
    _application: Application,
}
struct Inner {
    jobs: Jobs,
    revoked: crate::normal_revocation::Revoked,
    executor: Executor,
    schedule: crate::normal_backup_schedule::Schedule,
    schedule_auth: AuthService,
    identity: ProducerIdentitySource,
    worker: Arc<Semaphore>,
    http: Arc<Semaphore>,
    closed: AtomicBool,
    restore_claims_closed: AtomicBool,
    advanced: Arc<Notify>,
    uncertain: Mutex<Vec<Arc<dyn Send + Sync>>>,
    #[cfg(target_os = "linux")]
    taskless_pending: Mutex<Option<selected_taskless::Held>>,
}
#[derive(Clone)]
pub struct Application {
    inner: Arc<Inner>,
}
impl Application {
    pub(crate) fn scheduled_jobs(&self) -> Jobs {
        self.inner.jobs.clone()
    }

    pub(crate) fn schedule_pool(&self) -> &sqlx::PgPool {
        self.inner.schedule.pool()
    }

    pub(crate) fn schedule_identity(&self) -> ProducerIdentitySource {
        self.inner.identity.clone()
    }

    pub(crate) fn scheduled_admission_open(&self) -> bool {
        !self.inner.closed.load(Ordering::Acquire)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn prepare(
        jobs: Jobs,
        revoked: crate::normal_revocation::Revoked,
        store: Store,
        auth: AuthService,
        cache: String,
        config: PgConfig,
        runtime: Runtime,
        root: PathBuf,
        scratch: PathBuf,
        identity: ProducerIdentitySource,
    ) -> Result<Self, Error> {
        if !jobs.allows_defaults() && !jobs.allows_backup() && !jobs.allows_manual_restore() {
            return Err(Error::Configuration);
        }
        Ok(Self {
            inner: Arc::new(Inner {
                jobs,
                revoked,
                schedule: crate::normal_backup_schedule::Schedule::new(
                    store.clone(),
                    cache.clone(),
                ),
                schedule_auth: auth.clone(),
                executor: Executor::prepare(store, auth, cache, config, runtime, root, scratch)
                    .map_err(|_| Error::Configuration)?,
                identity,
                worker: Arc::new(Semaphore::new(1)),
                http: Arc::new(Semaphore::new(4)),
                closed: AtomicBool::new(false),
                restore_claims_closed: AtomicBool::new(false),
                advanced: Arc::new(Notify::new()),
                uncertain: Mutex::new(Vec::with_capacity(1)),
                #[cfg(target_os = "linux")]
                taskless_pending: Mutex::new(None),
            }),
        })
    }
    #[cfg(target_os = "linux")]
    pub fn bind_taskless_restore(
        &self,
        authority: crate::normal_queue::TasklessBuiltins,
        helper: VerifiedGuardedHelper,
    ) -> Result<(), Error> {
        if !self.inner.jobs.allows_defaults() || !authority.belongs_to_backup(self) {
            return Err(Error::Configuration);
        }
        self.inner
            .executor
            .taskless
            .set(SelectedTaskless { authority, helper })
            .map_err(|_| Error::Configuration)
    }

    pub(crate) fn same_instance(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.inner, &other.inner)
    }

    #[cfg(target_os = "linux")]
    pub fn retained_taskless_stage(
        &self,
    ) -> Result<Option<crate::normal_backup_taskless::SettlementStage>, Error> {
        let pending = self
            .inner
            .taskless_pending
            .lock()
            .map_err(|_| Error::Configuration)?;
        Ok(pending.as_ref().map(selected_taskless::Held::stage))
    }

    pub(crate) fn owns_core_restore_reservation(
        &self,
        reservation: &dispatcharr_store::TasklessRestoreReservation,
    ) -> bool {
        self.inner
            .executor
            .owns_core_restore_reservation(reservation)
    }

    pub fn bind_restore_plugins(&self, plugins: PluginActions) -> Result<(), Error> {
        if !self.inner.jobs.allows_normal_restore() {
            return Err(Error::Configuration);
        }
        self.inner
            .executor
            .bind_restore_plugins(plugins)
            .map_err(|_| Error::Configuration)
    }
    pub fn bind_restore_scheduler(
        &self,
        scheduler: &crate::normal_beat::Scheduler,
    ) -> Result<(), Error> {
        if !self.inner.jobs.allows_defaults() || !self.inner.jobs.allows_normal_restore() {
            return Err(Error::Configuration);
        }
        self.inner
            .executor
            .bind_restore_scheduler(scheduler)
            .map_err(|_| Error::Configuration)
    }
    pub async fn initialize_schedule(&self, owner: Arc<dyn Send + Sync>) -> Result<(), StoreError> {
        self.schedule_for_worker()?.initialize(owner).await
    }
    pub async fn initialize_core_schedule(
        &self,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<(), StoreError> {
        self.schedule_for_worker()?
            .initialize_preserving_tick(owner)
            .await
    }
    fn schedule_for_worker(&self) -> Result<&crate::normal_backup_schedule::Schedule, StoreError> {
        if !self.inner.jobs.allows_defaults() {
            return Err(StoreError::Unsupported(
                "Backup schedule consumer is not prepared",
            ));
        }
        Ok(&self.inner.schedule)
    }
    pub fn close_admission(&self) {
        self.inner.closed.store(true, Ordering::Release);
        self.inner.worker.close();
        self.close_http_admission();
    }

    pub(super) fn close_http_admission(&self) {
        self.inner.http.close();
    }

    pub(super) fn close_restore_claim_admission(&self) {
        self.inner
            .restore_claims_closed
            .store(true, Ordering::Release);
    }
    fn owner(
        &self,
        http: bool,
        caller: Arc<dyn Send + Sync>,
    ) -> Result<Arc<dyn Send + Sync>, Disposition> {
        if self.inner.closed.load(Ordering::Acquire) {
            return Err(Disposition::Operational("backup intake closed".into()));
        }
        let slots = if http {
            &self.inner.http
        } else {
            &self.inner.worker
        };
        let permit = slots
            .clone()
            .try_acquire_owned()
            .map_err(|_| Disposition::Capacity)?;
        if self.inner.closed.load(Ordering::Acquire) {
            return Err(Disposition::Operational("backup intake closed".into()));
        }
        Ok(Arc::new(Owner {
            _permit: permit,
            _caller: caller,
            _application: self.clone(),
        }))
    }
    async fn run(
        &self,
        expected: Option<Arc<[u8]>>,
        caller: Arc<dyn Send + Sync>,
    ) -> Result<Disposition, Disposition> {
        let owner = match self.owner(false, caller) {
            Ok(owner) => owner,
            Err(_) if self.manual_intake_closed() => return Ok(Disposition::NotDue),
            Err(error) => return Err(error),
        };
        let inner = self.inner.clone();
        let join_inner = inner.clone();
        let join_owner = owner.clone();
        // The entire claim→cache policy→archive→result remains physically owned if
        // the dispatcher/caller observer is dropped. No execute after ambiguous CAS.
        let joined = tokio::spawn(async move {
            let candidate = match inner
                .jobs
                .peek_normal_backup(expected.as_deref(), owner.clone())
                .await
            {
                Ok(Some(value)) => value,
                Ok(None) => return Ok(Disposition::Empty),
                Err(TransportError::ClaimRace) => return Ok(Disposition::HeadChanged),
                Err(TransportError::UnsupportedTask) => return Ok(Disposition::UnsupportedHead),
                Err(TransportError::Capacity) if !inner.jobs.allows_defaults() => {
                    return Ok(Disposition::Capacity)
                }
                Err(error) => {
                    return Err(Disposition::Operational(format!("backup peek {error:?}")))
                }
            };
            let mut restore_scope=None;
            let mut restore_preflight_result=None;
            let is_restore=matches!(&candidate.message.kind,Kind::Restore(_));
            let restore_claim_closed=||is_restore && inner.restore_claims_closed.load(Ordering::Acquire);
            if restore_claim_closed() {
                return Ok(Disposition::NotDue);
            }
            let pre_revoked=if is_restore { inner.revoked.contains(&candidate.message.id.to_string()).map_err(|_|Disposition::Operational("backup revocation set unavailable".into()))? } else { false };
            if is_restore && !pre_revoked {
                // This existing spawned worker owns admission through the
                // physical close receipt, even when intake closes. Cancelling
                // a partially acquired fence would hide session close failures.
                // Acquisition already has its actual thirty-second deadline;
                // the closed-intake branch below settles every acquired scope.
                let result=inner.executor.prepare_worker_restore(owner.clone()).await;
                match result {
                    Ok(scope)=>restore_scope=Some(scope),
                    Err(dispatcharr_store::BackupRestoreError::Capacity)=>return Ok(Disposition::Capacity),
                    Err(dispatcharr_store::BackupRestoreError::UnsupportedSchema)=>restore_preflight_result=Some(json!({"status":"failed","error":"Native restore requires the current schema migration graph"})),
                    Err(dispatcharr_store::BackupRestoreError::UnsupportedPlugins)=>{
                        #[cfg(target_os = "linux")]
                        if inner.executor.taskless.get().is_some() && candidate.message.parent.is_none() {
                            return selected_taskless::run(&inner, candidate, owner).await;
                        }
                        restore_preflight_result=Some(json!({"status":"failed","error":"Native restore plugin modules require an actual restore-task context"}));
                    },
                    Err(_)=>{retain(&inner,owner.clone());return Err(Disposition::Operational("restore SQL admission unsettled".into()));}
                }
            }
            if inner.closed.load(Ordering::Acquire) || restore_claim_closed() {
                if let Some(scope)=restore_scope.take() {
                    scope.decline_unstarted().await.map_err(|_|Disposition::Operational("restore preclaim drain unsettled".into()))?;
                }
                return if is_restore || !inner.jobs.allows_defaults() {
                    Ok(Disposition::NotDue)
                } else {
                    Err(Disposition::Operational("backup intake stopped before claim".into()))
                };
            }
            let message = match inner
                .jobs
                .claim_normal_backup(candidate, owner.clone())
                .await
            {
                Ok(value) => value.into_message(),
                Err(TransportError::ClaimRace) => {
                    if let Some(scope)=restore_scope.take() { scope.decline_unstarted().await.map_err(|_|Disposition::Operational("restore race drain unsettled".into()))?; }
                    return Ok(Disposition::HeadChanged);
                },
                Err(TransportError::Capacity) => {
                    if let Some(scope)=restore_scope.take() { scope.decline_unstarted().await.map_err(|_|Disposition::Operational("restore capacity drain unsettled".into()))?; }
                    return Err(Disposition::Capacity);
                },
                Err(error) => {
                    if let Some(scope)=restore_scope.take() { scope.retain_uncertain(); }
                    retain(&inner, owner.clone());
                    return Err(Disposition::Operational(format!("backup claim {error:?}")));
                }
            };
            inner.advanced.notify_one();
            let outcome = async {
                if pre_revoked || inner
                    .revoked
                    .contains(&message.id.to_string())
                    .map_err(|_| {
                        Disposition::Operational("backup revocation set unavailable".into())
                    })?
                {
                    if let Some(scope)=restore_scope.take() {
                        scope.decline_unstarted().await.map_err(|_|Disposition::Operational("restore revoked drain unsettled".into()))?;
                    }
                    inner
                        .jobs
                        .write_normal_backup_revoked(&message, owner.clone())
                        .await
                        .map_err(|_| {
                            Disposition::Operational("backup revoked result unsettled".into())
                        })?;
                    return Ok(Disposition::Revoked(message.id));
                }

                let result=match &message.kind {
                    Kind::Restore(filename)=>{
                        if let Some(value)=restore_preflight_result.take() { Ok(value) }
                        else {
                            let scope=restore_scope.as_mut().ok_or_else(||Disposition::Operational("restore actual SQL admission missing".into()))?;
                            scope.execute(filename.clone()).await
                        }
                    },
                    kind=>{
                        let retention=match kind { Kind::Scheduled(value)=>Some(value.clone()), _=>None };
                        inner.executor.execute(retention,owner.clone()).await
                    }
                }.map_err(|_| Disposition::Operational("backup archive/restore owner unsettled".into()))?;
                inner
                    .jobs
                    .write_normal_backup(&message, result, owner.clone())
                    .await
                    .map_err(|error| {
                        Disposition::Operational(format!("backup result {error:?}"))
                    })?;
                Ok(Disposition::Completed(message.id))
            }
            .await;
            if outcome.is_err() {
                if let Some(scope)=restore_scope.take() { scope.retain_uncertain(); }
                retain(&inner, owner.clone());
            }
            outcome
        })
        .await;
        match joined {
            Ok(outcome) => outcome,
            Err(_) => {
                retain(&join_inner, join_owner);
                Err(Disposition::Operational(
                    "backup supervisor; original owner retained".into(),
                ))
            }
        }
    }
    fn manual_intake_closed(&self) -> bool {
        !self.inner.jobs.allows_defaults() && self.inner.closed.load(Ordering::Acquire)
    }

    async fn drain(&self, budget: Duration) -> bool {
        self.close_admission();
        let deadline = tokio::time::Instant::now() + budget;
        while self.inner.worker.available_permits() != 1 || self.inner.http.available_permits() != 4
        {
            if tokio::time::Instant::now() >= deadline {
                return false;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        !self.inner.executor.restore_unsettled()
    }
}
fn retain(inner: &Inner, owner: Arc<dyn Send + Sync>) {
    let mut scopes = inner.uncertain.lock().unwrap_or_else(|e| e.into_inner());
    if !scopes.iter().any(|existing| Arc::ptr_eq(existing, &owner)) {
        scopes.push(owner)
    }
}
impl Handler for Application {
    fn maximum_concurrency(&self) -> usize {
        1
    }
    fn supports_selected_head(&self) -> bool {
        true
    }
    fn head_advance_notifier(&self) -> Option<Arc<Notify>> {
        Some(self.inner.advanced.clone())
    }
    fn run_selected(
        &self,
        expected: Arc<[u8]>,
        owner: Arc<dyn Send + Sync>,
    ) -> BoxFuture<'_, Result<Disposition, Disposition>> {
        Box::pin(self.run(Some(expected), owner))
    }
    fn run_once(
        &self,
        owner: Arc<dyn Send + Sync>,
    ) -> BoxFuture<'_, Result<Disposition, Disposition>> {
        Box::pin(self.run(None, owner))
    }
}
impl Lifecycle for Application {
    fn stop_admission(&self) {
        self.close_admission()
    }
    fn shutdown(&self, budget: Duration) -> BoxFuture<'_, Result<(), Disposition>> {
        Box::pin(async move {
            if self.drain(budget).await {
                Ok(())
            } else {
                Err(Disposition::Operational(
                    "backup accepted owners remain".into(),
                ))
            }
        })
    }
}
fn api_error() -> StoreError {
    StoreError::StatusBody(503, json!({"detail":"Backup transport unavailable"}))
}

fn backup_status_error(error: TransportError) -> StoreError {
    match error {
        TransportError::BackupResultInvalidJsonAtStart => StoreError::StatusBody(
            500,
            json!({"detail":"Failed to get task status: Expecting value: line 1 column 1 (char 0)"}),
        ),
        _ => api_error(),
    }
}
impl BackupJobApi for Application {
    fn create(&self, caller: Arc<dyn Send + Sync>) -> BoxFuture<'_, Result<String, StoreError>> {
        Box::pin(async move {
            let owner = self.owner(true, caller).map_err(|_| api_error())?;
            let identity_source = self.inner.identity.clone();
            let id = Uuid::new_v4();
            let jobs = self.inner.jobs.clone();
            tokio::spawn(async move {
                let identity = identity_source.current()?;
                jobs.publish_normal_backup(id, identity.reply_to, &identity.origin, owner)
                    .await
                    .map_err(|_| api_error())?;
                Ok(id.to_string())
            })
            .await
            .map_err(|_| api_error())?
        })
    }
    fn restore(
        &self,
        filename: String,
        caller: Arc<dyn Send + Sync>,
    ) -> BoxFuture<'_, Result<String, StoreError>> {
        Box::pin(async move {
            if !self.inner.jobs.allows_normal_restore() || !self.inner.executor.restore_ready() {
                return Err(api_error());
            }
            let owner = self.owner(true, caller).map_err(|_| api_error())?;
            let identity_source = self.inner.identity.clone();
            let jobs = self.inner.jobs.clone();
            let id = Uuid::new_v4();
            tokio::spawn(async move {
                let identity = identity_source.current()?;
                let (args_repr, kwargs_repr) =
                    identity_source.task_representations(&json!([filename]), &json!({}))?;
                jobs.publish_normal_restore(
                    filename,
                    args_repr,
                    kwargs_repr,
                    id,
                    identity.reply_to,
                    &identity.origin,
                    owner,
                )
                .await
                .map_err(|_| api_error())?;
                Ok(id.to_string())
            })
            .await
            .map_err(|_| api_error())?
        })
    }
    fn status<'a>(
        &'a self,
        id: &'a str,
        caller: Arc<dyn Send + Sync>,
    ) -> BoxFuture<'a, Result<Value, StoreError>> {
        Box::pin(async move {
            let owner = self.owner(true, caller).map_err(|_| api_error())?;
            let jobs = self.inner.jobs.clone();
            let id = id.to_string();
            tokio::spawn(async move {
                let observed = jobs
                    .read_normal_backup_status(&id, owner)
                    .await
                    .map_err(backup_status_error)?;
                if let Some(meta) = &observed {
                    if let Some(exception) = &meta.exception {
                        let first = exception.arguments.first().and_then(Value::as_str);
                        // Preserve the existing revoked projection; only the
                        // Source-compatible plain one-string ValueError is added.
                        let message = if meta.state == crate::decode::State::Revoked {
                            first
                        } else {
                            failure_status::single_failure_message(
                                meta.state == crate::decode::State::Failure,
                                meta.result.get("exc_module").and_then(Value::as_str),
                                meta.result.get("exc_type").and_then(Value::as_str),
                                !meta.tagged_values.is_empty(),
                                exception.arguments.len(),
                                first,
                            )
                        };
                        if let Some(message) = message {
                            return Err(StoreError::StatusBody(
                                500,
                                json!({"detail":format!("Failed to get task status: {message}")}),
                            ));
                        }
                    }
                }
                crate::backup_application::project_status(observed)
            })
            .await
            .map_err(|_| api_error())?
        })
    }
    fn update_schedule(
        &self,
        data: Value,
        caller: Arc<dyn Send + Sync>,
    ) -> BoxFuture<'_, Result<Value, StoreError>> {
        Box::pin(async move {
            if !self.inner.jobs.allows_defaults() {
                return Err(StoreError::Unsupported(
                    "Backup schedule consumer is not prepared",
                ));
            }
            let owner = self.owner(true, caller).map_err(|_| api_error())?;
            let schedule = self.inner.schedule.clone();
            let auth = self.inner.schedule_auth.clone();
            tokio::spawn(async move { schedule.update(data, &auth, owner).await })
                .await
                .map_err(|_| api_error())?
        })
    }
    fn shutdown(&self, budget: Duration) -> BoxFuture<'_, bool> {
        Box::pin(self.drain(budget))
    }
}

#[cfg(test)]
mod backup_status_error_tests {
    use super::*;

    #[test]
    fn backup_status_maps_bad_stored_json_to_source_and_keeps_transport_503() {
        assert!(matches!(
            backup_status_error(TransportError::BackupResultInvalidJsonAtStart),
            StoreError::StatusBody(500, body)
                if body == json!({
                    "detail":"Failed to get task status: Expecting value: line 1 column 1 (char 0)"
                })
        ));
        for error in [
            TransportError::Backend,
            TransportError::Closed,
            TransportError::Corrupt,
            TransportError::Decode(crate::decode::DecodeError::Identity),
            TransportError::Decode(crate::decode::DecodeError::Malformed),
        ] {
            assert!(matches!(
                backup_status_error(error),
                StoreError::StatusBody(503, body)
                    if body == json!({"detail":"Backup transport unavailable"})
            ));
        }
    }
}
