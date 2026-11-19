//! Real normal SD guide handlers and durable Source countdown90 retries.
//! Original Dispatcharr/Celery/Kombu authors retain all application credit.
use crate::{
    milestone_dispatch::{Disposition as D, Family, Handler, Lifecycle},
    normal_revocation::Revoked,
    sd_guide_eta::Reservation,
    sd_guide_protocol::{self as protocol, Kind, Message},
    transport::{Error as WireError, Jobs},
};
use dispatcharr_contracts::{BoxFuture, UpdateEventSink};
use dispatcharr_coordination::{TaskLockDetailedError, TaskLockError, TaskLocks};
use dispatcharr_store::{
    sd_guide::{Admission, Request},
    AuthService, ProducerIdentitySource,
};
use serde_json::{json, Value};
use std::{
    collections::VecDeque,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
use tokio::sync::Notify;
#[path = "sd_hook_trail.rs"]
mod hook_trail;
#[path = "normal_sd_tvg.rs"]
mod tvg;
type Owner = Arc<dyn Send + Sync>;
struct Inner {
    entry: Mutex<Option<std::sync::Weak<dyn Handler>>>,
    jobs: Jobs,
    hook_runtime: std::sync::OnceLock<dispatcharr_store::PluginRuntime>,
    auth: AuthService,
    identity: ProducerIdentitySource,
    events: Arc<dyn UpdateEventSink>,
    locks: TaskLocks,
    revoked: Revoked,
    closed: AtomicBool,
    active: AtomicUsize,
    changed: Notify,
    heads: Arc<Notify>,
    uncertain: Mutex<Vec<Owner>>,
    pending: Mutex<VecDeque<Reservation>>,
    visibility_cursor: AtomicUsize,
    next_visibility: Mutex<Option<tokio::time::Instant>>,
}
#[derive(Clone)]
pub struct Prepared {
    inner: Arc<Inner>,
}
/// Opaque live prepared-consumer authority; never a caller boolean/ID list.
#[derive(Clone)]
pub struct Capability {
    inner: std::sync::Weak<Inner>,
}
impl Capability {
    pub(crate) fn owns(&self, jobs: &Jobs) -> bool {
        self.inner.upgrade().is_some_and(|inner| {
            !inner.closed.load(Ordering::Acquire) && inner.jobs.same_sd_guide_scope(jobs)
        })
    }
}
struct Active(Arc<Inner>);
impl Drop for Active {
    fn drop(&mut self) {
        self.0.active.fetch_sub(1, Ordering::AcqRel);
        self.0.changed.notify_waiters();
    }
}
struct Guard {
    inner: Arc<Inner>,
    owner: Owner,
    armed: bool,
}
impl Drop for Guard {
    fn drop(&mut self) {
        if self.armed {
            self.inner
                .uncertain
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .push(self.owner.clone())
        }
    }
}
fn fault(stage: &str) -> D {
    D::Operational(format!("SD guide {stage} unsettled"))
}
impl Prepared {
    pub fn new(
        jobs: Jobs,
        auth: AuthService,
        events: Arc<dyn UpdateEventSink>,
        locks: TaskLocks,
        revoked: Revoked,
    ) -> Result<Self, D> {
        if !jobs.allows_defaults() {
            return Err(D::UnsupportedDomain);
        }
        let identity = auth.producer_identity_source();
        Ok(Self {
            inner: Arc::new(Inner {
                entry: Mutex::new(None),
                jobs,
                hook_runtime: std::sync::OnceLock::new(),
                auth,
                identity,
                events,
                locks,
                revoked,
                closed: AtomicBool::new(false),
                active: AtomicUsize::new(0),
                changed: Notify::new(),
                heads: Arc::new(Notify::new()),
                uncertain: Mutex::new(Vec::new()),
                pending: Mutex::new(VecDeque::new()),
                visibility_cursor: AtomicUsize::new(0),
                next_visibility: Mutex::new(None),
            }),
        })
    }
    /// Narrow future natural HTTP producer port. Only this already-prepared
    /// SD consumer can publish station roots; unknown broker replies remain
    /// ambiguous and callers must not retry/create a second root silently.
    pub async fn publish_stations(
        &self,
        source: i64,
        countdown: bool,
        owner: Owner,
    ) -> Result<uuid::Uuid, WireError> {
        use base64::{engine::general_purpose::STANDARD, Engine};
        if self.inner.closed.load(Ordering::Acquire) || !(1..=i64::from(i32::MAX)).contains(&source)
        {
            return Err(WireError::UnsupportedTask);
        }
        let identity = self
            .inner
            .identity
            .current()
            .map_err(|_| WireError::Protocol)?;
        let id = uuid::Uuid::new_v4();
        let raw = crate::protocol::encode(
            &crate::protocol::Task::CreateBackup,
            id,
            identity.reply_to,
            uuid::Uuid::new_v4(),
            &identity.origin,
        )
        .map_err(|_| WireError::Protocol)?;
        let mut frame: Value = serde_json::from_slice(&raw).map_err(|_| WireError::Protocol)?;
        frame["headers"]["task"] = json!("apps.epg.tasks.fetch_schedules_direct_stations");
        frame["headers"]["timelimit"] = json!([null, null]);
        frame["headers"]["argsrepr"] = json!(format!("({source},)"));
        frame["headers"]["kwargsrepr"] = json!("{}");
        if countdown {
            frame["headers"]["eta"] = json!((chrono::Utc::now() + chrono::Duration::seconds(3))
                .to_rfc3339_opts(chrono::SecondsFormat::Micros, false));
        }
        frame["body"] = json!(STANDARD.encode(
            serde_json::to_vec(
                &json!([[source],{},{"callbacks":null,"errbacks":null,"chain":null,"chord":null}])
            )
            .map_err(|_| WireError::Protocol)?
        ));
        let raw = serde_json::to_vec(&frame).map_err(|_| WireError::Protocol)?;
        let receipt = self.inner.jobs.publish_sd_guide(&raw, owner).await?;
        if receipt.task_id != id {
            return Err(WireError::Corrupt);
        }
        Ok(id)
    }
    pub fn bind_hook_runtime(&self, runtime: dispatcharr_store::PluginRuntime) -> Result<(), D> {
        self.inner
            .hook_runtime
            .set(runtime)
            .map_err(|_| D::UnsupportedDomain)
    }
    pub fn wrap_tvg(&self, xml: Arc<dyn Handler>) -> Arc<dyn Handler> {
        Arc::new(tvg::Bridge::new(self.clone(), xml))
    }
    pub fn capability(&self, queue: &crate::normal_queue::Application) -> Result<Capability, D> {
        let (_, entry) = self.entry();
        if !queue.contains_actual_handler(Family::SdGuide, &entry) {
            return Err(D::UnsupportedDomain);
        }
        Ok(Capability {
            inner: Arc::downgrade(&self.inner),
        })
    }
    pub fn entry(&self) -> (Family, Arc<dyn Handler>) {
        let mut entry = self.inner.entry.lock().unwrap_or_else(|e| e.into_inner());
        let handler = match entry.as_ref().and_then(std::sync::Weak::upgrade) {
            Some(handler) => handler,
            None => {
                let handler: Arc<dyn Handler> = Arc::new(self.clone());
                *entry = Some(Arc::downgrade(&handler));
                handler
            }
        };
        (Family::SdGuide, handler)
    }
    async fn reserve(&self, raw: &[u8], owner: Owner) -> Result<D, D> {
        let candidate = self
            .inner
            .jobs
            .peek_sd_guide_candidate(Some(raw), owner.clone(), Default::default())
            .await
            .map_err(wire)?
            .ok_or(D::HeadChanged)?;
        let reservation = self
            .inner
            .jobs
            .reserve_sd_guide_candidate_settled(candidate, owner.clone(), Default::default())
            .await
            .map_err(|error| {
                if !matches!(error, WireError::ClaimRace | WireError::Capacity) {
                    self.inner
                        .uncertain
                        .lock()
                        .unwrap_or_else(|e| e.into_inner())
                        .push(owner.clone())
                }
                wire(error)
            })?;
        let id = reservation.message().common.id;
        self.inner
            .pending
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push_back(reservation.clone());
        self.inner.heads.notify_one();
        if self
            .inner
            .revoked
            .contains(&id.to_string())
            .map_err(|_| fault("reservation revoked read"))?
        {
            let result = async {
                // Celery checks revoked immediately on intake, before ETA due;
                // actual backend settlement precedes removal of unacked state.
                self.inner
                    .jobs
                    .write_xc_live_revoked(&reservation.message().common, owner.clone())
                    .await
                    .map_err(|_| fault("reservation revoked backend"))?;
                self.inner
                    .jobs
                    .ack_sd_guide_eta(&reservation, owner.clone())
                    .await
                    .map_err(|_| fault("reservation revoked ACK"))?;
                self.inner
                    .pending
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .retain(|r| r.delivery_tag() != reservation.delivery_tag());
                Ok(D::Revoked(id))
            }
            .await;
            if result.is_err() {
                self.inner
                    .uncertain
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .push(owner);
            }
            return result;
        }
        Ok(D::Reserved(id))
    }
    async fn execute(&self, expected: Option<Arc<[u8]>>, due: bool, caller: Owner) -> Result<D, D> {
        if self.inner.closed.load(Ordering::Acquire) {
            return Ok(D::NotDue);
        }
        let inner = self.inner.clone();
        inner.active.fetch_add(1, Ordering::AcqRel);
        tokio::spawn(async move {
            let _active = Active(inner.clone());
            let owner: Owner = Arc::new((caller, inner.clone()));
            let mut guard = Guard {
                inner: inner.clone(),
                owner: owner.clone(),
                armed: false,
            };
            let result = async {
                let reservation = if due {
                    let expected = expected.as_deref().ok_or(D::NotDue)?;
                    inner
                        .pending
                        .lock()
                        .unwrap_or_else(|e| e.into_inner())
                        .iter()
                        .find(|r| {
                            r.receipt.queued_bytes.as_slice() == expected
                                && r.message().eta.is_some_and(|v| v <= chrono::Utc::now())
                        })
                        .cloned()
                } else {
                    None
                };
                let candidate = if due {
                    None
                } else {
                    match inner
                        .jobs
                        .peek_sd_guide(expected.as_deref(), owner.clone())
                        .await
                    {
                        Ok(Some(v)) => Some(v),
                        Ok(None) => return Ok(D::Empty),
                        Err(error) => return Err(wire(error)),
                    }
                };
                let message = if due {
                    reservation.as_ref().ok_or(D::NotDue)?.message().clone()
                } else {
                    candidate.as_ref().ok_or(D::HeadChanged)?.message.clone()
                };
                if !due && message.eta.is_some() {
                    return Prepared {
                        inner: inner.clone(),
                    }
                    .reserve(
                        candidate.as_ref().ok_or(D::HeadChanged)?.queued_bytes(),
                        owner.clone(),
                    )
                    .await;
                }
                let id = message.common.id;
                let revoked = inner
                    .revoked
                    .contains(&id.to_string())
                    .map_err(|_| fault("revoked read"))?;
                if due && revoked {
                    guard.armed = true;
                    inner
                        .jobs
                        .write_xc_live_revoked(&message.common, owner.clone())
                        .await
                        .map_err(|_| fault("revoked backend"))?;
                    let reservation = reservation.as_ref().ok_or(D::NotDue)?;
                    inner
                        .jobs
                        .ack_sd_guide_eta(reservation, owner.clone())
                        .await
                        .map_err(|_| fault("revoked ACK"))?;
                    inner
                        .pending
                        .lock()
                        .unwrap_or_else(|e| e.into_inner())
                        .retain(|r| r.delivery_tag() != reservation.delivery_tag());
                    return Ok(D::Revoked(id));
                }
                let request = match message.kind {
                    Kind::Stations => Request::Stations {
                        source: message.target,
                    },
                    Kind::Mapped => Request::Mapped {
                        source: message.target,
                        force: message.force,
                    },
                    Kind::Single => Request::Single {
                        epg: message.target,
                        force: message.force,
                    },
                };
                let actual_raw = if due {
                    reservation
                        .as_ref()
                        .ok_or(D::NotDue)?
                        .receipt
                        .queued_bytes
                        .clone()
                } else {
                    candidate
                        .as_ref()
                        .ok_or(D::HeadChanged)?
                        .queued_bytes()
                        .to_vec()
                };
                let admission = if let Some(runtime) = inner.hook_runtime.get() {
                    Admission::prepare_with_hook_runtime(
                        inner.auth.clone().with_retained_scope(owner.clone()),
                        request,
                        inner.events.clone(),
                        owner.clone(),
                        runtime.clone(),
                    )
                    .await
                } else {
                    Admission::prepare(
                        inner.auth.clone().with_retained_scope(owner.clone()),
                        request,
                        inner.events.clone(),
                        owner.clone(),
                    )
                    .await
                }
                .map_err(|error| match error {
                    dispatcharr_store::sd_guide::Error::Store(
                        dispatcharr_store::StoreError::StatusBody(503, _),
                    ) => D::Capacity,
                    _ => D::UnsupportedDomain,
                })?;
                let mut admission = admission;
                if inner.closed.load(Ordering::Acquire) {
                    admission
                        .close()
                        .await
                        .map_err(|_| fault("preclaim SQL close"))?;
                    return Ok(D::NotDue);
                }
                guard.armed = true;
                if due {
                    let reservation = reservation.as_ref().ok_or(D::NotDue)?;
                    inner
                        .jobs
                        .ack_sd_guide_eta(reservation, owner.clone())
                        .await
                        .map_err(|_| fault("execution ACK"))?;
                    inner
                        .pending
                        .lock()
                        .unwrap_or_else(|e| e.into_inner())
                        .retain(|r| r.delivery_tag() != reservation.delivery_tag());
                } else {
                    match inner
                        .jobs
                        .claim_sd_guide(candidate.ok_or(D::HeadChanged)?, owner.clone())
                        .await
                    {
                        Ok(_) => {}
                        Err(error @ (WireError::ClaimRace | WireError::Capacity)) => {
                            guard.armed = false;
                            admission
                                .close()
                                .await
                                .map_err(|_| fault("unclaimed SQL close"))?;
                            return Err(wire(error));
                        }
                        Err(_) => return Err(fault("claim")),
                    }
                }
                inner.heads.notify_one();
                if inner
                    .revoked
                    .contains(&id.to_string())
                    .map_err(|_| fault("revoked execution read"))?
                {
                    admission
                        .close()
                        .await
                        .map_err(|_| fault("revoked SQL close"))?;
                    inner
                        .jobs
                        .write_xc_live_revoked(&message.common, owner.clone())
                        .await
                        .map_err(|_| fault("revoked result"))?;
                    return Ok(D::Revoked(id));
                }
                let trail = Arc::new(hook_trail::Trail::new(id));
                if let Err(_error) =
                    admission.bind_native_hooks(&actual_raw, trail.clone(), owner.clone())
                {
                    trail.fault();
                    admission
                        .close()
                        .await
                        .map_err(|_| fault("hook setup SQL close"))?;
                    return Err(fault("hook context setup"));
                }
                let body = execute_body(&inner, &message, &mut admission, owner.clone()).await;
                let (value, children) = match body {
                    Ok(body) => body,
                    Err(error) => {
                        trail.fault();
                        admission
                            .close()
                            .await
                            .map_err(|_| fault("failed hook/SQL close"))?;
                        return Err(error);
                    }
                };
                trail
                    .close(&children)
                    .map_err(|_| fault("native publication trail close"))?;
                let hooks = admission.finish_hooks().await;
                admission.close().await.map_err(|_| fault("SQL close"))?;
                let hooks = hooks.map_err(|_| fault("hook context finish"))?;
                if let Some(hooks) = hooks {
                    let ack = inner
                        .jobs
                        .write_sd_hook_success(
                            &message.common,
                            value,
                            hooks.observed_request_children,
                            owner,
                        )
                        .await
                        .map_err(|_| fault("hook backend"))?;
                    return Ok(if ack {
                        D::Completed(id)
                    } else {
                        D::CompletedWithoutId
                    });
                }
                let ack = inner
                    .jobs
                    .write_xc_live(&message.common, value, children, owner)
                    .await
                    .map_err(|_| fault("backend"))?;
                Ok(if ack {
                    D::Completed(id)
                } else {
                    D::CompletedWithoutId
                })
            }
            .await;
            if result.is_ok() {
                guard.armed = false
            }
            result
        })
        .await
        .map_err(|_| fault("supervisor"))?
    }
}
async fn deferred(
    inner: &Arc<Inner>,
    message: &Message,
    owner: Owner,
    label: &str,
) -> Result<(Value, Vec<uuid::Uuid>), D> {
    let identity = inner.identity.current().map_err(|_| D::UnsupportedDomain)?;
    let id = uuid::Uuid::new_v4();
    let raw = protocol::retry_frame(
        message,
        id,
        identity.reply_to,
        &identity.origin,
        chrono::Utc::now(),
    )
    .map_err(|_| D::UnsupportedDomain)?;
    loop {
        if inner.closed.load(Ordering::Acquire) {
            return Err(fault("retry publication stopped"));
        }
        match inner.jobs.publish_sd_guide(&raw, owner.clone()).await {
            Ok(v) if v.task_id == id => break,
            Err(WireError::Capacity) => tokio::time::sleep(Duration::from_millis(100)).await,
            _ => return Err(fault("retry publication")),
        }
    }
    Ok((json!(label), vec![id]))
}
async fn execute_body(
    inner: &Arc<Inner>,
    message: &Message,
    admission: &mut Admission,
    owner: Owner,
) -> Result<(Value, Vec<uuid::Uuid>), D> {
    if let Some(value) = admission.early_result() {
        return Ok((value, Vec::new()));
    }
    let source = admission.source_id().ok_or(D::UnsupportedDomain)?;
    if message.kind == Kind::Stations {
        let stopped = || inner.closed.load(Ordering::Acquire);
        return Ok((
            admission
                .execute(&stopped)
                .await
                .map_err(|_| fault("station discovery effects"))?,
            Vec::new(),
        ));
    }
    if message.kind == Kind::Single
        && inner
            .locks
            .sd_mapped_guide_is_held(source, owner.clone())
            .await
            .map_err(|_| fault("batch lock observation"))?
        && message.defer < 2
    {
        return deferred(inner, message, owner, "Deferred - mapped batch in progress").await;
    }
    let (key, target) = match message.kind {
        Kind::Stations => unreachable!("station discovery has no Source task lock"),
        Kind::Mapped => ("sd_mapped_guide_fetch", source),
        Kind::Single => ("parse_epg_programs", message.target),
    };
    let lease = match inner
        .locks
        .acquire_retained_detailed(key, target, owner.clone())
        .await
    {
        Ok(lease) => lease,
        Err(TaskLockDetailedError::Operational(TaskLockError::Held)) => {
            if message.kind == Kind::Mapped && message.defer < 2 {
                return deferred(
                    inner,
                    message,
                    owner,
                    "Deferred - batch already in progress",
                )
                .await;
            }
            return Ok((json!("Task already running"), Vec::new()));
        }
        Err(_) => return Err(fault("task lock")),
    };
    let stopped = || inner.closed.load(Ordering::Acquire);
    let result = admission.execute(&stopped).await;
    if !lease.release_owned_detailed().await.is_ok_and(|v| v) {
        return Err(fault("task token release"));
    }
    Ok((result.map_err(|_| fault("guide effects"))?, Vec::new()))
}
fn wire(error: WireError) -> D {
    match error {
        WireError::Capacity => D::Capacity,
        WireError::ClaimRace => D::HeadChanged,
        WireError::UnsupportedTask => D::UnsupportedHead,
        _ => fault("broker"),
    }
}
pub(crate) fn descriptor() -> (usize, bool) {
    (crate::normal_queue::CELERY_CONCURRENCY, true)
}
impl Handler for Prepared {
    fn maximum_concurrency(&self) -> usize {
        descriptor().0
    }
    fn supports_selected_head(&self) -> bool {
        descriptor().1
    }
    fn head_advance_notifier(&self) -> Option<Arc<Notify>> {
        Some(self.inner.heads.clone())
    }
    fn run_selected(&self, raw: Arc<[u8]>, owner: Owner) -> BoxFuture<'_, Result<D, D>> {
        Box::pin(self.execute(Some(raw), false, owner))
    }
    fn run_once(&self, owner: Owner) -> BoxFuture<'_, Result<D, D>> {
        Box::pin(self.execute(None, false, owner))
    }
    fn due_head(&self) -> Option<Arc<[u8]>> {
        if self.inner.closed.load(Ordering::Acquire) {
            return None;
        }
        self.inner
            .pending
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .iter()
            .filter(|r| r.message().eta.is_some_and(|e| e <= chrono::Utc::now()))
            .min_by_key(|r| r.message().eta)
            .map(|r| Arc::from(r.receipt.queued_bytes.clone()))
    }
    fn run_due(&self, raw: Arc<[u8]>, owner: Owner) -> BoxFuture<'_, Result<D, D>> {
        Box::pin(self.execute(Some(raw), true, owner))
    }
    fn maintenance(&self, owner: Owner) -> BoxFuture<'_, Result<(), D>> {
        Box::pin(async move {
            let now = tokio::time::Instant::now();
            {
                let mut next = self
                    .inner
                    .next_visibility
                    .lock()
                    .unwrap_or_else(|e| e.into_inner());
                if next.is_some_and(|n| n > now) {
                    return Ok(());
                }
                *next = Some(now + Duration::from_secs(5));
            }
            let old = self.inner.visibility_cursor.load(Ordering::Acquire);
            let next = self
                .inner
                .jobs
                .recover_sd_guide_visible(owner, old)
                .await
                .map_err(wire)?;
            self.inner.visibility_cursor.store(next, Ordering::Release);
            Ok(())
        })
    }
}
impl Lifecycle for Prepared {
    fn stop_admission(&self) {
        self.inner.closed.store(true, Ordering::Release)
    }
    fn shutdown(&self, budget: Duration) -> BoxFuture<'_, Result<(), D>> {
        Box::pin(async move {
            self.stop_admission();
            let until = tokio::time::Instant::now() + budget;
            loop {
                let notified = self.inner.changed.notified();
                if self.inner.active.load(Ordering::Acquire) == 0 {
                    break;
                }
                if tokio::time::timeout_at(until, notified).await.is_err() {
                    return Err(fault("active drain"));
                }
            }
            loop {
                let receipt = self
                    .inner
                    .pending
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .front()
                    .cloned();
                let Some(receipt) = receipt else { break };
                self.inner
                    .jobs
                    .restore_sd_guide_eta(&receipt, self.inner.clone())
                    .await
                    .map_err(wire)?;
                self.inner
                    .pending
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .pop_front();
            }
            if self
                .inner
                .uncertain
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .is_empty()
            {
                Ok(())
            } else {
                Err(fault("accepted owner drain"))
            }
        })
    }
}

/// Construct the API from the actual prepared consumer, preserving its Auth /
/// pool, shared Jobs, configured producer identity and drain admission.
impl Prepared {
    pub fn source_http(
        &self,
        refresh: crate::refresh_application::RefreshApplication,
    ) -> dispatcharr_store::sd_source_http::SdSourceHttp {
        dispatcharr_store::sd_source_http::SdSourceHttp::prepare(
            Arc::new(self.clone()),
            Arc::new(refresh),
        )
    }
}
impl dispatcharr_store::sd_source_http::Publisher for Prepared {
    fn auth_service(&self) -> AuthService {
        self.inner.auth.clone()
    }
    fn admission(&self) -> Result<(), dispatcharr_store::StoreError> {
        if self.inner.closed.load(Ordering::Acquire) || !self.inner.jobs.allows_defaults() {
            return Err(dispatcharr_store::StoreError::Unsupported(
                "Prepared station consumer intake unavailable",
            ));
        }
        Ok(())
    }
    fn publish(
        &self,
        source: i64,
        trigger: dispatcharr_store::sd_source_http::Trigger,
        owner: Owner,
    ) -> BoxFuture<'_, Result<uuid::Uuid, dispatcharr_store::StoreError>> {
        Box::pin(async move {
            let countdown = matches!(trigger, dispatcharr_store::sd_source_http::Trigger::Created);
            self.publish_stations(source, countdown, owner)
                .await
                .map_err(|_| dispatcharr_store::StoreError::UnhandledRestFault)
        })
    }
}
