use crate::events::{Producer, Transition};
use crate::recovery_generation::{
    self as recovery,
    local::{
        Body as ScopedBody, Identity as ScopeIdentity, Initial as ScopeInitial,
        Limits as ScopeLimits, LocalScope, StopHook,
    },
};
use crate::resolver::{VodCandidate, VodRequest, VodSource};
use dispatcharr_coordination::{AcquireRequest, Coordinator, Lease, ProviderReservation};
use redis::{
    aio::{ConnectionManager, ConnectionManagerConfig},
    AsyncCommands, Script,
};
use serde_json::json;
use std::{
    collections::{BTreeMap, HashMap},
    sync::{
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
        Arc, Weak,
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::sync::{oneshot, watch, Mutex, OwnedSemaphorePermit, Semaphore};

fn millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}
fn now() -> String {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs_f64()
        .to_string()
}
fn unavailable() -> dispatcharr_store::StoreError {
    dispatcharr_store::StoreError::StatusBody(503, json!("No available stream"))
}
fn provider_full() -> dispatcharr_store::StoreError {
    dispatcharr_store::StoreError::StatusBody(429, json!("Connection limit exceeded for profile"))
}
fn invalid_worker_identity() -> dispatcharr_store::StoreError {
    dispatcharr_store::StoreError::StatusBody(
        500,
        json!("Streaming error: invalid worker response header"),
    )
}

fn redis_error() -> dispatcharr_store::StoreError {
    dispatcharr_store::StoreError::StatusBody(503, json!({"error":"Redis unavailable"}))
}

const UPDATE: &str = r#"
if redis.call('HGET',KEYS[1],'rust_owner_token')~=ARGV[1] then return 0 end
for i=2,#ARGV,2 do redis.call('HSET',KEYS[1],ARGV[i],ARGV[i+1]) end
redis.call('EXPIRE',KEYS[1],3600)
return 1
"#;
const ENTER: &str = r#"
if #KEYS>1 then
 if redis.call('GET',KEYS[2])~=ARGV[3] or redis.call('HGET',KEYS[3],'token')~=ARGV[3] then return -2 end
end
if redis.call('HGET',KEYS[1],'rust_owner_token')~=ARGV[1] then return 0 end
if #KEYS>1 then
 local active=tonumber(redis.call('HGET',KEYS[1],'active_streams')or'0')
 local requests=tonumber(redis.call('HGET',KEYS[1],'request_count')or'0')
 if not active or active<0 or active~=math.floor(active) or active>=9007199254740991
  or not requests or requests<0 or requests~=math.floor(requests) or requests>=9007199254740991 then return -4 end
end
local count=redis.call('HINCRBY',KEYS[1],'active_streams',1)
redis.call('HINCRBY',KEYS[1],'request_count',1)
redis.call('HSET',KEYS[1],'last_activity',ARGV[2])
return count
"#;
const LEAVE: &str = r#"
if redis.call('HGET',KEYS[1],'rust_owner_token')~=ARGV[1] then return -1 end
local count=tonumber(redis.call('HGET',KEYS[1],'active_streams') or '0')
if count>0 then count=count-1 end
redis.call('HSET',KEYS[1],'active_streams',count,'last_activity',ARGV[2])
if count==0 then redis.call('HDEL',KEYS[1],'rust_signed_idle_alias','rust_signed_idle_alias_previous_activity') end
return count
"#;
const DELETE: &str = r#"
-- -1: foreign/missing metadata; release only the immutable original ledger.
--  0: this public generation is active; never stop or release it.
if redis.call('HGET',KEYS[1],'rust_owner_token')~=ARGV[1] then return -1 end
if tonumber(redis.call('HGET',KEYS[1],'active_streams') or '0')~=0 then return 0 end
redis.call('DEL',KEYS[1])
return 1
"#;

struct State {
    active: usize,
    reserved: bool,
    closed: bool,
    epoch: u64,
    departing_pull: Option<oneshot::Sender<()>>,
}
pub(crate) struct Session {
    pub id: String,
    pub source: VodSource,
    pub candidate: VodCandidate,
    pub stop: watch::Sender<bool>,
    pub bytes: AtomicU64,
    pub content_length: AtomicU64,
    pub(super) final_provider_url: crate::provider_final_url::FinalProviderUrl,
    last_request: AtomicU64,
    client_ip: String,
    user_agent: String,
    timeshift: crate::offset::Timeshift<String>,
    lease: Arc<Lease>,
    scope: std::sync::OnceLock<LocalScope>,
    metadata_token: String,
    pub(super) authenticated_restart: bool,
    generation_epoch: u64,
    signed_alias_owner: bool,
    state: Mutex<State>,
    _slot: Arc<OwnedSemaphorePermit>,
}
impl Session {
    fn uses_scoped_body_lifecycle(&self) -> bool {
        self.scope
            .get()
            .is_some_and(|scope| !self.authenticated_restart || scope.activated())
    }
    pub(crate) fn provider_url(&self) -> std::borrow::Cow<'_, str> {
        crate::offset::provider_url(&self.candidate.url, self.timeshift.borrowed())
    }
}
#[derive(Clone)]
struct RecoveryMode {
    recovery: recovery::Recovery,
    extended: bool,
}
struct Inner {
    connection: ConnectionManager,
    coordinator: Coordinator,
    sessions: Mutex<HashMap<String, Arc<Session>>>,
    slots: Arc<Semaphore>,
    requests: Arc<Semaphore>,
    admin_controls: Arc<Semaphore>,
    idle_grace: Duration,
    stopped: watch::Sender<bool>,
    max_requests: usize,
    max_sessions: usize,
    recovery: std::sync::Mutex<Option<RecoveryMode>>,
    pending_cleanups: AtomicUsize,
    cleanup_failed: AtomicBool,
    events: std::sync::Mutex<Option<Producer>>,
}
#[derive(Clone)]
pub(crate) struct SessionPool {
    inner: Arc<Inner>,
}
pub(crate) struct Progress {
    session: Weak<Session>,
    pool: Weak<Inner>,
    acknowledged: u64,
    reported: u64,
    checked_boundary: u64,
    public_stopped: bool,
    body: Option<ScopedBody>,
}
impl Progress {
    /// True ends this HTTP body only. Hard service/foreign watches are separate.
    pub async fn acknowledge(&mut self, bytes: usize) -> bool {
        if self.public_stopped {
            return true;
        }
        self.acknowledged = self.acknowledged.saturating_add(bytes as u64);
        let milestone = (self.acknowledged / 819200) * 819200;
        if milestone > self.reported && self.public_stop_at(milestone / 8192).await {
            return true;
        }
        self.record(milestone).await;
        false
    }
    pub async fn finish(&mut self) {
        let yields = self.acknowledged.div_ceil(8192);
        if self.acknowledged > 0
            && yields.is_multiple_of(100)
            && !self.public_stopped
            && !self.public_stop_at(yields).await
        {
            self.record(self.acknowledged).await;
        }
    }
    async fn public_stop_at(&mut self, yields: u64) -> bool {
        if yields == 0 || !yields.is_multiple_of(100) || yields <= self.checked_boundary {
            return self.public_stopped;
        }
        self.checked_boundary = yields;
        if let (Some(session), Some(body)) = (self.session.upgrade(), self.body.as_ref()) {
            if let Some(scope) = session
                .scope
                .get()
                .filter(|_| !session.authenticated_restart)
            {
                // Unknown physical consume replies are never retried. Keep the
                // operational diagnostic and end only this observer's body.
                self.public_stopped = scope.consume_public_stop(body).await.unwrap_or(true);
            }
        }
        self.public_stopped
    }
    async fn record(&mut self, milestone: u64) {
        if milestone <= self.reported {
            return;
        }
        self.reported = milestone;
        let (Some(session), Some(pool)) = (self.session.upgrade(), self.pool.upgrade()) else {
            return;
        };
        session.bytes.store(milestone, Ordering::Relaxed);
        if let (Some(scope), Some(body)) = (
            session.scope.get().filter(|scope| scope.activated()),
            self.body.as_ref(),
        ) {
            let _ = scope.progress(body, milestone).await;
            return;
        }
        let _ = update(
            &pool,
            &session,
            &[
                ("bytes_sent", milestone.to_string()),
                ("last_activity", now()),
            ],
        )
        .await;
    }
}
struct RetainedEntry {
    previous: Arc<Session>,
    scope: LocalScope,
    resolver: Option<Arc<dyn crate::resolver::VodResolver>>,
}

pub(crate) struct Ticket {
    pub session: Arc<Session>,
    pool: Weak<Inner>,
    _request: Option<Arc<OwnedSemaphorePermit>>,
    is_new: bool,
    body_started: AtomicBool,
    scope_body: Option<ScopedBody>,
    cleanup_needed: bool,
}

#[derive(Default)]
pub(crate) struct EntryAuthorization {
    pub(super) restart_user: Option<i64>,
    pub(super) signed_alias: Option<String>,
}

impl SessionPool {
    pub async fn connect(
        url: &str,
        coordinator: Coordinator,
        max_sessions: usize,
        max_requests: usize,
        idle_grace: Duration,
    ) -> Result<Self, dispatcharr_store::StoreError> {
        let connection = tokio::time::timeout(
            Duration::from_secs(3),
            redis::Client::open(url)
                .map_err(|_| redis_error())?
                .get_connection_manager_with_config(
                    ConnectionManagerConfig::new()
                        .set_number_of_retries(0)
                        .set_connection_timeout(Duration::from_secs(2)),
                ),
        )
        .await
        .map_err(|_| redis_error())?
        .map_err(|_| redis_error())?;
        Ok(Self {
            inner: Arc::new(Inner {
                connection,
                coordinator,
                sessions: Mutex::new(HashMap::new()),
                slots: Arc::new(Semaphore::new(max_sessions)),
                requests: Arc::new(Semaphore::new(max_requests)),
                admin_controls: Arc::new(Semaphore::new(max_requests)),
                idle_grace,
                stopped: watch::channel(false).0,
                max_requests,
                max_sessions,
                recovery: std::sync::Mutex::new(None),
                pending_cleanups: AtomicUsize::new(0),
                cleanup_failed: AtomicBool::new(false),
                events: std::sync::Mutex::new(None),
            }),
        })
    }

    pub fn enable_recovery(&self, namespace: &str) -> Result<(), dispatcharr_store::StoreError> {
        self.configure_recovery(namespace, true)
    }

    pub fn enable_authenticated_restart(
        &self,
        namespace: &str,
    ) -> Result<(), dispatcharr_store::StoreError> {
        self.configure_recovery(namespace, false)
    }

    fn configure_recovery(
        &self,
        namespace: &str,
        extended: bool,
    ) -> Result<(), dispatcharr_store::StoreError> {
        let registry = self.inner.sessions.try_lock().map_err(|_| unavailable())?;
        if !registry.is_empty()
            || self.inner.requests.available_permits() != self.inner.max_requests
            || self.inner.slots.available_permits() != self.inner.max_sessions
            || extended && self.inner.max_requests > 4096
        {
            return Err(dispatcharr_store::StoreError::Unsupported(
                "VOD recovery must be configured before playback",
            ));
        }
        let mut mode = self.inner.recovery.lock().unwrap();
        if mode.is_some() {
            return Err(dispatcharr_store::StoreError::Unsupported(
                "VOD recovery already configured",
            ));
        }
        *mode = Some(RecoveryMode {
            extended,
            recovery: recovery::Recovery::new(
                self.inner.coordinator.clone(),
                self.inner.connection.clone(),
                namespace.to_owned(),
                self.inner.max_sessions.min(1024),
                Duration::from_secs(3),
            )
            .map_err(|_| unavailable())?,
        });
        Ok(())
    }
    async fn scoped_body(
        &self,
        session: &Arc<Session>,
        request: Arc<OwnedSemaphorePermit>,
        is_new: bool,
    ) -> Result<Option<ScopedBody>, dispatcharr_store::StoreError> {
        let mode = self.inner.recovery.lock().unwrap().clone();
        let Some(mode) = mode else {
            return Ok(None);
        };
        if session.scope.get().is_none() {
            if !mode.extended && !session.authenticated_restart {
                return Ok(None);
            }
            let scope = LocalScope::adopt_with_restart_policy(
                mode.recovery,
                ScopeInitial {
                    id: session.id.clone(),
                    lease: session.lease.clone(),
                    metadata_token: session.metadata_token.clone(),
                    profile_id: session.candidate.profile_id,
                    credential_pool: session.candidate.credential_pool.clone(),
                    identity: ScopeIdentity {
                        user_id: session
                            .source
                            .user
                            .as_ref()
                            .map_or("unknown".into(), |user| user.id.to_string()),
                        content_uuid: session.source.content_uuid.clone(),
                        content_kind: session.source.kind.storage_name().into(),
                    },
                    lifetime: session._slot.clone(),
                    stop: session.stop.clone(),
                },
                ScopeLimits {
                    bodies: self.inner.max_requests.min(4096),
                    controls: self.inner.max_requests.min(4096),
                    idle_grace: self.inner.idle_grace,
                },
                session.authenticated_restart,
            )
            .await
            .map_err(scope_error)?;
            session.scope.set(scope).map_err(|_| unavailable())?;
        }
        let hook = scope_hook(&self.inner, transition(session, false));
        let scope = session.scope.get().expect("installed scope");
        let body = if session.authenticated_restart {
            scope
                .attach_ordinary_reserved(
                    request,
                    is_new,
                    hook,
                    &session.lease,
                    &session.metadata_token,
                )
                .await
        } else {
            scope.attach_reserved(request, is_new, hook).await
        };
        body.map(Some).map_err(scope_error)
    }
    pub fn install_events(&self, producer: Producer) -> Result<(), dispatcharr_store::StoreError> {
        let sessions = self.inner.sessions.try_lock().map_err(|_| unavailable())?;
        let mut events = self.inner.events.lock().unwrap();
        if !sessions.is_empty() || events.is_some() {
            return Err(dispatcharr_store::StoreError::Unsupported(
                "VOD events must be configured before playback",
            ));
        }
        *events = Some(producer);
        Ok(())
    }
    pub fn event_diagnostics(&self) -> serde_json::Value {
        self.inner
            .events
            .lock()
            .unwrap()
            .as_ref()
            .map_or(json!({"pending":0,"failed":0}), Producer::diagnostics)
    }
    pub async fn finish_events(&self, timeout: Duration) -> bool {
        let events = self.inner.events.lock().unwrap().clone();
        if let Some(events) = events {
            events.shutdown(timeout).await
        } else {
            true
        }
    }
    pub fn cancel_events(&self) {
        if let Some(events) = self.inner.events.lock().unwrap().as_ref() {
            events.cancel();
        }
    }
    pub fn stopped(&self) -> bool {
        *self.inner.stopped.borrow()
    }
    pub fn cleanup_succeeded(&self) -> bool {
        !self.inner.cleanup_failed.load(Ordering::Acquire)
            && self
                .inner
                .recovery
                .lock()
                .unwrap()
                .as_ref()
                .is_none_or(|mode| mode.recovery.cleanup_succeeded())
    }
    pub fn stop_admission(&self) {
        self.inner.stopped.send_replace(true);
        self.inner.slots.close();
        self.inner.requests.close();
        self.inner.admin_controls.close();
    }
    pub async fn drain(&self) {
        loop {
            let sessions = self.inner.sessions.lock().await;
            for session in sessions.values() {
                session.stop.send_replace(true);
            }
            if sessions.is_empty()
                && self.inner.requests.available_permits() == self.inner.max_requests
                && self.inner.admin_controls.available_permits() == self.inner.max_requests
                && self.inner.pending_cleanups.load(Ordering::Acquire) == 0
                && self.inner.coordinator.is_idle()
                && (self.inner.recovery.lock().unwrap().is_none()
                    || self.inner.slots.available_permits() == self.inner.max_sessions)
            {
                return;
            }
            drop(sessions);
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
    pub async fn idle_session(&self, source: &VodSource, request: &VodRequest) -> Option<String> {
        idle_match(&*self.inner.sessions.lock().await, source, request)
            .map(|session| session.id.clone())
    }
    pub async fn redirect_candidate(
        &self,
        source: &VodSource,
    ) -> Result<String, dispatcharr_store::StoreError> {
        if self.stopped() {
            return Err(unavailable());
        }
        let mut connection = self.inner.connection.clone();
        for candidate in &source.candidates {
            // Source Redirect only checks capacity. It never reserves a slot,
            // creates an owner/ledger, or increments even a transient counter.
            if candidate.max_streams == 0 {
                return Ok(candidate.url.clone());
            }
            let count: Option<i64> = connection
                .get(format!("profile_connections:{}", candidate.profile_id))
                .await
                .map_err(|_| redis_error())?;
            let count = count.unwrap_or(0);
            if count < 0 {
                return Err(redis_error());
            }
            if count >= (candidate.max_streams as i64) {
                continue;
            }
            if let Some(pool) = &candidate.credential_pool {
                let used: Option<i64> = connection
                    .get(&pool.counter_key)
                    .await
                    .map_err(|_| redis_error())?;
                let used = used.unwrap_or(0);
                if used < 0 {
                    return Err(redis_error());
                }
                if used >= i64::from(pool.max_streams) {
                    continue;
                }
            }
            return Ok(candidate.url.clone());
        }
        Err(unavailable())
    }

    pub async fn remember_user(
        &self,
        id: &str,
        user: i64,
    ) -> Result<(), dispatcharr_store::StoreError> {
        if self.stopped() {
            return Err(unavailable());
        }
        let mut connection = self.inner.connection.clone();
        let _: () = connection
            .set_ex(format!("vod_session_user:{id}"), user, 300)
            .await
            .map_err(|_| redis_error())?;
        Ok(())
    }
    pub async fn user_for_session(
        &self,
        id: &str,
    ) -> Result<Option<i64>, dispatcharr_store::StoreError> {
        session_user_lookup::optional_user(self.inner.clone(), id).await
    }

    async fn enter_recovered(
        &self,
        id: &str,
        source: VodSource,
        request: &VodRequest,
        request_permit: Arc<OwnedSemaphorePermit>,
        retained: RetainedEntry,
    ) -> Result<Ticket, dispatcharr_store::StoreError> {
        let RetainedEntry {
            previous,
            scope,
            resolver,
        } = retained;
        // Source2's changed-request-ID cases keep the one retained public ID.
        // This is only an alias into an owned idle generation, never a new
        // Redis hash, channel owner, scope or session-slot allocation.
        let retained_id = previous.id.clone();
        let requested_alias = id != retained_id;
        let candidate = source.candidates.first().ok_or_else(unavailable)?.clone();
        let retained_available = source.candidates.iter().any(|next| {
            next.profile_id == previous.candidate.profile_id
                && same_provider_fingerprint(next, &previous.candidate)
        });
        let mut inactive_handoff = false;
        if requested_alias {
            let idle = {
                let state = previous.state.lock().await;
                state.active == 0 && !state.reserved && !state.closed
            };
            if !idle {
                return Err(dispatcharr_store::StoreError::Unsupported(
                    "Requested-ID adoption requires an owned idle generation",
                ));
            }
            if !retained_available {
                // The captured fallback stays in the same account/credential
                // fingerprint with the original nonzero capacity. Absence from
                // a permission-filtered candidate list does not prove inactive.
                let bounded = candidate.profile_id != previous.candidate.profile_id
                    && candidate.max_streams > 0
                    && same_provider_fingerprint(&candidate, &previous.candidate)
                    && source.user.as_ref().is_some_and(|u| u.stream_limit == 0)
                    && source.user.as_ref().map(|u| u.id)
                        == previous.source.user.as_ref().map(|u| u.id);
                if bounded {
                    if let Some(policy) = resolver.as_ref() {
                        inactive_handoff = policy
                            .inactive_profile_in_account(
                                previous.candidate.profile_id,
                                previous.candidate.account_id,
                            )
                            .await?;
                    }
                }
                if !inactive_handoff {
                    return Err(dispatcharr_store::StoreError::Unsupported(
                        "Requested-ID adoption outside retained profile fingerprint",
                    ));
                }
            }
        }
        // A fresh stable classification is safe; it never repeats an uncertain
        // allocation. Owned/partial cases remain explicit until separately gated.
        match scope.retire_missing_retained(request_permit.clone()).await {
            Ok(()) | Err(recovery::Error::Capacity) if requested_alias => {
                // Source's same-ID metadata-loss/cross-profile discrepancy is
                // not an authority for allocating a changed requested ID.
                return Err(dispatcharr_store::StoreError::Unsupported(
                    "Requested-ID adoption needs retained public metadata",
                ));
            }
            Ok(()) => {}
            Err(recovery::Error::Capacity) => {}
            Err(recovery::Error::StillOwned) => {
                let hook = scope_hook(
                    &self.inner,
                    transition_data(&source, &request.client_ip_text(), false),
                );
                let body = scope
                    .attach_entry_pending(request_permit.clone(), hook)
                    .await
                    .map_err(scope_error)?;
                let fields = metadata_fields(
                    &retained_id,
                    &source,
                    request,
                    if inactive_handoff {
                        &candidate
                    } else {
                        &previous.candidate
                    },
                    &previous.metadata_token,
                    previous.timeshift.borrowed(),
                )?;
                let limit = source
                    .user
                    .as_ref()
                    .map_or(0, |user| user.stream_limit.max(0));
                let admission = recovery::NewAdmission {
                    fields,
                    limit: limit as u32,
                    policy: source.limit_policy.clone(),
                };
                if inactive_handoff {
                    scope
                        .enter_idle_profile_handoff(
                            &body,
                            admission,
                            candidate.profile_id,
                            candidate.max_streams,
                        )
                        .await
                        .map_err(scope_error)?;
                    let (lease, metadata_token) = scope.current_identity().await;
                    let session = Arc::new(Session {
                        id: retained_id.clone(),
                        source,
                        candidate,
                        stop: previous.stop.clone(),
                        bytes: AtomicU64::new(previous.bytes.load(Ordering::Relaxed)),
                        content_length: AtomicU64::new(
                            previous.content_length.load(Ordering::Relaxed),
                        ),
                        final_provider_url: crate::provider_final_url::FinalProviderUrl::new(false),
                        last_request: AtomicU64::new(millis()),
                        client_ip: request.client_ip_text(),
                        user_agent: request.playback.header("user-agent").unwrap_or("").into(),
                        timeshift: previous.timeshift.clone(),
                        lease,
                        metadata_token,
                        scope: std::sync::OnceLock::new(),
                        authenticated_restart: false,
                        generation_epoch: 0,
                        signed_alias_owner: false,
                        state: Mutex::new(State {
                            active: 1,
                            reserved: false,
                            closed: false,
                            epoch: 0,
                            departing_pull: None,
                        }),
                        _slot: previous._slot.clone(),
                    });
                    session
                        .scope
                        .set(scope.clone())
                        .map_err(|_| unavailable())?;
                    let mut registry = self.inner.sessions.lock().await;
                    if registry.get(&retained_id).is_none_or(|current| {
                        current
                            .scope
                            .get()
                            .is_none_or(|value| !value.same_scope(&scope))
                    }) {
                        return Err(dispatcharr_store::StoreError::Unsupported(
                            "Idle profile handoff registry changed",
                        ));
                    }
                    registry.insert(retained_id, session.clone());
                    drop(registry);
                    return Ok(Ticket {
                        session,
                        pool: Arc::downgrade(&self.inner),
                        _request: Some(request_permit),
                        is_new: false,
                        body_started: AtomicBool::new(false),
                        scope_body: Some(body),
                        cleanup_needed: true,
                    });
                }
                scope
                    .enter_current_or_idle(
                        &body,
                        admission,
                        source
                            .candidates
                            .iter()
                            .find(|candidate| candidate.profile_id == scope.profile_id())
                            .map(|candidate| candidate.max_streams),
                    )
                    .await
                    .map_err(|error| {
                        if let recovery::Error::Admission(error) = error {
                            admission_error(error, limit)
                        } else {
                            scope_error(error)
                        }
                    })?;
                let ordinary = if previous.authenticated_restart {
                    Some(
                        scope
                            .ordinary_body_generation(&body, false)
                            .await
                            .map_err(scope_error)?,
                    )
                } else {
                    None
                };
                let session = if let Some(generation) = &ordinary {
                    let registry = self.inner.sessions.lock().await;
                    registry
                        .get(&retained_id)
                        .filter(|current| {
                            current.authenticated_restart
                                && current
                                    .scope
                                    .get()
                                    .is_some_and(|owner| owner.same_scope(&scope))
                                && current.generation_epoch == generation.epoch
                                && current.lease.token() == generation.lease.token()
                                && current.metadata_token == generation.metadata_token
                        })
                        .cloned()
                        .ok_or_else(unavailable)?
                } else {
                    previous
                };
                {
                    let mut state = session.state.lock().await;
                    if ordinary.is_some()
                        && (state.closed || *session.stop.borrow() || self.stopped())
                    {
                        return Err(unavailable());
                    }
                    state.active += 1;
                    state.epoch += 1;
                }
                session.last_request.store(millis(), Ordering::Relaxed);
                return Ok(Ticket {
                    session,
                    pool: Arc::downgrade(&self.inner),
                    _request: Some(request_permit),
                    is_new: false,
                    body_started: AtomicBool::new(false),
                    scope_body: Some(body),
                    cleanup_needed: true,
                });
            }
            Err(error) => return Err(scope_error(error)),
        }
        if candidate.profile_id != scope.profile_id() {
            return Err(dispatcharr_store::StoreError::Unsupported(
                "Cross-profile recovery is not yet implemented",
            ));
        }
        let hook = scope_hook(
            &self.inner,
            transition_data(&source, &request.client_ip_text(), false),
        );
        let body = scope
            .attach_pending(request_permit.clone(), hook)
            .await
            .map_err(scope_error)?;
        let timeshift = crate::offset::requested(&request.playback);
        let fields = metadata_fields(
            id,
            &source,
            request,
            &candidate,
            "recovery-pending",
            timeshift,
        )?;
        let limit = source
            .user
            .as_ref()
            .map_or(0, |user| user.stream_limit.max(0));
        scope
            .reopen(
                &body,
                AcquireRequest {
                    channel_id: id.into(),
                    provider_profile_id: Some(candidate.profile_id),
                    provider_max_streams: candidate.max_streams,
                    credential_pool: candidate.credential_pool.clone(),
                },
                recovery::NewAdmission {
                    fields,
                    limit: limit as u32,
                    policy: source.limit_policy.clone(),
                },
            )
            .await
            .map_err(|error| {
                if let recovery::Error::Admission(error) = error {
                    admission_error(error, limit)
                } else {
                    scope_error(error)
                }
            })?;
        let ordinary = if previous.authenticated_restart {
            Some(
                scope
                    .ordinary_body_generation(&body, true)
                    .await
                    .map_err(scope_error)?,
            )
        } else {
            None
        };
        let (lease, metadata_token, generation_epoch) = if let Some(generation) = &ordinary {
            (
                generation.lease.clone(),
                generation.metadata_token.clone(),
                generation.epoch,
            )
        } else {
            let (lease, metadata_token) = scope.current_identity().await;
            (lease, metadata_token, 0)
        };
        let session = Arc::new(Session {
            id: id.into(),
            source,
            candidate,
            stop: previous.stop.clone(),
            bytes: AtomicU64::new(0),
            content_length: AtomicU64::new(0),
            final_provider_url: crate::provider_final_url::FinalProviderUrl::new(false),
            last_request: AtomicU64::new(millis()),
            client_ip: request.client_ip_text(),
            user_agent: request.playback.header("user-agent").unwrap_or("").into(),
            timeshift: timeshift.owned(),
            lease,
            metadata_token,
            scope: std::sync::OnceLock::new(),
            authenticated_restart: previous.authenticated_restart,
            generation_epoch,
            signed_alias_owner: false,
            state: Mutex::new(State {
                active: 1,
                reserved: true,
                closed: false,
                epoch: 0,
                departing_pull: None,
            }),
            _slot: previous._slot.clone(),
        });
        session
            .scope
            .set(scope.clone())
            .map_err(|_| unavailable())?;
        let mut registry = self.inner.sessions.lock().await;
        if registry.get(id).is_none_or(|current| {
            let same_scope = current
                .scope
                .get()
                .is_some_and(|value| value.same_scope(&scope));
            !same_scope
                || ordinary.as_ref().is_some_and(|generation| {
                    !current.authenticated_restart || current.generation_epoch >= generation.epoch
                })
        }) || (ordinary.is_some() && (*session.stop.borrow() || self.stopped()))
        {
            return Err(dispatcharr_store::StoreError::Unsupported(
                "Recovery registry generation changed",
            ));
        }
        registry.insert(id.into(), session.clone());
        drop(registry);
        Ok(Ticket {
            session,
            pool: Arc::downgrade(&self.inner),
            _request: Some(request_permit),
            is_new: true,
            body_started: AtomicBool::new(false),
            scope_body: Some(body),
            cleanup_needed: true,
        })
    }
    // Ordinary component fixtures keep the original conservative constructor;
    // production enters through the same path with its actual SQL resolver.
    #[cfg(test)]
    pub async fn enter(
        &self,
        id: &str,
        source: VodSource,
        request: &VodRequest,
    ) -> Result<Ticket, dispatcharr_store::StoreError> {
        self.enter_selected(id, source, request, None, EntryAuthorization::default())
            .await
    }
    pub(crate) async fn enter_with_resolver(
        &self,
        id: &str,
        source: VodSource,
        request: &VodRequest,
        resolver: Arc<dyn crate::resolver::VodResolver>,
        authorization: EntryAuthorization,
    ) -> Result<Ticket, dispatcharr_store::StoreError> {
        self.enter_selected(id, source, request, Some(resolver), authorization)
            .await
    }
    async fn enter_selected(
        &self,
        id: &str,
        source: VodSource,
        request: &VodRequest,
        resolver: Option<Arc<dyn crate::resolver::VodResolver>>,
        authorization: EntryAuthorization,
    ) -> Result<Ticket, dispatcharr_store::StoreError> {
        // All entry/recovery paths share this immutable process identity. Fail
        // before a pending body, retirement job, ledger or quota can be created.
        crate::worker_identity::validate().map_err(|_| invalid_worker_identity())?;
        // Bound work before spawning. Dropping the caller only abandons its
        // observer; setup keeps admission through actual Redis replies and any
        // token-fenced cleanup, including before PendingCreate is installed.
        let request_permit = Arc::new(
            self.inner
                .requests
                .clone()
                .try_acquire_owned()
                .map_err(|_| unavailable())?,
        );
        let pool = self.clone();
        let id = id.to_owned();
        let request = request.clone();
        tokio::spawn(async move {
            pool.enter_owned(
                &id,
                source,
                &request,
                request_permit,
                resolver,
                authorization,
            )
            .await
        })
        .await
        .map_err(|_| unavailable())?
    }
    async fn enter_owned(
        &self,
        id: &str,
        source: VodSource,
        request: &VodRequest,
        request_permit: Arc<OwnedSemaphorePermit>,
        resolver: Option<Arc<dyn crate::resolver::VodResolver>>,
        authorization: EntryAuthorization,
    ) -> Result<Ticket, dispatcharr_store::StoreError> {
        let EntryAuthorization {
            restart_user,
            signed_alias,
        } = authorization;
        let mut connection = self.inner.connection.clone();
        let proven_claim = dispatcharr_clients::vod::check_signed_claim(
            &mut connection,
            id,
            signed_alias.as_deref(),
            source.user.as_ref().map(|user| user.id),
        )
        .await
        .map_err(|error| match error {
            dispatcharr_clients::Error::Stopped => dispatcharr_store::StoreError::Forbidden,
            _ => redis_error(),
        })?;
        let signed_alias = signed_alias.filter(|_| proven_claim);
        if let (Some(claim), Some(policy)) = (signed_alias.as_deref(), resolver.as_deref()) {
            if let Some(ticket) = self
                .enter_signed_idle_alias(id, claim, &source, request, policy, &request_permit)
                .await?
            {
                return Ok(ticket);
            }
        }
        // This lock serializes bounded session SETUP only, never media/network body I/O.
        let mut registry = self.inner.sessions.lock().await;
        if self.stopped() {
            return Err(unavailable());
        }
        let reusable = registry.get(id).cloned().or_else(|| {
            if request.session_id.is_none() {
                idle_match(&registry, &source, request)
            } else {
                None
            }
        });
        if let Some(session) = reusable {
            if session.source.content_uuid != source.content_uuid
                || session.source.kind != source.kind
            {
                return Err(dispatcharr_store::StoreError::Forbidden);
            }
            // Once known, content length is immutable. Reject this ordinary
            // explicit seek before taking the state mutex: even briefly owning
            // it could interfere with a departing body's pending provider read.
            if request.session_id.is_some()
                && !session.uses_scoped_body_lifecycle()
                && !*session.stop.borrow()
                && request.playback.header("range").is_some_and(|range| {
                    crate::engine::range_is_unsatisfiable(
                        range,
                        session.content_length.load(Ordering::Relaxed),
                    )
                })
            {
                return Err(dispatcharr_store::StoreError::StatusBody(
                    416,
                    json!(crate::engine::RANGE_NOT_SATISFIABLE),
                ));
            }
            drop(registry);
            let can_restart = !session.authenticated_restart
                || ordinary_restart_matches(
                    &source,
                    request,
                    restart_user,
                    &session.id,
                    &session.source,
                    &session.candidate,
                );
            if let Some(scope) = session.scope.get().cloned() {
                if !scope.activated() {
                    // Do not wait for the periodic monitor to observe Redis
                    // loss. This fresh read retains this request through its
                    // physical reply, even if the observer goes away.
                    match scope.retire_missing_retained(request_permit.clone()).await {
                        Err(recovery::Error::StillOwned) => {}
                        Ok(()) | Err(recovery::Error::Capacity) if can_restart => {
                            return self
                                .enter_recovered(
                                    id,
                                    source,
                                    request,
                                    request_permit,
                                    RetainedEntry {
                                        previous: session,
                                        scope,
                                        resolver: resolver.clone(),
                                    },
                                )
                                .await;
                        }
                        Ok(()) => return Err(unavailable()),
                        Err(error) => return Err(scope_error(error)),
                    }
                } else if can_restart {
                    return self
                        .enter_recovered(
                            id,
                            source,
                            request,
                            request_permit,
                            RetainedEntry {
                                previous: session,
                                scope,
                                resolver: resolver.clone(),
                            },
                        )
                        .await;
                } else {
                    return Err(unavailable());
                }
            }
            if let Some(scope) = session
                .scope
                .get()
                .filter(|_| !session.authenticated_restart)
                .cloned()
            {
                let locally_idle = {
                    let state = session.state.lock().await;
                    state.active == 0 && !state.reserved && !state.closed
                };
                if locally_idle {
                    scope.activate_initial_idle();
                    return self
                        .enter_recovered(
                            id,
                            source,
                            request,
                            request_permit,
                            RetainedEntry {
                                previous: session,
                                scope,
                                resolver: resolver.clone(),
                            },
                        )
                        .await;
                }
            }
            let mut state = session.state.lock().await;
            if state.closed || *session.stop.borrow() {
                return Err(unavailable());
            }
            if let Some(user) = &source.user {
                if user.stream_limit > 0 {
                    let fields = BTreeMap::from([
                        ("session_id".into(), session.id.clone()),
                        ("user_id".into(), user.id.to_string()),
                        ("content_uuid".into(), source.content_uuid.clone()),
                    ]);
                    let mut connection = self.inner.connection.clone();
                    let admission = dispatcharr_clients::vod::Admission {
                        session_id: &session.id,
                        fields: &fields,
                        limit: user.stream_limit as u32,
                        policy: &source.limit_policy,
                        create: false,
                        token: &session.metadata_token,
                        scan_limit: 50000,
                        timeout: Duration::from_secs(2),
                        ttl: Duration::from_secs(3600),
                    };
                    let admitted = if let Some(scope) = session
                        .scope
                        .get()
                        .filter(|_| !session.authenticated_restart)
                    {
                        let (owner, _, stop) = scope.entry_fence_keys(&session.lease);
                        dispatcharr_clients::vod::admit_source_owner_physical(
                            &mut connection,
                            admission,
                            dispatcharr_clients::vod::AdmissionFence {
                                owner_key: &owner,
                                owner_token: session.lease.token(),
                                stop_key: &stop,
                            },
                        )
                        .await
                    } else {
                        dispatcharr_clients::vod::admit(&mut connection, admission).await
                    };
                    admitted.map_err(|error| admission_error(error, user.stream_limit))?;
                }
            }
            let reserved_here = !state.reserved;
            if reserved_here {
                // In ordinary mode the stored profile can still be selected
                // before its late 0→1 capacity check. Use current limits when
                // the SQL selection confirms the retained provider identity.
                let current = if session.scope.get().is_none() {
                    current_retained_provider(&session.candidate, &source.candidates)
                } else {
                    None
                };
                let candidate = current.unwrap_or(&session.candidate);
                session
                    .lease
                    .reserve_provider(ProviderReservation {
                        profile_id: Some(candidate.profile_id),
                        max_streams: candidate.max_streams,
                        credential_pool: candidate.credential_pool.clone(),
                    })
                    .await
                    .map_err(|error| match error {
                        dispatcharr_coordination::Error::ProfileFull
                        | dispatcharr_coordination::Error::CredentialFull
                            if current.is_some() =>
                        {
                            provider_full()
                        }
                        _ => unavailable(),
                    })?;
                state.reserved = true;
            }
            session.last_request.store(millis(), Ordering::Relaxed);
            // The new viewer owns its own upstream response. End any detached
            // pull on the departing ticket without cancelling this admission.
            state.departing_pull = None;
            state.active += 1;
            state.epoch += 1;
            drop(state);
            // Own the request before sending ENTER: cancellation must still
            // undo the corresponding Redis increment on this connection.
            let mut ticket = Ticket {
                session,
                pool: Arc::downgrade(&self.inner),
                _request: Some(request_permit),
                is_new: false,
                body_started: AtomicBool::new(false),
                scope_body: None,
                cleanup_needed: true,
            };
            let mut connection = self.inner.connection.clone();
            let script = Script::new(ENTER);
            let mut invocation = script.prepare_invoke();
            invocation.key(key(&ticket.session.id));
            if let Some(scope) = ticket.session.scope.get() {
                let (owner, ledger, stop) = scope.entry_fence_keys(&ticket.session.lease);
                invocation.key(owner).key(ledger).key(stop);
            }
            invocation.arg(&ticket.session.metadata_token).arg(now());
            if ticket.session.scope.get().is_some() {
                invocation.arg(ticket.session.lease.token());
            }
            let count: i64 = invocation
                .invoke_async(&mut connection)
                .await
                .map_err(|_| redis_error())?;
            if count <= 0 {
                if let Some(scope) = ticket.session.scope.get().cloned() {
                    // A settled nonpositive reply guarantees ENTER wrote
                    // nothing. Never LEAVE/decrement an unrelated body here.
                    ticket.cleanup_needed = false;
                    let request_permit = ticket._request.take().expect("owned entry request");
                    let previous = ticket.session.clone();
                    let release_reserved = {
                        let mut state = previous.state.lock().await;
                        state.active = state.active.saturating_sub(1);
                        state.epoch += 1;
                        let release = reserved_here && state.active == 0;
                        if release {
                            state.reserved = false;
                        }
                        release
                    };
                    drop(ticket);
                    if release_reserved {
                        let result = previous.lease.release_provider().await;
                        previous.lease.settlement().wait().await;
                        result.map_err(|_| unavailable())?;
                    }
                    if !can_restart {
                        return Err(unavailable());
                    }
                    return self
                        .enter_recovered(
                            id,
                            source,
                            request,
                            request_permit,
                            RetainedEntry {
                                previous,
                                scope,
                                resolver: resolver.clone(),
                            },
                        )
                        .await;
                }
                ticket.session.stop.send_replace(true);
                return Err(unavailable());
            }
            let mut ticket = ticket;
            ticket.scope_body = self
                .scoped_body(
                    &ticket.session,
                    ticket._request.as_ref().expect("request scope").clone(),
                    false,
                )
                .await?;
            return Ok(ticket);
        }
        let slot = Arc::new(
            self.inner
                .slots
                .clone()
                .try_acquire_owned()
                .map_err(|_| unavailable())?,
        );
        let mut connection = self.inner.connection.clone();
        let existing: bool = connection
            .exists(key(id))
            .await
            .map_err(|_| redis_error())?;
        if existing {
            return Err(dispatcharr_store::StoreError::Unsupported(
                "Legacy or cross-worker VOD session adoption is not implemented",
            ));
        }
        let initial = source.candidates.first().ok_or_else(unavailable)?;
        let metadata_token = format!("rust-vod-metadata:{}", uuid::Uuid::new_v4());
        let timeshift = crate::offset::requested(&request.playback);
        let fields = metadata_fields(id, &source, request, initial, &metadata_token, timeshift)?;
        let mut pending = PendingCreate {
            connection: connection.clone(),
            id: id.into(),
            token: metadata_token.clone(),
            armed: true,
            pool: Arc::downgrade(&self.inner),
            retained_request: Some(request_permit.clone()),
            retained_slot: Some(slot.clone()),
        };
        let limit = source
            .user
            .as_ref()
            .map_or(0, |user| user.stream_limit.max(0));
        let admission = dispatcharr_clients::vod::Admission {
            session_id: id,
            fields: &fields,
            limit: limit as u32,
            policy: &source.limit_policy,
            create: true,
            token: &metadata_token,
            scan_limit: 50000,
            timeout: Duration::from_secs(2),
            ttl: Duration::from_secs(10),
        };
        let mode = self.inner.recovery.lock().unwrap().clone();
        let source_mode = mode.as_ref().is_some_and(|mode| mode.extended);
        let authenticated_restart = mode.is_some()
            && !source_mode
            && ordinary_restart_user(&source, request, restart_user).is_some();
        let created = dispatcharr_clients::vod::admit_signed_create(
            &mut connection,
            admission,
            source_mode,
            signed_alias.as_deref(),
        )
        .await
        .map_err(|error| match error {
            dispatcharr_clients::Error::Stopped => dispatcharr_store::StoreError::Forbidden,
            _ => admission_error(error, limit),
        })?;
        if !created {
            return Err(unavailable());
        }
        let mut selected = None;
        for candidate in &source.candidates {
            match self
                .inner
                .coordinator
                .acquire_vod(AcquireRequest {
                    channel_id: id.into(),
                    provider_profile_id: Some(candidate.profile_id),
                    provider_max_streams: candidate.max_streams,
                    credential_pool: candidate.credential_pool.clone(),
                })
                .await
            {
                Ok(lease) => {
                    selected = Some((candidate.clone(), lease));
                    break;
                }
                Err(
                    dispatcharr_coordination::Error::ProfileFull
                    | dispatcharr_coordination::Error::CredentialFull,
                ) => continue,
                Err(dispatcharr_coordination::Error::OwnerHeld) => {
                    return Err(dispatcharr_store::StoreError::Unsupported(
                        "Cross-worker VOD session adoption is not implemented",
                    ));
                }
                Err(_) => return Err(redis_error()),
            }
        }
        let Some((candidate, lease)) = selected else {
            // Source rejects a new contending session before creating public
            // metadata. Retire our provisional entry before returning that
            // same ordinary capacity response.
            drop(registry);
            pending.discard().await?;
            return Err(unavailable());
        };
        let saved: i64 = Script::new(UPDATE)
            .key(key(id))
            .arg(&metadata_token)
            .arg("stream_url")
            .arg(crate::offset::provider_url(&candidate.url, timeshift).as_ref())
            .arg("m3u_profile_id")
            .arg(candidate.profile_id)
            .invoke_async(&mut connection)
            .await
            .map_err(|_| redis_error())?;
        if saved != 1 {
            return Err(unavailable());
        }
        let (stop, _) = watch::channel(false);
        let session = Arc::new(Session {
            id: id.into(),
            source,
            candidate,
            stop,
            bytes: AtomicU64::new(0),
            content_length: AtomicU64::new(0),
            final_provider_url: crate::provider_final_url::FinalProviderUrl::new(!source_mode),
            last_request: AtomicU64::new(millis()),
            client_ip: request.client_ip_text(),
            user_agent: request.playback.header("user-agent").unwrap_or("").into(),
            timeshift: timeshift.owned(),
            lease: Arc::new(lease),
            scope: std::sync::OnceLock::new(),
            metadata_token,
            authenticated_restart,
            generation_epoch: 0,
            signed_alias_owner: signed_alias.is_some(),
            state: Mutex::new(State {
                active: 1,
                reserved: true,
                closed: false,
                epoch: 0,
                departing_pull: None,
            }),
            _slot: slot,
        });
        let scope_body = self
            .scoped_body(&session, request_permit.clone(), true)
            .await?;
        registry.insert(id.into(), session.clone());
        pending.armed = false;
        drop(registry);
        monitor(Arc::downgrade(&session), Arc::downgrade(&self.inner));
        Ok(Ticket {
            session,
            pool: Arc::downgrade(&self.inner),
            _request: Some(request_permit),
            is_new: true,
            body_started: AtomicBool::new(false),
            scope_body,
            cleanup_needed: true,
        })
    }

    pub async fn stop(&self, id: &str) -> Result<bool, dispatcharr_store::StoreError> {
        session_stop_owned::stop(self.inner.clone(), id).await
    }

    pub async fn update(
        &self,
        session: &Session,
        fields: &[(&str, String)],
    ) -> Result<(), dispatcharr_store::StoreError> {
        if let Some(scope) = session.scope.get().filter(|scope| scope.activated()) {
            return scope.update_headers(fields).await.map_err(scope_error);
        }
        update(&self.inner, session, fields).await
    }
}

fn ordinary_restart_user(
    source: &VodSource,
    request: &VodRequest,
    restart_user: Option<i64>,
) -> Option<i64> {
    let timeshift = crate::offset::requested(&request.playback);
    restart_user.filter(|id| {
        !source.redirect
            && source.user.as_ref().is_some_and(|user| user.id == *id)
            && request.session_id.is_some()
            && request.playback.header("range").is_none()
            && timeshift.utc_start.is_empty()
            && timeshift.utc_end.is_empty()
            && timeshift.offset.is_empty()
    })
}

fn ordinary_restart_matches(
    source: &VodSource,
    request: &VodRequest,
    restart_user: Option<i64>,
    retained_id: &str,
    retained_source: &VodSource,
    retained_provider: &VodCandidate,
) -> bool {
    ordinary_restart_user(source, request, restart_user).is_some_and(|id| {
        retained_source
            .user
            .as_ref()
            .is_some_and(|user| user.id == id)
            && request.session_id.as_deref() == Some(retained_id)
            && source.content_uuid == retained_source.content_uuid
            && source.kind == retained_source.kind
            && source
                .candidates
                .first()
                .is_some_and(|next| same_provider(next, retained_provider))
    })
}

fn same_provider(left: &VodCandidate, right: &VodCandidate) -> bool {
    left.profile_id == right.profile_id && same_provider_fingerprint(left, right)
}

fn current_retained_provider<'a>(
    retained: &VodCandidate,
    candidates: &'a [VodCandidate],
) -> Option<&'a VodCandidate> {
    candidates.iter().find(|current| {
        current.profile_id == retained.profile_id && same_provider_identity(current, retained)
    })
}

fn same_provider_identity(left: &VodCandidate, right: &VodCandidate) -> bool {
    left.relation_id == right.relation_id
        && left.account_id == right.account_id
        && left.url == right.url
        && left.user_agent == right.user_agent
        && left.credential_pool.as_ref().map(|pool| &pool.counter_key)
            == right.credential_pool.as_ref().map(|pool| &pool.counter_key)
}

fn same_provider_fingerprint(left: &VodCandidate, right: &VodCandidate) -> bool {
    same_provider_identity(left, right)
        && left.max_streams == right.max_streams
        && left.credential_pool.as_ref().map(|pool| pool.max_streams)
            == right.credential_pool.as_ref().map(|pool| pool.max_streams)
}

fn metadata_fields(
    id: &str,
    source: &VodSource,
    request: &VodRequest,
    initial: &VodCandidate,
    metadata_token: &str,
    timeshift: crate::offset::Timeshift<&str>,
) -> Result<BTreeMap<String, String>, dispatcharr_store::StoreError> {
    // Resolve identity before admission; conversion failure cannot invent a
    // different Redis owner label from the response identity.
    let worker_id = crate::worker_identity::metadata().map_err(|_| invalid_worker_identity())?;
    let timestamp = now();
    let user_id = source
        .user
        .as_ref()
        .map_or("unknown".into(), |user| user.id.to_string());
    let fields = vec![
        ("session_id", id.to_string()),
        (
            "stream_url",
            crate::offset::provider_url(&initial.url, timeshift).into_owned(),
        ),
        ("headers", "{}".into()),
        ("content_length", "".into()),
        ("content_type", "".into()),
        ("final_url", "".into()),
        ("m3u_profile_id", initial.profile_id.to_string()),
        ("last_activity", timestamp.clone()),
        ("request_count", "1".into()),
        ("active_streams", "1".into()),
        ("content_obj_type", source.kind.storage_name().into()),
        ("content_uuid", source.content_uuid.clone()),
        ("content_name", source.name.clone()),
        ("client_ip", request.client_ip_text()),
        (
            "client_user_agent",
            request.playback.header("user-agent").unwrap_or("").into(),
        ),
        ("utc_start", timeshift.utc_start.into()),
        ("utc_end", timeshift.utc_end.into()),
        ("offset", timeshift.offset.into()),
        ("worker_id", worker_id),
        ("connection_type", "redis_backed".into()),
        ("created_at", timestamp),
        ("bytes_sent", "0".into()),
        ("position_seconds", "0".into()),
        ("last_seek_byte", "0".into()),
        ("last_seek_percentage", "0".into()),
        ("total_content_size", "0".into()),
        ("last_seek_timestamp", "0".into()),
        ("user_id", user_id),
        ("rust_owner_token", metadata_token.to_owned()),
    ];

    Ok(fields
        .into_iter()
        .map(|(key, value)| (key.to_owned(), value))
        .collect())
}

fn key(id: &str) -> String {
    format!("vod_persistent_connection:{id}")
}
// The original delayed timer checks the current global session ID, not the
// timer's former local owner. Missing, malformed and failed reads are inactive.
async fn source_has_active_streams(connection: &mut ConnectionManager, id: &str) -> bool {
    let value: Result<Option<String>, _> = redis::cmd("HGET")
        .arg(key(id))
        .arg("active_streams")
        .query_async(connection)
        .await;
    value
        .ok()
        .flatten()
        .and_then(|value| dispatcharr_ingest_core::source_integer_literal(&value))
        .is_some_and(|value| value != "0" && !value.starts_with('-'))
}
async fn update(
    inner: &Inner,
    session: &Session,
    fields: &[(&str, String)],
) -> Result<(), dispatcharr_store::StoreError> {
    let script = Script::new(UPDATE);
    let mut invocation = script.prepare_invoke();
    invocation
        .key(key(&session.id))
        .arg(&session.metadata_token);
    for (field, value) in fields {
        invocation.arg(field).arg(value);
    }
    let saved: i64 = invocation
        .invoke_async(&mut inner.connection.clone())
        .await
        .map_err(|_| redis_error())?;
    if saved != 1 {
        return Err(unavailable());
    }
    Ok(())
}
fn monitor(session: Weak<Session>, pool: Weak<Inner>) {
    if let Some(scope) = session
        .upgrade()
        .and_then(|session| session.scope.get().cloned())
    {
        let Some(id) = session.upgrade().map(|session| session.id.clone()) else {
            return;
        };
        let authenticated_restart = session
            .upgrade()
            .is_some_and(|session| session.authenticated_restart);
        monitor_scope(id, scope, pool, authenticated_restart);
        return;
    }
    tokio::spawn(async move {
        let mut timer = tokio::time::interval(Duration::from_secs(1));
        loop {
            timer.tick().await;
            let (Some(session), Some(pool)) = (session.upgrade(), pool.upgrade()) else {
                return;
            };
            let lost = session.lease.lost();
            if *lost.borrow() {
                session.stop.send_replace(true);
                return;
            }
            if session.state.lock().await.closed {
                return;
            }
            // Private liveness/stop checks must not invent Source activity or
            // publish prefetched bytes. Only acknowledged body milestones do so.
            let mut connection = pool.connection.clone();
            let stop:bool=match Script::new("if redis.call('HGET',KEYS[1],'rust_owner_token')~=ARGV[1] then return 1 end return redis.call('EXISTS',KEYS[2])")
                .key(key(&session.id)).key(format!("vod_proxy:client:{}:stop",session.id)).arg(&session.metadata_token).invoke_async(&mut connection).await {
                    Ok(value)=>value,Err(_)=>{session.stop.send_replace(true);return;}
                };
            if stop {
                session.stop.send_replace(true);
                return;
            }
        }
    });
}

fn monitor_scope(id: String, scope: LocalScope, pool: Weak<Inner>, authenticated_restart: bool) {
    tokio::spawn(async move {
        let mut disconnected_since = None;
        loop {
            tokio::time::sleep(Duration::from_secs(1)).await;
            let Some(pool) = pool.upgrade() else {
                scope.stop();
                return;
            };
            // Unpublished setup has no Ticket to publish its later physical
            // cleanup failure. Preserve that diagnostic before eviction drops
            // the last actor/slot and allows global drain to report completion.
            if !scope.cleanup_succeeded() {
                pool.cleanup_failed.store(true, Ordering::Release);
            }
            if *pool.stopped.borrow() {
                scope.stop();
            }
            let same = pool
                .sessions
                .lock()
                .await
                .get(&id)
                .and_then(|session| session.scope.get())
                .is_some_and(|value| value.same_scope(&scope));
            if !same {
                return;
            }
            if scope.can_evict().await {
                let mut registry = pool.sessions.lock().await;
                if registry
                    .get(&id)
                    .and_then(|session| session.scope.get())
                    .is_some_and(|value| value.same_scope(&scope))
                {
                    registry.remove(&id);
                }
                return;
            }
            // Read-only classification/physical heartbeat may be retried as a
            // future operation. Never replay an uncertain allocation/mutation.
            if authenticated_restart {
                // A typed disconnect can precede the classified absence.
                // Preserve an already-owned body briefly, never admit new
                // readers on an unavailable or indeterminate observation.
                match scope.retire_missing().await {
                    Ok(()) | Err(recovery::Error::StillOwned) => disconnected_since = None,
                    Err(recovery::Error::Capacity) => {}
                    Err(recovery::Error::Disconnected)
                        if disconnected_since
                            .get_or_insert_with(tokio::time::Instant::now)
                            .elapsed()
                            < Duration::from_secs(10) => {}
                    Err(_) => scope.stop(),
                }
            } else {
                let _ = scope.retire_missing().await;
            }
        }
    });
}
fn scope_error(error: recovery::Error) -> dispatcharr_store::StoreError {
    match error {
        recovery::Error::ProfileFull => provider_full(),
        recovery::Error::StillOwned | recovery::Error::PartialState => {
            dispatcharr_store::StoreError::Unsupported(
                "VOD recovery state requires a separately verified transition",
            )
        }
        _ => redis_error(),
    }
}
struct ScopeEvent {
    producer: Producer,
    event: Transition,
}
impl StopHook for ScopeEvent {
    fn emit(&self) -> dispatcharr_contracts::BoxFuture<'_, ()> {
        Box::pin(async move {
            let _ = self.producer.emit(self.event.clone()).await;
        })
    }
}
fn scope_hook(pool: &Inner, event: Transition) -> Option<Arc<dyn StopHook>> {
    pool.events
        .lock()
        .unwrap()
        .clone()
        .map(|producer| Arc::new(ScopeEvent { producer, event }) as Arc<dyn StopHook>)
}

impl Ticket {
    pub(crate) fn departing_pull(&self) -> Option<oneshot::Receiver<()>> {
        // Recovery owns a separate generation ledger. This path uses only
        // the local legacy reservation, whose active count moves under state.
        if self.session.uses_scoped_body_lifecycle() || !self.body_started.load(Ordering::Acquire) {
            return None;
        }
        let pool = self.pool.upgrade()?;
        let mut state = self.session.state.try_lock().ok()?;
        if *pool.stopped.borrow()
            || *self.session.stop.borrow()
            || state.active != 1
            || !state.reserved
            || state.closed
            || state.departing_pull.is_some()
        {
            return None;
        }
        let (joined, cancelled) = oneshot::channel();
        state.departing_pull = Some(joined);
        Some(cancelled)
    }

    pub(crate) fn source_blocks(&self) -> bool {
        self.session.uses_scoped_body_lifecycle()
    }
    pub fn progress(&self) -> Progress {
        Progress {
            session: Arc::downgrade(&self.session),
            pool: self.pool.clone(),
            acknowledged: 0,
            reported: 0,
            checked_boundary: 0,
            public_stopped: false,
            body: self.scope_body.clone(),
        }
    }
    pub async fn begin_body(&self) {
        if let Some(body) = &self.scope_body {
            body.started();
        }
        if self.body_started.swap(true, Ordering::AcqRel) || !self.is_new {
            return;
        }
        if let Some(pool) = self.pool.upgrade() {
            let events = pool.events.lock().unwrap().clone();
            if let Some(events) = events {
                let _ = events.emit(transition(&self.session, true)).await;
            }
        }
    }
}
fn transition(session: &Session, started: bool) -> Transition {
    transition_data(&session.source, &session.client_ip, started)
}
fn transition_data(source: &VodSource, client_ip: &str, started: bool) -> Transition {
    Transition {
        started,
        content_name: source.name.clone(),
        content_uuid: source.content_uuid.clone(),
        client_ip: client_ip.into(),
        user_id: source
            .user
            .as_ref()
            .map_or_else(|| "0".into(), |user| user.id.to_string()),
        username: source.user.as_ref().map(|user| user.username.clone()),
    }
}

struct CleanupFinished(Arc<Inner>);
impl Drop for CleanupFinished {
    fn drop(&mut self) {
        self.0.pending_cleanups.fetch_sub(1, Ordering::AcqRel);
    }
}
impl Drop for Ticket {
    fn drop(&mut self) {
        if !self.cleanup_needed {
            return;
        }
        let session = self.session.clone();
        let emit_stop = !self.is_new || self.body_started.load(Ordering::Acquire);
        let request_permit = self._request.take();
        let Some(pool) = self.pool.upgrade() else {
            return;
        };
        let scoped = self.scope_body.take();
        if let (Some(scope), Some(body)) = (
            session
                .scope
                .get()
                .filter(|scope| scope.activated())
                .cloned(),
            scoped.as_ref(),
        ) {
            let body = body.clone();
            pool.pending_cleanups.fetch_add(1, Ordering::AcqRel);
            tokio::spawn(async move {
                let _request = request_permit;
                let _finished = CleanupFinished(pool.clone());
                {
                    let mut state = session.state.lock().await;
                    if state.active > 0 {
                        state.active -= 1;
                    }
                    state.epoch += 1;
                }
                let result = scope.close(&body).await;
                if result
                    .as_ref()
                    .is_err_and(|error| !matches!(error, recovery::Error::ObserverTimeout))
                {
                    pool.cleanup_failed.store(true, Ordering::Release);
                }
                if scope.can_evict().await {
                    let mut registry = pool.sessions.lock().await;
                    if registry
                        .get(&session.id)
                        .and_then(|current| current.scope.get())
                        .is_some_and(|value| value.same_scope(&scope))
                    {
                        registry.remove(&session.id);
                    }
                }
            });
            return;
        }
        let legacy_scope = match (session.scope.get(), scoped.as_ref()) {
            (Some(scope), Some(body)) => scope.detach_legacy(body),
            _ => None,
        };
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            pool.pending_cleanups.fetch_add(1, Ordering::AcqRel);
            runtime.spawn(async move {
                // Bound delayed timers and retain physical admission through
                // their Redis reply and event queue work after body drop.
                let _request_permit = request_permit;
                let _legacy_scope = legacy_scope;
                let _finished=CleanupFinished(pool.clone());
                let mut state = session.state.lock().await;
                if state.active == 0 {
                    return;
                }
                state.active -= 1;
                if state.active == 0 {
                    state.departing_pull = None;
                }
                state.epoch += 1;
                let epoch = state.epoch;
                let mut connection = pool.connection.clone();
                let count = match Script::new(LEAVE).key(key(&session.id)).arg(&session.metadata_token).arg(now()).invoke_async::<i64>(&mut connection).await {
                    Ok(count)=>count,Err(_)=>{pool.cleanup_failed.store(true,Ordering::Release);-1}
                };
                if state.active != 0 {
                    return;
                }
                if count < 0 {
                    state.closed = true;
                }
                if state.reserved {
                    if session.lease.release_provider().await.is_err() {
                        pool.cleanup_failed.store(true,Ordering::Release);
                        state.closed = true;
                    }
                    state.reserved = false;
                }
                drop(state);
                let mut shutdown=pool.stopped.subscribe();
                if !*shutdown.borrow(){tokio::select!{_=tokio::time::sleep(pool.idle_grace)=>{},_=shutdown.changed()=>{}}}
                // The previous local Session may have been replaced while
                // retaining this public ID. Share the true physical transition
                // lock; do not turn an observer timeout into early cleanup.
                {
                    let _generation = match session.scope.get() {
                        Some(scope) => match scope.legacy_finalizer_guard(
                            &session.lease, &session.metadata_token,
                            session.candidate.profile_id,
                        ).await {
                            Some(guard) => Some(guard),
                            None => return,
                        },
                        None => None,
                    };
                    let state = session.state.lock().await;
                    if state.active != 0 || state.epoch != epoch
                        || !pool.sessions.lock().await.get(&session.id)
                            .is_some_and(|current| Arc::ptr_eq(current, &session))
                    {
                        return;
                    }
                }
                // Every eligible Source timer can announce the stop. Do this
                // outside state/pool locks: a statistics read must not form a
                // cycle with the lifecycle actor. Physical cleanup remains
                // independently epoch/token fenced below.
                if count == 0 && emit_stop && !source_has_active_streams(&mut connection, &session.id).await {
                    let events = pool.events.lock().unwrap().clone();
                    if let Some(events) = events {
                        let _ = events.emit(transition(&session, false)).await;
                    }
                }
                // Publication above holds no lifecycle/Session/registry lock.
                // Revalidate after it, retaining serial -> Session -> registry
                // order through the actual metadata decision and lease release.
                let _generation = match session.scope.get() {
                    Some(scope) => match scope.legacy_finalizer_guard(
                        &session.lease, &session.metadata_token,
                        session.candidate.profile_id,
                    ).await {
                        Some(guard) => Some(guard),
                        None => return,
                    },
                    None => None,
                };
                let mut state = session.state.lock().await;
                if state.active != 0 || state.epoch != epoch
                    || !pool.sessions.lock().await.get(&session.id)
                        .is_some_and(|current| Arc::ptr_eq(current, &session))
                {
                    return;
                }
                let deleted = match Script::new(DELETE).key(key(&session.id)).arg(&session.metadata_token).invoke_async::<i64>(&mut connection).await {
                    Ok(deleted)=>deleted,Err(_)=>{pool.cleanup_failed.store(true,Ordering::Release);-1}
                };

                if deleted == 0 {
                    // Still this owned public token, now active. Unlike a
                    // foreign/missing hash, releasing would kill its readers.
                    return;
                }
                state.closed = true;
                session.stop.send_replace(true);
                if session.lease.release().await.is_err(){pool.cleanup_failed.store(true,Ordering::Release);}
                if _legacy_scope.is_some(){session.lease.settlement().wait().await;}
                let mut sessions = pool.sessions.lock().await;
                if sessions
                    .get(&session.id)
                    .is_some_and(|value| Arc::ptr_eq(value, &session))
                {
                    sessions.remove(&session.id);
                }
                drop(sessions);drop(state);
            });
        }
    }
}

// A canceled SETUP may have reached Redis even if its reply was never read.
// Cleanup is token fenced and cannot delete another worker's session.
struct PendingCreate {
    connection: ConnectionManager,
    id: String,
    token: String,
    armed: bool,
    pool: Weak<Inner>,
    retained_request: Option<Arc<OwnedSemaphorePermit>>,
    retained_slot: Option<Arc<OwnedSemaphorePermit>>,
}
const DISCARD_PENDING: &str = "if redis.call('HGET',KEYS[1],'rust_owner_token')==ARGV[1] then return redis.call('DEL',KEYS[1]) end return 0";

async fn discard_pending(
    connection: &mut ConnectionManager,
    id: &str,
    token: &str,
) -> redis::RedisResult<i64> {
    Script::new(DISCARD_PENDING)
        .key(key(id))
        .arg(token)
        .invoke_async(connection)
        .await
}

impl PendingCreate {
    async fn discard(&mut self) -> Result<(), dispatcharr_store::StoreError> {
        // If the reply is unknown, keep the token fence armed. Drop retains
        // physical admission while it settles that same idempotent deletion.
        tokio::time::timeout(
            Duration::from_secs(2),
            discard_pending(&mut self.connection, &self.id, &self.token),
        )
        .await
        .map_err(|_| redis_error())?
        .map_err(|_| redis_error())?;
        self.armed = false;
        Ok(())
    }
}
impl Drop for PendingCreate {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let mut connection = self.connection.clone();
        let id = self.id.clone();
        let token = self.token.clone();
        let pool = self.pool.upgrade();
        let retained_request = self.retained_request.take();
        let retained_slot = self.retained_slot.take();
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            if let Some(pool) = &pool {
                pool.pending_cleanups.fetch_add(1, Ordering::AcqRel);
            }
            runtime.spawn(async move {
                let _retained = (retained_request, retained_slot);
                let cleaned = discard_pending(&mut connection, &id, &token).await;
                if let Some(pool) = pool {
                    if cleaned.is_err() {
                        pool.cleanup_failed.store(true, Ordering::Release);
                    }
                    pool.pending_cleanups.fetch_sub(1, Ordering::AcqRel);
                }
            });
        }
    }
}

fn admission_error(error: dispatcharr_clients::Error, limit: i32) -> dispatcharr_store::StoreError {
    if error == dispatcharr_clients::Error::UserLimit {
        dispatcharr_store::StoreError::StatusBody(
            429,
            json!({"error":format!("Stream limit exceeded ({limit} concurrent streams allowed)")}),
        )
    } else {
        redis_error()
    }
}

// Source scores content=10, matching timeshift=7, IP=5, UA=3, and
// accepts scores>=13. VOD has no user-identity criterion in this idle matcher.
fn idle_match(
    registry: &HashMap<String, Arc<Session>>,
    source: &VodSource,
    request: &VodRequest,
) -> Option<Arc<Session>> {
    let timeshift = crate::offset::requested(&request.playback);
    registry
        .values()
        .filter(|session| {
            session.source.content_uuid == source.content_uuid
                && session.source.kind == source.kind
                && !*session.stop.borrow()
                && session
                    .state
                    .try_lock()
                    .is_ok_and(|state| state.active == 0 && !state.closed)
        })
        .filter_map(|session| {
            let score = crate::offset::idle_score(
                session.timeshift.borrowed() == timeshift,
                session.client_ip == request.client_ip_text(),
                !session.user_agent.is_empty()
                    && Some(session.user_agent.as_str()) == request.playback.header("user-agent"),
            )?;
            Some((score, session.last_request.load(Ordering::Relaxed), session))
        })
        .max_by_key(|(score, last_request, _)| (*score, *last_request))
        .map(|(_, _, session)| session.clone())
}

#[cfg(test)]
#[path = "session_restart_peer.rs"]
mod session_restart_peer;

#[path = "session_signed_idle_alias.rs"]
mod session_signed_idle_alias;
pub(crate) use session_signed_idle_alias::signed_alias_claim;

#[path = "session_user_lookup.rs"]
mod session_user_lookup;

#[path = "session_stop_owned.rs"]
mod session_stop_owned;

#[cfg(test)]
mod authenticated_restart_tests {
    use super::{current_retained_provider, ordinary_restart_matches, same_provider};
    use crate::{ContentKind, VodCandidate, VodRequest, VodSource};
    use dispatcharr_contracts::PlaybackRequest;
    use dispatcharr_store::User;

    fn fixture() -> (VodSource, VodRequest) {
        (
            VodSource {
                kind: ContentKind::Movie,
                content_uuid: "content".into(),
                name: "Synthetic playback fixture".into(),
                user: Some(User {
                    id: 7,
                    username: "viewer".into(),
                    password: "!".into(),
                    is_active: true,
                    user_level: 0,
                    stream_limit: 1,
                    custom_properties: serde_json::json!({}),
                }),
                candidates: vec![VodCandidate {
                    relation_id: 1,
                    account_id: 2,
                    profile_id: 3,
                    max_streams: 1,
                    credential_pool: None,
                    url: "http://fixture.invalid/movie".into(),
                    user_agent: "fixture".into(),
                }],
                redirect: false,
                limit_policy: Default::default(),
            },
            VodRequest {
                resolved_client_ip: None,
                playback: PlaybackRequest {
                    channel_id: "content".into(),
                    peer_ip: "127.0.0.1".parse().unwrap(),
                    headers: vec![],
                    query: vec![],
                },
                kind: "movie".into(),
                content_id: "content".into(),
                session_id: Some("retained".into()),
                profile_id: None,
            },
        )
    }

    #[test]
    fn ordinary_idle_reservation_uses_fresh_limits_only_for_the_retained_provider() {
        let (mut source, _) = fixture();
        let mut previous = source.candidates[0].clone();
        previous.credential_pool = Some(dispatcharr_coordination::CredentialPool {
            counter_key: "owned-test-pool".into(),
            max_streams: 2,
        });
        let current = &mut source.candidates[0];
        current.credential_pool = previous.credential_pool.clone();
        current.credential_pool.as_mut().unwrap().max_streams = 1;
        current.max_streams = 1;
        previous.max_streams = 2;
        assert_eq!(
            current_retained_provider(&previous, &source.candidates)
                .unwrap()
                .max_streams,
            1
        );
        assert!(!same_provider(&previous, &source.candidates[0]));
        let different: [fn(&mut VodCandidate); 6] = [
            |candidate: &mut VodCandidate| candidate.profile_id += 1,
            |candidate: &mut VodCandidate| candidate.relation_id += 1,
            |candidate: &mut VodCandidate| candidate.account_id += 1,
            |candidate: &mut VodCandidate| candidate.url.push_str("/different"),
            |candidate: &mut VodCandidate| candidate.user_agent.push_str("-different"),
            |candidate: &mut VodCandidate| {
                candidate
                    .credential_pool
                    .as_mut()
                    .unwrap()
                    .counter_key
                    .push_str("-different")
            },
        ];
        for change in different {
            let mut candidate = source.candidates[0].clone();
            change(&mut candidate);
            assert!(current_retained_provider(&previous, &[candidate]).is_none());
        }
    }

    #[test]
    fn ordinary_restart_requires_fresh_matching_subject() {
        let (retained, request) = fixture();
        let allows = |source: &VodSource, user| {
            ordinary_restart_matches(
                source,
                &request,
                user,
                "retained",
                &retained,
                &retained.candidates[0],
            )
        };
        assert!(allows(&retained, Some(7)));
        assert!(!allows(&retained, None));
        assert!(!allows(&retained, Some(8)));
        let mut other = retained.clone();
        other.user.as_mut().unwrap().id = 8;
        assert!(!allows(&other, Some(8)));
        let mut anonymous = retained.clone();
        anonymous.user = None;
        assert!(!allows(&anonymous, Some(7)));
    }

    #[test]
    fn ordinary_restart_rejects_other_ids_content_profiles_and_request_modes() {
        let (retained, request) = fixture();
        let allows = |source: &VodSource, request: &VodRequest| {
            ordinary_restart_matches(
                source,
                request,
                Some(7),
                "retained",
                &retained,
                &retained.candidates[0],
            )
        };
        let mut other = retained.clone();
        other.candidates[0].profile_id += 1;
        assert!(!allows(&other, &request));
        other = retained.clone();
        other.candidates[0].max_streams += 1;
        assert!(!allows(&other, &request));
        other = retained.clone();
        other.candidates[0].relation_id += 1;
        assert!(!allows(&other, &request));
        other = retained.clone();
        other.candidates[0].url.push_str("/new");
        assert!(!allows(&other, &request));
        other = retained.clone();
        other.content_uuid.push_str("-other");
        assert!(!allows(&other, &request));
        other = retained.clone();
        other.redirect = true;
        assert!(!allows(&other, &request));

        let mut modified = request.clone();
        modified.session_id = Some("alias".into());
        assert!(!allows(&retained, &modified));
        modified.session_id = None;
        assert!(!allows(&retained, &modified));
        modified = request.clone();
        modified.playback.headers = vec![("range".into(), "bytes=0-".into())];
        assert!(!allows(&retained, &modified));
        modified = request;
        modified.playback.query = vec![("utc_start".into(), "2025-01-01T00:00:00Z".into())];
        assert!(!allows(&retained, &modified));
    }
}
