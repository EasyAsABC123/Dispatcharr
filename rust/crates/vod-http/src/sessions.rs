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
use tokio::sync::{watch, Mutex, OwnedSemaphorePermit, Semaphore};

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
 if redis.call('EXISTS',KEYS[4])~=0 then return -3 end
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
return count
"#;
const DELETE: &str = r#"
if redis.call('HGET',KEYS[1],'rust_owner_token')~=ARGV[1] then return 0 end
if tonumber(redis.call('HGET',KEYS[1],'active_streams') or '0')~=0 then return 0 end
redis.call('DEL',KEYS[1])
return 1
"#;

struct State {
    active: usize,
    reserved: bool,
    closed: bool,
    epoch: u64,
}
pub(crate) struct Session {
    pub id: String,
    pub source: VodSource,
    pub candidate: VodCandidate,
    pub stop: watch::Sender<bool>,
    pub bytes: AtomicU64,
    pub content_length: AtomicU64,
    last_request: AtomicU64,
    client_ip: String,
    user_agent: String,
    lease: Arc<Lease>,
    scope: std::sync::OnceLock<LocalScope>,
    metadata_token: String,
    state: Mutex<State>,
    _slot: Arc<OwnedSemaphorePermit>,
}
struct Inner {
    connection: ConnectionManager,
    coordinator: Coordinator,
    sessions: Mutex<HashMap<String, Arc<Session>>>,
    slots: Arc<Semaphore>,
    requests: Arc<Semaphore>,
    idle_grace: Duration,
    stopped: watch::Sender<bool>,
    max_requests: usize,
    max_sessions: usize,
    recovery: std::sync::Mutex<Option<recovery::Recovery>>,
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
    body: Option<ScopedBody>,
}
impl Progress {
    pub async fn acknowledge(&mut self, bytes: usize) {
        self.acknowledged = self.acknowledged.saturating_add(bytes as u64);
        // Source writes after100 completed8192-byte generator yields. Count
        // consumed bytes, independently of network/TLS/frame fragmentation.
        let milestone = (self.acknowledged / 819200) * 819200;
        self.record(milestone).await;
    }
    pub async fn finish(&mut self) {
        // Requests yields a final short8192-byte block too. If that is block
        //100/200/etc, Source stores its actual byte count rather than rounding.
        if self.acknowledged > 0 && self.acknowledged.div_ceil(8192).is_multiple_of(100) {
            self.record(self.acknowledged).await;
        }
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
pub(crate) struct Ticket {
    pub session: Arc<Session>,
    pool: Weak<Inner>,
    _request: Option<Arc<OwnedSemaphorePermit>>,
    is_new: bool,
    body_started: AtomicBool,
    scope_body: Option<ScopedBody>,
    cleanup_needed: bool,
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
        let registry = self.inner.sessions.try_lock().map_err(|_| unavailable())?;
        if !registry.is_empty()
            || self.inner.requests.available_permits() != self.inner.max_requests
            || self.inner.slots.available_permits() != self.inner.max_sessions
            || self.inner.max_requests > 4096
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
        *mode = Some(
            recovery::Recovery::new(
                self.inner.coordinator.clone(),
                self.inner.connection.clone(),
                namespace.to_owned(),
                self.inner.max_sessions.min(1024),
                Duration::from_secs(3),
            )
            .map_err(|_| unavailable())?,
        );
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
            let scope = LocalScope::adopt(
                mode,
                ScopeInitial {
                    id: session.id.clone(),
                    lease: session.lease.clone(),
                    metadata_token: session.metadata_token.clone(),
                    profile_id: session.candidate.profile_id,
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
                    bodies: self.inner.max_requests,
                    controls: self.inner.max_requests,
                    idle_grace: self.inner.idle_grace,
                },
            )
            .await
            .map_err(scope_error)?;
            session.scope.set(scope).map_err(|_| unavailable())?;
        }
        let hook = scope_hook(&self.inner, transition(session, false));
        session
            .scope
            .get()
            .expect("installed scope")
            .attach_reserved(request, is_new, hook)
            .await
            .map(Some)
            .map_err(scope_error)
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
                .is_none_or(recovery::Recovery::cleanup_succeeded)
    }
    pub fn stop_admission(&self) {
        self.inner.stopped.send_replace(true);
        self.inner.slots.close();
        self.inner.requests.close();
    }
    pub async fn drain(&self) {
        loop {
            let sessions = self.inner.sessions.lock().await;
            for session in sessions.values() {
                session.stop.send_replace(true);
            }
            if sessions.is_empty()
                && self.inner.requests.available_permits() == self.inner.max_requests
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
            if count < (candidate.max_streams as i64) {
                return Ok(candidate.url.clone());
            }
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
        previous: Arc<Session>,
        scope: LocalScope,
    ) -> Result<Ticket, dispatcharr_store::StoreError> {
        if id != previous.id {
            return Err(dispatcharr_store::StoreError::Unsupported(
                "Recovery idle adoption is not yet implemented",
            ));
        }
        let candidate = source.candidates.first().ok_or_else(unavailable)?.clone();
        // A fresh stable classification is safe; it never repeats an uncertain
        // allocation. Owned/partial cases remain explicit until separately gated.
        match scope.retire_missing_retained(request_permit.clone()).await {
            Ok(()) => {}
            Err(recovery::Error::Capacity) => {}
            Err(recovery::Error::StillOwned) => {
                let hook = scope_hook(
                    &self.inner,
                    transition_data(&previous.source, &previous.client_ip, false),
                );
                let body = scope
                    .attach_entry_pending(request_permit.clone(), hook)
                    .await
                    .map_err(scope_error)?;
                let fields = metadata_fields(
                    id,
                    &source,
                    request,
                    &previous.candidate,
                    &previous.metadata_token,
                );
                let limit = source
                    .user
                    .as_ref()
                    .map_or(0, |user| user.stream_limit.max(0));
                scope
                    .enter_current(
                        &body,
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
                {
                    let mut state = previous.state.lock().await;
                    state.active += 1;
                    state.epoch += 1;
                }
                previous.last_request.store(millis(), Ordering::Relaxed);
                return Ok(Ticket {
                    session: previous,
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
        let fields = metadata_fields(id, &source, request, &candidate, "recovery-pending");
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
                    credential_pool: None,
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
        let (lease, metadata_token) = scope.current_identity().await;
        let session = Arc::new(Session {
            id: id.into(),
            source,
            candidate,
            stop: previous.stop.clone(),
            bytes: AtomicU64::new(0),
            content_length: AtomicU64::new(0),
            last_request: AtomicU64::new(millis()),
            client_ip: request.client_ip_text(),
            user_agent: request.playback.header("user-agent").unwrap_or("").into(),
            lease,
            metadata_token,
            scope: std::sync::OnceLock::new(),
            state: Mutex::new(State {
                active: 1,
                reserved: true,
                closed: false,
                epoch: 0,
            }),
            _slot: previous._slot.clone(),
        });
        session
            .scope
            .set(scope.clone())
            .map_err(|_| unavailable())?;
        let mut registry = self.inner.sessions.lock().await;
        if registry.get(id).is_none_or(|current| {
            current
                .scope
                .get()
                .is_none_or(|value| !value.same_scope(&scope))
        }) {
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
    pub async fn enter(
        &self,
        id: &str,
        source: VodSource,
        request: &VodRequest,
    ) -> Result<Ticket, dispatcharr_store::StoreError> {
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
            pool.enter_owned(&id, source, &request, request_permit)
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
    ) -> Result<Ticket, dispatcharr_store::StoreError> {
        // This lock serializes bounded session SETUP only, never media/network body I/O.
        let mut registry = self.inner.sessions.lock().await;
        if self.stopped() {
            return Err(unavailable());
        }
        let reusable = registry
            .get(id)
            .cloned()
            .or_else(|| idle_match(&registry, &source, request));
        if let Some(session) = reusable {
            if session.source.content_uuid != source.content_uuid
                || session.source.kind != source.kind
            {
                return Err(dispatcharr_store::StoreError::Forbidden);
            }
            drop(registry);
            if let Some(scope) = session.scope.get().cloned() {
                if !scope.activated() {
                    // Do not wait for the periodic monitor to observe Redis
                    // loss. This fresh read retains this request through its
                    // physical reply, even if the observer goes away.
                    match scope.retire_missing_retained(request_permit.clone()).await {
                        Err(recovery::Error::StillOwned) => {}
                        Ok(()) | Err(recovery::Error::Capacity) => {
                            return self
                                .enter_recovered(
                                    id,
                                    source,
                                    request,
                                    request_permit,
                                    session,
                                    scope,
                                )
                                .await;
                        }
                        Err(error) => return Err(scope_error(error)),
                    }
                } else {
                    return self
                        .enter_recovered(id, source, request, request_permit, session, scope)
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
                    dispatcharr_clients::vod::admit(
                        &mut connection,
                        dispatcharr_clients::vod::Admission {
                            session_id: &session.id,
                            fields: &fields,
                            limit: user.stream_limit as u32,
                            policy: &source.limit_policy,
                            create: false,
                            token: &session.metadata_token,
                            scan_limit: 50000,
                            timeout: Duration::from_secs(2),
                            ttl: Duration::from_secs(3600),
                        },
                    )
                    .await
                    .map_err(|error| admission_error(error, user.stream_limit))?;
                }
            }
            let reserved_here = !state.reserved;
            if reserved_here {
                session
                    .lease
                    .reserve_provider(ProviderReservation {
                        profile_id: Some(session.candidate.profile_id),
                        max_streams: session.candidate.max_streams,
                        credential_pool: None,
                    })
                    .await
                    .map_err(|_| unavailable())?;
                state.reserved = true;
            }
            session.last_request.store(millis(), Ordering::Relaxed);
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
                    return self
                        .enter_recovered(id, source, request, request_permit, previous, scope)
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
        let fields = metadata_fields(id, &source, request, initial, &metadata_token);
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
        let created = dispatcharr_clients::vod::admit(
            &mut connection,
            dispatcharr_clients::vod::Admission {
                session_id: id,
                fields: &fields,
                limit: limit as u32,
                policy: &source.limit_policy,
                create: true,
                token: &metadata_token,
                scan_limit: 50000,
                timeout: Duration::from_secs(2),
                ttl: Duration::from_secs(10),
            },
        )
        .await
        .map_err(|error| admission_error(error, limit))?;
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
                    credential_pool: None,
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
                    ))
                }
                Err(_) => return Err(redis_error()),
            }
        }
        let (candidate, lease) = selected.ok_or_else(unavailable)?;
        let saved: i64 = Script::new(UPDATE)
            .key(key(id))
            .arg(&metadata_token)
            .arg("stream_url")
            .arg(&candidate.url)
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
            last_request: AtomicU64::new(millis()),
            client_ip: request.client_ip_text(),
            user_agent: request.playback.header("user-agent").unwrap_or("").into(),
            lease: Arc::new(lease),
            scope: std::sync::OnceLock::new(),
            metadata_token,
            state: Mutex::new(State {
                active: 1,
                reserved: true,
                closed: false,
                epoch: 0,
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
        let mut connection = self.inner.connection.clone();
        if !connection
            .exists::<_, bool>(key(id))
            .await
            .map_err(|_| redis_error())?
        {
            return Ok(false);
        }
        let _: () = connection
            .set_ex(format!("vod_proxy:client:{id}:stop"), "true", 60)
            .await
            .map_err(|_| redis_error())?;
        if let Some(session) = self.inner.sessions.lock().await.get(id) {
            session.stop.send_replace(true);
        }
        Ok(true)
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

fn metadata_fields(
    id: &str,
    source: &VodSource,
    request: &VodRequest,
    initial: &VodCandidate,
    metadata_token: &str,
) -> BTreeMap<String, String> {
    let timestamp = now();
    let user_id = source
        .user
        .as_ref()
        .map_or("unknown".into(), |user| user.id.to_string());
    let worker_id = format!("rust-vod:{}", std::process::id());
    let fields = vec![
        ("session_id", id.to_string()),
        ("stream_url", initial.url.clone()),
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
        ("utc_start", "".into()),
        ("utc_end", "".into()),
        ("offset", "".into()),
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

    fields
        .into_iter()
        .map(|(key, value)| (key.to_owned(), value))
        .collect()
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
        monitor_scope(id, scope, pool);
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

fn monitor_scope(id: String, scope: LocalScope, pool: Weak<Inner>) {
    tokio::spawn(async move {
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
            let _ = scope.retire_missing().await;
        }
    });
}
fn scope_error(error: recovery::Error) -> dispatcharr_store::StoreError {
    match error {
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
    pub fn progress(&self) -> Progress {
        Progress {
            session: Arc::downgrade(&self.session),
            pool: self.pool.clone(),
            acknowledged: 0,
            reported: 0,
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
                let mut state = session.state.lock().await;
                if state.active != 0 || state.epoch != epoch {
                    return;
                }
                let deleted = match Script::new(DELETE).key(key(&session.id)).arg(&session.metadata_token).invoke_async::<i64>(&mut connection).await {
                    Ok(deleted)=>deleted,Err(_)=>{pool.cleanup_failed.store(true,Ordering::Release);0}
                };
                if deleted == 0 {
                    state.closed = true;
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
                let cleaned: Result<i64,_> = Script::new("if redis.call('HGET',KEYS[1],'rust_owner_token')==ARGV[1] then return redis.call('DEL',KEYS[1]) end return 0")
                    .key(key(&id)).arg(token).invoke_async(&mut connection).await;
                if let Some(pool)=pool{if cleaned.is_err(){pool.cleanup_failed.store(true,Ordering::Release);}pool.pending_cleanups.fetch_sub(1,Ordering::AcqRel);}
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

// Source scores content=10, matching empty timeshift=7, IP=5, UA=3, and
// accepts scores>=13. VOD has no user-identity criterion in this idle matcher.
fn idle_match(
    registry: &HashMap<String, Arc<Session>>,
    source: &VodSource,
    request: &VodRequest,
) -> Option<Arc<Session>> {
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
        .max_by_key(|session| {
            let ip = if session.client_ip == request.client_ip_text() {
                5
            } else {
                0
            };
            let ua = if !session.user_agent.is_empty()
                && Some(session.user_agent.as_str()) == request.playback.header("user-agent")
            {
                3
            } else {
                0
            };
            (17 + ip + ua, session.last_request.load(Ordering::Relaxed))
        })
        .cloned()
}

#[cfg(test)]
#[path = "session_restart_peer.rs"]
mod session_restart_peer;

#[path = "session_user_lookup.rs"]
mod session_user_lookup;
