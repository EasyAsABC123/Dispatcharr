use crate::resolver::{VodCandidate, VodRequest, VodSource};
use dispatcharr_coordination::{AcquireRequest, Coordinator, Lease, ProviderReservation};
use redis::{aio::MultiplexedConnection, AsyncCommands, Script};
use serde_json::json;
use std::{collections::HashMap, sync::{Arc, Weak, atomic::{AtomicU64, Ordering}}, time::{Duration, SystemTime, UNIX_EPOCH}};
use tokio::sync::{watch, Mutex, OwnedSemaphorePermit, Semaphore};

fn now() -> String { SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs_f64().to_string() }
fn unavailable() -> dispatcharr_store::StoreError { dispatcharr_store::StoreError::StatusBody(503, json!("No available stream")) }
fn redis_error() -> dispatcharr_store::StoreError { dispatcharr_store::StoreError::StatusBody(503, json!({"error":"Redis unavailable"})) }

const CREATE: &str = r#"
if redis.call('EXISTS',KEYS[1])~=0 then return 0 end
for i=1,#ARGV,2 do redis.call('HSET',KEYS[1],ARGV[i],ARGV[i+1]) end
redis.call('EXPIRE',KEYS[1],3600)
return 1
"#;
const UPDATE: &str = r#"
if redis.call('HGET',KEYS[1],'rust_owner_token')~=ARGV[1] then return 0 end
for i=2,#ARGV,2 do redis.call('HSET',KEYS[1],ARGV[i],ARGV[i+1]) end
redis.call('EXPIRE',KEYS[1],3600)
return 1
"#;
const ENTER: &str = r#"
if redis.call('HGET',KEYS[1],'rust_owner_token')~=ARGV[1] then return 0 end
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

struct State { active: usize, reserved: bool, closed: bool, epoch: u64 }
pub(crate) struct Session {
    pub id: String,
    pub source: VodSource,
    pub candidate: VodCandidate,
    pub stop: watch::Sender<bool>,
    pub bytes: AtomicU64,
    lease: Lease,
    state: Mutex<State>,
    _slot: OwnedSemaphorePermit,
}
struct Inner {
    connection: MultiplexedConnection,
    coordinator: Coordinator,
    sessions: Mutex<HashMap<String, Arc<Session>>>,
    slots: Arc<Semaphore>,
    requests: Arc<Semaphore>,
    idle_grace: Duration,
}
#[derive(Clone)]
pub(crate) struct SessionPool { inner: Arc<Inner> }
pub(crate) struct Ticket { pub session: Arc<Session>, pool: Weak<Inner>, _request: OwnedSemaphorePermit }

impl SessionPool {
    pub async fn connect(url: &str, coordinator: Coordinator, max_sessions: usize, max_requests: usize, idle_grace: Duration) -> Result<Self, dispatcharr_store::StoreError> {
        let connection = tokio::time::timeout(Duration::from_secs(3), redis::Client::open(url).map_err(|_| redis_error())?.get_multiplexed_async_connection_with_config(&redis::AsyncConnectionConfig::new().set_connection_timeout(Duration::from_secs(2)).set_response_timeout(Duration::from_secs(2))))
            .await.map_err(|_| redis_error())?.map_err(|_| redis_error())?;
        Ok(Self { inner: Arc::new(Inner { connection, coordinator, sessions: Mutex::new(HashMap::new()),
            slots: Arc::new(Semaphore::new(max_sessions)), requests: Arc::new(Semaphore::new(max_requests)), idle_grace }) })
    }

    pub async fn redirect_candidate(&self, source: &VodSource) -> Result<String, dispatcharr_store::StoreError> {
        for candidate in &source.candidates {
            match self.inner.coordinator.acquire_vod(AcquireRequest { channel_id: format!("redirect-{}",uuid::Uuid::new_v4()), provider_profile_id: Some(candidate.profile_id), provider_max_streams: candidate.max_streams, credential_pool: None }).await {
                Ok(lease) => { lease.release().await.map_err(|_| redis_error())?; return Ok(candidate.url.clone()); }
                Err(dispatcharr_coordination::Error::ProfileFull | dispatcharr_coordination::Error::CredentialFull) => continue,
                Err(_) => return Err(redis_error()),
            }
        }
        Err(unavailable())
    }

    pub async fn remember_user(&self, id: &str, user: i64) -> Result<(), dispatcharr_store::StoreError> {
        let mut connection = self.inner.connection.clone();
        let _: () = connection.set_ex(format!("vod_session_user:{id}"), user, 300).await.map_err(|_| redis_error())?;
        Ok(())
    }
    pub async fn user_for_session(&self, id: &str) -> Result<Option<i64>, dispatcharr_store::StoreError> {
        let mut connection = self.inner.connection.clone();
        connection.get(format!("vod_session_user:{id}")).await.map_err(|_| redis_error())
    }

    pub async fn enter(&self, id: &str, source: VodSource, request: &VodRequest) -> Result<Ticket, dispatcharr_store::StoreError> {
        let request_permit = self.inner.requests.clone().try_acquire_owned().map_err(|_| unavailable())?;
        // This lock serializes bounded session SETUP only, never media/network body I/O.
        let mut registry = self.inner.sessions.lock().await;
        if let Some(session) = registry.get(id).cloned() {
            if session.source.content_uuid != source.content_uuid || session.source.kind != source.kind ||
                session.source.user.as_ref().map(|user| user.id) != source.user.as_ref().map(|user| user.id) {
                return Err(dispatcharr_store::StoreError::Forbidden);
            }
            drop(registry);
            let mut state = session.state.lock().await;
            if state.closed || *session.stop.borrow() { return Err(unavailable()); }
            if !state.reserved {
                session.lease.reserve_provider(ProviderReservation { profile_id: Some(session.candidate.profile_id),
                    max_streams: session.candidate.max_streams, credential_pool: None }).await.map_err(|_| unavailable())?;
                state.reserved = true;
            }
            state.active += 1;
            state.epoch += 1;
            drop(state);
            // Own the request before sending ENTER: cancellation must still
            // undo the corresponding Redis increment on this connection.
            let ticket = Ticket { session, pool: Arc::downgrade(&self.inner), _request: request_permit };
            let mut connection = self.inner.connection.clone();
            let count: i64 = Script::new(ENTER).key(key(id)).arg(ticket.session.lease.token()).arg(now())
                .invoke_async(&mut connection).await.map_err(|_| redis_error())?;
            if count <= 0 { ticket.session.stop.send_replace(true); return Err(unavailable()); }
            return Ok(ticket);
        }
        let slot = self.inner.slots.clone().try_acquire_owned().map_err(|_| unavailable())?;
        let mut connection = self.inner.connection.clone();
        let existing: bool = connection.exists(key(id)).await.map_err(|_| redis_error())?;
        if existing { return Err(dispatcharr_store::StoreError::Unsupported("Legacy or cross-worker VOD session adoption is not implemented")); }
        let mut selected = None;
        for candidate in &source.candidates {
            match self.inner.coordinator.acquire_vod(AcquireRequest { channel_id: id.into(), provider_profile_id: Some(candidate.profile_id),
                provider_max_streams: candidate.max_streams, credential_pool: None }).await {
                Ok(lease) => { selected = Some((candidate.clone(), lease)); break; }
                Err(dispatcharr_coordination::Error::ProfileFull | dispatcharr_coordination::Error::CredentialFull) => continue,
                Err(dispatcharr_coordination::Error::OwnerHeld) => return Err(dispatcharr_store::StoreError::Unsupported("Cross-worker VOD session adoption is not implemented")),
                Err(_) => return Err(redis_error()),
            }
        }
        let (candidate, lease) = selected.ok_or_else(unavailable)?;
        let timestamp = now();
        let user_id = source.user.as_ref().map_or("unknown".into(), |user| user.id.to_string());
        let worker_id = format!("rust-vod:{}", std::process::id());
        let fields = vec![
            ("session_id", id.to_string()), ("stream_url", candidate.url.clone()), ("headers", "{}".into()),
            ("content_length", "".into()), ("content_type", "".into()), ("final_url", "".into()),
            ("m3u_profile_id", candidate.profile_id.to_string()), ("last_activity", timestamp.clone()),
            ("request_count", "1".into()), ("active_streams", "1".into()),
            ("content_obj_type", source.kind.storage_name().into()), ("content_uuid", source.content_uuid.clone()),
            ("content_name", source.name.clone()), ("client_ip", request.playback.peer_ip.to_string()),
            ("client_user_agent", request.playback.header("user-agent").unwrap_or("").into()),
            ("utc_start", "".into()), ("utc_end", "".into()), ("offset", "".into()),
            ("worker_id", worker_id), ("connection_type", "redis_backed".into()), ("created_at", timestamp),
            ("bytes_sent", "0".into()), ("position_seconds", "0".into()), ("last_seek_byte", "0".into()),
            ("last_seek_percentage", "0".into()), ("total_content_size", "0".into()), ("last_seek_timestamp", "0".into()),
            ("user_id", user_id), ("rust_owner_token", lease.token().into()),
        ];
        let mut pending = PendingCreate { connection: connection.clone(), id: id.into(), token: lease.token().into(), armed: true };
        let script = Script::new(CREATE);
        let mut invocation = script.prepare_invoke();
        invocation.key(key(id));
        for (field, value) in fields { invocation.arg(field).arg(value); }
        let created: i64 = invocation.invoke_async(&mut connection).await.map_err(|_| redis_error())?;
        if created != 1 { return Err(unavailable()); }
        let (stop, _) = watch::channel(false);
        let session = Arc::new(Session { id: id.into(), source, candidate, stop, bytes: AtomicU64::new(0), lease,
            state: Mutex::new(State { active: 1, reserved: true, closed: false, epoch: 0 }), _slot: slot });
        registry.insert(id.into(), session.clone());
        pending.armed = false;
        drop(registry);
        monitor(Arc::downgrade(&session), Arc::downgrade(&self.inner));
        Ok(Ticket { session, pool: Arc::downgrade(&self.inner), _request: request_permit })
    }

    pub async fn stop(&self, id: &str) -> Result<bool, dispatcharr_store::StoreError> {
        let mut connection = self.inner.connection.clone();
        if !connection.exists::<_, bool>(key(id)).await.map_err(|_| redis_error())? { return Ok(false); }
        let _: () = connection.set_ex(format!("vod_proxy:client:{id}:stop"), "true", 60).await.map_err(|_| redis_error())?;
        if let Some(session) = self.inner.sessions.lock().await.get(id) { session.stop.send_replace(true); }
        Ok(true)
    }

    pub async fn update(&self, session: &Session, fields: &[(&str, String)]) -> Result<(), dispatcharr_store::StoreError> {
        update(&self.inner, session, fields).await
    }
}

fn key(id: &str) -> String { format!("vod_persistent_connection:{id}") }
async fn update(inner: &Inner, session: &Session, fields: &[(&str, String)]) -> Result<(), dispatcharr_store::StoreError> {
    let script = Script::new(UPDATE);
    let mut invocation = script.prepare_invoke();
    invocation.key(key(&session.id)).arg(session.lease.token());
    for (field, value) in fields { invocation.arg(field).arg(value); }
    let saved: i64 = invocation.invoke_async(&mut inner.connection.clone()).await.map_err(|_| redis_error())?;
    if saved != 1 { return Err(unavailable()); }
    Ok(())
}
fn monitor(session: Weak<Session>, pool: Weak<Inner>) {
    tokio::spawn(async move {
        let mut timer = tokio::time::interval(Duration::from_secs(1));
        loop {
            timer.tick().await;
            let (Some(session), Some(pool)) = (session.upgrade(), pool.upgrade()) else { return; };
            let lost = session.lease.lost();
            if *lost.borrow() { session.stop.send_replace(true); return; }
            if session.state.lock().await.closed { return; }
            let stop: bool = match pool.connection.clone().exists(format!("vod_proxy:client:{}:stop", session.id)).await {
                Ok(value) => value, Err(_) => { session.stop.send_replace(true); return; }
            };
            if stop { session.stop.send_replace(true); return; }
            if update(&pool, &session, &[("bytes_sent", session.bytes.load(Ordering::Relaxed).to_string()), ("last_activity", now())]).await.is_err() {
                session.stop.send_replace(true); return;
            }

        }
    });
}

impl Drop for Ticket {
    fn drop(&mut self) {
        let session = self.session.clone();
        let Some(pool) = self.pool.upgrade() else { return; };
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn(async move {
                let mut state = session.state.lock().await;
                if state.active == 0 { return; }
                state.active -= 1;
                state.epoch += 1;
                let epoch = state.epoch;
                let mut connection = pool.connection.clone();
                let count = Script::new(LEAVE).key(key(&session.id)).arg(session.lease.token()).arg(now())
                    .invoke_async::<i64>(&mut connection).await.unwrap_or(-1);
                if state.active != 0 { return; }
                if count < 0 { state.closed = true; }
                if state.reserved {
                    if session.lease.release_provider().await.is_err() { state.closed = true; }
                    state.reserved = false;
                }
                drop(state);
                tokio::time::sleep(pool.idle_grace).await;
                let mut state = session.state.lock().await;
                if state.active != 0 || state.epoch != epoch { return; }
                let deleted = Script::new(DELETE).key(key(&session.id)).arg(session.lease.token())
                    .invoke_async::<i64>(&mut connection).await.unwrap_or(0);
                if deleted == 0 { state.closed = true; }
                state.closed = true;
                session.stop.send_replace(true);
                let _ = session.lease.release().await;
                let mut sessions = pool.sessions.lock().await;
                if sessions.get(&session.id).is_some_and(|value| Arc::ptr_eq(value, &session)) { sessions.remove(&session.id); }
            });
        }
    }
}

// A canceled SETUP may have reached Redis even if its reply was never read.
// Cleanup is token fenced and cannot delete another worker's session.
struct PendingCreate { connection: MultiplexedConnection, id: String, token: String, armed: bool }
impl Drop for PendingCreate {
    fn drop(&mut self) {
        if !self.armed { return; }
        let mut connection = self.connection.clone(); let id = self.id.clone(); let token = self.token.clone();
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn(async move {
                let _: Result<i64,_> = Script::new("if redis.call('HGET',KEYS[1],'rust_owner_token')==ARGV[1] then return redis.call('DEL',KEYS[1]) end return 0")
                    .key(key(&id)).arg(token).invoke_async(&mut connection).await;
            });
        }
    }
}
