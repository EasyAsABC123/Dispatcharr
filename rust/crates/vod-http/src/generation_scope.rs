//! Local body capabilities separate from public Source session accounting.
//! Original Dispatcharr authors retain full application/algorithm credit.
//! This adapter is opt-in/unregistered: no default SessionPool behavior changes.
use super::{Context, Error, NewAdmission, NewGeneration, Recovery, Retired, CLASSIFY};
#[path = "entry_receipts.rs"]
pub(crate) mod receipt;
use dispatcharr_contracts::BoxFuture;
use dispatcharr_coordination::{AcquireRequest, Lease};
use receipt::EntryReceipt;
use redis::Script;
use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex as StdMutex, Weak,
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::sync::{watch, Mutex, OwnedSemaphorePermit, Semaphore};

#[derive(Clone)]
pub struct Identity {
    pub user_id: String,
    pub content_uuid: String,
    pub content_kind: String,
}
#[derive(Clone)]
pub struct Limits {
    pub bodies: usize,
    pub controls: usize,
    pub idle_grace: Duration,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            bodies: 128,
            controls: 128,
            idle_grace: Duration::from_secs(1),
        }
    }
}
/// Concrete initial ownership, supplied by the existing successful setup path.
/// Adoption independently verifies both tokens and the actual unique ledger.
pub struct Initial {
    pub id: String,
    pub lease: Arc<Lease>,
    pub metadata_token: String,
    pub profile_id: i64,
    pub identity: Identity,
    pub lifetime: Arc<dyn Send + Sync>,
    pub stop: watch::Sender<bool>,
}
/// Hook must enqueue/retain its own bounded physical publication work. It must
/// not capture an engine/session which owns this scope (would create a cycle).
pub trait StopHook: Send + Sync {
    fn emit(&self) -> BoxFuture<'_, ()>;
}
#[derive(Clone)]
pub struct LocalScope {
    inner: Arc<Inner>,
}
struct Inner {
    recovery: Recovery,
    id: String,
    profile: i64,
    identity: Identity,
    lifecycle: Arc<dyn Send + Sync>,
    receipt_owner: String,
    limits: Limits,
    bodies: Arc<Semaphore>,
    controls: Arc<Semaphore>,
    serial: Arc<Mutex<()>>,
    state: Mutex<State>,
    stop: watch::Sender<bool>,
    shutdown: watch::Sender<bool>,
    failed: AtomicBool,
    activated: AtomicBool,
}
struct Generation {
    lease: Arc<Lease>,
    token: String,
    recovered: StdMutex<Option<Arc<NewGeneration>>>,
    retired: AtomicBool,
}
struct State {
    current: Arc<Generation>,
    retired: BTreeMap<String, Weak<Generation>>,
    pending: Option<Retired>,
}
struct BodyHold {
    _slot: OwnedSemaphorePermit,
    _caller: Arc<dyn Send + Sync>,
    _scope: Arc<Inner>,
    _origin: StdMutex<Arc<Generation>>,
    receipt: StdMutex<Option<Arc<EntryReceipt>>>,
}
struct BodyState {
    scope: Weak<Inner>,
    hold: StdMutex<Option<Arc<BodyHold>>>,
    closed: AtomicBool,
    admitted: AtomicBool,
    started: AtomicBool,
    is_new: bool,
    hook: Option<Arc<dyn StopHook>>,
}
#[derive(Clone)]
pub struct Body {
    state: Arc<BodyState>,
}
impl Body {
    pub fn started(&self) {
        self.state.started.store(true, Ordering::Release);
    }
    pub fn is_closed(&self) -> bool {
        self.state.closed.load(Ordering::Acquire)
    }
}
impl Drop for BodyState {
    fn drop(&mut self) {
        if self.closed.swap(true, Ordering::AcqRel) {
            return;
        }
        let Some(hold) = self.hold.lock().unwrap().take() else {
            return;
        };
        if !self.admitted.load(Ordering::Acquire) {
            return;
        }
        let Some(scope) = self.scope.upgrade() else {
            return;
        };
        let emit = !self.is_new || self.started.load(Ordering::Acquire);
        let hook = self.hook.clone();
        tokio::spawn(async move {
            if close_owned(scope.clone(), hold, emit, hook).await.is_err() {
                scope.failed.store(true, Ordering::Release);
            }
        });
    }
}
#[derive(Debug, PartialEq, Eq)]
pub enum Progress {
    Updated,
    Missing,
    Stopped,
    OwnerChanged,
    IdentityChanged,
    Partial,
}
#[derive(Debug, PartialEq, Eq)]
pub struct Closed {
    pub changed: bool,
    pub remaining: i64,
    pub deleted: bool,
    pub event_eligible: bool,
}
struct Control {
    _permit: OwnedSemaphorePermit,
    _serial: tokio::sync::OwnedMutexGuard<()>,
}
fn key(id: &str) -> String {
    format!("vod_persistent_connection:{id}")
}
fn owner(inner: &Inner) -> String {
    format!(
        "{}:vod:session:{}:owner",
        inner.recovery.inner.namespace, inner.id
    )
}
fn ledger(inner: &Inner, lease: &Lease) -> String {
    format!("{}:lease:{}", inner.recovery.inner.namespace, lease.token())
}
fn timestamp() -> String {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs_f64()
        .to_string()
}
const CHECK_INITIAL: &str = r#"
if redis.call('GET',KEYS[1])~=ARGV[1] or redis.call('HGET',KEYS[2],'rust_owner_token')~=ARGV[2]
 or redis.call('HGET',KEYS[3],'token')~=ARGV[1] or redis.call('HGET',KEYS[3],'profile')~=ARGV[6]
 or redis.call('HGET',KEYS[2],'user_id')~=ARGV[3] or redis.call('HGET',KEYS[2],'content_uuid')~=ARGV[4]
 or redis.call('HGET',KEYS[2],'content_obj_type')~=ARGV[5] or redis.call('HGET',KEYS[2],'m3u_profile_id')~=ARGV[7] then return 0 end
return 1
"#;
pub(crate) const MUTATE: &str = r#"
if ARGV[6]~='close' and ARGV[6]~='abandon' and redis.call('EXISTS',KEYS[4])~=0 then return {'STOP'} end
if redis.call('GET',KEYS[1])~=ARGV[1] then
 if redis.call('EXISTS',KEYS[1])==0 and redis.call('EXISTS',KEYS[2])==0 then return {'MISSING'} end
 return {'FOREIGN'}
end
if redis.call('EXISTS',KEYS[2])==0 then return {'MISSING'} end
if redis.call('HGET',KEYS[2],'rust_owner_token')~=ARGV[2] then return {'FOREIGN'} end
if redis.call('HGET',KEYS[3],'token')~=ARGV[1] then return {'PARTIAL'} end
if redis.call('HGET',KEYS[2],'user_id')~=ARGV[3] or redis.call('HGET',KEYS[2],'content_uuid')~=ARGV[4]
 or redis.call('HGET',KEYS[2],'content_obj_type')~=ARGV[5] then return {'IDENTITY'} end
if ARGV[6]=='headers' then
 local fields=cjson.decode(ARGV[7]);for field,value in pairs(fields) do redis.call('HSET',KEYS[2],field,value) end
 redis.call('EXPIRE',KEYS[2],3600);return {'UPDATED'}
end
if ARGV[6]=='progress' then
 redis.call('HSET',KEYS[2],'bytes_sent',ARGV[7],'last_activity',ARGV[8]);redis.call('EXPIRE',KEYS[2],3600)
 return {'UPDATED'}
end
local count=tonumber(redis.call('HGET',KEYS[2],'active_streams')or'0')
if not count or count<0 then return {'PARTIAL'} end
if ARGV[6]=='abandon' then
 if ARGV[11]~=ARGV[1] or ARGV[10]~=ARGV[2] or ARGV[9]=='' then return {'RECEIPT_ABSENT'} end
 if redis.call('HSTRLEN',KEYS[5],ARGV[9])~=string.len(ARGV[10]) then return {'RECEIPT_ABSENT'} end
 if redis.call('HGET',KEYS[5],ARGV[9])~=ARGV[10] then return {'RECEIPT_ABSENT'} end
 redis.call('HDEL',KEYS[5],ARGV[9])
 if count==0 then return {'LEFT','0'} end
end
if count==0 then return {'UNCHANGED','0'} end
count=count-1;redis.call('HSET',KEYS[2],'active_streams',count,'last_activity',ARGV[8])
return {'LEFT',tostring(count)}
"#;
const FINALIZE: &str = r#"
if redis.call('GET',KEYS[1])~=ARGV[1] then return {'FOREIGN'} end
if redis.call('HGET',KEYS[2],'rust_owner_token')~=ARGV[2] then return {'FOREIGN'} end
local count=tonumber(redis.call('HGET',KEYS[2],'active_streams')or'0')
if not count or count<0 then return {'PARTIAL'} end
if count>0 then return {'ACTIVE'} end
redis.call('DEL',KEYS[2]);return {'DELETED'}
"#;
pub(crate) const ENTER_CURRENT: &str = r#"
if redis.call('EXISTS',KEYS[4])~=0 then return {'STOP'} end
if redis.call('GET',KEYS[1])~=ARGV[1] or redis.call('HGET',KEYS[2],'rust_owner_token')~=ARGV[2]
 or redis.call('HGET',KEYS[3],'token')~=ARGV[1] then return {'FOREIGN'} end
if redis.call('HGET',KEYS[2],'user_id')~=ARGV[3] or redis.call('HGET',KEYS[2],'content_uuid')~=ARGV[4]
 or redis.call('HGET',KEYS[2],'content_obj_type')~=ARGV[5] then return {'IDENTITY'} end
local count=tonumber(redis.call('HGET',KEYS[2],'active_streams')or'0')
local requests=tonumber(redis.call('HGET',KEYS[2],'request_count')or'0')
local provider=tonumber(redis.call('GET',KEYS[5])or'0')
if not count or count<=0 or count~=math.floor(count) or count>=9007199254740991
 or not requests or requests<0 or requests~=math.floor(requests) or requests>=9007199254740991
 or not provider or provider<=0
 or redis.call('HGET',KEYS[3],'profile')~=ARGV[6]
 or redis.call('HGET',KEYS[2],'m3u_profile_id')~=ARGV[8] then return {'PARTIAL'} end
local kind=redis.call('TYPE',KEYS[6]).ok
if kind~='none' and kind~='hash' then return {'PARTIAL'} end
if redis.call('HLEN',KEYS[6])>=tonumber(ARGV[10]) then return {'CAPACITY'} end
local size=redis.call('MEMORY','USAGE',KEYS[6],'SAMPLES',0)or 0
if size>tonumber(ARGV[11]) then return {'CAPACITY'} end
redis.call('HINCRBY',KEYS[2],'active_streams',1)
redis.call('HINCRBY',KEYS[2],'request_count',1)
redis.call('HSET',KEYS[2],'last_activity',ARGV[7]);redis.call('EXPIRE',KEYS[2],3600)
redis.call('HSET',KEYS[6],ARGV[9],ARGV[2]);redis.call('EXPIRE',KEYS[6],3600)
return {'ENTERED'}
"#;
async fn control(inner: &Arc<Inner>) -> Result<Arc<Control>, Error> {
    let permit = inner
        .controls
        .clone()
        .try_acquire_owned()
        .map_err(|_| Error::Capacity)?;
    let serial = inner.serial.clone().lock_owned().await;
    Ok(Arc::new(Control {
        _permit: permit,
        _serial: serial,
    }))
}
async fn mutate(
    inner: &Arc<Inner>,
    gen: &Generation,
    op: &str,
    payload: &str,
    receipt: Option<&EntryReceipt>,
) -> Result<Vec<String>, Error> {
    Script::new(MUTATE)
        .key(owner(inner))
        .key(key(&inner.id))
        .key(ledger(inner, &gen.lease))
        .key(format!("vod_proxy:client:{}:stop", inner.id))
        .key(receipt.map_or_else(|| owner(inner), |entry| entry.key.clone()))
        .arg(gen.lease.token())
        .arg(&gen.token)
        .arg(&inner.identity.user_id)
        .arg(&inner.identity.content_uuid)
        .arg(&inner.identity.content_kind)
        .arg(op)
        .arg(payload)
        .arg(timestamp())
        .arg(receipt.map_or("", |entry| entry.field.as_str()))
        .arg(receipt.map_or("", |entry| entry.metadata_token.as_str()))
        .arg(receipt.map_or("", |entry| entry.lease_token.as_str()))
        .invoke_async(&mut inner.recovery.inner.connection.clone())
        .await
        .map_err(|_| Error::Backend)
}
impl LocalScope {
    pub async fn adopt(
        recovery: Recovery,
        initial: Initial,
        limits: Limits,
    ) -> Result<Self, Error> {
        if initial.id.is_empty()
            || initial.id.len() > 128
            || initial.id.contains(':')
            || initial.profile_id <= 0
            || initial.metadata_token.is_empty()
            || initial.metadata_token.len() > 128
            || limits.bodies == 0
            || limits.bodies > 4096
            || limits.controls == 0
            || limits.controls > 4096
            || limits.idle_grace.is_zero()
            || limits.idle_grace > Duration::from_secs(10)
            || initial.identity.user_id.is_empty()
            || initial.identity.user_id.len() > 128
            || initial.identity.content_uuid.is_empty()
            || initial.identity.content_uuid.len() > 128
            || !matches!(initial.identity.content_kind.as_str(), "movie" | "episode")
        {
            return Err(Error::Configuration);
        }
        let gen = Arc::new(Generation {
            lease: initial.lease,
            token: initial.metadata_token,
            recovered: StdMutex::new(None),
            retired: AtomicBool::new(false),
        });
        let inner = Arc::new(Inner {
            recovery,
            id: initial.id,
            profile: initial.profile_id,
            identity: initial.identity,
            lifecycle: initial.lifetime,
            receipt_owner: uuid::Uuid::new_v4().to_string(),
            bodies: Arc::new(Semaphore::new(limits.bodies)),
            controls: Arc::new(Semaphore::new(limits.controls)),
            limits,
            serial: Arc::new(Mutex::new(())),
            state: Mutex::new(State {
                current: gen.clone(),
                retired: BTreeMap::new(),
                pending: None,
            }),
            stop: initial.stop,
            shutdown: watch::channel(false).0,
            failed: AtomicBool::new(false),
            activated: AtomicBool::new(false),
        });
        let task_inner = inner.clone();
        let task = tokio::spawn(async move {
            let _control = control(&task_inner).await?;
            let valid: i64 = Script::new(CHECK_INITIAL)
                .key(owner(&task_inner))
                .key(key(&task_inner.id))
                .key(ledger(&task_inner, &gen.lease))
                .arg(gen.lease.token())
                .arg(&gen.token)
                .arg(&task_inner.identity.user_id)
                .arg(&task_inner.identity.content_uuid)
                .arg(&task_inner.identity.content_kind)
                .arg(format!("profile_connections:{}", task_inner.profile))
                .arg(task_inner.profile)
                .invoke_async(&mut task_inner.recovery.inner.connection.clone())
                .await
                .map_err(|_| Error::Backend)?;
            if valid != 1 {
                return Err(Error::ForeignOwner);
            }
            Ok(())
        });
        tokio::time::timeout(inner.recovery.inner.observer_timeout, task)
            .await
            .map_err(|_| Error::ObserverTimeout)?
            .map_err(|_| Error::Backend)??;
        Ok(Self { inner })
    }
    /// Caller already reserved this request's public count in successful setup.
    pub async fn attach_reserved(
        &self,
        caller: Arc<dyn Send + Sync>,
        is_new: bool,
        hook: Option<Arc<dyn StopHook>>,
    ) -> Result<Body, Error> {
        self.attach_inner(caller, is_new, hook, true).await
    }
    /// New recovery setup: no public slot belongs to this body until reopen's
    /// actual success. Dropping a failed pending setup must not DECR a sibling.
    pub async fn attach_pending(
        &self,
        caller: Arc<dyn Send + Sync>,
        hook: Option<Arc<dyn StopHook>>,
    ) -> Result<Body, Error> {
        self.attach_inner(caller, true, hook, false).await
    }
    pub async fn attach_entry_pending(
        &self,
        caller: Arc<dyn Send + Sync>,
        hook: Option<Arc<dyn StopHook>>,
    ) -> Result<Body, Error> {
        self.attach_inner(caller, false, hook, false).await
    }
    async fn attach_inner(
        &self,
        caller: Arc<dyn Send + Sync>,
        is_new: bool,
        hook: Option<Arc<dyn StopHook>>,
        admitted: bool,
    ) -> Result<Body, Error> {
        if *self.inner.stop.borrow() {
            return Err(Error::Stopped);
        }
        if self.inner.failed.load(Ordering::Acquire) {
            return Err(Error::Backend);
        }
        let slot = self
            .inner
            .bodies
            .clone()
            .try_acquire_owned()
            .map_err(|_| Error::Capacity)?;
        let origin = self.inner.state.lock().await.current.clone();
        Ok(Body {
            state: Arc::new(BodyState {
                scope: Arc::downgrade(&self.inner),
                hold: StdMutex::new(Some(Arc::new(BodyHold {
                    _slot: slot,
                    _caller: caller,
                    _scope: self.inner.clone(),
                    _origin: StdMutex::new(origin),
                    receipt: StdMutex::new(None),
                }))),
                closed: AtomicBool::new(false),
                admitted: AtomicBool::new(admitted),
                started: AtomicBool::new(false),
                is_new,
                hook,
            }),
        })
    }
    pub fn activated(&self) -> bool {
        self.inner.activated.load(Ordering::Acquire)
    }
    pub fn same_scope(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.inner, &other.inner)
    }
    pub fn profile_id(&self) -> i64 {
        self.inner.profile
    }
    pub fn entry_fence_keys(&self, lease: &Lease) -> (String, String, String) {
        (
            owner(&self.inner),
            ledger(&self.inner, lease),
            format!("vod_proxy:client:{}:stop", self.inner.id),
        )
    }
    pub async fn current_identity(&self) -> (Arc<Lease>, String) {
        let current = self.inner.state.lock().await.current.clone();
        (current.lease.clone(), current.token.clone())
    }
    pub async fn can_evict(&self) -> bool {
        self.body_slots() == self.inner.limits.bodies
            && self.control_slots() == self.inner.limits.controls
            && self
                .inner
                .state
                .lock()
                .await
                .current
                .retired
                .load(Ordering::Acquire)
    }
    /// Healthy legacy setup/teardown keeps its existing policy until actual
    /// state loss activates this adapter. The caller retains this guard through
    /// the legacy physical cleanup; it must not also delegate a second DECR.
    pub fn detach_legacy(&self, body: &Body) -> Option<Arc<dyn Send + Sync>> {
        if !Weak::ptr_eq(&body.state.scope, &Arc::downgrade(&self.inner))
            || body.state.closed.swap(true, Ordering::AcqRel)
        {
            return None;
        }
        body.state
            .hold
            .lock()
            .unwrap()
            .take()
            .map(|hold| hold as Arc<dyn Send + Sync>)
    }
    pub async fn update_headers(&self, fields: &[(&str, String)]) -> Result<(), Error> {
        let mut bytes = 0usize;
        let mut values = BTreeMap::new();
        for (field, value) in fields {
            if !matches!(*field, "content_length" | "content_type" | "final_url") {
                return Err(Error::Configuration);
            }
            bytes = bytes
                .checked_add(field.len() + value.len())
                .ok_or(Error::Capacity)?;
            values.insert(*field, value.clone());
        }
        if fields.len() > 3 || bytes > 65536 {
            return Err(Error::Capacity);
        }
        let payload = serde_json::to_string(&values).map_err(|_| Error::Configuration)?;
        let inner = self.inner.clone();
        let task = tokio::spawn(async move {
            let _control = control(&inner).await?;
            let gen = inner.state.lock().await.current.clone();
            let result = mutate(&inner, &gen, "headers", &payload, None).await?;
            if result.first().is_some_and(|value| value == "UPDATED") {
                Ok(())
            } else {
                Err(Error::ForeignOwner)
            }
        });
        tokio::time::timeout(self.inner.recovery.inner.observer_timeout, task)
            .await
            .map_err(|_| Error::ObserverTimeout)?
            .map_err(|_| Error::Backend)?
    }
    pub fn body_slots(&self) -> usize {
        self.inner.bodies.available_permits()
    }
    pub fn control_slots(&self) -> usize {
        self.inner.controls.available_permits()
    }
    /// Local service shutdown, distinct from discovering a persistent stop key.
    pub fn stop(&self) {
        self.inner.stop.send_replace(true);
        self.inner.shutdown.send_replace(true);
    }
    pub async fn drain(&self) {
        while self.body_slots() != self.inner.limits.bodies
            || self.control_slots() != self.inner.limits.controls
        {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
    pub fn cleanup_succeeded(&self) -> bool {
        !self.inner.failed.load(Ordering::Acquire) && self.inner.recovery.cleanup_succeeded()
    }
    pub async fn retire_missing(&self) -> Result<(), Error> {
        self.retire_missing_retained(Arc::new(())).await
    }
    pub async fn retire_missing_retained(&self, caller: Arc<dyn Send + Sync>) -> Result<(), Error> {
        let inner = self.inner.clone();
        let task = tokio::spawn(async move {
            let _caller = caller;
            let _control = control(&inner).await?;
            let gen = inner.state.lock().await.current.clone();
            if !*gen.lease.lost().borrow() {
                let _ = gen.lease.heartbeat().await;
            }
            gen.lease.settlement().wait().await;
            let observed: String = Script::new(CLASSIFY)
                .key(owner(&inner))
                .key(key(&inner.id))
                .key(ledger(&inner, &gen.lease))
                .key(format!("vod_proxy:client:{}:stop", inner.id))
                .arg(gen.lease.token())
                .arg(&gen.token)
                .invoke_async(&mut inner.recovery.inner.connection.clone())
                .await
                .map_err(|_| Error::Backend)?;
            match observed.as_str() {
                "ABSENT" => {
                    // Ownership is physically absent; release the previous
                    // recovered setup budget before creating the next one.
                    disarm_recovered(&gen);
                    let slot = inner
                        .recovery
                        .inner
                        .jobs
                        .clone()
                        .try_acquire_owned()
                        .map_err(|_| Error::Capacity)?;
                    let retired = Retired {
                        inner: inner.recovery.inner.clone(),
                        context: Arc::new(Context {
                            _slot: slot,
                            _caller: inner.lifecycle.clone(),
                        }),
                        old: gen.lease.clone(),
                        id: inner.id.clone(),
                    };
                    inner.activated.store(true, Ordering::Release);
                    gen.retired.store(true, Ordering::Release);
                    let mut state = inner.state.lock().await;
                    state.retired.retain(|_, value| value.strong_count() > 0);
                    state
                        .retired
                        .insert(gen.lease.token().into(), Arc::downgrade(&gen));
                    state.pending = Some(retired);
                    Ok(())
                }
                "STOP" => {
                    inner.activated.store(true, Ordering::Release);
                    inner.stop.send_replace(true);
                    Err(Error::Stopped)
                }
                "FOREIGN" => {
                    inner.activated.store(true, Ordering::Release);
                    inner.stop.send_replace(true);
                    Err(Error::ForeignOwner)
                }
                "OWNED" => {
                    if *gen.lease.lost().borrow() {
                        // Do not reset sticky loss. A fresh fenced heartbeat
                        // can renew the still-owned identity after connectivity
                        // returned; old automatic heartbeat may have stopped.
                        let result = gen.lease.heartbeat().await;
                        gen.lease.settlement().wait().await;
                        if result != Ok(true) {
                            inner.activated.store(true, Ordering::Release);
                            return Err(Error::PartialState);
                        }
                    }
                    Err(Error::StillOwned)
                }
                _ => {
                    inner.activated.store(true, Ordering::Release);
                    Err(Error::PartialState)
                }
            }
        });
        tokio::time::timeout(self.inner.recovery.inner.observer_timeout, task)
            .await
            .map_err(|_| Error::ObserverTimeout)?
            .map_err(|_| Error::Backend)?
    }
    pub async fn reopen(
        &self,
        body: &Body,
        request: AcquireRequest,
        admission: NewAdmission,
    ) -> Result<(), Error> {
        if !Weak::ptr_eq(&body.state.scope, &Arc::downgrade(&self.inner))
            || body.is_closed()
            || body.state.admitted.load(Ordering::Acquire)
            || request.channel_id != self.inner.id
            || request.provider_profile_id != Some(self.inner.profile)
            || admission.fields.get("user_id") != Some(&self.inner.identity.user_id)
            || admission.fields.get("content_uuid") != Some(&self.inner.identity.content_uuid)
            || admission.fields.get("content_obj_type") != Some(&self.inner.identity.content_kind)
        {
            return Err(Error::Configuration);
        }
        let body = body.clone();
        let inner = self.inner.clone();
        let task = tokio::spawn(async move {
            let guard = control(&inner).await?;
            if *inner.stop.borrow() {
                return Err(Error::Stopped);
            }
            let retired = inner
                .state
                .lock()
                .await
                .pending
                .take()
                .ok_or(Error::PartialState)?;
            let hold = body
                .state
                .hold
                .lock()
                .unwrap()
                .clone()
                .ok_or(Error::Configuration)?;
            // This extra guard, released on successful delivery, keeps serialization
            // through ambiguous initial acquisition and abandoned ADMIT cleanup too.
            let caller: Arc<dyn Send + Sync> = Arc::new((guard.clone(), hold.clone()));
            let generation = retired.reopen_retained(request, admission, caller).await?;
            if *inner.stop.borrow() || body.is_closed() {
                let _ = generation.close().await;
                return Err(Error::Stopped);
            }
            let lease = generation
                .cleanup
                .as_ref()
                .expect("live generation")
                .lease
                .clone();
            let token = generation.metadata_token().to_owned();
            let recovered = Arc::new(generation);
            let next = Arc::new(Generation {
                lease,
                token,
                recovered: StdMutex::new(Some(recovered)),
                retired: AtomicBool::new(false),
            });
            *body
                .state
                .hold
                .lock()
                .unwrap()
                .as_ref()
                .expect("owned body")
                ._origin
                .lock()
                .unwrap() = next.clone();
            inner.state.lock().await.current = next;
            body.state.admitted.store(true, Ordering::Release);
            Ok(())
        });
        tokio::time::timeout(self.inner.recovery.inner.observer_timeout, task)
            .await
            .map_err(|_| Error::ObserverTimeout)?
            .map_err(|_| Error::Backend)?
    }
    /// Source187: an additional current request owns a body but reuses the
    /// existing provider reservation. This narrowly supports an active current
    /// generation; idle adoption remains a separate policy/physical transition.
    pub async fn enter_current(&self, body: &Body, admission: NewAdmission) -> Result<(), Error> {
        if !Weak::ptr_eq(&body.state.scope, &Arc::downgrade(&self.inner))
            || body.is_closed()
            || body.state.admitted.load(Ordering::Acquire)
            || admission.fields.get("user_id") != Some(&self.inner.identity.user_id)
            || admission.fields.get("content_uuid") != Some(&self.inner.identity.content_uuid)
            || admission.fields.get("content_obj_type") != Some(&self.inner.identity.content_kind)
        {
            return Err(Error::Configuration);
        }
        let hold = body
            .state
            .hold
            .lock()
            .unwrap()
            .clone()
            .ok_or(Error::Configuration)?;
        let body = body.clone();
        let inner = self.inner.clone();
        let task = tokio::spawn(async move {
            let guard = control(&inner).await?;
            let gen = inner.state.lock().await.current.clone();
            if *inner.stop.borrow() || body.is_closed() {
                return Err(Error::Stopped);
            }
            let owner_key = owner(&inner);
            let stop_key = format!("vod_proxy:client:{}:stop", inner.id);
            if admission.limit > 0 {
                dispatcharr_clients::vod::admit_fenced_physical(
                    &mut inner.recovery.inner.connection.clone(),
                    dispatcharr_clients::vod::Admission {
                        session_id: &inner.id,
                        fields: &admission.fields,
                        limit: admission.limit,
                        policy: &admission.policy,
                        create: false,
                        token: &gen.token,
                        scan_limit: 50000,
                        timeout: inner.recovery.inner.observer_timeout,
                        ttl: Duration::from_secs(3600),
                    },
                    dispatcharr_clients::vod::AdmissionFence {
                        owner_key: &owner_key,
                        owner_token: gen.lease.token(),
                        stop_key: &stop_key,
                    },
                )
                .await
                .map_err(Error::Admission)?;
            }
            if *inner.stop.borrow() || body.is_closed() {
                return Err(Error::Stopped);
            }
            let receipt = Arc::new(EntryReceipt::new(
                &inner.recovery.inner.namespace,
                &inner.id,
                &inner.receipt_owner,
                gen.lease.token(),
                &gen.token,
                inner.limits.bodies,
            ));
            {
                let mut entry = hold.receipt.lock().unwrap();
                if entry.is_some() {
                    return Err(Error::Configuration);
                }
                *entry = Some(receipt.clone());
            }
            let reply: Result<Vec<String>, redis::RedisError> = Script::new(ENTER_CURRENT)
                .key(owner_key)
                .key(key(&inner.id))
                .key(ledger(&inner, &gen.lease))
                .key(stop_key)
                .key(format!("profile_connections:{}", inner.profile))
                .key(&receipt.key)
                .arg(gen.lease.token())
                .arg(&gen.token)
                .arg(&inner.identity.user_id)
                .arg(&inner.identity.content_uuid)
                .arg(&inner.identity.content_kind)
                .arg(format!("profile_connections:{}", inner.profile))
                .arg(timestamp())
                .arg(inner.profile)
                .arg(&receipt.field)
                .arg(receipt.max_fields)
                .arg(receipt.max_bytes)
                .invoke_async(&mut inner.recovery.inner.connection.clone())
                .await;
            let reply = match reply {
                Ok(reply) => reply,
                Err(_) => {
                    let Control { _permit, _serial } =
                        Arc::try_unwrap(guard).map_err(|_| Error::Configuration)?;
                    drop(_serial);
                    let _retained_control = _permit;
                    // A fresh conditional claim, never a replay of ENTER. Missing
                    // receipt is unproven, not evidence that admission never ran.
                    close_owned_kind(inner, hold, true, body.state.hook.clone(), true).await?;
                    return Err(Error::Backend);
                }
            };
            match reply.first().map(String::as_str) {
                Some("ENTERED") => {}
                Some("STOP") => return Err(Error::Stopped),
                Some("FOREIGN") => return Err(Error::ForeignOwner),
                Some("IDENTITY") => return Err(Error::Configuration),
                Some("CAPACITY") => return Err(Error::Capacity),
                _ => return Err(Error::PartialState),
            }
            let abandoned = {
                // Publish admission while holding the same lock close uses to
                // take the body capability. Exactly one path owns any increment.
                let _hold_lock = body.state.hold.lock().unwrap();
                if body.is_closed() {
                    true
                } else {
                    body.state.admitted.store(true, Ordering::Release);
                    false
                }
            };
            if abandoned {
                let Control { _permit, _serial } =
                    Arc::try_unwrap(guard).map_err(|_| Error::Configuration)?;
                drop(_serial);
                let _retained_control = _permit;
                close_owned(inner, hold, true, body.state.hook.clone()).await?;
                return Err(Error::Stopped);
            }
            // Both the body and this owned task retain physical admission until
            // ENTER's actual reply. Dropped observers cannot abandon an increment.
            drop(hold);
            Ok(())
        });
        tokio::time::timeout(self.inner.recovery.inner.observer_timeout, task)
            .await
            .map_err(|_| Error::ObserverTimeout)?
            .map_err(|_| Error::Backend)?
    }
    pub async fn progress(&self, body: &Body, bytes: u64) -> Result<Progress, Error> {
        if !Weak::ptr_eq(&body.state.scope, &Arc::downgrade(&self.inner)) || body.is_closed() {
            return Err(Error::Configuration);
        }
        let hold = body
            .state
            .hold
            .lock()
            .unwrap()
            .clone()
            .ok_or(Error::Configuration)?;
        let inner = self.inner.clone();
        let task = tokio::spawn(async move {
            let _hold = hold;
            let _control = control(&inner).await?;
            let gen = inner.state.lock().await.current.clone();
            let reply = mutate(&inner, &gen, "progress", &bytes.to_string(), None).await?;
            match reply.first().map(String::as_str) {
                Some("UPDATED") => Ok(Progress::Updated),
                Some("MISSING") => Ok(Progress::Missing),
                Some("STOP") => {
                    inner.stop.send_replace(true);
                    Ok(Progress::Stopped)
                }
                Some("FOREIGN") => {
                    inner.stop.send_replace(true);
                    Ok(Progress::OwnerChanged)
                }
                Some("IDENTITY") => Ok(Progress::IdentityChanged),
                _ => Ok(Progress::Partial),
            }
        });
        tokio::time::timeout(self.inner.recovery.inner.observer_timeout, task)
            .await
            .map_err(|_| Error::ObserverTimeout)?
            .map_err(|_| Error::Backend)?
    }
    pub async fn close(&self, body: &Body) -> Result<Closed, Error> {
        if !Weak::ptr_eq(&body.state.scope, &Arc::downgrade(&self.inner)) {
            return Err(Error::Configuration);
        }
        if body.state.closed.swap(true, Ordering::AcqRel) {
            return Ok(Closed {
                changed: false,
                remaining: 0,
                deleted: false,
                event_eligible: false,
            });
        }
        let hold = body
            .state
            .hold
            .lock()
            .unwrap()
            .take()
            .ok_or(Error::Configuration)?;
        if !body.state.admitted.load(Ordering::Acquire) {
            drop(hold);
            return Ok(Closed {
                changed: false,
                remaining: 0,
                deleted: false,
                event_eligible: false,
            });
        }
        let inner = self.inner.clone();
        let emit = !body.state.is_new || body.state.started.load(Ordering::Acquire);
        let hook = body.state.hook.clone();
        let task = tokio::spawn(async move {
            let result = close_owned(inner.clone(), hold, emit, hook).await;
            if result.is_err() {
                inner.failed.store(true, Ordering::Release);
            }
            result
        });
        tokio::time::timeout(self.inner.recovery.inner.observer_timeout, task)
            .await
            .map_err(|_| Error::ObserverTimeout)?
            .map_err(|_| Error::Backend)?
    }
    pub async fn retired_generations(&self) -> usize {
        let mut state = self.inner.state.lock().await;
        state.retired.retain(|_, value| value.strong_count() > 0);
        state.retired.len()
    }
}
async fn close_owned(
    inner: Arc<Inner>,
    hold: Arc<BodyHold>,
    emit: bool,
    hook: Option<Arc<dyn StopHook>>,
) -> Result<Closed, Error> {
    close_owned_kind(inner, hold, emit, hook, false).await
}
async fn close_owned_kind(
    inner: Arc<Inner>,
    hold: Arc<BodyHold>,
    emit: bool,
    hook: Option<Arc<dyn StopHook>>,
    uncertain: bool,
) -> Result<Closed, Error> {
    let result = close_public(inner.clone(), hold.clone(), emit, hook, uncertain).await;
    let receipt = hold.receipt.lock().unwrap().clone();
    if let Some(receipt) = receipt {
        if receipt
            .forget(&mut inner.recovery.inner.connection.clone())
            .await
            .is_err()
        {
            inner.failed.store(true, Ordering::Release);
            return Err(Error::Backend);
        }
    }
    if result.is_err() {
        inner.failed.store(true, Ordering::Release);
    }
    result
}
async fn close_public(
    inner: Arc<Inner>,
    hold: Arc<BodyHold>,
    emit: bool,
    hook: Option<Arc<dyn StopHook>>,
    uncertain: bool,
) -> Result<Closed, Error> {
    let receipt = hold.receipt.lock().unwrap().clone();
    let _hold = hold;
    // One finalizer per true body; its body permit bounds this queue even when
    // the separate ordinary-control admission is full.
    let serial = inner.serial.clone().lock_owned().await;
    let gen = inner.state.lock().await.current.clone();
    if uncertain
        && receipt.as_ref().is_none_or(|entry| {
            entry.lease_token != gen.lease.token() || entry.metadata_token != gen.token
        })
    {
        return Err(Error::UnprovenEntry);
    }
    let reply = mutate(
        &inner,
        &gen,
        if uncertain { "abandon" } else { "close" },
        "0",
        receipt.as_deref(),
    )
    .await?;
    let status = reply.first().map(String::as_str);
    if status == Some("RECEIPT_ABSENT") {
        return Err(Error::UnprovenEntry);
    }
    if status != Some("LEFT") {
        if matches!(status, Some("STOP") | Some("FOREIGN")) {
            inner.stop.send_replace(true);
        }
        if status == Some("FOREIGN") {
            // Release only this immutable unique ledger; never delete or
            // decrement a replacement owner's reservation or metadata.
            let result = gen.lease.release().await;
            gen.lease.settlement().wait().await;
            if result.is_err() {
                return Err(Error::Backend);
            }
            gen.retired.store(true, Ordering::Release);
            disarm_recovered(&gen);
        }
        if status == Some("PARTIAL") {
            inner.failed.store(true, Ordering::Release);
        }
        return Ok(Closed {
            changed: false,
            remaining: 0,
            deleted: false,
            event_eligible: false,
        });
    }
    let remaining = reply
        .get(1)
        .and_then(|x| x.parse::<i64>().ok())
        .ok_or(Error::Backend)?;
    if remaining > 0 {
        return Ok(Closed {
            changed: true,
            remaining,
            deleted: false,
            event_eligible: false,
        });
    }
    let released = gen.lease.release_provider().await;
    gen.lease.settlement().wait().await;
    if released.is_err() {
        inner.failed.store(true, Ordering::Release);
        return Err(Error::Backend);
    }
    drop(serial);
    let mut stop = inner.shutdown.subscribe();
    if !*stop.borrow() {
        tokio::select! {_=tokio::time::sleep(inner.limits.idle_grace)=>{},_=stop.changed()=>{}}
    }
    let _serial = inner.serial.clone().lock_owned().await;
    // Resolve CURRENT again after Source's grace window. Never use an old token
    // as permission to mutate a newly installed local or foreign generation.
    let current = inner.state.lock().await.current.clone();
    let result: Vec<String> = Script::new(FINALIZE)
        .key(owner(&inner))
        .key(key(&inner.id))
        .arg(current.lease.token())
        .arg(&current.token)
        .invoke_async(&mut inner.recovery.inner.connection.clone())
        .await
        .map_err(|_| Error::Backend)?;
    let deleted = result.first().is_some_and(|x| x == "DELETED");
    if deleted {
        current.retired.store(true, Ordering::Release);
        let result = current.lease.release().await;
        current.lease.settlement().wait().await;
        if result.is_err() {
            inner.failed.store(true, Ordering::Release);
            return Err(Error::Backend);
        }
        disarm_recovered(&current);
    }
    let event_eligible = deleted && emit;
    if event_eligible {
        if let Some(hook) = hook {
            hook.emit().await;
        }
    }
    Ok(Closed {
        changed: true,
        remaining: 0,
        deleted,
        event_eligible,
    })
}

fn disarm_recovered(generation: &Generation) {
    if let Some(owner) = generation.recovered.lock().unwrap().take() {
        if let Ok(mut owner) = Arc::try_unwrap(owner) {
            // Only call after actual all-missing proof or final owned deletion
            // plus lease settlement. Keep old body identity, not old setup jobs.
            owner.cleanup.take();
        }
    }
}
