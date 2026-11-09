//! Bounded archive reservation/lifecycle prototype ported from Timeshift views.
//! Original application credit belongs entirely to upstream Dispatcharr authors.
use dispatcharr_clients::timeshift::{self, Admission};
use dispatcharr_contracts::UserLimitPolicy;
use dispatcharr_coordination::{AcquireRequest, Coordinator, Lease};
use dispatcharr_output_store::transport::{RedisTransport, TransportConfig};
use dispatcharr_store::StoreError;
use serde_json::json;
use std::{
    collections::{BTreeMap, HashMap},
    sync::{
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::sync::{watch, Notify, OwnedSemaphorePermit, Semaphore};

/// Maps contain source metadata only; no Debug implementation because provider
/// URLs can contain credentials. The pool adds its private ownership fence.
pub struct ArchivePoolRequest {
    pub session_id: String,
    pub channel_id: i64,
    pub virtual_channel_id: String,
    pub provider_profile_id: i64,
    pub provider_max_streams: u32,
    pub stream_limit: u32,
    pub limit_policy: UserLimitPolicy,
    pub pool_fields: BTreeMap<String, String>,
    pub client_fields: BTreeMap<String, String>,
    pub channel_fields: BTreeMap<String, String>,
}
#[derive(Clone)]
pub struct ArchivePoolConfig {
    pub max_sessions: usize,
    pub max_redis_operations: usize,
    pub max_queued_redis_operations: usize,
    pub command_timeout: Duration,
    pub disconnect_grace: Duration,
    pub startup_idle: Duration,
    pub shutdown_timeout: Duration,
    pub admission_scan_limit: usize,
    pub pending_admission_ttl: Duration,
    pub setup_refresh_interval: Duration,
}
impl Default for ArchivePoolConfig {
    fn default() -> Self {
        Self {
            max_sessions: 1024,
            max_redis_operations: 64,
            max_queued_redis_operations: 1024,
            command_timeout: Duration::from_secs(3),
            disconnect_grace: Duration::from_millis(6500),
            startup_idle: Duration::from_secs(60),
            shutdown_timeout: Duration::from_secs(5),
            admission_scan_limit: 50000,
            pending_admission_ttl: Duration::from_secs(60),
            setup_refresh_interval: Duration::from_secs(5),
        }
    }
}
struct Entry {
    request: ArchivePoolRequest,
    token: String,
    lease: Mutex<Option<Arc<Lease>>>,
    active: AtomicBool,
    streaming: AtomicBool,
    bytes: AtomicU64,
    started: Mutex<Instant>,
    operations: AtomicUsize,
    setup_refresh: Mutex<Option<tokio::task::JoinHandle<()>>>,
    operations_done: Notify,
    stop: watch::Sender<bool>,
    _permit: OwnedSemaphorePermit,
}
struct Inner {
    transport: RedisTransport,
    runtime: tokio::runtime::Handle,
    coordinator: Coordinator,
    entries: Mutex<HashMap<String, Arc<Entry>>>,
    slots: Arc<Semaphore>,
    config: ArchivePoolConfig,
    stopped: watch::Sender<bool>,
    cleanup_failed: AtomicBool,
}
#[derive(Clone)]
pub struct ArchivePool {
    inner: Arc<Inner>,
}
/// Dropping a setup or body ticket releases the provider and schedules bounded
/// cleanup. An unpolled body must retain this ticket in a supervised task.
pub struct ArchiveTicket {
    rollback: Option<Rollback>,
}
struct Rollback {
    inner: Arc<Inner>,
    entry: Arc<Entry>,
}
struct Operation {
    entry: Arc<Entry>,
}
impl Operation {
    fn new(entry: &Arc<Entry>) -> Self {
        entry.operations.fetch_add(1, Ordering::SeqCst);
        Self {
            entry: entry.clone(),
        }
    }
}
impl Drop for Operation {
    fn drop(&mut self) {
        self.entry.operations.fetch_sub(1, Ordering::SeqCst);
        self.entry.operations_done.notify_waiters();
    }
}
impl Drop for Rollback {
    fn drop(&mut self) {
        let inner = self.inner.clone();
        let entry = self.entry.clone();
        let runtime = inner.runtime.clone();
        runtime.spawn(async move {
            finish(inner, entry).await;
        });
        // If that runtime has shut down, the fenced expiry ledger recovers quota.
    }
}
fn unavailable() -> StoreError {
    StoreError::StatusBody(503, json!("Archive session service unavailable"))
}
fn timestamp() -> String {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs_f64()
        .to_string()
}
fn pool(entry: &Entry) -> String {
    format!("timeshift:pool:{}", entry.request.session_id)
}
fn stats(entry: &Entry) -> String {
    format!("{}_{}", entry.request.channel_id, entry.request.session_id)
}
fn keys(entry: &Entry) -> Vec<String> {
    let sid = &entry.request.session_id;
    let stats = stats(entry);
    vec![
        pool(entry),
        format!("timeshift:channel:{stats}:clients:{sid}"),
        format!("timeshift:channel:{stats}:clients"),
        format!("timeshift:channel:{stats}:metadata"),
        format!("timeshift:session:{sid}"),
        format!(
            "timeshift:stream_gen:{}:{sid}",
            entry.request.virtual_channel_id
        ),
        format!(
            "timeshift:channel:{}:client:{sid}:stop",
            entry.request.virtual_channel_id
        ),
    ]
}
impl ArchivePool {
    pub async fn connect(
        redis_url: &str,
        coordinator: Coordinator,
        config: ArchivePoolConfig,
    ) -> Result<Self, StoreError> {
        if config.admission_scan_limit == 0
            || config.pending_admission_ttl.is_zero()
            || config.pending_admission_ttl > Duration::from_secs(600)
            || config.setup_refresh_interval.is_zero()
            || config.setup_refresh_interval >= config.pending_admission_ttl
            || config.max_sessions == 0
            || config.max_sessions > 65536
            || config.shutdown_timeout.is_zero()
            || config.disconnect_grace.is_zero()
            || config.startup_idle.is_zero()
        {
            return Err(unavailable());
        }
        let transport = RedisTransport::connect(
            redis_url,
            TransportConfig {
                max_operations: config.max_redis_operations,
                max_queued_operations: config.max_queued_redis_operations,
                command_timeout: config.command_timeout,
            },
        )
        .await
        .map_err(|_| unavailable())?;
        let slots = Arc::new(Semaphore::new(config.max_sessions));
        let (stopped, _) = watch::channel(false);
        Ok(Self {
            inner: Arc::new(Inner {
                transport,
                runtime: tokio::runtime::Handle::current(),
                coordinator,
                entries: Mutex::new(HashMap::new()),
                slots,
                config,
                stopped,
                cleanup_failed: AtomicBool::new(false),
            }),
        })
    }
    pub async fn open(&self, request: ArchivePoolRequest) -> Result<ArchiveTicket, StoreError> {
        if *self.inner.stopped.borrow() {
            return Err(unavailable());
        }
        let sid = &request.session_id;
        let maps = [
            &request.pool_fields,
            &request.client_fields,
            &request.channel_fields,
        ];
        if maps
            .iter()
            .any(|fields| fields.len() > 128 || fields.keys().any(|key| key.len() > 128))
            || maps
                .iter()
                .flat_map(|fields| fields.iter())
                .map(|(key, value)| key.len().saturating_add(value.len()))
                .fold(0usize, usize::saturating_add)
                > 65536
        {
            return Err(StoreError::Unsupported(
                "archive descriptor exceeds bounded metadata budget",
            ));
        }
        if sid.is_empty()
            || sid.len() > 128
            || !sid
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'-')
            || request.channel_id <= 0
            || request.virtual_channel_id.is_empty()
            || request.virtual_channel_id.len() > 512
            || request.virtual_channel_id.contains(':')
            || request.provider_profile_id <= 0
            || request.pool_fields.get("user_id") != request.client_fields.get("user_id")
            || !request.pool_fields.contains_key("user_id")
        {
            return Err(StoreError::Unsupported("invalid archive descriptor"));
        }
        let permit = self
            .inner
            .slots
            .clone()
            .try_acquire_owned()
            .map_err(|_| unavailable())?;
        let (stop, _) = watch::channel(false);
        let entry = Arc::new(Entry {
            request,
            token: uuid::Uuid::new_v4().to_string(),
            lease: Mutex::new(None),
            active: AtomicBool::new(true),
            streaming: AtomicBool::new(false),
            bytes: AtomicU64::new(0),
            started: Mutex::new(Instant::now()),
            operations: AtomicUsize::new(0),
            setup_refresh: Mutex::new(None),
            operations_done: Notify::new(),
            stop,
            _permit: permit,
        });
        {
            let mut entries = self.inner.entries.lock().unwrap();
            if let Some(existing) = entries.get(&entry.request.session_id) {
                return Err(if existing.active.load(Ordering::SeqCst) {
                    StoreError::StatusBody(503, json!("Stream slot busy"))
                } else {
                    StoreError::Unsupported("archive idle adoption is not yet implemented")
                });
            }
            entries.insert(entry.request.session_id.clone(), entry.clone());
        }
        let rollback = Rollback {
            inner: self.inner.clone(),
            entry: entry.clone(),
        };
        if *self.inner.stopped.borrow() {
            entry.stop.send_replace(true);
            return Err(unavailable());
        }
        let admitted = timeshift::prepare(Admission {
            mode: timeshift::AdmissionMode::Create,
            session_id: &entry.request.session_id,
            channel_id: entry.request.channel_id,
            programme_media_id: entry
                .request
                .pool_fields
                .get("media_id")
                .map(String::as_str)
                .unwrap_or(""),
            fields: &entry.request.client_fields,
            stream_limit: entry.request.stream_limit,
            policy: &entry.request.limit_policy,
            owner_token: &entry.token,
            now: &timestamp(),
            ttl: self.inner.config.pending_admission_ttl,
            scan_limit: self.inner.config.admission_scan_limit,
        })
        .map_err(|_| StoreError::Unsupported("invalid Timeshift admission descriptor"))?;
        let args: Vec<_> = admitted.args.iter().map(Vec::as_slice).collect();
        let (result, _): (Vec<String>, Operation) = self
            .inner
            .transport
            .command(
                timeshift::SCRIPT,
                &admitted.keys,
                &args,
                Operation::new(&entry),
            )
            .await
            .map_err(|_| unavailable())?;
        let admission_error = match timeshift::decode(result) {
            Ok(true) => None,
            Ok(false) => Some(StoreError::Unsupported(
                "existing archive admission/adoption is not yet implemented",
            )),
            Err(dispatcharr_clients::Error::UserLimit) => {
                Some(StoreError::StatusBody(403, json!("Stream limit exceeded")))
            }
            Err(_) => Some(unavailable()),
        };
        if let Some(error) = admission_error {
            drop(rollback);
            wait_removed(&self.inner, &entry).await?;
            return Err(error);
        }
        start_setup_refresh(&self.inner, &entry);
        let lease = match self
            .inner
            .coordinator
            .acquire_timeshift(AcquireRequest {
                channel_id: entry.request.session_id.clone(),
                provider_profile_id: Some(entry.request.provider_profile_id),
                provider_max_streams: entry.request.provider_max_streams,
                credential_pool: None,
            })
            .await
        {
            Ok(lease) => lease,
            Err(error) => {
                drop(rollback);
                wait_removed(&self.inner, &entry).await?;
                return Err(match error {
                    dispatcharr_coordination::Error::ProfileFull => {
                        StoreError::StatusBody(503, json!("No available stream"))
                    }
                    _ => unavailable(),
                });
            }
        };
        *entry.lease.lock().unwrap() = Some(Arc::new(lease));
        let fields = serde_json::to_vec(&entry.request.pool_fields).map_err(|_| unavailable())?;
        // Transfer rollback through the actual remote reply. Caller cancellation
        // cannot delete before a delayed CREATE later creates an orphan hash.
        let (result, rollback): (i64, Rollback) = self
            .inner
            .transport
            .command(
                CREATE,
                &[pool(&entry)],
                &[entry.token.as_bytes(), &fields, timestamp().as_bytes()],
                rollback,
            )
            .await
            .map_err(|_| unavailable())?;
        if result != 1 {
            return Err(StoreError::Unsupported(
                "legacy or cross-worker archive adoption is not yet implemented",
            ));
        }
        if *self.inner.stopped.borrow() {
            entry.stop.send_replace(true);
            return Err(unavailable());
        }
        Ok(ArchiveTicket {
            rollback: Some(rollback),
        })
    }
    pub async fn format_index(&self, account: i64) -> Option<usize> {
        let (value, _) = self
            .inner
            .transport
            .command::<Option<usize>, _>(
                "return redis.call('GET',KEYS[1])",
                &[format!(":1:timeshift:format_idx:{account}")],
                &[],
                (),
            )
            .await
            .ok()?;
        value.filter(|index| *index < 7)
    }
    pub async fn remember_format(&self, account: i64, index: usize) -> Result<(), StoreError> {
        if index >= 7 {
            return Err(unavailable());
        }
        self.inner
            .transport
            .command::<String, _>(
                "return redis.call('SET',KEYS[1],ARGV[1],'EX',3600)",
                &[format!(":1:timeshift:format_idx:{account}")],
                &[index.to_string().as_bytes()],
                (),
            )
            .await
            .map_err(|_| unavailable())?;
        Ok(())
    }
    /// Snapshot only; stop new admissions before using this in shutdown logic.
    pub fn is_idle(&self) -> bool {
        self.inner.entries.lock().unwrap().is_empty()
    }
    pub async fn shutdown(&self) -> bool {
        self.inner.stopped.send_replace(true);
        self.inner.slots.close();
        for entry in self.inner.entries.lock().unwrap().values() {
            entry.stop.send_replace(true);
        }
        let result = tokio::time::timeout(self.inner.config.shutdown_timeout, async {
            loop {
                if self.inner.entries.lock().unwrap().is_empty() && self.inner.coordinator.is_idle()
                {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await;
        result.is_ok() && !self.inner.cleanup_failed.load(Ordering::SeqCst)
    }
}
impl ArchiveTicket {
    pub async fn abort(mut self) -> Result<(), StoreError> {
        let owned = self.rollback.take().expect("owned archive ticket");
        let inner = owned.inner.clone();
        let entry = owned.entry.clone();
        drop(owned);
        wait_removed(&inner, &entry).await
    }
    fn owned(&self) -> &Rollback {
        self.rollback.as_ref().expect("owned archive ticket")
    }
    pub fn stopped(&self) -> watch::Receiver<bool> {
        self.owned().entry.stop.subscribe()
    }
    pub fn lease_lost(&self) -> watch::Receiver<bool> {
        self.owned()
            .entry
            .lease
            .lock()
            .unwrap()
            .as_ref()
            .expect("provider lease")
            .lost()
    }
    pub fn forwarded(&self, bytes: usize) {
        self.owned()
            .entry
            .bytes
            .fetch_add(bytes as u64, Ordering::Relaxed);
    }
    pub fn total_bytes(&self) -> u64 {
        self.owned().entry.bytes.load(Ordering::Relaxed)
    }
    pub async fn start(
        &self,
        final_url: &str,
        content_length: Option<i128>,
        range: Option<&str>,
    ) -> Result<(), StoreError> {
        let owned = self.owned();
        let entry = &owned.entry;
        let clients =
            serde_json::to_vec(&entry.request.client_fields).map_err(|_| unavailable())?;
        let metadata =
            serde_json::to_vec(&entry.request.channel_fields).map_err(|_| unavailable())?;
        let serving_range = match range.filter(|value| !value.is_empty()) {
            None => "none",
            Some(value)
                if dispatcharr_timeshift_core::parse_client_range(Some(value))
                    .is_some_and(|(start, _)| start == 0) =>
            {
                "start"
            }
            Some(_) => "range",
        };
        let length = content_length
            .map(|value| value.to_string())
            .unwrap_or_default();
        let (result, _): (i64, Operation) = owned
            .inner
            .transport
            .command(
                START,
                &keys(entry),
                &[
                    entry.token.as_bytes(),
                    &clients,
                    &metadata,
                    entry.request.session_id.as_bytes(),
                    timestamp().as_bytes(),
                    final_url.as_bytes(),
                    length.as_bytes(),
                    serving_range.as_bytes(),
                ],
                Operation::new(entry),
            )
            .await
            .map_err(|_| unavailable())?;
        if result != 1 {
            return Err(unavailable());
        }
        *entry.started.lock().unwrap() = Instant::now();
        entry.streaming.store(true, Ordering::SeqCst);
        Ok(())
    }
    /// Source heartbeat/stop check. Call at a bounded interval while streaming;
    /// it also refreshes the API session without recreating missing metadata.
    pub async fn alive(&self) -> Result<bool, StoreError> {
        let owned = self.owned();
        let entry = &owned.entry;
        let(result,_):(i64,Operation)=owned.inner.transport.command("if redis.call('HGET',KEYS[1],'rust_owner_token')~=ARGV[1] or redis.call('EXISTS',KEYS[2])~=0 then return 0 end return 1",&[pool(entry),keys(entry)[6].clone()],&[entry.token.as_bytes()],Operation::new(entry)).await.map_err(|_|unavailable())?;
        Ok(result == 1)
    }
    pub async fn heartbeat(&self) -> Result<bool, StoreError> {
        let owned = self.owned();
        let entry = &owned.entry;
        let (result, _): (i64, Operation) = owned
            .inner
            .transport
            .command(
                HEARTBEAT,
                &keys(entry),
                &[
                    entry.token.as_bytes(),
                    timestamp().as_bytes(),
                    entry.bytes.load(Ordering::Relaxed).to_string().as_bytes(),
                ],
                Operation::new(entry),
            )
            .await
            .map_err(|_| unavailable())?;
        Ok(result == 1)
    }
}
async fn wait_removed(inner: &Arc<Inner>, entry: &Arc<Entry>) -> Result<(), StoreError> {
    tokio::time::timeout(inner.config.shutdown_timeout, async {
        loop {
            if !inner
                .entries
                .lock()
                .unwrap()
                .get(&entry.request.session_id)
                .is_some_and(|current| Arc::ptr_eq(current, entry))
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .map_err(|_| unavailable())
}
fn start_setup_refresh(inner: &Arc<Inner>, entry: &Arc<Entry>) {
    let weak_inner = Arc::downgrade(inner);
    let weak_entry = Arc::downgrade(entry);
    let interval = inner.config.setup_refresh_interval;
    let task=inner.runtime.spawn(async move {
        loop {
            tokio::time::sleep(interval).await;
            let (Some(inner),Some(entry))=(weak_inner.upgrade(),weak_entry.upgrade())else{return;};
            if !entry.active.load(Ordering::SeqCst) || entry.streaming.load(Ordering::SeqCst){return;}
            let key=keys(&entry)[1].clone();let ttl=inner.config.pending_admission_ttl.as_millis().to_string();
            let result=inner.transport.command::<i64,_>("if redis.call('HGET',KEYS[1],'rust_owner_token')~=ARGV[1] or redis.call('HGET',KEYS[1],'rust_preempted')=='1' then return 0 end redis.call('PEXPIRE',KEYS[1],ARGV[2]) return 1",&[key],&[entry.token.as_bytes(),ttl.as_bytes()],Operation::new(&entry)).await;
            if !matches!(result,Ok((1,_))){entry.stop.send_replace(true);return;}
        }
    });
    *entry.setup_refresh.lock().unwrap() = Some(task);
}
async fn finish(inner: Arc<Inner>, entry: Arc<Entry>) {
    entry.active.store(false, Ordering::SeqCst);
    let refresh = entry.setup_refresh.lock().unwrap().take();
    if let Some(refresh) = refresh {
        refresh.abort();
        let _ = refresh.await;
    }
    loop {
        let notified = entry.operations_done.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        if entry.operations.load(Ordering::SeqCst) == 0 {
            break;
        }
        notified.await;
    }
    let lease = entry.lease.lock().unwrap().clone();
    if let Some(lease) = &lease {
        if lease.release_provider().await.is_err() {
            inner.cleanup_failed.store(true, Ordering::SeqCst);
        }
    }
    entry.active.store(false, Ordering::SeqCst);
    let streaming = entry.streaming.load(Ordering::SeqCst);
    let mut remove_api = false;
    if streaming {
        let bytes = entry.bytes.load(Ordering::Relaxed);
        let (result, _) = inner
            .transport
            .command::<i64, _>(
                IDLE,
                &keys(&entry),
                &[
                    entry.token.as_bytes(),
                    timestamp().as_bytes(),
                    bytes.to_string().as_bytes(),
                ],
                (),
            )
            .await
            .unwrap_or((0, ()));
        if result != 1 {
            inner.cleanup_failed.store(true, Ordering::SeqCst);
        }
        // Existing source pools suppress native grace for every transfer under
        // two seconds, including a large startup read. Preserve that branch.
        remove_api = entry.started.lock().unwrap().elapsed() >= Duration::from_secs(2);
        let delay = if remove_api {
            inner.config.disconnect_grace
        } else {
            inner.config.startup_idle
        };
        let mut stopped = inner.stopped.subscribe();
        if !*stopped.borrow() {
            tokio::select! {_=tokio::time::sleep(delay)=>{},_=stopped.changed()=>{}}
        }
    }
    if *inner.stopped.borrow() {
        remove_api = false;
    }
    let (result, _) = inner
        .transport
        .command::<i64, _>(
            CLEANUP,
            &keys(&entry),
            &[
                entry.token.as_bytes(),
                if remove_api { b"1" } else { b"0" },
                entry
                    .request
                    .pool_fields
                    .get("user_id")
                    .map(String::as_bytes)
                    .unwrap_or(b""),
                entry.request.session_id.as_bytes(),
            ],
            (),
        )
        .await
        .unwrap_or((-1, ()));
    if result < 0 {
        inner.cleanup_failed.store(true, Ordering::SeqCst);
    }
    if let Some(lease) = &lease {
        if lease.release().await.is_err() {
            inner.cleanup_failed.store(true, Ordering::SeqCst);
        }
    }
    let mut entries = inner.entries.lock().unwrap();
    if entries
        .get(&entry.request.session_id)
        .is_some_and(|current| Arc::ptr_eq(current, &entry))
    {
        entries.remove(&entry.request.session_id);
    }
}
const CREATE: &str = r#"
if redis.call('EXISTS',KEYS[1])~=0 then return 0 end
local fields=cjson.decode(ARGV[2])
for key,value in pairs(fields) do redis.call('HSET',KEYS[1],key,value) end
redis.call('HSET',KEYS[1],'rust_owner_token',ARGV[1],'busy','1','last_activity',ARGV[3])
redis.call('EXPIRE',KEYS[1],600)
return 1
"#;
const START: &str = r#"
local function kind(key) local value=redis.call('TYPE',key);if type(value)=='table' then return value.ok end return value end
for i,expected in ipairs({'hash','hash','set','hash','hash','string','string'}) do local actual=kind(KEYS[i]);if actual~='none' and actual~=expected then return redis.error_reply('CORRUPT archive metadata') end end
if redis.call('HGET',KEYS[1],'rust_owner_token')~=ARGV[1] or redis.call('HGET',KEYS[2],'rust_owner_token')~=ARGV[1] or redis.call('HGET',KEYS[2],'rust_preempted')=='1' then return 0 end
local clients=cjson.decode(ARGV[2]);local metadata=cjson.decode(ARGV[3])
for key,value in pairs(clients) do redis.call('HSET',KEYS[2],key,value) end
for key,value in pairs(metadata) do redis.call('HSET',KEYS[4],key,value) end
redis.call('HSET',KEYS[2],'rust_owner_token',ARGV[1],'connected_at',ARGV[5],'last_active',ARGV[5],'position_anchor_at',ARGV[5],'rust_total_bytes','0')
redis.call('HSET',KEYS[4],'rust_owner_token',ARGV[1],'state','active','init_time',ARGV[5])
redis.call('SADD',KEYS[3],ARGV[4])
redis.call('HSET',KEYS[1],'final_url',ARGV[6],'serving_range',ARGV[8])
if ARGV[7]~='' then redis.call('HSET',KEYS[1],'content_length',ARGV[7]) end
for i=2,4 do redis.call('EXPIRE',KEYS[i],60) end
redis.call('SET',KEYS[6],'1','EX',600)
return 1
"#;
const HEARTBEAT: &str = r#"
local function kind(key) local value=redis.call('TYPE',key);if type(value)=='table' then return value.ok end return value end
for i,expected in ipairs({'hash','hash','set','hash','hash','string','string'}) do local actual=kind(KEYS[i]);if actual~='none' and actual~=expected then return redis.error_reply('CORRUPT archive metadata') end end
if redis.call('HGET',KEYS[1],'rust_owner_token')~=ARGV[1] or redis.call('EXISTS',KEYS[7])~=0 then return 0 end
if redis.call('HGET',KEYS[2],'rust_owner_token')~=ARGV[1] then return 0 end
if redis.call('EXISTS',KEYS[2])~=0 then
 local recorded=tonumber(redis.call('HGET',KEYS[2],'rust_total_bytes') or '0');local incoming=tonumber(ARGV[3])
 if not recorded or not incoming or recorded<0 or incoming<0 or incoming>9007199254740991 then return redis.error_reply('CORRUPT archive byte watermark') end
 if incoming>recorded then redis.call('HINCRBY',KEYS[4],'total_bytes',incoming-recorded);redis.call('HSET',KEYS[2],'rust_total_bytes',ARGV[3]) end
 redis.call('HSET',KEYS[2],'last_active',ARGV[2])
 for i=2,4 do redis.call('EXPIRE',KEYS[i],60) end
end
redis.call('HSET',KEYS[1],'last_activity',ARGV[2]);redis.call('EXPIRE',KEYS[1],600)
for i=5,6 do if redis.call('EXISTS',KEYS[i])~=0 then redis.call('EXPIRE',KEYS[i],600) end end
return 1
"#;
const IDLE: &str = r#"
if redis.call('HGET',KEYS[1],'rust_owner_token')~=ARGV[1] then return 0 end
if redis.call('EXISTS',KEYS[4])~=0 then
 local recorded=tonumber(redis.call('HGET',KEYS[2],'rust_total_bytes') or '0');local incoming=tonumber(ARGV[3])
 if not recorded or not incoming or recorded<0 or incoming<0 or incoming>9007199254740991 then return redis.error_reply('CORRUPT archive byte watermark') end
 if incoming>recorded then redis.call('HINCRBY',KEYS[4],'total_bytes',incoming-recorded);redis.call('HSET',KEYS[2],'rust_total_bytes',ARGV[3]) end
end
redis.call('HSET',KEYS[1],'busy','0','last_activity',ARGV[2]);redis.call('EXPIRE',KEYS[1],60)
return 1
"#;
const CLEANUP: &str = r#"
local token=redis.call('HGET',KEYS[1],'rust_owner_token')
if token and token~=ARGV[1] then return 0 end
local client_token=redis.call('HGET',KEYS[2],'rust_owner_token')
if not token and client_token~=ARGV[1] then return 0 end
if token==ARGV[1] then redis.call('DEL',KEYS[1]) end
if client_token==ARGV[1] then
 redis.call('DEL',KEYS[2]);redis.call('SREM',KEYS[3],ARGV[4])
 if redis.call('SCARD',KEYS[3])==0 then redis.call('DEL',KEYS[3]) end
end
if redis.call('HGET',KEYS[4],'rust_owner_token')==ARGV[1] then redis.call('DEL',KEYS[4]) end
if redis.call('GET',KEYS[6])=='1' then redis.call('DEL',KEYS[6]) end
redis.call('DEL',KEYS[7])
if ARGV[2]=='1' and redis.call('HGET',KEYS[5],'user_id')==ARGV[3] then redis.call('DEL',KEYS[5]) end
return 1
"#;
