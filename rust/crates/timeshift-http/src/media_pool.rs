//! Bounded archive reservation/lifecycle prototype ported from Timeshift views.
//! Original application credit belongs entirely to upstream Dispatcharr authors.
use dispatcharr_clients::timeshift::{self, Admission};
use dispatcharr_contracts::UserLimitPolicy;
use dispatcharr_coordination::{AcquireRequest, Coordinator, Lease, ProviderReservation};
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
    lifecycle: AtomicU64,
    handoff_pending: AtomicBool,
    retained_provider: AtomicBool,
    reopened: Notify,
    active: AtomicBool,
    streaming: AtomicBool,
    generation: AtomicU64,
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
    probe_slots: Arc<Semaphore>,
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
    refreshed: Option<ArchivePoolRequest>,
    rollback: Option<Rollback>,
}
/// A cached tail probe does not own or release the active stream's provider.
pub struct ArchiveProbe {
    inner: Arc<Inner>,
    final_url: String,
    length: Option<i128>,
    _permit: OwnedSemaphorePermit,
}
impl ArchiveProbe {
    pub fn final_url(&self) -> &str {
        &self.final_url
    }
    pub fn length(&self) -> Option<i128> {
        self.length
    }
    pub fn stopped(&self) -> watch::Receiver<bool> {
        self.inner.stopped.subscribe()
    }
}
struct Rollback {
    inner: Arc<Inner>,
    entry: Arc<Entry>,
}
struct Operation {
    entry: Arc<Entry>,
}
struct SeekAttempt {
    entry: Arc<Entry>,
}
impl Drop for SeekAttempt {
    fn drop(&mut self) {
        self.entry.handoff_pending.store(false, Ordering::SeqCst);
        self.entry.reopened.notify_waiters();
    }
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
        let state = self.entry.lifecycle.load(Ordering::SeqCst);
        let _ = self.entry.lifecycle.compare_exchange(
            state & !3,
            (state & !3) | 1,
            Ordering::SeqCst,
            Ordering::SeqCst,
        );
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
        format!("timeshift:grace:{stats}:{sid}"),
    ]
}
impl ArchivePool {
    pub(crate) fn is_local(&self, sid: &str) -> bool {
        self.inner.entries.lock().unwrap().contains_key(sid)
    }
    /// -1 means missing, 0 unclaimed/owned, 1 claimed by another user.
    pub(crate) async fn owner_state(&self, sid: &str, user: i64) -> Result<i64, StoreError> {
        if sid.len() > 128
            || sid
                .bytes()
                .any(|b| !b.is_ascii_alphanumeric() && b != b'_' && b != b'-')
        {
            return Err(StoreError::Unsupported(
                "archive client session ID exceeds supported key format",
            ));
        }
        let (state,_):(i64,())=self.inner.transport.command(
            "if redis.call('EXISTS',KEYS[1])==0 then return -1 end local profile=redis.call('HGET',KEYS[1],'profile_id');if not profile or profile=='' then return 0 end if redis.call('HGET',KEYS[1],'user_id')~=ARGV[1] then return 1 end return 0",
            &[format!("timeshift:pool:{sid}")],&[user.to_string().as_bytes()],()
        ).await.map_err(|_|unavailable())?;
        Ok(state)
    }
    pub(crate) async fn matching_session(
        &self,
        user: i64,
        channel: i64,
        media: &str,
        ip: &str,
        agent: &str,
        fresh: bool,
    ) -> Result<Option<String>, StoreError> {
        let (matched, _): (Option<String>, ()) = self
            .inner
            .transport
            .command(
                FIND_MATCH,
                &[],
                &[
                    user.to_string().as_bytes(),
                    format!("{channel}_").as_bytes(),
                    media.as_bytes(),
                    ip.as_bytes(),
                    agent.as_bytes(),
                    if fresh { b"1" } else { b"0" },
                    self.inner
                        .config
                        .admission_scan_limit
                        .to_string()
                        .as_bytes(),
                ],
                (),
            )
            .await
            .map_err(|_| unavailable())?;
        Ok(matched)
    }
    pub(crate) async fn redirect_capacity(
        &self,
        profile: i64,
        maximum: u32,
    ) -> Result<bool, StoreError> {
        if maximum == 0 {
            return Ok(true);
        }
        let (available,_):(i64,())=self.inner.transport.command(
            "local value=tonumber(redis.call('GET',KEYS[1]) or '0');if not value then return redis.error_reply('invalid profile counter') end return value<tonumber(ARGV[1]) and 1 or 0",
            &[format!("profile_connections:{profile}")],&[maximum.to_string().as_bytes()],()
        ).await.map_err(|_|unavailable())?;
        Ok(available == 1)
    }
    pub async fn busy_probe(
        &self,
        request: &ArchivePoolRequest,
        range: Option<&str>,
    ) -> Result<Option<ArchiveProbe>, StoreError> {
        if dispatcharr_timeshift_core::parse_client_range(range).is_none() {
            return Ok(None);
        }
        if *self.inner.stopped.borrow() {
            return Err(unavailable());
        }
        let existing = self
            .inner
            .entries
            .lock()
            .unwrap()
            .get(&request.session_id)
            .cloned();
        let Some(entry) = existing.filter(|entry| {
            entry.lifecycle.load(Ordering::SeqCst) & 3 == 0
                && entry.streaming.load(Ordering::SeqCst)
        }) else {
            return Ok(None);
        };
        if entry.request.provider_profile_id != request.provider_profile_id
            || entry.request.virtual_channel_id != request.virtual_channel_id
            || entry.request.pool_fields.get("user_id") != request.pool_fields.get("user_id")
        {
            return Ok(None);
        }
        let permit = self
            .inner
            .probe_slots
            .clone()
            .try_acquire_owned()
            .map_err(|_| unavailable())?;
        let (snapshot,_):(Vec<Option<String>>,Operation)=self.inner.transport.command(
            "if redis.call('HGET',KEYS[1],'rust_owner_token')~=ARGV[1] or redis.call('HGET',KEYS[1],'busy')~='1' then return {} end return {redis.call('HGET',KEYS[1],'final_url'),redis.call('HGET',KEYS[1],'content_length')}",
            &[pool(&entry)],&[entry.token.as_bytes()],Operation::new(&entry)
        ).await.map_err(|_|unavailable())?;
        if snapshot.len() != 2 {
            return Ok(None);
        }
        let length = snapshot[1].as_deref().and_then(|value| value.parse().ok());
        if !dispatcharr_timeshift_core::is_near_eof_probe(range, length) {
            return Ok(None);
        }
        let Some(final_url) = snapshot[0]
            .clone()
            .filter(|url| !url.is_empty() && url.len() <= 16384)
        else {
            return Ok(None);
        };
        let probe = ArchiveProbe {
            inner: self.inner.clone(),
            final_url,
            length,
            _permit: permit,
        };
        Ok(Some(
            check_existing(&self.inner, &entry, request, probe).await?,
        ))
    }
    /// A seek waits for the old supervised body to drain its final mutation,
    /// while retaining its provider reservation continuously across handoff.
    pub async fn open_playback(
        &self,
        request: ArchivePoolRequest,
        range: Option<&str>,
    ) -> Result<ArchiveTicket, StoreError> {
        if *self.inner.stopped.borrow() {
            return Err(unavailable());
        }
        let existing = self
            .inner
            .entries
            .lock()
            .unwrap()
            .get(&request.session_id)
            .cloned();
        let Some(entry) = existing.filter(|entry| {
            entry.lifecycle.load(Ordering::SeqCst) & 3 == 0
                && entry.streaming.load(Ordering::SeqCst)
        }) else {
            return self.open(request).await;
        };
        if entry.request.provider_profile_id != request.provider_profile_id
            || entry.request.virtual_channel_id != request.virtual_channel_id
            || entry.request.pool_fields.get("user_id") != request.pool_fields.get("user_id")
        {
            return Err(StoreError::Unsupported(
                "archive programme/profile busy adoption is not yet implemented",
            ));
        }
        let (snapshot,_):(Vec<Option<String>>,Operation)=self.inner.transport.command(
            "if redis.call('HGET',KEYS[1],'rust_owner_token')~=ARGV[1] then return {} end return {redis.call('HGET',KEYS[1],'content_length'),redis.call('HGET',KEYS[1],'serving_range')}",
            &[pool(&entry)],&[entry.token.as_bytes()],Operation::new(&entry)
        ).await.map_err(|_|unavailable())?;
        if snapshot.len() != 2 {
            return Err(unavailable());
        }
        let length = snapshot[0].as_deref().and_then(|value| value.parse().ok());
        let displace = dispatcharr_timeshift_core::should_displace_busy_playback(
            range,
            length,
            snapshot[1].as_deref(),
        );
        let restart = dispatcharr_timeshift_core::is_full_restart_range(range);
        if !(displace || restart) {
            return Err(StoreError::StatusBody(503, json!("Stream slot busy")));
        }
        if entry
            .handoff_pending
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return Err(StoreError::StatusBody(503, json!("Stream slot busy")));
        }
        let attempt = SeekAttempt {
            entry: entry.clone(),
        };
        let pool = self.clone();
        // Task ownership outlives a dropped HTTP setup future. Once the old
        // stream is cancelled, every exit must release or transfer its quota.
        self.inner.runtime.spawn(async move {
            let attempt=attempt;
            let (result,_):(i64,Operation)=pool.inner.transport.command(
                "if redis.call('HGET',KEYS[1],'rust_owner_token')~=ARGV[1] or redis.call('EXISTS',KEYS[2])~=0 then return 0 end local generation=redis.call('GET',KEYS[3]) or '1';redis.call('SET',KEYS[2],generation,'EX',60);return 1",
                &[super::media_pool::pool(&entry),keys(&entry)[6].clone(),keys(&entry)[5].clone()],&[entry.token.as_bytes()],Operation::new(&entry)
            ).await.map_err(|_|unavailable())?;
            if result!=1 {return Err(unavailable());}
            entry.stop.send_replace(true);
            let ticket=pool.reopen(entry,request).await;
            drop(attempt);
            ticket
        }).await.map_err(|_|unavailable())?
    }
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
        let probe_slots = Arc::new(Semaphore::new(config.max_sessions));
        let (stopped, _) = watch::channel(false);
        Ok(Self {
            inner: Arc::new(Inner {
                transport,
                runtime: tokio::runtime::Handle::current(),
                coordinator,
                entries: Mutex::new(HashMap::new()),
                slots,
                probe_slots,
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
        let existing = self.inner.entries.lock().unwrap().get(sid).cloned();
        if let Some(existing) = existing {
            return self.reopen(existing, request).await;
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
            lifecycle: AtomicU64::new(0),
            handoff_pending: AtomicBool::new(false),
            retained_provider: AtomicBool::new(false),
            reopened: Notify::new(),
            active: AtomicBool::new(true),
            streaming: AtomicBool::new(false),
            generation: AtomicU64::new(0),
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
            refreshed: None,
            rollback: Some(rollback),
        })
    }
    async fn reopen(
        &self,
        entry: Arc<Entry>,
        request: ArchivePoolRequest,
    ) -> Result<ArchiveTicket, StoreError> {
        if entry.request.provider_profile_id != request.provider_profile_id
            || entry.request.virtual_channel_id != request.virtual_channel_id
            || entry.request.pool_fields.get("user_id") != request.pool_fields.get("user_id")
            || entry.request.pool_fields.get("media_id") != request.pool_fields.get("media_id")
        {
            return Err(StoreError::Unsupported(
                "archive programme/profile adoption is not yet implemented",
            ));
        }
        // A just-disconnected supervised body may still be flushing its final
        // byte watermark and provider release. Never reopen before both finish.
        let deadline = tokio::time::Instant::now() + self.inner.config.command_timeout;
        let epoch = loop {
            let state = entry.lifecycle.load(Ordering::SeqCst);
            match state & 3 {
                0 | 1
                    if tokio::time::Instant::now() < deadline
                        && (state & 3 == 1 || entry.handoff_pending.load(Ordering::SeqCst)) =>
                {
                    tokio::time::sleep(Duration::from_millis(5)).await
                }
                2 => {
                    if entry
                        .lifecycle
                        .compare_exchange(
                            state,
                            (state & !3) + 4,
                            Ordering::SeqCst,
                            Ordering::SeqCst,
                        )
                        .is_ok()
                    {
                        break (state & !3) + 4;
                    }
                }
                _ => return Err(StoreError::StatusBody(503, json!("Stream slot busy"))),
            }
        };
        entry.reopened.notify_waiters();
        let was_handoff = entry.handoff_pending.swap(false, Ordering::SeqCst);
        let retained = entry.retained_provider.swap(false, Ordering::SeqCst);
        if was_handoff && !*self.inner.stopped.borrow() {
            entry.stop.send_replace(false);
        }
        entry.active.store(true, Ordering::SeqCst);
        entry.streaming.store(false, Ordering::SeqCst);
        entry.bytes.store(0, Ordering::SeqCst);
        debug_assert_eq!(entry.lifecycle.load(Ordering::SeqCst), epoch);
        let rollback = Rollback {
            inner: self.inner.clone(),
            entry: entry.clone(),
        };
        check_existing(&self.inner, &entry, &request, ()).await?;
        start_setup_refresh(&self.inner, &entry);
        let lease = entry
            .lease
            .lock()
            .unwrap()
            .clone()
            .ok_or_else(unavailable)?;
        let provider = ProviderReservation {
            profile_id: Some(request.provider_profile_id),
            max_streams: request.provider_max_streams,
            credential_pool: None,
        };
        // This owned task retains rollback through quota reservation even when
        // the HTTP setup future is dropped before Redis returns.
        let task = self.inner.runtime.spawn(async move {
            let result = if retained {
                Ok(())
            } else {
                lease.reserve_provider(provider).await
            };
            (result, rollback)
        });
        let (reserved, rollback) = task.await.map_err(|_| unavailable())?;
        reserved.map_err(|error| match error {
            dispatcharr_coordination::Error::ProfileFull => {
                StoreError::StatusBody(503, json!("No available stream"))
            }
            _ => unavailable(),
        })?;
        let (result,rollback):(i64,Rollback)=self.inner.transport.command(
            "if redis.call('HGET',KEYS[1],'rust_owner_token')~=ARGV[1] or (redis.call('HGET',KEYS[1],'busy')~='0' and ARGV[3]~='1') or (redis.call('EXISTS',KEYS[2])~=0 and ARGV[3]~='1') then return 0 end redis.call('HSET',KEYS[1],'busy','1','last_activity',ARGV[2]);redis.call('EXPIRE',KEYS[1],600);return 1",
            &[pool(&entry),keys(&entry)[6].clone()],&[entry.token.as_bytes(),timestamp().as_bytes(),if retained {b"1"} else {b"0"}],rollback
        ).await.map_err(|_|unavailable())?;
        if result != 1 || *self.inner.stopped.borrow() || *entry.stop.borrow() {
            return Err(unavailable());
        }
        Ok(ArchiveTicket {
            refreshed: Some(request),
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
    pub(crate) fn signal_management_stop(&self, sid: &str) {
        if let Some(entry) = self.inner.entries.lock().unwrap().get(sid) {
            entry.stop.send_replace(true);
        }
    }
    /// Snapshot only; stop new admissions before using this in shutdown logic.
    pub fn is_idle(&self) -> bool {
        self.inner.entries.lock().unwrap().is_empty()
    }
    pub async fn shutdown(&self) -> bool {
        self.inner.stopped.send_replace(true);
        self.inner.slots.close();
        self.inner.probe_slots.close();
        for entry in self.inner.entries.lock().unwrap().values() {
            entry.stop.send_replace(true);
        }
        let result = tokio::time::timeout(self.inner.config.shutdown_timeout, async {
            loop {
                if self.inner.entries.lock().unwrap().is_empty()
                    && self.inner.coordinator.is_idle()
                    && self.inner.probe_slots.available_permits() == self.inner.config.max_sessions
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
    pub async fn cached_final_url(&self) -> Result<Option<String>, StoreError> {
        let owned = self.owned();
        let (url,_):(Option<String>,Operation)=owned.inner.transport.command(
            "if redis.call('HGET',KEYS[1],'rust_owner_token')~=ARGV[1] then return nil end return redis.call('HGET',KEYS[1],'final_url')",
            &[pool(&owned.entry)],&[owned.entry.token.as_bytes()],Operation::new(&owned.entry)
        ).await.map_err(|_|unavailable())?;
        Ok(url)
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
        let request = self.refreshed.as_ref().unwrap_or(&entry.request);
        let clients = serde_json::to_vec(&request.client_fields).map_err(|_| unavailable())?;
        let metadata = serde_json::to_vec(&request.channel_fields).map_err(|_| unavailable())?;
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
        let range_start =
            dispatcharr_timeshift_core::parse_client_range(range).map(|(start, _)| start);
        let duration = request
            .pool_fields
            .get("rust_duration_secs")
            .and_then(|value| value.parse::<f64>().ok());
        let base = range_start
            .filter(|start| *start > 0)
            .zip(content_length.filter(|total| *total > 0))
            .zip(duration.filter(|duration| duration.is_finite() && *duration > 0.0))
            .map(|((start, total), duration)| {
                ((start as f64 / total as f64).clamp(0.0, 1.0) * duration).to_string()
            })
            .unwrap_or_default();
        let near_eof = range_start
            .zip(content_length)
            .is_some_and(|(start, total)| {
                total > 0 && start >= total.saturating_sub(1_880_000).max(0)
            });
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
                    base.as_bytes(),
                    if near_eof { b"1" } else { b"0" },
                ],
                Operation::new(entry),
            )
            .await
            .map_err(|_| unavailable())?;
        if result < 1 {
            return Err(unavailable());
        }
        entry.generation.store(result as u64, Ordering::SeqCst);
        *entry.started.lock().unwrap() = Instant::now();
        entry.streaming.store(true, Ordering::SeqCst);
        Ok(())
    }
    /// Source heartbeat/stop check. Call at a bounded interval while streaming;
    /// it also refreshes the API session without recreating missing metadata.
    pub async fn alive(&self) -> Result<bool, StoreError> {
        let owned = self.owned();
        let entry = &owned.entry;
        let(result,_):(i64,Operation)=owned.inner.transport.command("if redis.call('HGET',KEYS[1],'rust_owner_token')~=ARGV[1] then return 0 end local stop=redis.call('GET',KEYS[2]);if stop=='reuse' or stop=='hop' or stop=='admin' or stop=='limit' then return 0 end if stop and string.match(stop,'^%d+$') and tonumber(stop)>=tonumber(ARGV[2]) then return 0 end return 1",&[pool(entry),keys(entry)[6].clone()],&[entry.token.as_bytes(),entry.generation.load(Ordering::SeqCst).to_string().as_bytes()],Operation::new(entry)).await.map_err(|_|unavailable())?;
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
                    entry
                        .generation
                        .load(Ordering::SeqCst)
                        .to_string()
                        .as_bytes(),
                ],
                Operation::new(entry),
            )
            .await
            .map_err(|_| unavailable())?;
        Ok(result == 1)
    }
}
async fn check_existing<G: Send + 'static>(
    inner: &Arc<Inner>,
    entry: &Arc<Entry>,
    request: &ArchivePoolRequest,
    guard: G,
) -> Result<G, StoreError> {
    if request
        .client_fields
        .iter()
        .map(|(key, value)| key.len().saturating_add(value.len()))
        .fold(0usize, usize::saturating_add)
        > 65536
    {
        return Err(StoreError::Unsupported(
            "archive admission exceeds bounded metadata budget",
        ));
    }
    let prepared = timeshift::prepare(Admission {
        mode: timeshift::AdmissionMode::Check,
        session_id: &request.session_id,
        channel_id: request.channel_id,
        programme_media_id: request
            .pool_fields
            .get("media_id")
            .map(String::as_str)
            .unwrap_or(""),
        fields: &request.client_fields,
        stream_limit: request.stream_limit,
        policy: &request.limit_policy,
        owner_token: &entry.token,
        now: &timestamp(),
        ttl: inner.config.pending_admission_ttl,
        scan_limit: inner.config.admission_scan_limit,
    })
    .map_err(|_| unavailable())?;
    let args: Vec<_> = prepared.args.iter().map(Vec::as_slice).collect();
    let (result, (_, guard)): (Vec<String>, (Operation, G)) = inner
        .transport
        .command(
            timeshift::SCRIPT,
            &prepared.keys,
            &args,
            (Operation::new(entry), guard),
        )
        .await
        .map_err(|_| unavailable())?;
    match timeshift::decode(result) {
        Ok(true) => Ok(guard),
        Err(dispatcharr_clients::Error::UserLimit) => {
            Err(StoreError::StatusBody(403, json!("Stream limit exceeded")))
        }
        _ => Err(unavailable()),
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
    let epoch = entry.lifecycle.load(Ordering::SeqCst) & !3;
    entry.lifecycle.store(epoch | 1, Ordering::SeqCst);
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
    let handoff =
        entry.handoff_pending.load(Ordering::SeqCst) && entry.streaming.load(Ordering::SeqCst);
    entry.retained_provider.store(handoff, Ordering::SeqCst);
    let lease = entry.lease.lock().unwrap().clone();
    if let Some(lease) = lease.as_ref().filter(|_| !handoff) {
        if lease.release_provider().await.is_err() {
            inner.cleanup_failed.store(true, Ordering::SeqCst);
        }
    }
    entry.active.store(false, Ordering::SeqCst);
    let streaming = entry.streaming.load(Ordering::SeqCst);
    let mut remove_api = false;
    let grace_token = format!("{}-{epoch}", entry.token);
    if streaming {
        remove_api = !handoff && entry.started.lock().unwrap().elapsed() >= Duration::from_secs(2);
        let bytes = entry.bytes.load(Ordering::Relaxed);
        let cleanup_generation = !handoff
            && (bytes >= 65536
                || entry.started.lock().unwrap().elapsed() >= Duration::from_secs(2));
        let (result, _) = inner
            .transport
            .command::<i64, _>(
                IDLE,
                &keys(&entry),
                &[
                    entry.token.as_bytes(),
                    timestamp().as_bytes(),
                    bytes.to_string().as_bytes(),
                    if handoff { b"1" } else { b"0" },
                    if cleanup_generation { b"1" } else { b"0" },
                    if remove_api { b"1" } else { b"0" },
                    grace_token.as_bytes(),
                    (SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_secs_f64()
                        + inner.config.disconnect_grace.as_secs_f64())
                    .to_string()
                    .as_bytes(),
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
        let delay = if handoff {
            inner.config.command_timeout
        } else if remove_api {
            inner.config.disconnect_grace
        } else {
            inner.config.startup_idle
        };
        let reopened = entry.reopened.notified();
        tokio::pin!(reopened);
        reopened.as_mut().enable();
        entry.lifecycle.store(epoch | 2, Ordering::SeqCst);
        let mut stopped = inner.stopped.subscribe();
        if !*stopped.borrow() && (!handoff || entry.handoff_pending.load(Ordering::SeqCst)) {
            tokio::select! {_=tokio::time::sleep(delay)=>{},_=stopped.changed()=>{},_=reopened=>{}}
        }
        if entry
            .lifecycle
            .compare_exchange(epoch | 2, epoch | 3, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return;
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
                grace_token.as_bytes(),
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
redis.call('DEL',KEYS[8]);redis.call('HDEL',KEYS[2],'stats_grace_deadline','stats_grace_disconnected_at')
local clients=cjson.decode(ARGV[2]);local metadata=cjson.decode(ARGV[3])
local old_programme=redis.call('HGET',KEYS[2],'programme_start')
local keep_anchor=ARGV[10]=='1' and (not old_programme or old_programme==clients['programme_start'])
local old_anchor=redis.call('HGET',KEYS[2],'position_anchor_at')
for key,value in pairs(clients) do redis.call('HSET',KEYS[2],key,value) end
for key,value in pairs(metadata) do redis.call('HSET',KEYS[4],key,value) end
redis.call('HSETNX',KEYS[2],'connected_at',ARGV[5])
redis.call('HSET',KEYS[2],'rust_owner_token',ARGV[1],'last_active',ARGV[5],'rust_total_bytes','0')
if not keep_anchor then
 redis.call('HSET',KEYS[2],'position_anchor_at',ARGV[5]);redis.call('HDEL',KEYS[2],'paused')
 if ARGV[9]~='' and (not old_programme or old_programme==clients['programme_start']) then redis.call('HSET',KEYS[2],'playback_base_secs',ARGV[9]) else redis.call('HDEL',KEYS[2],'playback_base_secs') end
elseif not old_anchor then redis.call('HSET',KEYS[2],'position_anchor_at',ARGV[5]) end
redis.call('HSETNX',KEYS[4],'init_time',ARGV[5])
redis.call('HSET',KEYS[4],'rust_owner_token',ARGV[1],'state','active')
redis.call('SADD',KEYS[3],ARGV[4])
redis.call('HSET',KEYS[1],'final_url',ARGV[6],'serving_range',ARGV[8])
if ARGV[7]~='' then redis.call('HSET',KEYS[1],'content_length',ARGV[7]) end
for i=2,4 do redis.call('EXPIRE',KEYS[i],60) end
local generation=redis.call('INCR',KEYS[6]);redis.call('EXPIRE',KEYS[6],600);redis.call('DEL',KEYS[7])
return generation
"#;
const HEARTBEAT: &str = r#"
local function kind(key) local value=redis.call('TYPE',key);if type(value)=='table' then return value.ok end return value end
for i,expected in ipairs({'hash','hash','set','hash','hash','string','string'}) do local actual=kind(KEYS[i]);if actual~='none' and actual~=expected then return redis.error_reply('CORRUPT archive metadata') end end
if redis.call('HGET',KEYS[1],'rust_owner_token')~=ARGV[1] then return 0 end
local stop=redis.call('GET',KEYS[7]);if stop=='reuse' or stop=='hop' or stop=='admin' or stop=='limit' then return 0 end
if stop and string.match(stop,'^%d+$') and tonumber(stop)>=tonumber(ARGV[4]) then return 0 end
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
if ARGV[5]=='1' then redis.call('DEL',KEYS[6]) end
if ARGV[6]=='1' then
 redis.call('SET',KEYS[8],ARGV[7],'EX',math.max(1,math.floor(tonumber(ARGV[8])-tonumber(ARGV[2])+2)))
 redis.call('HSET',KEYS[2],'stats_grace_deadline',ARGV[8],'stats_grace_disconnected_at',ARGV[2])
end
redis.call('HSET',KEYS[1],'busy',ARGV[4]=='1' and '1' or '0','last_activity',ARGV[2]);redis.call('EXPIRE',KEYS[1],ARGV[4]=='1' and 600 or 60)
return 1
"#;
const CLEANUP: &str = r#"
local token=redis.call('HGET',KEYS[1],'rust_owner_token')
if token and token~=ARGV[1] then return 0 end
local client_token=redis.call('HGET',KEYS[2],'rust_owner_token')
local own_grace=redis.call('GET',KEYS[8])==ARGV[5]
if not token and client_token~=ARGV[1] and not own_grace then return 0 end
local keep_idle_pool=ARGV[2]=='1' and redis.call('EXISTS',KEYS[5])==0 and redis.call('GET',KEYS[7])=='admin'
if token==ARGV[1] then
 if keep_idle_pool then redis.call('EXPIRE',KEYS[1],60) else redis.call('DEL',KEYS[1]) end
end
if client_token==ARGV[1] or (not client_token and own_grace) then
 redis.call('DEL',KEYS[2]);redis.call('SREM',KEYS[3],ARGV[4])
 if redis.call('SCARD',KEYS[3])==0 then redis.call('DEL',KEYS[3]) end
end
if redis.call('HGET',KEYS[4],'rust_owner_token')==ARGV[1] then redis.call('DEL',KEYS[4]) end
redis.call('DEL',KEYS[6])
if redis.call('GET',KEYS[7])~='admin' then redis.call('DEL',KEYS[7]) end
if own_grace then redis.call('DEL',KEYS[8]) end
if ARGV[2]=='1' and redis.call('HGET',KEYS[5],'user_id')==ARGV[3] then redis.call('DEL',KEYS[5]) end
return 1
"#;

const FIND_MATCH: &str = r#"
local cursor='0';local examined=0;local best=nil;local best_exact=-1;local best_time=0
repeat
 examined=examined+1;if examined>tonumber(ARGV[7]) then return redis.error_reply('archive pool scan exceeds limit') end
 local result=redis.call('SCAN',cursor,'MATCH','timeshift:pool:*','COUNT',100);cursor=result[1]
 for _,key in ipairs(result[2]) do
  examined=examined+1;if examined>tonumber(ARGV[7]) then return redis.error_reply('archive pool scan exceeds limit') end
  local kind=redis.call('TYPE',key);if type(kind)=='table' then kind=kind.ok end
  if kind=='hash' then
   local data=redis.call('HMGET',key,'user_id','media_id','busy','client_ip','client_user_agent','last_activity')
   local exact=data[2]==ARGV[3] and 1 or 0
   local channel=data[2] and string.sub(data[2],1,string.len(ARGV[2]))==ARGV[2]
   local eligible=not (ARGV[6]=='1' and exact==1 and data[3]~='1')
   local fingerprint=ARGV[4]~='' and ARGV[5]~='' and data[4]==ARGV[4] and data[5]==ARGV[5]
   local activity=tonumber(data[6] or '0')
   if data[1]==ARGV[1] and (exact==1 or channel) and eligible and fingerprint and activity then
    if not best or exact>best_exact or (exact==best_exact and activity>best_time) then
     best=string.match(key,':([^:]+)$');best_exact=exact;best_time=activity
    end
   end
  end
 end
until cursor=='0'
return best
"#;

#[cfg(test)]
mod direct_tests {
    use super::*;
    use redis::AsyncCommands;
    #[tokio::test]
    #[ignore = "requires disposable REDIS_TEST_URL; source-derived readonly helper contracts"]
    async fn fingerprint_exact_recency_fresh_probe_and_redirect_capacity_are_readonly() {
        let url = std::env::var("REDIS_TEST_URL").unwrap();
        let coordinator = Coordinator::connect(
            &url,
            dispatcharr_coordination::LeaseConfig {
                namespace: format!("test:direct-lookup:{}", uuid::Uuid::new_v4()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        let pool = ArchivePool::connect(&url, coordinator, ArchivePoolConfig::default())
            .await
            .unwrap();
        let mut redis = redis::Client::open(url)
            .unwrap()
            .get_multiplexed_async_connection()
            .await
            .unwrap();
        let user = (uuid::Uuid::new_v4().as_u128() % 1_000_000_000_000) as i64 + 1000;
        let first = format!("fp_{}", uuid::Uuid::new_v4().simple());
        let recent = format!("fp_{}", uuid::Uuid::new_v4().simple());
        let exact = format!("fp_{}", uuid::Uuid::new_v4().simple());
        let pairs = [
            (&first, "123_programme", 10),
            (&recent, "123_other", 40),
            (&exact, "123_programme", 30),
        ];
        for (sid, media, time) in pairs {
            let _: () = redis
                .hset_multiple(
                    format!("timeshift:pool:{sid}"),
                    &[
                        ("user_id", user.to_string()),
                        ("media_id", media.into()),
                        ("client_ip", "127.0.0.1".into()),
                        ("client_user_agent", "source-fingerprint-agent".into()),
                        ("busy", "0".into()),
                        ("last_activity", time.to_string()),
                        ("profile_id", user.to_string()),
                    ],
                )
                .await
                .unwrap();
        }
        assert_eq!(
            pool.matching_session(
                user,
                123,
                "123_programme",
                "127.0.0.1",
                "source-fingerprint-agent",
                false
            )
            .await
            .unwrap(),
            Some(exact.clone())
        );
        assert_eq!(
            pool.matching_session(
                user,
                123,
                "123_programme",
                "127.0.0.1",
                "source-fingerprint-agent",
                true
            )
            .await
            .unwrap(),
            Some(recent.clone())
        );
        let _: () = redis
            .hset(format!("timeshift:pool:{exact}"), "busy", "1")
            .await
            .unwrap();
        assert_eq!(
            pool.matching_session(
                user,
                123,
                "123_programme",
                "127.0.0.1",
                "source-fingerprint-agent",
                true
            )
            .await
            .unwrap(),
            Some(exact.clone())
        );
        assert!(pool
            .matching_session(
                user,
                123,
                "123_programme",
                "127.0.0.1",
                "wrong-agent",
                false
            )
            .await
            .unwrap()
            .is_none());
        assert!(pool
            .matching_session(
                user + 1,
                123,
                "123_programme",
                "127.0.0.1",
                "source-fingerprint-agent",
                false
            )
            .await
            .unwrap()
            .is_none());
        assert_eq!(pool.owner_state(&exact, user).await.unwrap(), 0);
        assert_eq!(pool.owner_state(&exact, user + 1).await.unwrap(), 1);
        let counter = format!("profile_connections:{user}");
        let _: () = redis.set(&counter, 1).await.unwrap();
        assert!(!pool.redirect_capacity(user, 1).await.unwrap());
        assert!(pool.redirect_capacity(user, 0).await.unwrap());
        assert_eq!(redis.get::<_, i64>(&counter).await.unwrap(), 1);
        let _: () = redis
            .del(&[
                format!("timeshift:pool:{first}"),
                format!("timeshift:pool:{recent}"),
                format!("timeshift:pool:{exact}"),
                counter,
            ])
            .await
            .unwrap();
        assert!(pool.shutdown().await);
    }
}
