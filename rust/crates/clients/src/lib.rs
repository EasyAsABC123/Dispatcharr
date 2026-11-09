//! Live client metadata, stream-limit admission, and bounded batched telemetry.
//! Original Dispatcharr author credit and retained license: ATTRIBUTION.md.
mod admission;
mod channel;
mod era;
mod hls;
mod scripts;
mod statistics;
mod statistics_catalog;
pub use statistics_catalog::{ChannelStatisticsPage, ChannelStatisticsRecord};
pub mod timeshift;
pub mod vod;
pub use hls::HlsSessionInfo;

use dispatcharr_contracts::PlaybackViewer;
use redis::{aio::MultiplexedConnection, Script};
use serde_json::json;
use std::{
    collections::{BTreeMap, HashMap},
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, Mutex, Weak,
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::{
    sync::{watch, OwnedSemaphorePermit, Semaphore},
    time,
};

#[derive(Clone, Debug)]
pub struct ClientConfig {
    pub worker_id: String,
    pub max_clients: usize,
    pub ttl: Duration,
    pub flush_interval: Duration,
    pub ghost_timeout: Duration,
    pub hls_ghost_timeout: Duration,
    pub command_timeout: Duration,
    pub admission_scan_limit: usize,
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            worker_id: format!("rust-{}", uuid::Uuid::new_v4()),
            max_clients: 12_800,
            ttl: Duration::from_secs(60),
            flush_interval: Duration::from_secs(1),
            ghost_timeout: Duration::from_secs(50),
            hls_ghost_timeout: Duration::from_secs(12),
            command_timeout: Duration::from_secs(3),
            admission_scan_limit: 50_000,
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum Error {
    InvalidConfiguration,
    InvalidClient,
    Capacity,
    UserLimit,
    ScanLimit,
    CorruptMetadata,
    RedisUnavailable,
    Stopped,
}

struct Inner {
    runtime: tokio::runtime::Handle,
    control_slots: Arc<Semaphore>,
    control_queue: Arc<Semaphore>,
    connection: MultiplexedConnection,
    config: ClientConfig,
    clients: Mutex<HashMap<String, Weak<ClientInner>>>,
    live_stats: Mutex<statistics::State>,
    statistics_bytes: Arc<Semaphore>,
    slots: Arc<Semaphore>,
    stopped: AtomicBool,
    register: Script,
    heartbeat_sha: String,
}

#[derive(Clone)]
pub struct ClientStore {
    inner: Arc<Inner>,
}

struct ClientInner {
    runtime: tokio::runtime::Handle,
    store: Weak<Inner>,
    connection: MultiplexedConnection,
    config: ClientConfig,
    id: String,
    channel: String,
    token: String,
    bytes: AtomicU64,
    chunks: AtomicU64,
    connected_at: f64,
    user_id: Option<i64>,
    output_format: String,
    touched: AtomicBool,
    last_flush_bytes: AtomicU64,
    stop: watch::Sender<bool>,
    released: AtomicBool,
    registered: AtomicBool,
    stats_announced: AtomicBool,
    cleanup_done: watch::Sender<bool>,
    cleanup_error: Arc<AtomicBool>,
    _permit: Arc<OwnedSemaphorePermit>,
}

#[derive(Clone)]
pub struct ClientLease {
    inner: Arc<ClientInner>,
}

fn now() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs_f64()
}
fn keys(channel: &str, id: &str) -> (String, String, String) {
    (
        format!("live:channel:{channel}:clients:{id}"),
        format!("live:channel:{channel}:clients"),
        format!("live:channel:{channel}:client:{id}:stop"),
    )
}

impl ClientStore {
    pub fn worker_id(&self) -> &str {
        &self.inner.config.worker_id
    }

    /// Update legacy channel metadata without ever overwriting a newer owner.
    /// Media itself remains in the bounded Rust ring; only the legacy index and
    /// telemetry hashes are mirrored into Redis.
    pub async fn publish_channel(
        &self,
        channel: &str,
        owner: &str,
        fence: &str,
        fields: &BTreeMap<String, String>,
        index: u64,
    ) -> Result<(), Error> {
        self.publish_channel_fields(channel, owner, fence, fields, Some(index))
            .await
    }

    /// Shared input publication owns its index; telemetry must not rewind it.
    pub async fn publish_channel_shared(
        &self,
        channel: &str,
        owner: &str,
        fence: &str,
        fields: &BTreeMap<String, String>,
    ) -> Result<(), Error> {
        self.publish_channel_fields(channel, owner, fence, fields, None)
            .await
    }
    async fn publish_channel_fields(
        &self,
        channel: &str,
        owner: &str,
        fence: &str,
        fields: &BTreeMap<String, String>,
        index: Option<u64>,
    ) -> Result<(), Error> {
        let script = Script::new(
            r#"
if redis.call('EXISTS',KEYS[5])==1 then return 0 end
local current=redis.call('GET',KEYS[2])
if (ARGV[1]~='' and current~=ARGV[1]) or (ARGV[1]=='' and current) then return 0 end
if redis.call('HGET',KEYS[1],'rust_token')~=ARGV[2] then redis.call('DEL',KEYS[1]) end
local fields=cjson.decode(ARGV[3])
for key,value in pairs(fields) do redis.call('HSET',KEYS[1],key,value) end
redis.call('HSET',KEYS[1],'rust_token',ARGV[2])
redis.call('PEXPIRE',KEYS[1],ARGV[5])
if ARGV[4]~='' then redis.call('SET',KEYS[3],ARGV[4],'PX',ARGV[5]) end
redis.call('SET',KEYS[4],ARGV[6],'PX',ARGV[5])
return 1
"#,
        );
        let mut connection = self.inner.connection.clone();
        let result: i64 = time::timeout(
            self.inner.config.command_timeout,
            script
                .key(format!("live:channel:{channel}:metadata"))
                .key(format!("live:channel:{channel}:owner"))
                .key(format!("live:channel:{channel}:input:buffer:index"))
                .key(format!(
                    "live:worker:{}:heartbeat",
                    self.inner.config.worker_id
                ))
                .key(format!("live:channel:{channel}:stopping"))
                .arg(owner)
                .arg(fence)
                .arg(serde_json::to_string(fields).map_err(|_| Error::InvalidClient)?)
                .arg(index.map_or(String::new(), |value| value.to_string()))
                .arg(self.inner.config.ttl.as_millis() as u64)
                .arg(now())
                .invoke_async(&mut connection),
        )
        .await
        .map_err(|_| Error::RedisUnavailable)?
        .map_err(|_| Error::RedisUnavailable)?;
        if result == 1 {
            if fields
                .get("state")
                .is_some_and(|state| !matches!(state.as_str(), "initializing" | "error"))
            {
                statistics::channel_ready(&self.inner, channel);
            }
            Ok(())
        } else {
            Err(Error::Stopped)
        }
    }

    pub async fn remove_channel(&self, channel: &str, fence: &str) -> Result<(), Error> {
        self.remove_channel_fields(channel, fence, true).await
    }
    pub async fn remove_channel_shared(&self, channel: &str, fence: &str) -> Result<(), Error> {
        self.remove_channel_fields(channel, fence, false).await
    }
    async fn remove_channel_fields(
        &self,
        channel: &str,
        fence: &str,
        remove_index: bool,
    ) -> Result<(), Error> {
        let script=Script::new("if redis.call('HGET',KEYS[1],'rust_token')~=ARGV[1] then return 0 end if ARGV[2]=='1' then redis.call('DEL',KEYS[1],KEYS[2]) else redis.call('DEL',KEYS[1]) end return 1");
        let mut connection = self.inner.connection.clone();
        let removed: i64 = time::timeout(
            self.inner.config.command_timeout,
            script
                .key(format!("live:channel:{channel}:metadata"))
                .key(format!("live:channel:{channel}:input:buffer:index"))
                .arg(fence)
                .arg(if remove_index { "1" } else { "0" })
                .invoke_async(&mut connection),
        )
        .await
        .map_err(|_| Error::RedisUnavailable)?
        .map_err(|_| Error::RedisUnavailable)?;
        if removed == 1 {
            statistics::channel_closed(&self.inner, channel);
        }
        Ok(())
    }

    pub async fn channel_snapshot(
        &self,
        channel: &str,
    ) -> Result<Option<(BTreeMap<String, String>, u64)>, Error> {
        if channel.is_empty() || channel.len() > 256 || channel.contains(':') {
            return Err(Error::InvalidClient);
        }
        let mut connection = self.inner.connection.clone();
        let mut pipeline = redis::pipe();
        pipeline
            .atomic()
            .cmd("HGETALL")
            .arg(format!("live:channel:{channel}:metadata"))
            .cmd("GET")
            .arg(format!("live:channel:{channel}:input:buffer:index"));
        let (metadata, index): (BTreeMap<String, String>, Option<u64>) = time::timeout(
            self.inner.config.command_timeout,
            pipeline.query_async(&mut connection),
        )
        .await
        .map_err(|_| Error::RedisUnavailable)?
        .map_err(|_| Error::RedisUnavailable)?;
        Ok((!metadata.is_empty()).then_some((metadata, index.unwrap_or(0))))
    }

    pub async fn channel_clients(&self, channel: &str) -> Result<Vec<serde_json::Value>, Error> {
        let set = format!("live:channel:{channel}:clients");
        let mut connection = self.inner.connection.clone();
        let script=Script::new("if redis.call('SCARD',KEYS[1])>tonumber(ARGV[1]) then return redis.error_reply('client-read-limit') end return redis.call('SMEMBERS',KEYS[1])");
        let ids: Vec<String> = time::timeout(
            self.inner.config.command_timeout,
            script
                .key(&set)
                .arg(self.inner.config.max_clients)
                .invoke_async(&mut connection),
        )
        .await
        .map_err(|_| Error::RedisUnavailable)?
        .map_err(|_| Error::RedisUnavailable)?;
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let mut pipeline = redis::pipe();
        for id in &ids {
            pipeline.cmd("HGETALL").arg(keys(channel, id).0);
        }
        let rows: Vec<BTreeMap<String, String>> = time::timeout(
            self.inner.config.command_timeout,
            pipeline.query_async(&mut connection),
        )
        .await
        .map_err(|_| Error::RedisUnavailable)?
        .map_err(|_| Error::RedisUnavailable)?;
        if rows.len() != ids.len() {
            return Err(Error::RedisUnavailable);
        }
        let mut output = Vec::new();
        let timestamp = now();
        for (id, row) in ids.into_iter().zip(rows) {
            if row.is_empty() {
                continue;
            }
            let mut info = json!({"client_id":id,"user_agent":row.get("user_agent").map_or("unknown",String::as_str),
                "worker_id":row.get("worker_id").map_or("unknown",String::as_str),
                "ip_address":row.get("ip_address").map_or("unknown",String::as_str),
                "user_id":row.get("user_id").map_or("0",String::as_str),
                "output_format":row.get("output_format").map_or("mpegts",String::as_str),"output_profile_id":null});
            if let Some(profile) = row
                .get("output_profile_id")
                .and_then(|value| value.parse::<u64>().ok())
                .filter(|value| *value > 0)
            {
                info["output_profile_id"] = json!(profile);
            }
            for name in [
                "connected_at",
                "last_active",
                "avg_rate_KBps",
                "current_rate_KBps",
            ] {
                if let Some(value) = row
                    .get(name)
                    .and_then(|value| value.parse::<f64>().ok())
                    .filter(|value| value.is_finite())
                {
                    info[name] = json!(value);
                    if name == "last_active" {
                        info["last_active_ago"] = json!(timestamp - value);
                    }
                }
            }
            if let Some(bytes) = row
                .get("bytes_sent")
                .and_then(|value| value.parse::<u64>().ok())
            {
                info["bytes_sent"] = json!(bytes);
            }
            output.push(info);
        }
        output.sort_by(|a, b| a["client_id"].as_str().cmp(&b["client_id"].as_str()));
        Ok(output)
    }
    pub async fn connect(url: &str, config: ClientConfig) -> Result<Self, Error> {
        if config.worker_id.is_empty()
            || config.worker_id.len() > 128
            || config.max_clients == 0
            || config.max_clients > Semaphore::MAX_PERMITS
            || config.ttl < Duration::from_secs(1)
            || config.ttl.as_millis() > i64::MAX as u128
            || config.flush_interval.is_zero()
            || config.flush_interval >= config.ttl
            || config.ghost_timeout.is_zero()
            || config.hls_ghost_timeout.is_zero()
            || config.command_timeout.is_zero()
            || config.admission_scan_limit == 0
            || config.admission_scan_limit > 100_000
        {
            return Err(Error::InvalidConfiguration);
        }
        let client = redis::Client::open(url).map_err(|_| Error::InvalidConfiguration)?;
        let connection = time::timeout(
            config.command_timeout,
            client.get_multiplexed_async_connection(),
        )
        .await
        .map_err(|_| Error::RedisUnavailable)?
        .map_err(|_| Error::RedisUnavailable)?;
        let mut loader = connection.clone();
        let heartbeat_sha: String = time::timeout(
            config.command_timeout,
            redis::cmd("SCRIPT")
                .arg("LOAD")
                .arg(scripts::HEARTBEAT)
                .query_async(&mut loader),
        )
        .await
        .map_err(|_| Error::RedisUnavailable)?
        .map_err(|_| Error::RedisUnavailable)?;
        let inner = Arc::new(Inner {
            runtime: tokio::runtime::Handle::current(),
            control_slots: Arc::new(Semaphore::new(32)),
            control_queue: Arc::new(Semaphore::new(1024)),
            connection,
            slots: Arc::new(Semaphore::new(config.max_clients)),
            config,
            clients: Mutex::new(HashMap::new()),
            live_stats: Mutex::new(statistics::State::default()),
            statistics_bytes: Arc::new(Semaphore::new(16 * 1024 * 1024)),
            stopped: AtomicBool::new(false),
            register: Script::new(scripts::REGISTER),
            heartbeat_sha,
        });
        let weak = Arc::downgrade(&inner);
        let interval = inner.config.flush_interval;
        tokio::spawn(async move {
            let mut timer = time::interval(interval);
            timer.set_missed_tick_behavior(time::MissedTickBehavior::Skip);
            loop {
                timer.tick().await;
                let Some(inner) = weak.upgrade() else { break };
                if inner.stopped.load(Ordering::Acquire) {
                    break;
                }
                let clients = {
                    let mut map = inner.clients.lock().unwrap();
                    map.retain(|_, client| client.strong_count() > 0);
                    map.values().filter_map(Weak::upgrade).collect::<Vec<_>>()
                };
                let clients = clients
                    .into_iter()
                    .filter(|client| {
                        client.registered.load(Ordering::Acquire)
                            && !client.released.load(Ordering::Acquire)
                    })
                    .collect::<Vec<_>>();
                if clients.is_empty() {
                    continue;
                }
                if flush(&inner, &clients).await.is_err() {
                    // Once coordination is lost, viewers cannot safely remain
                    // admitted against a distributed user limit.
                    for client in clients {
                        client.stop.send_replace(true);
                    }
                }
            }
        });
        Ok(Self { inner })
    }

    pub async fn register(
        &self,
        channel: &str,
        viewer: &PlaybackViewer,
        output_format: &str,
    ) -> Result<ClientLease, Error> {
        if self.inner.stopped.load(Ordering::Acquire) {
            return Err(Error::Stopped);
        }
        if channel.is_empty()
            || channel.len() > 256
            || channel.contains(':')
            || viewer.user_agent.len() > 4096
            || !matches!(output_format, "ts" | "mpegts" | "fmp4" | "hls")
        {
            return Err(Error::InvalidClient);
        }
        let permit = self
            .inner
            .slots
            .clone()
            .try_acquire_owned()
            .map_err(|_| Error::Capacity)?;
        let id = format!("client_{}", uuid::Uuid::new_v4().simple());
        let token = uuid::Uuid::new_v4().to_string();
        let (metadata, set, stop) = keys(channel, &id);
        let timestamp = now();
        let mut data = json!({"channel_id":channel,"user_agent":if viewer.user_agent.is_empty(){"unknown"}else{viewer.user_agent.as_str()},
            "ip_address":viewer.ip_address.to_string(),"connected_at":timestamp.to_string(),
            "last_active":timestamp.to_string(),"worker_id":self.inner.config.worker_id,
            "user_id":viewer.user_id.unwrap_or(0).to_string(),"username":viewer.username.as_deref().unwrap_or(""),
            "output_format":output_format,"output_profile_id":"","rust_token":token,
            "bytes_sent":"0","chunks_sent":"0","current_rate_KBps":"0","avg_rate_KBps":"0","stats_updated_at":timestamp.to_string()});
        if output_format == "hls" {
            for name in [
                "bytes_sent",
                "chunks_sent",
                "current_rate_KBps",
                "avg_rate_KBps",
                "stats_updated_at",
            ] {
                data.as_object_mut().unwrap().remove(name);
            }
        }
        let policy = json!({"terminate_on_limit_exceeded":viewer.limit_policy.terminate_on_limit_exceeded,
            "prioritize_single_client_channels":viewer.limit_policy.prioritize_single_client_channels,
            "ignore_same_channel_connections":viewer.limit_policy.ignore_same_channel_connections,
            "terminate_oldest":viewer.limit_policy.terminate_oldest});
        // Construct the cleanup guard before the first await that can register
        // Redis metadata. Cancelling an HTTP request after Redis applied Lua
        // must not leave a new admission behind until its TTL.
        let (stop_watch, _) = watch::channel(false);
        let (cleanup_done, _) = watch::channel(false);
        let inner = Arc::new(ClientInner {
            runtime: self.inner.runtime.clone(),
            store: Arc::downgrade(&self.inner),
            connection: self.inner.connection.clone(),
            config: self.inner.config.clone(),
            id: id.clone(),
            channel: channel.to_owned(),
            token: token.clone(),
            bytes: AtomicU64::new(0),
            chunks: AtomicU64::new(0),
            connected_at: timestamp,
            user_id: viewer.user_id,
            output_format: output_format.to_owned(),
            touched: AtomicBool::new(false),
            last_flush_bytes: AtomicU64::new(0),
            stop: stop_watch,
            released: AtomicBool::new(false),
            registered: AtomicBool::new(false),
            stats_announced: AtomicBool::new(false),
            cleanup_done,
            cleanup_error: Arc::new(AtomicBool::new(false)),
            _permit: Arc::new(permit),
        });
        {
            let mut clients = self.inner.clients.lock().unwrap();
            if self.inner.stopped.load(Ordering::Acquire) {
                inner.released.store(true, Ordering::Release);
                inner.cleanup_done.send_replace(true);
                return Err(Error::Stopped);
            }
            clients.insert(inner.id.clone(), Arc::downgrade(&inner));
        }
        let request = admission::Registration {
            keys: [
                metadata,
                set,
                stop,
                format!("live:channel:{channel}:stopping"),
            ],
            data: data.to_string(),
            policy: policy.to_string(),
            stream_limit: viewer.stream_limit,
        };
        let task =
            self.inner
                .runtime
                .spawn(admission::register_job(self.inner.clone(), inner, request));
        time::timeout(self.inner.config.command_timeout, task)
            .await
            .map_err(|_| Error::RedisUnavailable)?
            .map_err(|_| Error::RedisUnavailable)?
    }

    /// Stop this worker's clients without requiring their HTTP bodies to poll.
    /// Other workers and other channels retain their own token-fenced records.
    pub async fn stop_local_channel(&self, channel: &str) -> Result<usize, Error> {
        admission::stop_local_channel(self.inner.clone(), channel).await
    }

    pub async fn stop_client(&self, channel: &str, id: &str) -> Result<bool, Error> {
        if channel.is_empty()
            || channel.len() > 256
            || channel.contains(':')
            || id.is_empty()
            || id.len() > 256
            || id.contains(':')
        {
            return Err(Error::InvalidClient);
        }
        let (metadata, _, stop) = keys(channel, id);
        let script=Script::new("local exists=redis.call('EXISTS',KEYS[1]);redis.call('SET',KEYS[2],'true','EX',30);return exists");
        let mut connection = self.inner.connection.clone();
        let result: i64 = time::timeout(
            self.inner.config.command_timeout,
            script.key(metadata).key(stop).invoke_async(&mut connection),
        )
        .await
        .map_err(|_| Error::RedisUnavailable)?
        .map_err(|_| Error::RedisUnavailable)?;
        let mut locally_processed = false;
        if result == 1 {
            let local = self
                .inner
                .clients
                .lock()
                .unwrap()
                .get(id)
                .and_then(Weak::upgrade);
            if let Some(local) = local.filter(|client| client.channel == channel) {
                local.stop.send_replace(true);
                release(&local).await?;
                locally_processed = true;
            }
        }
        Ok(locally_processed)
    }

    pub async fn shutdown(&self) {
        self.inner.stopped.store(true, Ordering::Release);
        self.inner.slots.close();
        let clients = self
            .inner
            .clients
            .lock()
            .unwrap()
            .values()
            .filter_map(Weak::upgrade)
            .collect::<Vec<_>>();
        for client in &clients {
            client.stop.send_replace(true);
        }
        let _ = time::timeout(self.inner.config.command_timeout, async {
            for client in clients {
                let _ = release(&client).await;
            }
        })
        .await;
    }
}

async fn flush(inner: &Inner, clients: &[Arc<ClientInner>]) -> Result<(), Error> {
    // One pipeline per worker interval, rather than per-frame/per-viewer round
    // trips. Script source is EVALSHA-cached after first initialization.
    let mut connection = inner.connection.clone();
    let mut pipeline = redis::pipe();
    let timestamp = now();
    for client in clients {
        let (metadata, set, stop) = keys(&client.channel, &client.id);
        let bytes = client.bytes.load(Ordering::Relaxed);
        let old = client.last_flush_bytes.swap(bytes, Ordering::Relaxed);
        let rate =
            (bytes.saturating_sub(old) as f64) / inner.config.flush_interval.as_secs_f64() / 1024.0;
        let average = bytes as f64 / (timestamp - client.connected_at).max(0.000001) / 1024.0;
        let rate = (rate * 10.0).round() / 10.0;
        let average = (average * 10.0).round() / 10.0;
        pipeline
            .cmd("EVALSHA")
            .arg(&inner.heartbeat_sha)
            .arg(3)
            .arg(metadata)
            .arg(set)
            .arg(stop)
            .arg(&client.token)
            .arg(timestamp)
            .arg(bytes)
            .arg(rate)
            .arg(
                if bytes != old || client.touched.swap(false, Ordering::Relaxed) {
                    "1"
                } else {
                    "0"
                },
            )
            .arg(inner.config.ttl.as_millis() as u64)
            .arg(if client.output_format == "hls" {
                inner.config.hls_ghost_timeout.as_secs_f64()
            } else {
                inner.config.ghost_timeout.as_secs_f64()
            });
        pipeline
            .arg(client.chunks.load(Ordering::Relaxed))
            .arg(average)
            .arg(&client.output_format);
    }
    let result: Vec<i64> = time::timeout(
        inner.config.command_timeout,
        pipeline.query_async(&mut connection),
    )
    .await
    .map_err(|_| Error::RedisUnavailable)?
    .map_err(|_| Error::RedisUnavailable)?;
    if result.len() != clients.len() {
        return Err(Error::RedisUnavailable);
    }
    for (client, status) in clients.iter().zip(result) {
        if status != 1 {
            client.stop.send_replace(true);
            let _ = release(client).await;
        }
    }
    Ok(())
}

async fn release(client: &ClientInner) -> Result<(), Error> {
    admission::release(client).await
}

impl ClientLease {
    pub fn id(&self) -> &str {
        &self.inner.id
    }
    pub fn stopped(&self) -> watch::Receiver<bool> {
        self.inner.stop.subscribe()
    }
    pub fn record_bytes(&self, bytes: usize) {
        self.inner.bytes.fetch_add(bytes as u64, Ordering::Relaxed);
        self.inner.chunks.fetch_add(1, Ordering::Relaxed);
    }
    pub fn record_keepalive(&self, bytes: usize) {
        self.inner.bytes.fetch_add(bytes as u64, Ordering::Relaxed);
    }
    pub fn touch(&self) {
        self.inner.touched.store(true, Ordering::Relaxed);
    }
    pub async fn release(&self) -> Result<(), Error> {
        release(&self.inner).await
    }
}

impl Drop for ClientInner {
    fn drop(&mut self) {
        admission::drop_client(self);
    }
}
