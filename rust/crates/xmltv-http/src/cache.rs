//! Fenced XMLTV chunk cache using the original public key layout.
//! Original Dispatcharr authors retain credit. HTTP/RAII integration remains gated.
use dispatcharr_output_store::{
    transport::{RedisTransport, TransportConfig},
    Error as TransportError,
};
use std::{
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
#[derive(Clone, Debug)]
pub struct CacheConfig {
    pub max_entries: usize,
    pub max_cache_bytes: usize,
    pub max_export_bytes: usize,
    pub max_chunks: usize,
    pub chunk_bytes: usize,
    /// Maximum physical reply batch; legacy members may span multiple reads.
    pub read_batch_bytes: usize,
    pub max_read_bytes: usize,
    pub max_legacy_chunk_bytes: usize,
    pub cache_ttl: Duration,
    pub lock_ttl: Duration,
    pub error_ttl: Duration,
}
impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            max_entries: 128,
            max_cache_bytes: 512 * 1024 * 1024,
            max_export_bytes: 256 * 1024 * 1024,
            max_chunks: 65536,
            chunk_bytes: 64 * 1024,
            read_batch_bytes: 512 * 1024,
            max_read_bytes: 64 * 1024 * 1024,
            max_legacy_chunk_bytes: 8 * 1024 * 1024,
            cache_ttl: Duration::from_secs(300),
            lock_ttl: Duration::from_secs(120),
            error_ttl: Duration::from_secs(60),
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CacheError {
    Configuration,
    Capacity,
    Unavailable,
    Corrupt,
    LostOwner,
}
#[derive(Clone)]
pub struct Cache {
    inner: Arc<Inner>,
}
struct Inner {
    transport: RedisTransport,
    config: CacheConfig,
    io: Arc<Semaphore>,
    cleanups: AtomicUsize,
    active: AtomicUsize,
    metrics: Metrics,
}
#[derive(Default)]
struct Metrics {
    leader: AtomicUsize,
    follower: AtomicUsize,
    ready: AtomicUsize,
    reads: AtomicUsize,
    data: AtomicUsize,
    waits: AtomicUsize,
    ends: AtomicUsize,
    writes: AtomicUsize,
    members: AtomicUsize,
    written_bytes: AtomicUsize,
}
struct Active(Arc<Inner>);
impl Active {
    fn new(inner: &Arc<Inner>) -> Self {
        inner.active.fetch_add(1, Ordering::AcqRel);
        Self(inner.clone())
    }
}
impl Drop for Active {
    fn drop(&mut self) {
        self.0.active.fetch_sub(1, Ordering::AcqRel);
    }
}
struct Cleanup(Arc<Inner>);
impl Drop for Cleanup {
    fn drop(&mut self) {
        self.0.cleanups.fetch_sub(1, Ordering::AcqRel);
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EntryState {
    Leader,
    Ready,
    Follower,
}
pub struct Entry {
    pub base: String,
    pub state: EntryState,
    token: String,
    cache: Cache,
    acquisition: Option<Arc<Acquisition>>,
}
struct Acquisition {
    cache: Cache,
    token: String,
    armed: AtomicBool,
    caller: Mutex<Option<Arc<dyn Send + Sync>>>,
    _active: Active,
}
impl Drop for Acquisition {
    fn drop(&mut self) {
        if !self.armed.load(Ordering::Acquire) {
            return;
        }
        let caller = self.caller.lock().unwrap().take();
        if let (Some(caller), Ok(runtime)) = (caller, tokio::runtime::Handle::try_current()) {
            let cache = self.cache.clone();
            let token = self.token.clone();
            cache.inner.cleanups.fetch_add(1, Ordering::AcqRel);
            let cleanup = Arc::new(Cleanup(cache.inner.clone()));
            runtime.spawn(async move {
                let _ = cache
                    .command(
                        CANCEL_TOKEN,
                        &cache.args("", &token),
                        Arc::new((caller, cleanup)),
                    )
                    .await;
            });
        }
    }
}
#[derive(Clone, Debug)]
pub struct Cursor {
    pub chunk: usize,
    pub offset: usize,
    pub delivered: usize,
    frame: Arc<Semaphore>,
}
impl Default for Cursor {
    fn default() -> Self {
        Self {
            chunk: 0,
            offset: 0,
            delivered: 0,
            frame: Arc::new(Semaphore::new(1)),
        }
    }
}
pub enum Read {
    Data(CacheBytes),
    Wait,
    End,
    Failed,
}
pub struct CacheBytes {
    data: Vec<u8>,
    _bytes: OwnedSemaphorePermit,
    _request: Arc<dyn Send + Sync>,
    _frame: OwnedSemaphorePermit,
}
impl AsRef<[u8]> for CacheBytes {
    fn as_ref(&self) -> &[u8] {
        &self.data
    }
}
impl CacheBytes {
    /// Physical READ has completed. The HTTP actor keeps its request admission;
    /// detached body frames retain the independent memory budget and frame slot.
    pub(crate) fn retain_frame_guard(&mut self, guard: Arc<dyn Send + Sync>) {
        self._request = guard;
    }
}
fn map(error: TransportError) -> CacheError {
    match error {
        TransportError::Configuration => CacheError::Configuration,
        TransportError::Capacity => CacheError::Capacity,
        TransportError::Corrupt => CacheError::Corrupt,
        _ => CacheError::Unavailable,
    }
}
impl Cache {
    pub async fn connect(url: &str, config: CacheConfig) -> Result<Self, CacheError> {
        if config.max_entries == 0
            || config.max_entries > 4096
            || config.max_export_bytes == 0
            || config.max_export_bytes > config.max_cache_bytes
            || config.max_cache_bytes > 2 * 1024 * 1024 * 1024
            || config.max_chunks == 0
            || config.max_chunks > 1_000_000
            || config.chunk_bytes == 0
            || config.chunk_bytes > 1024 * 1024
            || config.read_batch_bytes == 0
            || config.read_batch_bytes > 1024 * 1024
            || config.max_read_bytes < config.chunk_bytes * 4
            || config.max_read_bytes > 512 * 1024 * 1024
            || config.max_legacy_chunk_bytes < config.chunk_bytes
            || config.max_legacy_chunk_bytes > 64 * 1024 * 1024
            || [config.cache_ttl, config.lock_ttl, config.error_ttl]
                .iter()
                .any(|ttl| ttl.as_secs() == 0 || ttl.as_secs() > 3600)
        {
            return Err(CacheError::Configuration);
        }
        let transport = RedisTransport::connect(
            url,
            TransportConfig {
                max_operations: 16,
                max_queued_operations: 256,
                command_timeout: Duration::from_secs(3),
            },
        )
        .await
        .map_err(map)?;
        Ok(Self {
            inner: Arc::new(Inner {
                transport,
                io: Arc::new(Semaphore::new(config.max_read_bytes)),
                config,
                cleanups: AtomicUsize::new(0),
                active: AtomicUsize::new(0),
                metrics: Metrics::default(),
            }),
        })
    }
    fn args(&self, base: &str, token: &str) -> Vec<Vec<u8>> {
        let c = &self.inner.config;
        [
            c.max_entries.to_string(),
            c.max_cache_bytes.to_string(),
            c.cache_ttl.as_secs().to_string(),
            c.lock_ttl.as_secs().to_string(),
            c.error_ttl.as_secs().to_string(),
            c.max_export_bytes.to_string(),
            c.max_chunks.to_string(),
            c.chunk_bytes.to_string(),
            c.max_legacy_chunk_bytes.to_string(),
            base.into(),
            token.into(),
        ]
        .into_iter()
        .map(String::into_bytes)
        .collect()
    }
    async fn command(
        &self,
        body: &str,
        args: &[Vec<u8>],
        guard: Arc<dyn Send + Sync>,
    ) -> Result<i64, CacheError> {
        let source = format!("{PRELUDE}\n{body}");
        let args: Vec<_> = args.iter().map(Vec::as_slice).collect();
        let (value, _) = self
            .inner
            .transport
            .command(&source, &[], &args, guard)
            .await
            .map_err(map)?;
        Ok(value)
    }
    /// Caller supplies a fresh unique fencing token, not a capability secret.
    /// Physical acquisition and later writes retain cleanup ownership through
    /// real replies. Dropping a lease schedules token-fenced bounded cleanup.
    pub async fn open(
        &self,
        prefix: &str,
        token: &str,
        guard: Arc<dyn Send + Sync>,
    ) -> Result<Entry, CacheError> {
        if !prefix.starts_with("epg_content:")
            || prefix.len() > 4096
            || !(16..=128).contains(&token.len())
            || !token
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"_-".contains(&b))
        {
            return Err(CacheError::Configuration);
        }
        let source = format!("{PRELUDE}\n{OPEN}");
        let acquisition = Arc::new(Acquisition {
            cache: self.clone(),
            token: token.into(),
            armed: AtomicBool::new(true),
            caller: Mutex::new(Some(guard)),
            _active: Active::new(&self.inner),
        });
        let args = self.args(prefix, token);
        let values: Vec<_> = args.iter().map(Vec::as_slice).collect();
        let ((status, base), _): ((i64, String), _) = self
            .inner
            .transport
            .command(&source, &[], &values, acquisition.clone())
            .await
            .map_err(map)?;
        let state = match status {
            1 => EntryState::Leader,
            2 => EntryState::Ready,
            3 => EntryState::Follower,
            -1 => return Err(CacheError::Capacity),
            _ => return Err(CacheError::Corrupt),
        };
        match state {
            EntryState::Leader => &self.inner.metrics.leader,
            EntryState::Follower => &self.inner.metrics.follower,
            EntryState::Ready => &self.inner.metrics.ready,
        }
        .fetch_add(1, Ordering::Relaxed);
        if state != EntryState::Leader {
            acquisition.armed.store(false, Ordering::Release);
        }
        Ok(Entry {
            base,
            state,
            token: token.into(),
            cache: self.clone(),
            acquisition: if state == EntryState::Leader {
                Some(acquisition)
            } else {
                None
            },
        })
    }
    pub async fn read(
        &self,
        base: &str,
        cursor: &mut Cursor,
        renew: bool,
        guard: Arc<dyn Send + Sync>,
    ) -> Result<Read, CacheError> {
        if !base.starts_with("epg_content:")
            || base.len() > 8192
            || cursor.chunk > self.inner.config.max_chunks
            || cursor.offset > self.inner.config.max_legacy_chunk_bytes
        {
            return Err(CacheError::Configuration);
        }
        let frame = cursor
            .frame
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| CacheError::Unavailable)?;
        let read_bytes = self.read_batch_bytes();
        let budget = self
            .inner
            .io
            .clone()
            .try_acquire_many_owned((read_bytes * 2) as u32)
            .map_err(|_| CacheError::Capacity)?;
        let mut args = self.args(base, "");
        args.extend([
            cursor.chunk.to_string().into_bytes(),
            cursor.offset.to_string().into_bytes(),
            if renew { b"1".to_vec() } else { b"0".to_vec() },
            read_bytes.to_string().into_bytes(),
        ]);
        let source = format!("{PRELUDE}\n{READ}");
        self.inner.metrics.reads.fetch_add(1, Ordering::Relaxed);
        let values: Vec<_> = args.iter().map(Vec::as_slice).collect();
        let ((status, data, next, offset), (budget, frame, guard, _active)): (
            (i64, Vec<Vec<u8>>, usize, usize),
            _,
        ) = self
            .inner
            .transport
            .command(
                &source,
                &[],
                &values,
                (budget, frame, guard, Active::new(&self.inner)),
            )
            .await
            .map_err(map)?;
        match status {
            1 => {
                self.inner.metrics.data.fetch_add(1, Ordering::Relaxed);
                // Redis returns bounded bulk parts directly. Avoid copying every
                // member through Lua substring + concatenation before RESP encoding.
                // Keep both the decoded parts and final exact-sized allocation
                // under the existing twice-reply-byte permit until parts drop.
                let size = data.iter().try_fold(0usize, |size, part| {
                    size.checked_add(part.len()).ok_or(CacheError::Capacity)
                })?;
                if data.len() > 32 || size > read_bytes {
                    return Err(CacheError::Capacity);
                }
                let mut joined = Vec::with_capacity(size);
                for part in data {
                    joined.extend_from_slice(&part);
                }
                let data = joined;
                let delivered = cursor
                    .delivered
                    .checked_add(data.len())
                    .ok_or(CacheError::Capacity)?;
                if delivered > self.inner.config.max_export_bytes || data.len() > read_bytes {
                    return Err(CacheError::Capacity);
                }
                *cursor = Cursor {
                    chunk: next,
                    offset,
                    delivered,
                    frame: cursor.frame.clone(),
                };
                Ok(Read::Data(CacheBytes {
                    data,
                    _bytes: budget,
                    _request: guard,
                    _frame: frame,
                }))
            }
            0 => {
                self.inner.metrics.waits.fetch_add(1, Ordering::Relaxed);
                Ok(Read::Wait)
            }
            2 => {
                self.inner.metrics.ends.fetch_add(1, Ordering::Relaxed);
                Ok(Read::End)
            }
            3 => Ok(Read::Failed),
            -1 => Err(CacheError::Capacity),
            _ => Err(CacheError::Corrupt),
        }
    }
    pub fn diagnostics(&self) -> serde_json::Value {
        let m = &self.inner.metrics;
        let n = |v: &AtomicUsize| v.load(Ordering::Relaxed);
        serde_json::json!({"leader_opens":n(&m.leader),"follower_opens":n(&m.follower),"ready_opens":n(&m.ready),"read_command_attempts":n(&m.reads),"data_replies":n(&m.data),"wait_replies":n(&m.waits),"end_replies":n(&m.ends),"put_command_attempts":n(&m.writes),"put_members":n(&m.members),"put_payload_bytes":n(&m.written_bytes),"available_io_bytes":self.available_io_bytes()})
    }
    pub fn read_batch_bytes(&self) -> usize {
        self.inner
            .config
            .read_batch_bytes
            .min(self.inner.config.max_read_bytes / 4)
    }
    pub fn available_io_bytes(&self) -> usize {
        self.inner.io.available_permits()
    }
    /// Call after stopping admission and joining request/build actors. The
    /// count stays held through actual Redis replies, even after task timeout.
    pub async fn drain_cleanup(&self, deadline: Duration) -> bool {
        tokio::time::timeout(deadline, async {
            while self.inner.cleanups.load(Ordering::Acquire) > 0
                || self.inner.active.load(Ordering::Acquire) > 0
            {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .is_ok()
    }
    pub fn chunk_bytes(&self) -> usize {
        self.inner.config.chunk_bytes
    }
    pub fn refresh_interval(&self) -> Duration {
        Duration::from_secs((self.inner.config.cache_ttl.as_secs() / 4).max(1))
    }
    pub(crate) async fn event_seen(
        &self,
        key: &str,
        guard: Arc<dyn Send + Sync>,
    ) -> Result<bool, CacheError> {
        if key.len() > 8192 || !key.starts_with(":1:epg_download:") {
            return Err(CacheError::Configuration);
        }
        let (raw,_):(Option<Vec<u8>>,_) = self.inner.transport.command("if redis.call('STRLEN',KEYS[1])>65536 then return redis.error_reply('CORRUPT event cache')end;return redis.call('GET',KEYS[1])",&[key.into()],&[],(guard,Active::new(&self.inner))).await.map_err(map)?;
        Ok(raw
            .as_deref()
            .and_then(dispatcharr_store::decode_source_cache)
            .is_some_and(|v| match v {
                serde_json::Value::Null => false,
                serde_json::Value::Bool(v) => v,
                serde_json::Value::Number(v) => v.as_f64() != Some(0.0),
                serde_json::Value::String(v) => !v.is_empty(),
                serde_json::Value::Array(v) => !v.is_empty(),
                serde_json::Value::Object(v) => !v.is_empty(),
            }))
    }
    pub(crate) async fn mark_event(
        &self,
        key: &str,
        guard: Arc<dyn Send + Sync>,
    ) -> Result<(), CacheError> {
        if key.len() > 8192 || !key.starts_with(":1:epg_download:") {
            return Err(CacheError::Configuration);
        }
        let data = dispatcharr_store::encode_source_cache(&serde_json::Value::Bool(true))
            .ok_or(CacheError::Corrupt)?;
        let (_, _): (String, _) = self
            .inner
            .transport
            .command(
                "return redis.call('SET',KEYS[1],ARGV[1],'EX',2)",
                &[key.into()],
                &[&data],
                (guard, Active::new(&self.inner)),
            )
            .await
            .map_err(map)?;
        Ok(())
    }
}
#[must_use]
pub(crate) struct PutBatch {
    pub accepted: usize,
    pub capacity_reached: bool,
}
impl Entry {
    /// Append a bounded valid prefix atomically. A later invalid/over-cap
    /// member is explicit so the owner can deliver the prior good bytes first.
    pub(crate) async fn put_batch(
        &self,
        chunks: &[&str],
        guard: Arc<dyn Send + Sync>,
    ) -> Result<PutBatch, CacheError> {
        if chunks.is_empty() || chunks.len() > 32 {
            return Err(CacheError::Configuration);
        }
        let bound = self.cache.read_batch_bytes();
        let mut bytes = 0usize;
        let mut valid = 0usize;
        for chunk in chunks {
            if chunk.len() > self.cache.inner.config.chunk_bytes
                || bytes.checked_add(chunk.len()).is_none_or(|n| n > bound)
            {
                break;
            }
            bytes += chunk.len();
            valid += 1;
        }
        if valid == 0 {
            return Err(CacheError::Capacity);
        }
        let budget = self
            .cache
            .inner
            .io
            .clone()
            .try_acquire_many_owned(
                (4 * bytes.max(self.cache.inner.config.chunk_bytes.min(bound))) as u32,
            )
            .map_err(|_| CacheError::Capacity)?;
        let mut args = self.cache.args(&self.base, &self.token);
        args.extend(
            chunks[..valid]
                .iter()
                .map(|chunk| chunk.as_bytes().to_vec()),
        );
        let values: Vec<_> = args.iter().map(Vec::as_slice).collect();
        self.cache
            .inner
            .metrics
            .writes
            .fetch_add(1, Ordering::Relaxed);
        self.cache
            .inner
            .metrics
            .members
            .fetch_add(valid, Ordering::Relaxed);
        self.cache
            .inner
            .metrics
            .written_bytes
            .fetch_add(bytes, Ordering::Relaxed);
        let ((accepted, capacity), _): ((i64, i64), _) = self
            .cache
            .inner
            .transport
            .command(
                &format!("{PRELUDE}\n{PUT_BATCH}"),
                &[],
                &values,
                (budget, guard, self.acquisition.clone()),
            )
            .await
            .map_err(map)?;
        if accepted == -2 {
            return Err(CacheError::LostOwner);
        }
        let accepted = usize::try_from(accepted).map_err(|_| CacheError::Corrupt)?;
        if accepted > valid || !matches!(capacity, 0 | 1) {
            return Err(CacheError::Corrupt);
        }
        Ok(PutBatch {
            accepted,
            capacity_reached: capacity == 1 || valid < chunks.len(),
        })
    }
    pub async fn put(&self, chunk: &str, guard: Arc<dyn Send + Sync>) -> Result<usize, CacheError> {
        if chunk.len() > self.cache.inner.config.chunk_bytes {
            return Err(CacheError::Capacity);
        }
        let budget = self
            .cache
            .inner
            .io
            .clone()
            .try_acquire_many_owned((self.cache.inner.config.chunk_bytes * 4) as u32)
            .map_err(|_| CacheError::Capacity)?;
        let mut args = self.cache.args(&self.base, &self.token);
        args.push(chunk.as_bytes().to_vec());
        let source = format!("{PRELUDE}\n{PUT}");
        self.cache
            .inner
            .metrics
            .writes
            .fetch_add(1, Ordering::Relaxed);
        self.cache
            .inner
            .metrics
            .members
            .fetch_add(1, Ordering::Relaxed);
        self.cache
            .inner
            .metrics
            .written_bytes
            .fetch_add(chunk.len(), Ordering::Relaxed);
        let values: Vec<_> = args.iter().map(Vec::as_slice).collect();
        let (status, _): (i64, _) = self
            .cache
            .inner
            .transport
            .command(
                &source,
                &[],
                &values,
                (budget, guard, self.acquisition.clone()),
            )
            .await
            .map_err(map)?;
        match status {
            value if value >= 0 => Ok(value as usize),
            -1 => Err(CacheError::Capacity),
            -2 => Err(CacheError::LostOwner),
            _ => Err(CacheError::Corrupt),
        }
    }
    pub async fn finish(&self, guard: Arc<dyn Send + Sync>) -> Result<(), CacheError> {
        let status: i64 = self
            .cache
            .command(
                FINISH,
                &self.cache.args(&self.base, &self.token),
                Arc::new((guard, self.acquisition.clone())),
            )
            .await?;
        if status == 1 {
            if let Some(acquisition) = &self.acquisition {
                acquisition.armed.store(false, Ordering::Release);
            }
            Ok(())
        } else {
            Err(CacheError::LostOwner)
        }
    }
    pub async fn abort(&self, guard: Arc<dyn Send + Sync>) -> Result<bool, CacheError> {
        let status: i64 = self
            .cache
            .command(
                ABORT,
                &self.cache.args(&self.base, &self.token),
                Arc::new((guard, self.acquisition.clone())),
            )
            .await?;
        if status == 1 {
            if let Some(acquisition) = &self.acquisition {
                acquisition.armed.store(false, Ordering::Release);
            }
        }
        Ok(status == 1)
    }
}
const PRELUDE: &str = r#"
local catalog='rust:xmltv:v1:catalog';local expiry=catalog..':expiry';local byteskey=catalog..':bytes'
local maxentries=tonumber(ARGV[1]);local maxbytes=tonumber(ARGV[2]);local readyttl=tonumber(ARGV[3]);local lockttl=tonumber(ARGV[4]);local errorttl=tonumber(ARGV[5]);local maxexport=tonumber(ARGV[6]);local maxchunks=tonumber(ARGV[7]);local chunkbytes=tonumber(ARGV[8]);local legacybytes=tonumber(ARGV[9]);local base=ARGV[10];local owner=ARGV[11]
local clock=redis.call('TIME');local now=tonumber(clock[1])+tonumber(clock[2])/1000000
local function typ(key,want)local t=redis.call('TYPE',key).ok;if t~='none' and t~=want then error('CORRUPT XMLTV type')end end
local function row(raw)if #raw>512 then error('CORRUPT XMLTV ledger size')end;local ok,value=pcall(cjson.decode,raw);if not ok or type(value)~='table' or type(value.owner)~='string' or type(value.bytes)~='number' or value.bytes<0 or value.bytes%1~=0 or value.bytes>2147483648 or #value.owner>128 then error('CORRUPT XMLTV ledger')end;return value end
local function save(key,value,ttl)redis.call('HSET',catalog,key,cjson.encode(value));redis.call('ZADD',expiry,now+ttl,key)end
local function sweep()
 typ(catalog,'hash');typ(expiry,'zset');typ(byteskey,'string')
 local total=tonumber(redis.call('GET',byteskey)or'0');if not total or total<0 or redis.call('HLEN',catalog)>4096 or redis.call('HLEN',catalog)~=redis.call('ZCARD',expiry) then error('CORRUPT XMLTV catalog')end
 local expired=redis.call('ZRANGEBYSCORE',expiry,'-inf',now,'LIMIT',0,maxentries)
 local expiredbytes=0;for _,key in ipairs(expired)do local raw=redis.call('HGET',catalog,key);if not raw then error('CORRUPT XMLTV entry')end;expiredbytes=expiredbytes+row(raw).bytes;typ(key..':rust_fence','string');typ(key..':lock','string')end;if expiredbytes>total then error('CORRUPT XMLTV bytes')end
 for _,key in ipairs(expired)do local value=row(redis.call('HGET',catalog,key));local fence=redis.call('GET',key..':rust_fence');local active=redis.call('GET',key..':lock');if fence==value.owner and(not active or active==value.owner)then redis.call('DEL',key..':chunks',key..':status',key..':ready',key..':lock',key..':rust_fence')end;total=total-value.bytes;redis.call('HDEL',catalog,key);redis.call('ZREM',expiry,key)end
 if total<0 then error('CORRUPT XMLTV bytes')end;redis.call('SET',byteskey,total);return total
end
local function check(key)typ(key..':chunks','list');typ(key..':status','string');typ(key..':ready','string');typ(key..':lock','string');typ(key..':rust_fence','string')end
local function ours()local raw=redis.call('HGET',catalog,base);if not raw then return nil end;local value=row(raw);if value.owner~=owner or redis.call('GET',base..':rust_fence')~=owner or redis.call('GET',base..':lock')~=owner then return nil end;return value end
"#;
const OPEN: &str = r#"
local total=sweep();typ('epg_content_generation','string');if redis.call('STRLEN','epg_content_generation')>64 then error('CORRUPT XMLTV generation')end;base=base..':g='..(redis.call('GET','epg_content_generation')or'0');check(base)
local ready=redis.call('GET',base..':ready');if ready and ready~=''then return{2,base}end
local old=redis.call('HGET',catalog,base);if old and row(old).bytes>total then error('CORRUPT XMLTV bytes')end
if not redis.call('SET',base..':lock',owner,'NX','EX',lockttl)then return{3,base}end
if not old and redis.call('HLEN',catalog)>=maxentries then redis.call('DEL',base..':lock');return{-1,base}end
if old then total=total-row(old).bytes end
redis.call('DEL',base..':chunks',base..':status',base..':ready',':1:'..base);redis.call('SET',base..':rust_fence',owner,'EX',lockttl);redis.call('SET',base..':status','building','EX',lockttl);save(base,{owner=owner,bytes=0},lockttl);redis.call('SET',byteskey,total);return{1,base}
"#;
const PUT: &str = r#"
local total=sweep();check(base);local value=ours();if not value then return -2 end;if value.bytes>total then error('CORRUPT XMLTV bytes')end;local payload=ARGV[12]
if #payload>chunkbytes or value.bytes+#payload>maxexport or total+#payload>maxbytes or redis.call('LLEN',base..':chunks')>=maxchunks then return -1 end
local count=redis.call('RPUSH',base..':chunks',payload);value.bytes=value.bytes+#payload;save(base,value,lockttl);redis.call('SET',byteskey,total+#payload);for _,tail in ipairs({':chunks',':status',':lock',':rust_fence'})do redis.call('EXPIRE',base..tail,lockttl)end;return count
"#;
const PUT_BATCH: &str = r#"
local total=sweep();check(base);local value=ours();if not value then return{-2,0}end;if value.bytes>total then error('CORRUPT XMLTV bytes')end
local count=redis.call('LLEN',base..':chunks');local accepted=0;local size=0;local capacity=0
if #ARGV<12 or #ARGV>43 then return redis.error_reply('CORRUPT XMLTV batch')end
for i=12,#ARGV do local payload=ARGV[i]
 if #payload>chunkbytes or value.bytes+size+#payload>maxexport or total+size+#payload>maxbytes or count+accepted>=maxchunks then capacity=1;break end
 size=size+#payload;accepted=accepted+1
end
if accepted>0 then
 local payloads={};for i=12,11+accepted do payloads[#payloads+1]=ARGV[i]end
 redis.call('RPUSH',base..':chunks',unpack(payloads));value.bytes=value.bytes+size;save(base,value,lockttl);redis.call('SET',byteskey,total+size)
 for _,tail in ipairs({':chunks',':status',':lock',':rust_fence'})do redis.call('EXPIRE',base..tail,lockttl)end
end
return{accepted,capacity}
"#;
const FINISH: &str = r#"
local total=sweep();check(base);local value=ours();if not value then return 0 end;if value.bytes>total then error('CORRUPT XMLTV bytes')end
redis.call('SET',base..':status','ready','EX',readyttl);redis.call('SET',base..':ready','1','EX',readyttl);redis.call('EXPIRE',base..':chunks',readyttl);redis.call('EXPIRE',base..':rust_fence',readyttl);redis.call('DEL',base..':lock');save(base,value,readyttl);return 1
"#;
const ABORT: &str = r#"
local total=sweep();check(base);local value=ours();if not value then return 0 end;if value.bytes>total then error('CORRUPT XMLTV bytes')end
redis.call('DEL',base..':chunks',base..':ready',base..':lock');redis.call('SET',base..':status','error','EX',errorttl);redis.call('EXPIRE',base..':rust_fence',errorttl);redis.call('SET',byteskey,total-value.bytes);value.bytes=0;save(base,value,errorttl);return 1
"#;
const CANCEL_TOKEN: &str = r#"
local total=sweep();local rows=redis.call('HGETALL',catalog)
local removed=0;for index=1,#rows,2 do local key=rows[index];local value=row(rows[index+1]);if value.owner==owner then check(key);if redis.call('GET',key..':lock')==owner and redis.call('GET',key..':rust_fence')==owner then removed=removed+value.bytes end end end;if removed>total then error('CORRUPT XMLTV bytes')end
for index=1,#rows,2 do base=rows[index];local value=row(rows[index+1]);if value.owner==owner and redis.call('GET',base..':lock')==owner and redis.call('GET',base..':rust_fence')==owner then
 redis.call('DEL',base..':chunks',base..':ready',base..':lock');total=total-value.bytes
 if value.bytes==0 then redis.call('DEL',base..':status',base..':rust_fence');redis.call('HDEL',catalog,base);redis.call('ZREM',expiry,base)else redis.call('SET',base..':status','error','EX',errorttl);redis.call('EXPIRE',base..':rust_fence',errorttl);value.bytes=0;save(base,value,errorttl)end
end end;redis.call('SET',byteskey,total);return 1
"#;
const READ: &str = r#"
sweep();check(base);local index=tonumber(ARGV[12]);local offset=tonumber(ARGV[13]);if index>maxchunks then return{-1,{},index,offset}end
local status=redis.call('GET',base..':status')or'';if #status>64 then error('CORRUPT XMLTV status')end
if ARGV[14]=='1'then for _,tail in ipairs({':chunks',':status',':ready'})do redis.call('EXPIRE',base..tail,readyttl)end;local raw=redis.call('HGET',catalog,base);if raw and status=='ready'then local value=row(raw);if redis.call('GET',base..':rust_fence')==value.owner then redis.call('EXPIRE',base..':rust_fence',readyttl);save(base,value,readyttl)end end end
local batchbytes=tonumber(ARGV[15]);if not batchbytes or batchbytes<1 or batchbytes>1048576 then return{-1,{},index,offset}end
local parts={};local size=0;local advanced=false
-- Bounded by bytes AND visited members; legacy large members retain their
-- offset instead of becoming oversized replies. Stored members are unchanged.
for step=1,32 do
 local data=redis.call('LINDEX',base..':chunks',index);if not data then break end
 if index>=maxchunks or #data>legacybytes or offset>#data then
  if advanced then break else return{-1,{},index,offset}end
 end
 local last=math.min(#data,offset+batchbytes-size)
 if offset==0 and last==#data then parts[#parts+1]=data else parts[#parts+1]=string.sub(data,offset+1,last)end;size=size+last-offset;advanced=true
 if last==#data then index=index+1;offset=0 else offset=last end
 if size>=batchbytes then break end
end
if advanced then return{1,parts,index,offset}end
if status=='ready'or(redis.call('GET',base..':ready')or'')~=''then return{2,{},index,offset}elseif status=='error'then return{3,{},index,offset}else return{0,{},index,offset}end
"#;
#[cfg(test)]
#[path = "cache_tests.rs"]
mod tests;
