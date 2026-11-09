//! Bounded shared TS input buffer derived from Dispatcharr's StreamBuffer.
//! Full original author/contributor credit retained in ATTRIBUTION.md.
mod diagnostics;
mod gc;
mod retained;
use dispatcharr_output_store::transport::{RedisTransport, TransportConfig};
use std::{sync::Arc, time::Duration};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
#[derive(Clone, Debug)]
pub struct InputConfig {
    pub max_chunk_bytes: usize,
    pub max_channel_bytes: usize,
    pub max_chunks: usize,
    /// Durable Rust-only catalog admission; never grows beyond this bound.
    pub max_catalog_entries: usize,
    pub max_read_bytes: usize,
    pub max_operations: usize,
    pub max_queued_operations: usize,
    pub command_timeout: Duration,
    pub chunk_ttl: Duration,
}
impl Default for InputConfig {
    fn default() -> Self {
        Self {
            max_chunk_bytes: 4 * 1024 * 1024,
            max_channel_bytes: 16 * 1024 * 1024,
            max_chunks: 4096,
            max_catalog_entries: 4096,
            max_read_bytes: 64 * 1024 * 1024,
            max_operations: 16,
            max_queued_operations: 1024,
            command_timeout: Duration::from_secs(3),
            chunk_ttl: Duration::from_secs(60),
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    Configuration,
    InvalidKey,
    InvalidChunk,
    Capacity,
    Unavailable,
    Corrupt,
    LostOwnership,
}
impl From<dispatcharr_output_store::Error> for Error {
    fn from(error: dispatcharr_output_store::Error) -> Self {
        use dispatcharr_output_store::Error as E;
        match error {
            E::Configuration => Self::Configuration,
            E::InvalidKey => Self::InvalidKey,
            E::Capacity => Self::Capacity,
            E::Unavailable => Self::Unavailable,
            E::Corrupt => Self::Corrupt,
            E::LostOwnership => Self::LostOwnership,
        }
    }
}
#[derive(Clone)]
pub struct InputStore {
    inner: Arc<Inner>,
}
struct Inner {
    transport: RedisTransport,
    config: InputConfig,
    bytes: Arc<Semaphore>,
}
#[derive(Clone, Debug)]
pub struct InputStamp {
    pub sequence: u64,
    pub received_at: f64,
    pub discontinuity: bool,
}
#[derive(Debug)]
pub struct InputSnapshot {
    pub owner: Option<String>,
    pub index: u64,
    pub chunks: Arc<SnapshotEntries>,
}
#[derive(Debug)]
pub struct SnapshotEntries {
    entries: Vec<InputStamp>,
    _budget: OwnedSemaphorePermit,
}
impl std::ops::Deref for SnapshotEntries {
    type Target = [InputStamp];
    fn deref(&self) -> &Self::Target {
        &self.entries
    }
}
impl InputSnapshot {
    /// Original ZREVRANGEBYSCORE selection, then oldest fallback. The returned
    /// cursor is the last consumed index; its next read starts at cursor + 1.
    pub fn cursor_for_age(&self, seconds_behind: f64, now: f64) -> Option<u64> {
        if !seconds_behind.is_finite() || seconds_behind < 0.0 || !now.is_finite() {
            return None;
        }
        let compare = |a: &&InputStamp, b: &&InputStamp| {
            a.received_at
                .total_cmp(&b.received_at)
                .then_with(|| a.sequence.to_string().cmp(&b.sequence.to_string()))
        };
        let target = now - seconds_behind;
        let chosen = self
            .chunks
            .iter()
            .filter(|stamp| stamp.received_at <= target)
            .max_by(compare)
            .or_else(|| self.chunks.iter().min_by(compare))?;
        Some(chosen.sequence.saturating_sub(1))
    }
}
pub struct InputBytes {
    pub sequence: u64,
    pub owner: String,
    pub received_at: f64,
    pub discontinuity: bool,
    data: Vec<u8>,
    _budget: OwnedSemaphorePermit,
}
impl AsRef<[u8]> for InputBytes {
    fn as_ref(&self) -> &[u8] {
        &self.data
    }
}
fn keys(channel: &str) -> Result<[String; 6], Error> {
    if channel.is_empty() || channel.len() > 256 || channel.contains(':') {
        return Err(Error::InvalidKey);
    }
    let prefix = format!("live:channel:{channel}");
    Ok([
        format!("{prefix}:owner"),
        format!("{prefix}:input:buffer:rust_owner"),
        format!("{prefix}:input:buffer:index"),
        format!("{prefix}:input:buffer:chunk_timestamps"),
        format!("{prefix}:input:buffer:discontinuities"),
        format!("{prefix}:input:buffer:chunk:"),
    ])
}
fn valid_owner(owner: &str) -> bool {
    !owner.is_empty() && owner.len() <= 256 && !owner.bytes().any(|b| b == 0)
}
impl InputStore {
    pub async fn connect(url: &str, config: InputConfig) -> Result<Self, Error> {
        let metadata = config
            .max_chunks
            .checked_mul(512)
            .and_then(|value| value.checked_add(1024))
            .ok_or(Error::Configuration)?;
        if config.max_catalog_entries == 0
            || config.max_catalog_entries > 65536
            || config.max_chunks == 0
            || config.max_chunks > 16384
            || config.max_chunk_bytes < 188
            || config.max_chunk_bytes > u32::MAX as usize
            || config.max_channel_bytes < config.max_chunk_bytes
            || config.max_channel_bytes > u32::MAX as usize
            || config.max_read_bytes < config.max_chunk_bytes.max(metadata)
            || config.max_read_bytes > u32::MAX as usize
            || config.max_read_bytes > Semaphore::MAX_PERMITS
            || config.chunk_ttl.as_millis() < 1
            || config.chunk_ttl > Duration::from_secs(3600)
        {
            return Err(Error::Configuration);
        }
        let transport = RedisTransport::connect(
            url,
            TransportConfig {
                max_operations: config.max_operations,
                max_queued_operations: config.max_queued_operations,
                command_timeout: config.command_timeout,
            },
        )
        .await?;
        let store = Self {
            inner: Arc::new(Inner {
                transport,
                bytes: Arc::new(Semaphore::new(config.max_read_bytes)),
                config,
            }),
        };
        gc::start(&store.inner);
        Ok(store)
    }
    pub async fn put(
        &self,
        channel: &str,
        owner: &str,
        data: &[u8],
        timestamp: f64,
        discontinuity: bool,
    ) -> Result<u64, Error> {
        let keys = gc::publication_keys(channel)?;
        let config = &self.inner.config;
        if !valid_owner(owner) {
            return Err(Error::InvalidKey);
        }
        if data.is_empty()
            || !data.len().is_multiple_of(188)
            || data.len() > config.max_chunk_bytes
            || !timestamp.is_finite()
        {
            return Err(Error::InvalidChunk);
        }
        let time = timestamp.to_string();
        let ttl = config.chunk_ttl.as_millis().to_string();
        let count = config.max_chunks.to_string();
        let bytes = config.max_channel_bytes.to_string();
        let catalog_limit = config.max_catalog_entries.to_string();
        let (result, ()): (i64, ()) = self
            .inner
            .transport
            .command(
                PUT,
                &keys,
                &[
                    owner.as_bytes(),
                    data,
                    time.as_bytes(),
                    ttl.as_bytes(),
                    count.as_bytes(),
                    bytes.as_bytes(),
                    if discontinuity { b"1" } else { b"0" },
                    channel.as_bytes(),
                    catalog_limit.as_bytes(),
                ],
                (),
            )
            .await?;
        if result > 0 {
            Ok(result as u64)
        } else if result == -1 {
            Err(Error::Capacity)
        } else {
            Err(Error::LostOwnership)
        }
    }
    pub async fn snapshot(&self, channel: &str) -> Result<InputSnapshot, Error> {
        let keys = keys(channel)?;
        let maximum = self.inner.config.max_chunks.to_string();
        let size = self.inner.config.max_chunks * 512 + 1024;
        let budget = self
            .inner
            .bytes
            .clone()
            .try_acquire_many_owned(size as u32)
            .map_err(|_| Error::Capacity)?;
        type Wire = (String, u64, Vec<(u64, f64, bool)>);
        let ((owner, index, chunks), mut budget): (Wire, OwnedSemaphorePermit) = self
            .inner
            .transport
            .command(SNAPSHOT, &keys, &[maximum.as_bytes()], budget)
            .await?;
        if chunks.iter().any(|(_, stamp, _)| !stamp.is_finite()) {
            return Err(Error::Corrupt);
        }
        let chunks: Vec<InputStamp> = chunks
            .into_iter()
            .map(|(sequence, received_at, discontinuity)| InputStamp {
                sequence,
                received_at,
                discontinuity,
            })
            .collect();
        let retained_size =
            chunks.capacity() * std::mem::size_of::<InputStamp>() + owner.capacity() + 512;
        let retained = budget.split(retained_size).ok_or(Error::Corrupt)?;
        drop(budget);
        Ok(InputSnapshot {
            owner: (!owner.is_empty()).then_some(owner),
            index,
            chunks: Arc::new(SnapshotEntries {
                entries: chunks,
                _budget: retained,
            }),
        })
    }

    pub async fn get(&self, channel: &str, sequence: u64) -> Result<Option<InputBytes>, Error> {
        if sequence > 9_007_199_254_740_000 {
            return Err(Error::InvalidKey);
        }
        let keys = keys(channel)?;
        let maximum = self.inner.config.max_chunk_bytes.to_string();
        let sequence_text = sequence.to_string();
        let budget = self
            .inner
            .bytes
            .clone()
            .try_acquire_many_owned(self.inner.config.max_chunk_bytes as u32)
            .map_err(|_| Error::Capacity)?;
        type Wire = Option<(String, f64, bool, Vec<u8>)>;
        let (data, mut budget): (Wire, OwnedSemaphorePermit) = self
            .inner
            .transport
            .command(
                GET,
                &keys,
                &[sequence_text.as_bytes(), maximum.as_bytes()],
                budget,
            )
            .await?;
        let Some((owner, received_at, discontinuity, data)) = data else {
            return Ok(None);
        };
        if !received_at.is_finite() || !data.len().is_multiple_of(188) {
            return Err(Error::Corrupt);
        }
        let retained = budget.split(data.len()).ok_or(Error::Corrupt)?;
        drop(budget);
        Ok(Some(InputBytes {
            sequence,
            owner,
            received_at,
            discontinuity,
            data,
            _budget: retained,
        }))
    }
    /// Reclaim only a known Rust era after coordinator ownership is absent.
    /// A newly acquired owner makes this operation a no-op, even before its
    /// first publication. This never deletes coordinator/provider keys.
    pub async fn cleanup_abandoned(
        &self,
        channel: &str,
        expected_old_owner: &str,
    ) -> Result<bool, Error> {
        if !valid_owner(expected_old_owner) {
            return Err(Error::InvalidKey);
        }
        let keys = gc::publication_keys(channel)?;
        let maximum = self.inner.config.max_chunks.to_string();
        let (result, ()): (i64, ()) = self
            .inner
            .transport
            .command(
                CLEANUP_ABANDONED,
                &keys,
                &[
                    expected_old_owner.as_bytes(),
                    maximum.as_bytes(),
                    channel.as_bytes(),
                ],
                (),
            )
            .await?;
        Ok(result == 1)
    }
    /// Compare both current coordinator ownership and published buffer fencing.
    /// Never deletes or modifies the coordinator owner or provider reservations.
    pub async fn cleanup(&self, channel: &str, owner: &str) -> Result<bool, Error> {
        if !valid_owner(owner) {
            return Err(Error::InvalidKey);
        }
        let keys = gc::publication_keys(channel)?;
        let maximum = self.inner.config.max_chunks.to_string();
        let (result, ()): (i64, ()) = self
            .inner
            .transport
            .command(
                CLEANUP,
                &keys,
                &[owner.as_bytes(), maximum.as_bytes(), channel.as_bytes()],
                (),
            )
            .await?;
        Ok(result == 1)
    }
}
const SNAPSHOT: &str = r#"
if redis.call('STRLEN',KEYS[1])>256 or redis.call('STRLEN',KEYS[2])>256 then return redis.error_reply('CORRUPT owner') end
local owner=redis.call('GET',KEYS[1]);if not owner then return {'',0,{}} end
if redis.call('GET',KEYS[2])~=owner then return {owner,0,{}} end
if redis.call('STRLEN',KEYS[3])>32 then return redis.error_reply('CORRUPT index') end
local index=tonumber(redis.call('GET',KEYS[3]) or '0');if not index or index<0 or index>9007199254740000 or index~=math.floor(index) then return redis.error_reply('CORRUPT index') end
if redis.call('ZCARD',KEYS[4])>tonumber(ARGV[1]) or redis.call('ZCARD',KEYS[5])>tonumber(ARGV[1]) then return redis.error_reply('CORRUPT cardinality') end
local raw=redis.call('ZRANGE',KEYS[4],0,-1,'WITHSCORES');local entries={}
for i=1,#raw,2 do
 local seq=tonumber(raw[i]);if not seq or seq<=0 or seq>index or seq~=math.floor(seq) then return redis.error_reply('CORRUPT sequence') end
 if redis.call('EXISTS',KEYS[6]..raw[i])==1 then local disc=0;if redis.call('ZSCORE',KEYS[5],raw[i]) then disc=1 end table.insert(entries,{raw[i],raw[i+1],disc}) end
end
return {owner,index,entries}
"#;
const GET: &str = r#"
if redis.call('STRLEN',KEYS[1])>256 then return redis.error_reply('CORRUPT owner') end
local owner=redis.call('GET',KEYS[1]);if not owner or redis.call('GET',KEYS[2])~=owner then return nil end
local chunk=KEYS[6]..ARGV[1];local size=redis.call('STRLEN',chunk)
if size==0 then return nil end
if size>tonumber(ARGV[2]) then return redis.error_reply('CORRUPT oversized input') end
local timestamp=redis.call('ZSCORE',KEYS[4],ARGV[1]);if not timestamp then return redis.error_reply('CORRUPT missing timestamp') end
local disc=0;if redis.call('ZSCORE',KEYS[5],ARGV[1]) then disc=1 end
return {owner,timestamp,disc,redis.call('GET',chunk)}
"#;
const PUT: &str = r#"
if redis.call('GET',KEYS[1])~=ARGV[1] then return 0 end
local ct=redis.call('TYPE',KEYS[7]).ok;local dt=redis.call('TYPE',KEYS[8]).ok;local mt=redis.call('TYPE',KEYS[9]).ok
if (ct~='none' and ct~='hash') or (dt~='none' and dt~='zset') or (mt~='none' and mt~='string') then return redis.error_reply('CORRUPT GC catalog') end
if redis.call('HEXISTS',KEYS[7],ARGV[8])==0 and redis.call('HLEN',KEYS[7])>=tonumber(ARGV[9]) then return -1 end
if redis.call('STRLEN',KEYS[3])>32 then return redis.error_reply('CORRUPT index') end
local index=tonumber(redis.call('GET',KEYS[3]) or '0');if not index or index<0 or index>=9007199254740000 or index~=math.floor(index) then return redis.error_reply('CORRUPT index') end
if redis.call('ZCARD',KEYS[4])>tonumber(ARGV[5]) or redis.call('ZCARD',KEYS[5])>tonumber(ARGV[5]) then return redis.error_reply('CORRUPT cardinality') end
local prior=redis.call('GET',KEYS[2]);local same=prior==ARGV[1];local all=redis.call('ZRANGE',KEYS[4],0,-1,'WITHSCORES');local total=0;local count=0;local records={}
for i=1,#all,2 do
 local seq=tonumber(all[i]);if not seq or seq<=0 or seq>index or seq~=math.floor(seq) then return redis.error_reply('CORRUPT sequence') end
 local size=redis.call('STRLEN',KEYS[6]..all[i]);table.insert(records,{id=all[i],size=size})
 if same and size>0 then total=total+size;count=count+1 end
end
local next=index+1;local name=string.format('%.0f',next);local key=KEYS[6]..name
if redis.call('EXISTS',key)==1 then return redis.error_reply('CORRUPT index collision') end
local removed={}
for _,record in ipairs(records) do
 if not same or record.size==0 or total+string.len(ARGV[2])>tonumber(ARGV[6]) or count>=tonumber(ARGV[5]) then
  table.insert(removed,record.id);if same and record.size>0 then total=total-record.size;count=count-1 end
 end
end
for _,member in ipairs(removed) do redis.call('DEL',KEYS[6]..member);redis.call('ZREM',KEYS[4],member);redis.call('ZREM',KEYS[5],member) end
if not same then redis.call('DEL',KEYS[4],KEYS[5]) end
redis.call('SET',key,ARGV[2],'PX',ARGV[4]);redis.call('SET',KEYS[3],name)
redis.call('SET',KEYS[2],ARGV[1],'PX',ARGV[4]);redis.call('ZADD',KEYS[4],ARGV[3],name);redis.call('PEXPIRE',KEYS[4],ARGV[4])
if (not same and (prior or index>0 or #all>0)) or ARGV[7]=='1' then redis.call('ZADD',KEYS[5],name,name) end
redis.call('PEXPIRE',KEYS[5],ARGV[4]);
local entry=cjson.encode({owner=ARGV[1],index=name});local clock=redis.call('TIME');local due=tonumber(clock[1])*1000+math.floor(tonumber(clock[2])/1000)+tonumber(ARGV[4])+2000
redis.call('HSET',KEYS[7],ARGV[8],entry);redis.call('ZADD',KEYS[8],due,ARGV[8]);redis.call('SET',KEYS[9],entry);return next
"#;
const CLEANUP: &str = r#"
if redis.call('GET',KEYS[1])~=ARGV[1] or redis.call('GET',KEYS[2])~=ARGV[1] then return 0 end
if redis.call('ZCARD',KEYS[4])>tonumber(ARGV[2]) then return redis.error_reply('CORRUPT cardinality') end
local ct=redis.call('TYPE',KEYS[7]).ok;local dt=redis.call('TYPE',KEYS[8]).ok;local mt=redis.call('TYPE',KEYS[9]).ok
if (ct~='none' and ct~='hash') or (dt~='none' and dt~='zset') or (mt~='none' and mt~='string') then return redis.error_reply('CORRUPT GC catalog') end
local members=redis.call('ZRANGE',KEYS[4],0,-1);for _,member in ipairs(members) do redis.call('DEL',KEYS[6]..member) end
redis.call('DEL',KEYS[2],KEYS[3],KEYS[4],KEYS[5]);
local entry=redis.call('GET',KEYS[9]);if entry and entry==redis.call('HGET',KEYS[7],ARGV[3]) then redis.call('DEL',KEYS[9]);redis.call('HDEL',KEYS[7],ARGV[3]);redis.call('ZREM',KEYS[8],ARGV[3]) end;return 1
"#;

const CLEANUP_ABANDONED: &str = r#"
if redis.call('EXISTS',KEYS[1])==1 or redis.call('GET',KEYS[2])~=ARGV[1] then return 0 end
if redis.call('ZCARD',KEYS[4])>tonumber(ARGV[2]) then return redis.error_reply('CORRUPT cardinality') end
local ct=redis.call('TYPE',KEYS[7]).ok;local dt=redis.call('TYPE',KEYS[8]).ok;local mt=redis.call('TYPE',KEYS[9]).ok
if (ct~='none' and ct~='hash') or (dt~='none' and dt~='zset') or (mt~='none' and mt~='string') then return redis.error_reply('CORRUPT GC catalog') end
local members=redis.call('ZRANGE',KEYS[4],0,-1);for _,member in ipairs(members) do redis.call('DEL',KEYS[6]..member) end
redis.call('DEL',KEYS[2],KEYS[3],KEYS[4],KEYS[5]);
local entry=redis.call('GET',KEYS[9]);if entry and entry==redis.call('HGET',KEYS[7],ARGV[3]) then redis.call('DEL',KEYS[9]);redis.call('HDEL',KEYS[7],ARGV[3]);redis.call('ZREM',KEYS[8],ARGV[3]) end;return 1
"#;
