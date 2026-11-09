//! Bounded Redis output buffer ported from Dispatcharr's OutputStreamBuffer.
//! All original application credit belongs to upstream Dispatcharr authors.
use redis::{aio::MultiplexedConnection, AsyncConnectionConfig, Script};
use std::{sync::Arc, time::Duration};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

#[derive(Clone, Debug)]
pub struct OutputConfig {
    pub max_segment_bytes: usize,
    pub max_channel_bytes: usize,
    pub max_segments: usize,
    pub max_read_bytes: usize,
    pub max_operations: usize,
    pub command_timeout: Duration,
    pub owner_ttl: Duration,
    pub chunk_ttl: Duration,
}
impl Default for OutputConfig {
    fn default() -> Self {
        Self {
            max_segment_bytes: 32 * 1024 * 1024,
            max_channel_bytes: 256 * 1024 * 1024,
            max_segments: 256,
            max_read_bytes: 256 * 1024 * 1024,
            max_operations: 16,
            command_timeout: Duration::from_secs(3),
            owner_ttl: Duration::from_secs(60),
            chunk_ttl: Duration::from_secs(100),
        }
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Error {
    Configuration,
    InvalidKey,
    Capacity,
    Unavailable,
    Corrupt,
    LostOwnership,
}
#[derive(Clone)]
pub struct OutputStore {
    inner: Arc<Inner>,
}
struct Inner {
    connection: MultiplexedConnection,
    config: OutputConfig,
    operations: Arc<Semaphore>,
    read_bytes: Arc<Semaphore>,
}
#[derive(Clone)]
pub struct OutputLease {
    store: OutputStore,
    channel: String,
    format: String,
    token: String,
}
#[derive(Debug)]
pub struct Snapshot {
    pub owner: Option<String>,
    pub index: u64,
    pub descriptor: Option<serde_json::Value>,
    pub segments: Vec<(u64, f64)>,
}
pub struct SegmentBytes {
    data: Vec<u8>,
    _bytes: OwnedSemaphorePermit,
}
impl AsRef<[u8]> for SegmentBytes {
    fn as_ref(&self) -> &[u8] {
        &self.data
    }
}
fn keys(channel: &str, format: &str) -> Result<[String; 7], Error> {
    if channel.is_empty()
        || channel.len() > 256
        || channel.contains(':')
        || !(matches!(format, "hls" | "fmp4")
            || format.split_once(":p").is_some_and(|(kind, id)| {
                matches!(kind, "hls" | "fmp4") && id.parse::<u64>().is_ok_and(|id| id > 0)
            }))
    {
        return Err(Error::InvalidKey);
    }
    let prefix = format!("live:channel:{channel}:output:{format}");
    Ok([
        format!("{prefix}:owner"),
        format!("{prefix}:buffer:index"),
        format!("{prefix}:playlist"),
        format!("{prefix}:buffer:chunk_timestamps"),
        format!("{prefix}:buffer:chunk:"),
        format!("{prefix}:init"),
        format!("{prefix}:state"),
    ])
}
fn valid_token(token: &str) -> bool {
    !token.is_empty()
        && token.len() <= 128
        && token
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_'))
}
impl OutputStore {
    pub async fn connect(url: &str, config: OutputConfig) -> Result<Self, Error> {
        if config.max_segment_bytes == 0
            || config.max_segment_bytes > u32::MAX as usize
            || config.max_read_bytes > u32::MAX as usize
            || config.max_read_bytes < config.max_segment_bytes
            || config.max_channel_bytes < config.max_segment_bytes
            || config.max_segments == 0
            || config.max_segments > 4096
            || config.max_operations == 0
            || config.max_operations > 65536
            || config.command_timeout.is_zero()
            || config.owner_ttl.is_zero()
            || config.owner_ttl > Duration::from_secs(3600)
            || config.chunk_ttl.is_zero()
            || config.chunk_ttl > Duration::from_secs(3600)
        {
            return Err(Error::Configuration);
        }
        let client = redis::Client::open(url).map_err(|_| Error::Configuration)?;
        let connection = client
            .get_multiplexed_async_connection_with_config(
                &AsyncConnectionConfig::new().set_connection_timeout(config.command_timeout),
            )
            .await
            .map_err(|_| Error::Unavailable)?;
        Ok(Self {
            inner: Arc::new(Inner {
                connection,
                operations: Arc::new(Semaphore::new(config.max_operations)),
                read_bytes: Arc::new(Semaphore::new(config.max_read_bytes)),
                config,
            }),
        })
    }
    /// Caller cancellation or deadline detaches a bounded supervised operation.
    /// The task retains admission until Redis actually responds or disconnects;
    /// dropping a timed-out multiplexed response future would release too early.
    async fn command<T: redis::FromRedisValue + Send + 'static, G: Send + 'static>(
        &self,
        source: &str,
        keys: &[String],
        args: &[&[u8]],
        guard: G,
    ) -> Result<(T, G), Error> {
        let operation = self
            .inner
            .operations
            .clone()
            .try_acquire_owned()
            .map_err(|_| Error::Capacity)?;
        let script = Script::new(source);
        let keys = keys.to_vec();
        let args: Vec<Vec<u8>> = args.iter().map(|value| value.to_vec()).collect();
        let mut connection = self.inner.connection.clone();
        let task = tokio::spawn(async move {
            let mut invocation = script.prepare_invoke();
            for key in keys {
                invocation.key(key);
            }
            for arg in args {
                invocation.arg(arg);
            }
            let result: Result<T, redis::RedisError> =
                invocation.invoke_async(&mut connection).await;
            (result, guard, operation)
        });
        let (result, guard, _operation) =
            tokio::time::timeout(self.inner.config.command_timeout, task)
                .await
                .map_err(|_| Error::Unavailable)?
                .map_err(|_| Error::Unavailable)?;
        result.map(|value| (value, guard)).map_err(|error| {
            if error.to_string().contains("CORRUPT") {
                Error::Corrupt
            } else {
                Error::Unavailable
            }
        })
    }
    async fn invoke<T: redis::FromRedisValue + Send + 'static>(
        &self,
        source: &str,
        keys: &[String],
        args: &[&[u8]],
    ) -> Result<T, Error> {
        self.command(source, keys, args, ())
            .await
            .map(|(value, ())| value)
    }
    pub async fn claim(
        &self,
        channel: &str,
        format: &str,
        token: &str,
    ) -> Result<Option<OutputLease>, Error> {
        let keys = keys(channel, format)?;
        if !valid_token(token) {
            return Err(Error::InvalidKey);
        }
        let ttl = self.inner.config.owner_ttl.as_millis().to_string();
        let result:i64=self.invoke("local old=redis.call('GET',KEYS[1]); if old and old~=ARGV[1] then return 0 end redis.call('SET',KEYS[1],ARGV[1],'PX',ARGV[2]); return 1",&keys[..1],&[token.as_bytes(),ttl.as_bytes()]).await?;
        Ok((result == 1).then(|| OutputLease {
            store: self.clone(),
            channel: channel.to_owned(),
            format: format.to_owned(),
            token: token.to_owned(),
        }))
    }
    pub async fn snapshot(&self, channel: &str, format: &str) -> Result<Snapshot, Error> {
        let keys = keys(channel, format)?;
        let maximum = self.inner.config.max_segments.to_string();
        let (owner, index, descriptor, segments): (String, u64, String, Vec<(u64, f64)>) =
            self.invoke(SNAPSHOT, &keys, &[maximum.as_bytes()]).await?;
        let descriptor = if descriptor.is_empty() {
            None
        } else {
            Some(serde_json::from_str(&descriptor).map_err(|_| Error::Corrupt)?)
        };
        Ok(Snapshot {
            owner: (!owner.is_empty()).then_some(owner),
            index,
            descriptor,
            segments,
        })
    }
    /// Init media uses the same pre-reserved byte admission as an output chunk.
    pub async fn get_init(
        &self,
        channel: &str,
        format: &str,
    ) -> Result<Option<SegmentBytes>, Error> {
        let keys = keys(channel, format)?;
        self.read_key(keys[5].clone()).await
    }
    pub async fn state(&self, channel: &str, format: &str) -> Result<Option<String>, Error> {
        let keys = keys(channel, format)?;
        self.invoke("if redis.call('STRLEN',KEYS[1])>128 then return redis.error_reply('CORRUPT output state') end return redis.call('GET',KEYS[1])",&[keys[6].clone()],&[]).await
    }
    pub async fn get(
        &self,
        channel: &str,
        format: &str,
        sequence: u64,
    ) -> Result<Option<SegmentBytes>, Error> {
        let keys = keys(channel, format)?;
        self.read_key(format!("{}{sequence}", keys[4])).await
    }
    async fn read_key(&self, key: String) -> Result<Option<SegmentBytes>, Error> {
        let reserved = self
            .inner
            .read_bytes
            .clone()
            .try_acquire_many_owned(self.inner.config.max_segment_bytes as u32)
            .map_err(|_| Error::Capacity)?;
        let maximum = self.inner.config.max_segment_bytes.to_string();
        let (data,mut reserved):(Option<Vec<u8>>,OwnedSemaphorePermit)=self.command(
            "local n=redis.call('STRLEN',KEYS[1]);if n>tonumber(ARGV[1]) then return redis.error_reply('CORRUPT oversized output') end return redis.call('GET',KEYS[1])",
            &[key],&[maximum.as_bytes()],reserved).await?;
        let Some(data) = data else {
            return Ok(None);
        };
        let retained = reserved.split(data.len()).ok_or(Error::Corrupt)?;
        drop(reserved);
        Ok(Some(SegmentBytes {
            data,
            _bytes: retained,
        }))
    }
}
impl OutputLease {
    pub fn token(&self) -> &str {
        &self.token
    }
    pub async fn renew(&self) -> Result<(), Error> {
        let keys = keys(&self.channel, &self.format)?;
        let ttl = self.store.inner.config.owner_ttl.as_millis().to_string();
        let result:i64=self.store.invoke("if redis.call('GET',KEYS[1])~=ARGV[1] then return 0 end redis.call('PEXPIRE',KEYS[1],ARGV[2]);return 1",&keys[..1],&[self.token.as_bytes(),ttl.as_bytes()]).await?;
        if result == 1 {
            Ok(())
        } else {
            Err(Error::LostOwnership)
        }
    }
    pub async fn put(&self, data: &[u8], timestamp: f64) -> Result<u64, Error> {
        let config = &self.store.inner.config;
        if data.is_empty() || data.len() > config.max_segment_bytes || !timestamp.is_finite() {
            return Err(Error::Capacity);
        }
        let keys = keys(&self.channel, &self.format)?;
        let ttl = config.chunk_ttl.as_millis().to_string();
        let stamp = timestamp.to_string();
        let count = config.max_segments.to_string();
        let bytes = config.max_channel_bytes.to_string();
        let result: i64 = self
            .store
            .invoke(
                PUT,
                &keys,
                &[
                    self.token.as_bytes(),
                    data,
                    stamp.as_bytes(),
                    ttl.as_bytes(),
                    count.as_bytes(),
                    bytes.as_bytes(),
                ],
            )
            .await?;
        match result {
            0 => Err(Error::LostOwnership),
            -1 => Err(Error::Capacity),
            value if value > 0 => Ok(value as u64),
            _ => Err(Error::Corrupt),
        }
    }
    pub async fn publish(&self, descriptor: &serde_json::Value) -> Result<(), Error> {
        let text = descriptor.to_string();
        if text.len() > 65_536 {
            return Err(Error::Capacity);
        }
        let keys = keys(&self.channel, &self.format)?;
        let result:i64=self.store.invoke("if redis.call('GET',KEYS[1])~=ARGV[1] then return 0 end redis.call('SET',KEYS[3],ARGV[2],'EX',3600);return 1",&keys,&[self.token.as_bytes(),text.as_bytes()]).await?;
        if result == 1 {
            Ok(())
        } else {
            Err(Error::LostOwnership)
        }
    }
    pub async fn put_init(&self, data: &[u8]) -> Result<(), Error> {
        if data.is_empty() || data.len() > self.store.inner.config.max_segment_bytes {
            return Err(Error::Capacity);
        }
        let keys = keys(&self.channel, &self.format)?;
        let result:i64=self.store.invoke("if redis.call('GET',KEYS[1])~=ARGV[1] then return 0 end redis.call('SET',KEYS[6],ARGV[2],'EX',3600);return 1",&keys,&[self.token.as_bytes(),data]).await?;
        if result == 1 {
            Ok(())
        } else {
            Err(Error::LostOwnership)
        }
    }
    pub async fn set_state(&self, state: &str) -> Result<(), Error> {
        if state.is_empty() || state.len() > 128 {
            return Err(Error::Capacity);
        }
        let keys = keys(&self.channel, &self.format)?;
        let result:i64=self.store.invoke("if redis.call('GET',KEYS[1])~=ARGV[1] then return 0 end redis.call('SET',KEYS[7],ARGV[2],'EX',3600);return 1",&keys,&[self.token.as_bytes(),state.as_bytes()]).await?;
        if result == 1 {
            Ok(())
        } else {
            Err(Error::LostOwnership)
        }
    }
    pub async fn release(&self) -> Result<bool, Error> {
        let keys = keys(&self.channel, &self.format)?;
        let maximum = self.store.inner.config.max_segments.to_string();
        let result: i64 = self
            .store
            .invoke(RELEASE, &keys, &[self.token.as_bytes(), maximum.as_bytes()])
            .await?;
        Ok(result == 1)
    }
}
const SNAPSHOT: &str = r#"
if redis.call('STRLEN',KEYS[1])>128 or redis.call('STRLEN',KEYS[2])>32 or redis.call('STRLEN',KEYS[3])>65536 then return redis.error_reply('CORRUPT metadata') end
local index=redis.call('GET',KEYS[2]) or '0';if not tonumber(index) or tonumber(index)<0 or tonumber(index)>9007199254740000 then return redis.error_reply('CORRUPT index') end
if redis.call('ZCARD',KEYS[4])>tonumber(ARGV[1]) then return redis.error_reply('CORRUPT cardinality') end
local raw=redis.call('ZRANGE',KEYS[4],0,-1,'WITHSCORES');local entries={}
for i=1,#raw,2 do if not tonumber(raw[i]) then return redis.error_reply('CORRUPT sequence') end table.insert(entries,{raw[i],raw[i+1]}) end
return {redis.call('GET',KEYS[1]) or '',index,redis.call('GET',KEYS[3]) or '',entries}
"#;
const PUT: &str = r#"
if redis.call('GET',KEYS[1])~=ARGV[1] then return 0 end
if redis.call('STRLEN',KEYS[2])>32 then return redis.error_reply('CORRUPT index') end
local index=tonumber(redis.call('GET',KEYS[2]) or '0');if not index or index<0 or index>=9007199254740000 or index~=math.floor(index) then return redis.error_reply('CORRUPT index') end
if redis.call('ZCARD',KEYS[4])>tonumber(ARGV[5]) then return redis.error_reply('CORRUPT cardinality') end
local all=redis.call('ZRANGE',KEYS[4],0,-1,'WITHSCORES');local total=0;local live=0;local expired={}
for i=1,#all,2 do
 if not tonumber(all[i]) then return redis.error_reply('CORRUPT sequence') end
 local key=KEYS[5]..all[i];local size=redis.call('STRLEN',key)
 if size==0 then table.insert(expired,all[i]) else total=total+size;live=live+1 end
end
if live>=tonumber(ARGV[5]) or total+string.len(ARGV[2])>tonumber(ARGV[6]) then return -1 end
local next=index+1;local chunk=KEYS[5]..string.format('%.0f',next)
if redis.call('EXISTS',chunk)==1 then return redis.error_reply('CORRUPT index collision') end
for _,member in ipairs(expired) do redis.call('ZREM',KEYS[4],member) end
redis.call('SET',chunk,ARGV[2],'PX',ARGV[4]);redis.call('SET',KEYS[2],string.format('%.0f',next))
redis.call('ZADD',KEYS[4],ARGV[3],string.format('%.0f',next));redis.call('PEXPIRE',KEYS[4],ARGV[4]);return next
"#;
const RELEASE: &str = r#"
if redis.call('GET',KEYS[1])~=ARGV[1] then return 0 end
if redis.call('ZCARD',KEYS[4])>tonumber(ARGV[2]) then return redis.error_reply('CORRUPT cardinality') end
local members=redis.call('ZRANGE',KEYS[4],0,-1);for _,member in ipairs(members) do redis.call('DEL',KEYS[5]..member) end
redis.call('DEL',KEYS[1],KEYS[2],KEYS[3],KEYS[4],KEYS[6],KEYS[7]);return 1
"#;
