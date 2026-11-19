//! Bounded Redis output buffer ported from Dispatcharr's OutputStreamBuffer.
//! All original application credit belongs to upstream Dispatcharr authors.
mod profile_events;
pub mod transport;
pub use profile_events::{
    OutputProfileOwner, OutputProfileRequest, ProfileEventObservation, ProfileEventState,
    ProfileEvents,
};
use std::{sync::Arc, time::Duration};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use transport::{RedisTransport, TransportConfig};

#[derive(Clone, Debug)]
pub struct OutputConfig {
    pub max_segment_bytes: usize,
    pub max_channel_bytes: usize,
    pub max_segments: usize,
    pub max_read_bytes: usize,
    pub max_operations: usize,
    pub max_queued_operations: usize,
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
            max_queued_operations: 1024,
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
    address: redis::ConnectionInfo,
    transport: RedisTransport,
    config: OutputConfig,
    read_bytes: Arc<Semaphore>,
}
#[derive(Clone)]
pub struct OutputLease {
    store: OutputStore,
    channel: String,
    format: String,
    token: String,
    owner_ttl: Duration,
    input_owner: Option<String>,
    chunk_ttl: Duration,
}
/// A channel's pre-delivery MPEG-TS transcode history, shared by raw TS,
/// HLS and fMP4 consumers. It reuses this store's physical Redis transport and
/// byte admission; it does not create another connection or provider lease.
#[derive(Clone)]
pub struct ProfileStore {
    store: OutputStore,
    format: String,
    chunk_ttl: Duration,
}
impl OutputStore {
    pub fn profile(&self, id: i64) -> Result<ProfileStore, Error> {
        self.profile_with_chunk_ttl(id, self.inner.config.chunk_ttl)
    }
    /// Main supplies the same actual raw StreamBuffer TTL; HLS segment TTL
    /// must not silently become pre-delivery profile history's retention.
    pub fn profile_with_chunk_ttl(
        &self,
        id: i64,
        chunk_ttl: Duration,
    ) -> Result<ProfileStore, Error> {
        if chunk_ttl.as_secs() == 0 || chunk_ttl > Duration::from_secs(3600) {
            return Err(Error::Configuration);
        }
        if id <= 0 {
            return Err(Error::InvalidKey);
        }
        Ok(ProfileStore {
            store: self.clone(),
            format: format!("mpegts:p{id}"),
            chunk_ttl,
        })
    }
}
impl ProfileStore {
    pub async fn claim_retained<G: Send + 'static>(
        &self,
        channel: &str,
        raw_owner: &str,
        token: &str,
        owner: G,
    ) -> Result<(Option<OutputLease>, G), Error> {
        self.store
            .claim_inner(
                channel,
                &self.format,
                token,
                Some(raw_owner),
                self.chunk_ttl,
                owner,
            )
            .await
    }
    pub async fn snapshot(&self, channel: &str) -> Result<Snapshot, Error> {
        self.store.snapshot(channel, &self.format).await
    }
    /// Pin a profile cursor to its observed producer, including the actual
    /// queued read's caller ownership/admission if observation is cancelled.
    pub async fn get_owned_retained<G: Send + 'static>(
        &self,
        channel: &str,
        expected_owner: &str,
        sequence: u64,
        guard: G,
    ) -> Result<(Option<SegmentBytes>, G), Error> {
        self.store
            .get_owned_retained(channel, &self.format, expected_owner, sequence, guard)
            .await
    }
    pub async fn get(&self, channel: &str, sequence: u64) -> Result<Option<SegmentBytes>, Error> {
        self.store.get(channel, &self.format, sequence).await
    }
    pub async fn state(&self, channel: &str) -> Result<Option<String>, Error> {
        self.store.state(channel, &self.format).await
    }
}
#[derive(Debug)]
pub struct Snapshot {
    pub owner: Option<String>,
    pub index: u64,
    pub descriptor: Option<serde_json::Value>,
    pub segments: Vec<(u64, f64)>,
}
/// Source channel state for one cold HLS playlist poll.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HlsColdState {
    Starting,
    Ready,
    Stopped,
}
pub struct HlsColdPoll {
    pub descriptor: Option<serde_json::Value>,
    pub state: HlsColdState,
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
                matches!(kind, "hls" | "fmp4" | "mpegts")
                    && id.parse::<u64>().is_ok_and(|id| id > 0)
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
        if config.max_read_bytes > Semaphore::MAX_PERMITS
            || config.max_channel_bytes > u32::MAX as usize
            || config.max_segment_bytes == 0
            || config.max_segment_bytes > u32::MAX as usize
            || config.max_read_bytes > u32::MAX as usize
            || config.max_read_bytes < config.max_segment_bytes
            || config.max_channel_bytes < config.max_segment_bytes
            || config.max_segments == 0
            || config.max_segments > 4096
            || config.max_queued_operations == 0
            || config.max_queued_operations > 65536
            || config.max_operations == 0
            || config.max_operations > 65536
            || config.command_timeout.is_zero()
            || config.owner_ttl.as_millis() < 1
            || config.owner_ttl > Duration::from_secs(3600)
            || config.chunk_ttl.as_millis() < 1
            || config.chunk_ttl > Duration::from_secs(3600)
        {
            return Err(Error::Configuration);
        }
        let address = redis::Client::open(url)
            .map_err(|_| Error::Configuration)?
            .get_connection_info()
            .clone();
        let transport = RedisTransport::connect(
            url,
            TransportConfig {
                max_operations: config.max_operations,
                max_queued_operations: config.max_queued_operations,
                command_timeout: config.command_timeout,
            },
        )
        .await?;
        Ok(Self {
            inner: Arc::new(Inner {
                address,
                transport,
                read_bytes: Arc::new(Semaphore::new(config.max_read_bytes)),
                config,
            }),
        })
    }
    async fn command<T: redis::FromRedisValue + Send + 'static, G: Send + 'static>(
        &self,
        source: &str,
        keys: &[String],
        args: &[&[u8]],
        guard: G,
    ) -> Result<(T, G), Error> {
        self.inner
            .transport
            .command(source, keys, args, guard)
            .await
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
        self.claim_retained(channel, format, token, ())
            .await
            .map(|(lease, ())| lease)
    }
    /// The accepted producer owner outlives a cancelled claim observer until
    /// the actual queued Redis reply/disconnect settles.
    pub async fn claim_retained<G: Send + 'static>(
        &self,
        channel: &str,
        format: &str,
        token: &str,
        guard: G,
    ) -> Result<(Option<OutputLease>, G), Error> {
        self.claim_inner(
            channel,
            format,
            token,
            None,
            self.inner.config.chunk_ttl,
            guard,
        )
        .await
    }
    async fn claim_inner<G: Send + 'static>(
        &self,
        channel: &str,
        format: &str,
        token: &str,
        raw_owner: Option<&str>,
        chunk_ttl: Duration,
        guard: G,
    ) -> Result<(Option<OutputLease>, G), Error> {
        if format.starts_with("mpegts:p") && raw_owner.is_none() {
            return Err(Error::Configuration);
        }
        if raw_owner
            .is_some_and(|owner| owner.is_empty() || owner.len() > 256 || owner.contains('\0'))
        {
            return Err(Error::InvalidKey);
        }
        let keys = keys(channel, format)?;
        if !valid_token(token) {
            return Err(Error::InvalidKey);
        }
        let owner_ttl = if format.starts_with("mpegts:p") {
            Duration::from_secs(3600)
        } else {
            self.inner.config.owner_ttl
        };
        let ttl = owner_ttl.as_millis().to_string();
        let mut claim_keys = vec![keys[0].clone()];
        if raw_owner.is_some() {
            claim_keys.push(format!("live:channel:{channel}:owner"));
        }
        let raw = raw_owner.unwrap_or("");
        let (result, guard):(i64,G)=self.command("if ARGV[3]~='' and redis.call('GET',KEYS[2])~=ARGV[3] then return 0 end local old=redis.call('GET',KEYS[1]); if old and old~=ARGV[1] then return 0 end redis.call('SET',KEYS[1],ARGV[1],'PX',ARGV[2]); return 1",&claim_keys,&[token.as_bytes(),ttl.as_bytes(),raw.as_bytes()],guard).await?;
        Ok((
            (result == 1).then(|| OutputLease {
                store: self.clone(),
                channel: channel.to_owned(),
                format: format.to_owned(),
                token: token.to_owned(),
                owner_ttl,
                input_owner: raw_owner.map(str::to_owned),
                chunk_ttl,
            }),
            guard,
        ))
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
    /// Read the five Source cold-start keys together. Keep media bytes out of
    /// this poll and bound the descriptor before Redis sends it to the worker.
    pub async fn poll_hls_cold_playlist(
        &self,
        channel: &str,
        format: &str,
        client: &str,
    ) -> Result<HlsColdPoll, Error> {
        if !format.starts_with("hls")
            || client.is_empty()
            || client.len() > 256
            || client.contains(':')
        {
            return Err(Error::InvalidKey);
        }
        let keys = keys(channel, format)?;
        let poll_keys = [
            keys[2].clone(),
            keys[6].clone(),
            format!("live:channel:{channel}:metadata"),
            format!("live:channel:{channel}:stopping"),
            format!("live:channel:{channel}:client:{client}:stop"),
        ];
        let (state, descriptor): (u8, String) =
            self.invoke(HLS_COLD_PLAYLIST_POLL, &poll_keys, &[]).await?;
        let state = match state {
            0 => HlsColdState::Starting,
            1 => HlsColdState::Ready,
            2 => HlsColdState::Stopped,
            _ => return Err(Error::Corrupt),
        };
        Ok(HlsColdPoll {
            state,
            descriptor: if descriptor.is_empty() {
                None
            } else {
                Some(serde_json::from_str(&descriptor).map_err(|_| Error::Corrupt)?)
            },
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
    /// Compare the observed producer and fetch its payload atomically. An
    /// owner change between a cursor snapshot and GET cannot mix generations.
    pub async fn get_owned_retained<G: Send + 'static>(
        &self,
        channel: &str,
        format: &str,
        expected_owner: &str,
        sequence: u64,
        guard: G,
    ) -> Result<(Option<SegmentBytes>, G), Error> {
        if !valid_token(expected_owner) {
            return Err(Error::InvalidKey);
        }
        let keys = keys(channel, format)?;
        self.read_key_retained(
            format!("{}{sequence}", keys[4]),
            Some((keys[0].clone(), expected_owner.to_owned())),
            guard,
        )
        .await
    }
    /// The init and media fragments of an fMP4 response must share one producer.
    pub async fn get_init_owned_retained<G: Send + 'static>(
        &self,
        channel: &str,
        format: &str,
        expected_owner: &str,
        guard: G,
    ) -> Result<(Option<SegmentBytes>, G), Error> {
        if !valid_token(expected_owner) {
            return Err(Error::InvalidKey);
        }
        let keys = keys(channel, format)?;
        self.read_key_retained(
            keys[5].clone(),
            Some((keys[0].clone(), expected_owner.to_owned())),
            guard,
        )
        .await
    }
    async fn read_key(&self, key: String) -> Result<Option<SegmentBytes>, Error> {
        self.read_key_retained(key, None, ())
            .await
            .map(|(data, ())| data)
    }
    async fn read_key_retained<G: Send + 'static>(
        &self,
        key: String,
        expected: Option<(String, String)>,
        guard: G,
    ) -> Result<(Option<SegmentBytes>, G), Error> {
        let reserved = self
            .inner
            .read_bytes
            .clone()
            .try_acquire_many_owned(self.inner.config.max_segment_bytes as u32)
            .map_err(|_| Error::Capacity)?;
        let maximum = self.inner.config.max_segment_bytes.to_string();
        let mut keys = vec![key];
        let mut owner = String::new();
        if let Some((owner_key, expected)) = expected {
            keys.push(owner_key);
            owner = expected;
        }
        let ((matched,data),(mut reserved,guard)) = self.command::<(i64,Option<Vec<u8>>),_>(
            "if ARGV[2]~='' and redis.call('GET',KEYS[2])~=ARGV[2] then return {0,false} end local n=redis.call('STRLEN',KEYS[1]);if n>tonumber(ARGV[1]) then return redis.error_reply('CORRUPT oversized output') end return {1,redis.call('GET',KEYS[1])}",
            &keys,&[maximum.as_bytes(),owner.as_bytes()],(reserved,guard)).await?;
        if matched != 1 {
            return Err(Error::LostOwnership);
        }
        let Some(data) = data else {
            return Ok((None, guard));
        };
        let retained = reserved.split(data.len()).ok_or(Error::Corrupt)?;
        drop(reserved);
        Ok((
            Some(SegmentBytes {
                data,
                _bytes: retained,
            }),
            guard,
        ))
    }
}
impl OutputLease {
    pub fn token(&self) -> &str {
        &self.token
    }
    pub async fn renew(&self) -> Result<(), Error> {
        self.renew_retained(()).await
    }
    pub async fn renew_retained<G: Send + 'static>(&self, guard: G) -> Result<G, Error> {
        let keys = keys(&self.channel, &self.format)?;
        let ttl = self.owner_ttl.as_millis().to_string();
        let mut renew_keys = vec![keys[0].clone()];
        if self.input_owner.is_some() {
            renew_keys.push(format!("live:channel:{}:owner", self.channel));
            renew_keys.push(keys[6].clone());
        }
        let raw = self.input_owner.as_deref().unwrap_or("");
        let (result, guard):(i64,G)=self.store.command("if redis.call('GET',KEYS[1])~=ARGV[1] then return 0 end if ARGV[3]~='' and redis.call('GET',KEYS[2])~=ARGV[3] then return 0 end redis.call('PEXPIRE',KEYS[1],ARGV[2]);if ARGV[3]~='' then redis.call('PEXPIRE',KEYS[3],ARGV[2]) end return 1",&renew_keys,&[self.token.as_bytes(),ttl.as_bytes(),raw.as_bytes()],guard).await?;
        if result == 1 {
            Ok(guard)
        } else {
            Err(Error::LostOwnership)
        }
    }
    pub async fn put(&self, data: &[u8], timestamp: f64) -> Result<u64, Error> {
        self.put_retained(data, timestamp, ())
            .await
            .map(|(sequence, ())| sequence)
    }
    pub async fn put_retained<G: Send + 'static>(
        &self,
        data: &[u8],
        timestamp: f64,
        guard: G,
    ) -> Result<(u64, G), Error> {
        let config = &self.store.inner.config;
        if data.is_empty() || data.len() > config.max_segment_bytes || !timestamp.is_finite() {
            return Err(Error::Capacity);
        }
        let mut keys = keys(&self.channel, &self.format)?.to_vec();
        if self.input_owner.is_some() {
            keys.push(format!("live:channel:{}:owner", self.channel));
        }
        let raw = self.input_owner.as_deref().unwrap_or("");
        let ttl = self.chunk_ttl.as_millis().to_string();
        let stamp = timestamp.to_string();
        let count = config.max_segments.to_string();
        let bytes = config.max_channel_bytes.to_string();
        let (result, guard): (i64, G) = self
            .store
            .command(
                PUT,
                &keys,
                &[
                    self.token.as_bytes(),
                    data,
                    stamp.as_bytes(),
                    ttl.as_bytes(),
                    count.as_bytes(),
                    bytes.as_bytes(),
                    raw.as_bytes(),
                ],
                guard,
            )
            .await?;
        match result {
            0 => Err(Error::LostOwnership),
            -1 => Err(Error::Capacity),
            value if value > 0 => Ok((value as u64, guard)),
            _ => Err(Error::Corrupt),
        }
    }
    pub async fn publish(&self, descriptor: &serde_json::Value) -> Result<(), Error> {
        if self.input_owner.is_some() {
            return Err(Error::Configuration);
        }
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
        if self.input_owner.is_some() {
            return Err(Error::Configuration);
        }
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
        self.set_state_retained(state, ()).await
    }
    pub async fn set_state_retained<G: Send + 'static>(
        &self,
        state: &str,
        guard: G,
    ) -> Result<G, Error> {
        if state.is_empty() || state.len() > 128 {
            return Err(Error::Capacity);
        }
        let keys = keys(&self.channel, &self.format)?;
        let mut keys = keys.to_vec();
        if self.input_owner.is_some() {
            keys.push(format!("live:channel:{}:owner", self.channel));
        }
        let raw = self.input_owner.as_deref().unwrap_or("");
        let (result, guard):(i64,G)=self.store.command("if redis.call('GET',KEYS[1])~=ARGV[1] then return 0 end if ARGV[3]~='' and redis.call('GET',KEYS[8])~=ARGV[3] then return 0 end redis.call('SET',KEYS[7],ARGV[2],'EX',3600);return 1",&keys,&[self.token.as_bytes(),state.as_bytes(),raw.as_bytes()],guard).await?;
        if result == 1 {
            Ok(guard)
        } else {
            Err(Error::LostOwnership)
        }
    }
    pub async fn release(&self) -> Result<bool, Error> {
        self.release_retained(())
            .await
            .map(|(released, ())| released)
    }
    pub async fn release_retained<G: Send + 'static>(&self, guard: G) -> Result<(bool, G), Error> {
        let keys = keys(&self.channel, &self.format)?;
        let maximum = self.store.inner.config.max_segments.to_string();
        let (result, guard): (i64, G) = self
            .store
            .command(
                RELEASE,
                &keys,
                &[self.token.as_bytes(), maximum.as_bytes()],
                guard,
            )
            .await?;
        Ok((result == 1, guard))
    }
}
// Original HLS views poll this playlist, the output and channel states,
// and the stopping keys in one Redis exchange.
const HLS_COLD_PLAYLIST_POLL: &str = r#"
if redis.call('STRLEN', KEYS[2]) > 128 or redis.call('HSTRLEN', KEYS[3], 'state') > 128 then
    return redis.error_reply('CORRUPT HLS cold state')
end
local output = redis.call('GET', KEYS[2])
local state = redis.call('HGET', KEYS[3], 'state')
if output == 'stopped' or redis.call('EXISTS', KEYS[4], KEYS[5]) ~= 0
    or state == 'error' or state == 'stopping' or state == 'stopped' then
    return {2, ''}
end
if redis.call('STRLEN', KEYS[1]) > 65536 then
    return redis.error_reply('CORRUPT HLS cold descriptor')
end
local playlist = redis.call('GET', KEYS[1]) or ''
if not state or state == 'initializing' or state == 'connecting' or state == 'buffering' then
    return {0, playlist}
end
return {1, playlist}
"#;
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
if ARGV[7]~='' and redis.call('GET',KEYS[8])~=ARGV[7] then return 0 end
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
