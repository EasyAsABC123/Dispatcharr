//! Source Channel.get_stream public aliases, without a private VOD owner grant.
//! Original Dispatcharr authors retain all selection/release algorithm credit.
use super::*;
/// Only a real atomic Redis observation can mint a borrowed assignment.
/// Borrowing never acquires or releases a provider/credential slot.
pub struct RedirectAssignment {
    stream: i64,
    profile: i64,
    pub(super) numeric: i64,
    pub(super) channel: String,
}
impl RedirectAssignment {
    pub fn stream_id(&self) -> i64 {
        self.stream
    }
    pub fn profile_id(&self) -> i64 {
        self.profile
    }
}
pub enum RedirectClaim {
    Borrowed(RedirectAssignment),
    Owned(RedirectGrant),
    Full,
}
pub struct RedirectGrant {
    inner: Option<Grant>,
}
struct Grant {
    coordinator: Coordinator,
    channel: String,
    numeric: i64,
    stream: i64,
    profile: i64,
    token: String,
    retained: Arc<dyn Send + Sync>,
}
impl RedirectGrant {
    pub fn stream_id(&self) -> i64 {
        self.inner.as_ref().unwrap().stream
    }
    pub fn profile_id(&self) -> i64 {
        self.inner.as_ref().unwrap().profile
    }
    /// A failed primary health probe may try another URL while this exact
    /// unshared single-slot Redirect assignment is still ours.
    pub async fn holds_single_slot(&self, profile: i64) -> Result<bool, Error> {
        let Some(grant) = &self.inner else {
            return Ok(false);
        };
        if grant.profile != profile {
            return Ok(false);
        }
        let coordinator = &grant.coordinator;
        let permit = coordinator.permit().await?;
        let retained = grant.retained.clone();
        let mut connection = coordinator.inner.connection.clone();
        let timeout = coordinator.inner.config.command_timeout;
        let prefix = format!("{}:redirect:", coordinator.inner.config.namespace);
        let keys = [
            format!("{prefix}channel:{}", grant.numeric),
            format!("{prefix}stream:{}", grant.stream),
            format!("channel_stream:{}", grant.numeric),
            format!("stream_profile:{}", grant.stream),
            format!("profile_connections:{profile}"),
            format!("profile_credential_release:{profile}"),
            format!("live:channel:{}:owner", grant.channel),
        ];
        let token = grant.token.clone();
        let stream = grant.stream;
        let task = tokio::spawn(async move {
            let _scope = (permit, retained);
            let script = Script::new(OWN_SINGLE_SLOT);
            let mut invocation = script.prepare_invoke();
            for key in keys {
                invocation.key(key);
            }
            invocation.arg(token).arg(stream).arg(profile);
            invocation
                .invoke_async::<i64>(&mut connection)
                .await
                .map(|result| result == 1)
                .map_err(|_| Error::RedisUnavailable)
        });
        time::timeout(timeout, task)
            .await
            .map_err(|_| Error::RedisUnavailable)?
            .map_err(|_| Error::RedisUnavailable)?
    }

    pub async fn release(mut self) -> Result<bool, Error> {
        match self.inner.take() {
            Some(grant) => release(grant).await,
            None => Ok(false),
        }
    }
}
impl Drop for RedirectGrant {
    fn drop(&mut self) {
        if let Some(grant) = self.inner.take() {
            if let Ok(runtime) = tokio::runtime::Handle::try_current() {
                runtime.spawn(async move {
                    let _ = release(grant).await;
                });
            }
        }
    }
}
impl Coordinator {
    /// Source accepts active metadata OR the reservation-before-metadata window.
    /// Existing-but-inactive metadata is the ONLY stale-state branch; absent
    /// metadata is never interpreted as proof a provider reservation is stale.
    pub async fn observe_redirect_assignment(
        &self,
        numeric: i64,
        channel: &str,
        retained: Arc<dyn Send + Sync>,
    ) -> Result<Option<RedirectAssignment>, Error> {
        validate(numeric, channel)?;
        let (kind, stream, profile): (i64, i64, i64) = command(
            self,
            SELECT,
            vec![
                format!("channel_stream:{numeric}"),
                format!("live:channel:{channel}:metadata"),
                format!("{}:redirect:channel:{numeric}", self.inner.config.namespace),
            ],
            Vec::new(),
            retained,
        )
        .await?;
        match kind {
            0 => Ok(None),
            1 => Ok(Some(RedirectAssignment {
                stream,
                profile,
                numeric,
                channel: channel.to_owned(),
            })),
            _ => Err(Error::InvalidRequest),
        }
    }
    /// Rechecks public aliases inside the actual reserve/publish operation, so
    /// two native redirect callers cannot both reserve an initializing channel.
    pub async fn reserve_redirect_assignment(
        &self,
        numeric: i64,
        channel: &str,
        stream: i64,
        provider: ProviderReservation,
        retained: Arc<dyn Send + Sync>,
    ) -> Result<RedirectClaim, Error> {
        validate(numeric, channel)?;
        let profile = provider
            .profile_id
            .filter(|n| *n > 0)
            .ok_or(Error::InvalidRequest)?;
        if stream <= 0 {
            return Err(Error::InvalidRequest);
        }
        let group = provider
            .credential_pool
            .as_ref()
            .map(|p| p.counter_key.clone())
            .unwrap_or_default();
        if group.len() > 256
            || (!group.is_empty() && !group.starts_with("server_group_connections:"))
        {
            return Err(Error::InvalidRequest);
        }
        let token = uuid::Uuid::new_v4().to_string();
        let prefix = format!("{}:redirect:", self.inner.config.namespace);
        let keys = vec![
            format!("channel_stream:{numeric}"),
            format!("live:channel:{channel}:metadata"),
            format!("stream_profile:{stream}"),
            format!("profile_connections:{profile}"),
            group,
            format!("profile_credential_release:{profile}"),
            format!("{prefix}channel:{numeric}"),
            format!("{prefix}stream:{stream}"),
        ];
        let args = vec![
            stream.to_string(),
            profile.to_string(),
            provider.max_streams.to_string(),
            provider
                .credential_pool
                .as_ref()
                .map_or(0, |v| v.max_streams)
                .to_string(),
            token.clone(),
        ];
        let coordinator = self.clone();
        let channel = channel.to_owned();
        command_map(
            self,
            RESERVE,
            keys,
            args,
            retained.clone(),
            move |(kind, selected, selected_profile): (i64, i64, i64)| match kind {
                0 => Ok(RedirectClaim::Full),
                1 => Ok(RedirectClaim::Borrowed(RedirectAssignment {
                    stream: selected,
                    profile: selected_profile,
                    numeric,
                    channel: channel.clone(),
                })),
                2 => Ok(RedirectClaim::Owned(RedirectGrant {
                    inner: Some(Grant {
                        coordinator,
                        channel,
                        numeric,
                        stream,
                        profile,
                        token,
                        retained,
                    }),
                })),
                _ => Err(Error::InvalidRequest),
            },
        )
        .await
    }
}
fn validate(numeric: i64, channel: &str) -> Result<(), Error> {
    if numeric <= 0
        || channel.is_empty()
        || channel.len() > 256
        || channel.contains(['\0', '\r', '\n'])
    {
        Err(Error::InvalidRequest)
    } else {
        Ok(())
    }
}
async fn command<T: redis::FromRedisValue + Send + 'static>(
    coordinator: &Coordinator,
    script: &'static str,
    keys: Vec<String>,
    args: Vec<String>,
    retained: Arc<dyn Send + Sync>,
) -> Result<T, Error> {
    command_map(coordinator, script, keys, args, retained, Ok::<T, Error>).await
}
async fn command_map<T, U, F>(
    coordinator: &Coordinator,
    script: &'static str,
    keys: Vec<String>,
    args: Vec<String>,
    retained: Arc<dyn Send + Sync>,
    finish: F,
) -> Result<U, Error>
where
    T: redis::FromRedisValue + Send + 'static,
    U: Send + 'static,
    F: FnOnce(T) -> Result<U, Error> + Send + 'static,
{
    let permit = coordinator.permit().await?;
    let mut connection = coordinator.inner.connection.clone();
    let timeout = coordinator.inner.config.command_timeout;
    let task = tokio::spawn(async move {
        let _scope = (permit, retained);
        let script = Script::new(&format!("{COMMON}\n{script}"));
        let mut invocation = script.prepare_invoke();
        for key in keys {
            invocation.key(key);
        }
        for value in args {
            invocation.arg(value);
        }
        match invocation.invoke_async::<T>(&mut connection).await {
            Ok(value) => finish(value),
            // A mutating command may have committed before reply loss. Keep
            // accepted work/admission visible rather than advertising a release
            // or retrying a second provider reservation from an unknown result.
            Err(_) => std::future::pending::<Result<U, Error>>().await,
        }
    });
    time::timeout(timeout, task)
        .await
        .map_err(|_| Error::RedisUnavailable)?
        .map_err(|_| Error::RedisUnavailable)?
}
async fn release(grant: Grant) -> Result<bool, Error> {
    let prefix = format!("{}:redirect:", grant.coordinator.inner.config.namespace);
    let keys = vec![
        format!("channel_stream:{}", grant.numeric),
        format!("live:channel:{}:metadata", grant.channel),
        format!("stream_profile:{}", grant.stream),
        format!("{prefix}channel:{}", grant.numeric),
        format!("{prefix}stream:{}", grant.stream),
    ];
    let args = vec![
        grant.token,
        grant.stream.to_string(),
        grant.profile.to_string(),
    ];
    let released: i64 = command(
        &grant.coordinator,
        RELEASE_ALIAS,
        keys,
        args,
        grant.retained,
    )
    .await?;
    Ok(released == 1)
}
const COMMON: &str = r#"
local function bounded(key,maximum)
 local kind=redis.call('TYPE',key).ok
 if kind~='none' and kind~='string' then error('CORRUPT alias type') end
 if redis.call('STRLEN',key)>maximum then error('CORRUPT alias bound') end
 return redis.call('GET',key)
end
local function number(value)
 if not value then return nil end
 local trimmed=string.match(value,'^%s*(.-)%s*$')
 if not string.match(trimmed,'^[+-]?%d+$') then return nil end
 local n=tonumber(trimmed)
 if not n or n~=n or math.abs(n)>9007199254740000 or n~=math.floor(n) then return nil end
 return n
end
local function counter(key)
 local value=bounded(key,32)
 if not value then return 0 end
 local n=number(value);if not n then error('CORRUPT alias counter') end
 return n
end
local function release_profile(profile,metadata,meta_profile,reserved)
 local key='profile_connections:'..string.format('%.0f',profile)
 local count=counter(key);local remember='profile_credential_release:'..string.format('%.0f',profile)
 local group=bounded(remember,256);local group_count=0
 if group then
  if string.sub(group,1,25)~='server_group_connections:' then error('CORRUPT alias credential key') end
  group_count=counter(group)
 end
 if meta_profile and redis.call('HGET',metadata,'m3u_profile')~=meta_profile then return false end
 if meta_profile then redis.call('HDEL',metadata,'m3u_profile') end
 if reserved then
  if group and count>0 and group_count>0 then redis.call('DECR',group) end
  if count>0 then redis.call('DECR',key);count=count-1 end
  if group and count<=0 then redis.call('DEL',remember) end
 end
 redis.call('HDEL',metadata,'stream_id')
 return true
end
local function existing()
 local raw=bounded(KEYS[1],128);if not raw then return nil end
 local stream=number(raw);if not stream then return nil end
 local streamkey='stream_profile:'..string.format('%.0f',stream)
 local profile_raw=bounded(streamkey,128);local profile=number(profile_raw)
 local kind=redis.call('TYPE',KEYS[2]).ok
 if kind~='none' and kind~='hash' then error('CORRUPT alias metadata') end
 if redis.call('HSTRLEN',KEYS[2],'state')>128 or redis.call('HSTRLEN',KEYS[2],'m3u_profile')>128 then error('CORRUPT alias metadata bound') end
 local state=redis.call('HGET',KEYS[2],'state')
 local active=state=='active' or state=='waiting_for_clients' or state=='buffering' or state=='initializing' or state=='connecting'
 if active or kind=='none' then
  if profile then return {1,stream,profile} end
  return nil
 end
 -- Source stale only when metadata EXISTS and its state is not active.
 local meta=redis.call('HGET',KEYS[2],'m3u_profile')
 local stale_profile=profile or number(meta)
 local receipt=KEYS[7] or KEYS[3]
 local native_ledger=nil
 if receipt and redis.call('TYPE',receipt).ok=='hash' and (redis.call('HGET',receipt,'scope')=='live' or redis.call('HGET',receipt,'scope')=='live_borrowed')
  and redis.call('HGET',receipt,'stream')==string.format('%.0f',stream)
  and stale_profile and redis.call('HGET',receipt,'profile')==string.format('%.0f',stale_profile) then
  local ledger=redis.call('HGET',receipt,'ledger');local token=redis.call('HGET',receipt,'token')
  if ledger and token and redis.call('TYPE',ledger).ok=='hash' and redis.call('HGET',ledger,'token')==token
   and redis.call('HGET',ledger,'profile')=='profile_connections:'..string.format('%.0f',stale_profile) then native_ledger=ledger end
 end
 -- Validate all counter/key inputs before removing any alias or metadata token.
 if stale_profile then
  counter('profile_connections:'..string.format('%.0f',stale_profile))
  local remember=bounded('profile_credential_release:'..string.format('%.0f',stale_profile),256)
  if remember then if string.sub(remember,1,25)~='server_group_connections:' then error('CORRUPT alias credential key') end counter(remember) end
 end
 redis.call('DEL',streamkey,KEYS[1])
 if stale_profile then
  local released=release_profile(stale_profile,KEYS[2],meta,true)
  -- Native live ledger observes the SAME actual settled Source slot release;
  -- later normal clear/reaper must not decrement this reservation again.
  if released and native_ledger then redis.call('HSET',native_ledger,'reserved','0','credential','') end
 end
 return nil
end
"#;
// This validation read leaves the already-owned provider count and both
// aliases untouched. A transfer to a live adopter invalidates the old receipt.
const OWN_SINGLE_SLOT: &str = r#"
if redis.call('HGET',KEYS[1],'token')~=ARGV[1] or redis.call('HGET',KEYS[1],'scope')~='redirect'
   or redis.call('HGET',KEYS[2],'token')~=ARGV[1]
   or redis.call('HGET',KEYS[1],'reserved')~='1'
   or redis.call('HGET',KEYS[1],'stream')~=ARGV[2]
   or redis.call('HGET',KEYS[1],'profile')~=ARGV[3]
   or redis.call('HEXISTS',KEYS[1],'borrower_ledger')==1
   or redis.call('EXISTS',KEYS[6],KEYS[7])~=0 then return 0 end
if redis.call('GET',KEYS[3])~=ARGV[2] or redis.call('GET',KEYS[4])~=ARGV[3]
   or redis.call('GET',KEYS[5])~='1' then return 0 end
return 1
"#;
const SELECT: &str = r#"local value=existing();return value or {0,0,0}"#;
const RESERVE: &str = r#"
local value=existing();if value then return value end
local profile_count=counter(KEYS[4]);local maximum=tonumber(ARGV[3]);local groupmax=tonumber(ARGV[4]);local group_count=0
local remembered=bounded(KEYS[6],256)
if maximum>0 and groupmax>0 then group_count=counter(KEYS[5]) end
for i=7,8 do local kind=redis.call('TYPE',KEYS[i]).ok;if kind~='none' and kind~='hash' then error('CORRUPT native alias receipt') end end
if maximum>0 and profile_count>=maximum then return {0,0,0} end
if maximum>0 and groupmax>0 and group_count>=groupmax then return {0,0,0} end
if maximum>0 and groupmax>0 and profile_count>0 and remembered and remembered~=KEYS[5] then return {0,0,0} end
if maximum>0 then
 redis.call('INCR',KEYS[4])
 if groupmax>0 then redis.call('INCR',KEYS[5]);redis.call('SET',KEYS[6],KEYS[5]) end
end
redis.call('SET',KEYS[1],ARGV[1]);redis.call('SET',KEYS[3],ARGV[2])
redis.call('HSET',KEYS[7],'token',ARGV[5],'scope','redirect','ledger','','stream',ARGV[1],'profile',ARGV[2],'reserved',maximum>0 and '1' or '0')
redis.call('HSET',KEYS[8],'token',ARGV[5])
return {2,tonumber(ARGV[1]),tonumber(ARGV[2])}
"#;
const RELEASE_ALIAS: &str = r#"
local kind=redis.call('TYPE',KEYS[4]).ok
if kind=='none' then return 0 end
if kind~='hash' then error('CORRUPT native alias receipt') end
local direct=redis.call('HGET',KEYS[4],'token')==ARGV[1]
local borrowed=redis.call('HGET',KEYS[4],'borrowed_from')==ARGV[1]
if not direct and not borrowed then return 0 end
local raw=bounded(KEYS[1],128)
if raw~=ARGV[2] then return 0 end
local meta=redis.call('HGET',KEYS[2],'m3u_profile')
-- Same-native-generation receipts protect native successor cleanup. They do
-- NOT certify versions of a foreign Source SET with identical integer bytes.
local stream_owned=redis.call('HGET',KEYS[5],'token')==ARGV[1] or (borrowed and redis.call('HGET',KEYS[5],'token')==redis.call('HGET',KEYS[4],'token'))
if stream_owned and bounded(KEYS[3],128)~=ARGV[3] then return 0 end
local ledger=borrowed and redis.call('HGET',KEYS[4],'ledger') or redis.call('HGET',KEYS[4],'borrower_ledger')
local ledger_token=borrowed and redis.call('HGET',KEYS[4],'token') or redis.call('HGET',KEYS[4],'borrower_token')
local reserved=redis.call('HGET',KEYS[4],'reserved')=='1'
release_profile(tonumber(ARGV[3]),KEYS[2],meta,reserved)
if ledger and redis.call('TYPE',ledger).ok=='hash' and redis.call('HGET',ledger,'token')==ledger_token and redis.call('HGET',ledger,'source_parent_token')==ARGV[1] then redis.call('HSET',ledger,'reserved','0','credential','') end
redis.call('DEL',KEYS[1])
if direct then redis.call('DEL',KEYS[4]) else redis.call('HSET',KEYS[4],'reserved','0','borrowed_from','') end
if stream_owned then redis.call('DEL',KEYS[3]);if direct then redis.call('DEL',KEYS[5]) end end
return 1
"#;
