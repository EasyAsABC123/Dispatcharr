//! VOD-scoped admission ported from Dispatcharr apps/proxy/utils.py.
//! Original application credit belongs to upstream Dispatcharr contributors.
//! This writes actual VOD hashes atomically, sharing live REGISTER's counters
//! and preemption keys without creating any phantom live metadata.
use crate::Error;
use dispatcharr_contracts::UserLimitPolicy;
use redis::Script;
use serde_json::json;
use std::{collections::BTreeMap, time::Duration};

pub struct Admission<'a> {
    pub session_id: &'a str,
    pub fields: &'a BTreeMap<String, String>,
    pub limit: u32,
    pub policy: &'a UserLimitPolicy,
    pub create: bool,
    pub token: &'a str,
    pub scan_limit: usize,
    pub timeout: Duration,
    pub ttl: Duration,
}
pub async fn admit<C: redis::aio::ConnectionLike + Send + Unpin>(
    connection: &mut C,
    request: Admission<'_>,
) -> Result<bool, Error> {
    admit_inner(connection, request, None, false).await
}

/// Literal Source policy for the opt-in VOD body-stop protocol. Public stop
/// signals are consumed by a body later, and do not hide counted metadata.
pub async fn admit_source<C: redis::aio::ConnectionLike + Send + Unpin>(
    connection: &mut C,
    request: Admission<'_>,
) -> Result<bool, Error> {
    admit_inner(connection, request, None, true).await
}

/// Additional current-owner/stop fence for a supervised recovery transition.
/// The caller must retain bounded ownership until this physical future finishes;
/// unlike ordinary admission this variant has no inner logical timeout.
pub struct AdmissionFence<'a> {
    pub owner_key: &'a str,
    pub owner_token: &'a str,
    pub stop_key: &'a str,
}
pub async fn admit_fenced_physical<C: redis::aio::ConnectionLike + Send + Unpin>(
    connection: &mut C,
    request: Admission<'_>,
    fence: AdmissionFence<'_>,
) -> Result<bool, Error> {
    if fence.owner_key.is_empty()
        || fence.owner_key.len() > 512
        || fence.owner_token.is_empty()
        || fence.owner_token.len() > 128
        || fence.stop_key.is_empty()
        || fence.stop_key.len() > 512
    {
        return Err(Error::InvalidClient);
    }
    admit_inner(connection, request, Some(fence), false).await
}

/// Same physical ownership contract as the stopped-owner variant. Source's
/// public stop does not deny admission; the current coordinator owner is fenced.
pub async fn admit_source_owner_physical<C: redis::aio::ConnectionLike + Send + Unpin>(
    connection: &mut C,
    request: Admission<'_>,
    fence: AdmissionFence<'_>,
) -> Result<bool, Error> {
    if fence.owner_key.is_empty()
        || fence.owner_key.len() > 512
        || fence.owner_token.is_empty()
        || fence.owner_token.len() > 128
        || fence.stop_key.is_empty()
        || fence.stop_key.len() > 512
    {
        return Err(Error::InvalidClient);
    }
    admit_inner(connection, request, Some(fence), true).await
}

async fn admit_inner<C: redis::aio::ConnectionLike + Send + Unpin>(
    connection: &mut C,
    request: Admission<'_>,
    fence: Option<AdmissionFence<'_>>,
    source_policy: bool,
) -> Result<bool, Error> {
    let Admission {
        session_id,
        fields,
        limit,
        policy,
        create,
        token,
        scan_limit,
        timeout,
        ttl,
    } = request;
    if session_id.is_empty()
        || session_id.len() > 128
        || session_id.contains(':')
        || fields.get("session_id").map(String::as_str) != Some(session_id)
        || scan_limit == 0
        || ttl.is_zero()
    {
        return Err(Error::InvalidClient);
    }
    let policy = json!({"terminate_on_limit_exceeded":policy.terminate_on_limit_exceeded,"prioritize_single_client_channels":policy.prioritize_single_client_channels,"ignore_same_channel_connections":policy.ignore_same_channel_connections,"terminate_oldest":policy.terminate_oldest});
    const FENCE: &str = "if redis.call('EXISTS',KEYS[3])~=0 or redis.call('GET',KEYS[2])~=ARGV[8] then return {'STOPPED'} end\n";
    let script = if fence.is_some() {
        if source_policy {
            format!("if redis.call('GET',KEYS[2])~=ARGV[8] then return {{'STOPPED'}} end\n{ADMIT}")
        } else {
            format!("{FENCE}{ADMIT}")
        }
    } else {
        ADMIT.to_owned()
    };
    let script = Script::new(&script);
    let mut invocation = script.prepare_invoke();
    invocation.key(format!("vod_persistent_connection:{session_id}"));
    if let Some(fence) = &fence {
        invocation.key(fence.owner_key).key(fence.stop_key);
    }
    invocation
        .arg(serde_json::to_string(fields).map_err(|_| Error::InvalidClient)?)
        .arg(policy.to_string())
        .arg(limit)
        .arg(ttl.as_millis().min(u64::MAX as u128) as u64)
        .arg(scan_limit)
        .arg(if create { "create" } else { "check" })
        .arg(token);
    if let Some(fence) = &fence {
        invocation.arg(fence.owner_token);
    } else {
        invocation.arg("");
    }
    invocation.arg(if source_policy { "source" } else { "legacy" });
    let command = invocation.invoke_async::<Vec<String>>(connection);
    let result = if fence.is_some() {
        command.await.map_err(|_| Error::RedisUnavailable)?
    } else {
        tokio::time::timeout(timeout, command)
            .await
            .map_err(|_| Error::RedisUnavailable)?
            .map_err(|_| Error::RedisUnavailable)?
    };
    match result.first().map(String::as_str) {
        Some("OK") => Ok(true),
        Some("EXISTS") => Ok(false),
        Some("LIMIT") => Err(Error::UserLimit),
        Some("SCAN_LIMIT") => Err(Error::ScanLimit),
        Some("CORRUPT") => Err(Error::CorruptMetadata),
        Some("STOPPED") => Err(Error::Stopped),
        _ => Err(Error::RedisUnavailable),
    }
}
/// One fresh, signed alias may enter a proven otherwise idle retained row.
/// The caller owns the retained Session's setup mutex and provider reservation
/// until this physical mutation or its token-checked cancellation settles.
pub struct SignedIdleAlias<'a> {
    pub requested: &'a str,
    pub prior: &'a str,
    pub claim: &'a str,
    pub requester: i64,
    pub prior_viewer: i64,
    pub requester_limit: u32,
    pub content: &'a str,
    pub profile: i64,
    pub metadata_token: &'a str,
    pub lease_key: &'a str,
    pub lease_token: &'a str,
    pub now: &'a str,
}

pub async fn enter_signed_idle_alias<C: redis::aio::ConnectionLike + Send + Unpin>(
    connection: &mut C,
    alias: &SignedIdleAlias<'_>,
) -> Result<bool, Error> {
    let data = json!({"user_id":alias.requester.to_string(),"content_uuid":alias.content});
    let reply: Vec<String> = Script::new(ADMIT)
        .key(format!("vod_persistent_connection:{}", alias.prior))
        .key(alias.lease_key)
        .key(format!("vod_proxy:client:{}:stop", alias.prior))
        .key(format!("vod_persistent_connection:{}", alias.requested))
        .key(format!("vod_signed_idle_alias:{}", alias.requested))
        .key(format!("vod_session_user:{}", alias.requested))
        .arg(data.to_string())
        .arg("{}")
        .arg(alias.requester_limit)
        .arg(0)
        .arg(50000)
        .arg("check")
        .arg(alias.metadata_token)
        .arg(alias.lease_token)
        .arg("legacy")
        .arg(alias.claim)
        .arg(alias.prior_viewer)
        .arg(alias.profile)
        .arg(alias.requested)
        .arg(alias.now)
        .invoke_async(connection)
        .await
        .map_err(|_| Error::RedisUnavailable)?;
    match reply.first().map(String::as_str) {
        Some("OK") => Ok(true),
        Some("ALIAS_MISS") => Ok(false),
        Some("LIMIT") => Err(Error::UserLimit),
        Some("SCAN_LIMIT") => Err(Error::ScanLimit),
        Some("CORRUPT") => Err(Error::CorruptMetadata),
        Some("STOPPED") => Err(Error::Stopped),
        _ => Err(Error::RedisUnavailable),
    }
}

const ADMIT: &str = r#"
local function kind(key)
  local result=redis.call('TYPE',key)
  if type(result)=='table' then return result.ok end
  return result
end
if ARGV[6]=='create' then
  if kind(KEYS[1])~='none' then return {'EXISTS'} end
elseif kind(KEYS[1])~='hash' or redis.call('HGET',KEYS[1],'rust_owner_token')~=ARGV[7] then
  return {'CORRUPT'}
end
local data=cjson.decode(ARGV[1])
local policy=cjson.decode(ARGV[2])
local limit=tonumber(ARGV[3])
local ttl=tonumber(ARGV[4])
local user=tostring(data.user_id)
if ARGV[6]=='create' then
  local claim='vod_signed_idle_alias:'..data.session_id
  if kind(claim)=='string' and redis.call('GET',claim)=='used' then return {'EXISTS'} end
end
local claim_ttl, previous_activity
if ARGV[10] then
  -- A mismatched or consumed alias can never mutate another row.
  if kind(KEYS[5])~='string' or redis.call('GET',KEYS[5])~=ARGV[10] then return {'ALIAS_MISS'} end
  claim_ttl=redis.call('PTTL',KEYS[5])
  if claim_ttl<=0 or claim_ttl>300000 then return {'ALIAS_MISS'} end
  if kind(KEYS[4])~='none' or kind(KEYS[6])~='string' or redis.call('GET',KEYS[6])~=user then return {'STOPPED'} end
  if redis.call('GET',KEYS[2])~=ARGV[8] or redis.call('EXISTS',KEYS[3])~=0 then return {'STOPPED'} end
  local requests=redis.call('HGET',KEYS[1],'request_count')
  previous_activity=redis.call('HGET',KEYS[1],'last_activity')
  if not previous_activity or not requests or not string.match(requests,'^[1-9]%d*$') or #requests>15
    or redis.call('HGET',KEYS[1],'active_streams')~='0'
    or redis.call('HGET',KEYS[1],'rust_preempted')=='1'
    or redis.call('HGET',KEYS[1],'user_id')~=ARGV[11]
    or redis.call('HGET',KEYS[1],'content_uuid')~=data.content_uuid
    or redis.call('HGET',KEYS[1],'m3u_profile_id')~=ARGV[12]
    or user==ARGV[11] or limit<=0 then return {'STOPPED'} end
end
local clients={}
local counts={}
local unique={}
local unique_count=0
local examined=0
local seen_keys={}
local stop_targets={}
local function scan(pattern,typ)
  local cursor='0'
  repeat
    local result=redis.call('SCAN',cursor,'MATCH',pattern,'COUNT',256)
    cursor=result[1]
    for _,key in ipairs(result[2]) do
      if not seen_keys[key] then
      seen_keys[key]=true
      examined=examined+1
      if examined>tonumber(ARGV[5]) then return false end
      if kind(key)=='hash' and redis.call('HGET',key,'user_id')==user and
          (ARGV[9]=='source' or redis.call('HGET',key,'rust_preempted')~='1') then
        local media,client,started,stop
        if typ=='vod' then
          client=string.sub(key,string.len('vod_persistent_connection:')+1)
          media=redis.call('HGET',key,'content_uuid') or client
          started=tonumber(redis.call('HGET',key,'created_at') or '0')
          stop='vod_proxy:client:'..client..':stop'
        else
          local prefix=typ..':channel:'
          local at=string.find(key,':clients:',string.len(prefix)+1,true)
          if at then
            media=string.sub(key,string.len(prefix)+1,at-1)
            client=string.sub(key,at+string.len(':clients:'))
            started=tonumber(redis.call('HGET',key,'connected_at') or '0')
            local stop_media=media
            if typ=='timeshift' then stop_media=redis.call('HGET',key,'programme_vid') or media end
            stop=typ..':channel:'..stop_media..':client:'..client..':stop'
          end
        end
        if media and client and started then
          counts[media]=(counts[media] or 0)+1
          if not unique[media] then unique[media]=true; unique_count=unique_count+1 end
          table.insert(clients,{key=key,media=media,client=client,started=started,typ=typ,stop=stop})
        end
      end
      end
    end
  until cursor=='0'
  return true
end
if limit>0 then
  if not scan('live:channel:*:clients:*','live') or
      not scan('timeshift:channel:*:clients:*','timeshift') or
      not scan('vod_persistent_connection:*','vod') then return {'SCAN_LIMIT'} end
  local same=false
  if policy.ignore_same_channel_connections then
    for _,client in ipairs(clients) do
      if client.typ=='live' and client.media==data.content_uuid then same=true end
    end
  end
  local count=#clients
  if policy.ignore_same_channel_connections then count=unique_count end
  if not same and count>=limit then
    if not policy.terminate_on_limit_exceeded then return {'LIMIT'} end
    table.sort(clients,function(a,b)
      local am=counts[a.media]>1
      local bm=counts[b.media]>1
      local asame=(policy.ignore_same_channel_connections and am) and 1 or 0
      local bsame=(policy.ignore_same_channel_connections and bm) and 1 or 0
      if asame~=bsame then return asame<bsame end
      local asingle=(policy.prioritize_single_client_channels and not am) and 0 or 1
      local bsingle=(policy.prioritize_single_client_channels and not bm) and 0 or 1
      if asingle~=bsingle then return asingle<bsingle end
      if a.started==b.started then return a.key<b.key end
      if policy.terminate_oldest then return a.started<b.started end
      return a.started>b.started
    end)
    local target=clients[1]
    if not target then return {'LIMIT'} end
    for _,client in ipairs(clients) do
      if client.key==target.key or (policy.ignore_same_channel_connections and client.media==target.media) then
        table.insert(stop_targets,client)
      end
    end
  end
end
-- Validate every write target before mutating anything. Redis Lua errors do not
-- roll back earlier writes, so corrupt keys must fail before preemption.
for _,client in ipairs(stop_targets) do
  local stop_kind=kind(client.stop)
  if stop_kind~='none' and stop_kind~='string' then return {'CORRUPT'} end
end
for _,client in ipairs(stop_targets) do
  if ARGV[9]=='source' then
    redis.call('SET',client.stop,'true','PX',60000)
  else
    redis.call('SET',client.stop,'limit','PX',60000)
    redis.call('HSET',client.key,'rust_preempted','1')
  end
end
if ARGV[6]=='create' then
  for key,value in pairs(data) do redis.call('HSET',KEYS[1],key,tostring(value)) end
  redis.call('PEXPIRE',KEYS[1],ttl)
end
if ARGV[10] then
  redis.call('HINCRBY',KEYS[1],'active_streams',1)
  redis.call('HINCRBY',KEYS[1],'request_count',1)
  redis.call('HSET',KEYS[1],'last_activity',ARGV[14],'rust_signed_idle_alias',ARGV[13],
    'rust_signed_idle_alias_previous_activity',previous_activity)
  redis.call('PSETEX',KEYS[5],claim_ttl,'used')
end
return {'OK',tostring(#stop_targets)}
"#;
