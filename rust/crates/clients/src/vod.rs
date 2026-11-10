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
    admit_inner(connection, request, None).await
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
    admit_inner(connection, request, Some(fence)).await
}

async fn admit_inner<C: redis::aio::ConnectionLike + Send + Unpin>(
    connection: &mut C,
    request: Admission<'_>,
    fence: Option<AdmissionFence<'_>>,
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
        format!("{FENCE}{ADMIT}")
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
    }
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
          redis.call('HGET',key,'rust_preempted')~='1' then
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
  redis.call('SET',client.stop,'limit','PX',60000)
  redis.call('HSET',client.key,'rust_preempted','1')
end
if ARGV[6]=='create' then
  for key,value in pairs(data) do redis.call('HSET',KEYS[1],key,tostring(value)) end
  redis.call('PEXPIRE',KEYS[1],ttl)
end
return {'OK',tostring(#stop_targets)}
"#;
