//! Timeshift-scoped atomic admission derived from apps/proxy/utils.py and
//! apps/timeshift/views.py. Original application credit: upstream Dispatcharr.
//! This is a separate scope; existing live/VOD REGISTER scripts are unchanged.
use crate::Error;
use dispatcharr_contracts::UserLimitPolicy;
use serde_json::json;
use std::{collections::BTreeMap, time::Duration};

#[derive(Clone, Copy)]
pub enum AdmissionMode {
    Create,
    Check,
}
pub struct Admission<'a> {
    pub mode: AdmissionMode,
    pub session_id: &'a str,
    pub channel_id: i64,
    pub programme_media_id: &'a str,
    pub fields: &'a BTreeMap<String, String>,
    pub stream_limit: u32,
    pub policy: &'a UserLimitPolicy,
    pub owner_token: &'a str,
    pub now: &'a str,
    pub ttl: Duration,
    pub scan_limit: usize,
}
/// Feed these owned bytes to the existing supervised Redis transport; retain a
/// token-fenced rollback guard through the actual reply, including cancellation.
pub struct PreparedAdmission {
    pub keys: Vec<String>,
    pub args: Vec<Vec<u8>>,
}
pub fn prepare(request: Admission<'_>) -> Result<PreparedAdmission, Error> {
    let sid = request.session_id;
    if sid.is_empty()
        || sid.len() > 128
        || !sid
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'-')
        || request.channel_id <= 0
        || !request
            .programme_media_id
            .starts_with(&format!("{}_", request.channel_id))
        || request.owner_token.is_empty()
        || request.owner_token.len() > 128
        || request.ttl.is_zero()
        || request.ttl > Duration::from_secs(600)
        || request.scan_limit == 0
        || request.scan_limit > 1_000_000
        || request
            .fields
            .get("user_id")
            .and_then(|value| value.parse::<i64>().ok())
            .is_none_or(|value| value <= 0)
        || request
            .now
            .parse::<f64>()
            .ok()
            .is_none_or(|value| !value.is_finite() || value < 0.0)
    {
        return Err(Error::InvalidClient);
    }
    let mut fields = request.fields.clone();
    fields.insert("rust_owner_token".into(), request.owner_token.into());
    fields.insert("connected_at".into(), request.now.into());
    let policy = json!({"terminate_on_limit_exceeded":request.policy.terminate_on_limit_exceeded,"prioritize_single_client_channels":request.policy.prioritize_single_client_channels,"ignore_same_channel_connections":request.policy.ignore_same_channel_connections,"terminate_oldest":request.policy.terminate_oldest});
    let strings = [
        serde_json::to_string(&fields).map_err(|_| Error::InvalidClient)?,
        policy.to_string(),
        request.stream_limit.to_string(),
        request.ttl.as_millis().to_string(),
        request.scan_limit.to_string(),
        sid.into(),
        request.programme_media_id.into(),
        request.channel_id.to_string(),
        match request.mode {
            AdmissionMode::Create => "create".into(),
            AdmissionMode::Check => "check".into(),
        },
    ];
    Ok(PreparedAdmission {
        keys: vec![format!(
            "timeshift:channel:{}_{sid}:clients:{sid}",
            request.channel_id
        )],
        args: strings.into_iter().map(String::into_bytes).collect(),
    })
}
pub fn decode(result: Vec<String>) -> Result<bool, Error> {
    match result.first().map(String::as_str) {
        Some("OK") => Ok(true),
        Some("EXISTS") => Ok(false),
        Some("LIMIT") => Err(Error::UserLimit),
        Some("SCAN_LIMIT") => Err(Error::ScanLimit),
        Some("CORRUPT") => Err(Error::CorruptMetadata),
        _ => Err(Error::RedisUnavailable),
    }
}

pub const SCRIPT: &str = r#"
local function kind(key)
 local result=redis.call('TYPE',key);if type(result)=='table' then return result.ok end return result
end
local data=cjson.decode(ARGV[1]);
if ARGV[9]=='create' then if kind(KEYS[1])~='none' then return {'EXISTS'} end
elseif kind(KEYS[1])~='hash' or redis.call('HGET',KEYS[1],'rust_owner_token')~=tostring(data.rust_owner_token) or redis.call('HGET',KEYS[1],'user_id')~=tostring(data.user_id) then return {'CORRUPT'} end
local policy=cjson.decode(ARGV[2]);local limit=tonumber(ARGV[3]);local maximum=tonumber(ARGV[5]);local sid=ARGV[6];local media_id=ARGV[7];local channel=ARGV[8];local user=tostring(data.user_id)
local examined=0;local clients={};local seen={};local hops={};local active={};local counts={};local unique={};local unique_count=0;local stops={}
local function scan(pattern,typ)
 local cursor='0'
 repeat
  examined=examined+1;if examined>maximum then return false end
  local result=redis.call('SCAN',cursor,'MATCH',pattern,'COUNT',256);cursor=result[1]
  for _,key in ipairs(result[2]) do
   if not seen[key] then
    seen[key]=true;examined=examined+1;if examined>maximum then return false end
    if kind(key)=='hash' and redis.call('HGET',key,'user_id')==user and redis.call('HGET',key,'rust_preempted')~='1' then
     local media,client,started,stop
     if typ=='vod' then
      client=string.sub(key,string.len('vod_persistent_connection:')+1);media=redis.call('HGET',key,'content_uuid') or client;started=tonumber(redis.call('HGET',key,'created_at') or '0');stop='vod_proxy:client:'..client..':stop'
     else
      local prefix=typ..':channel:';local at=string.find(key,':clients:',string.len(prefix)+1,true)
      if at then
       media=string.sub(key,string.len(prefix)+1,at-1);client=string.sub(key,at+string.len(':clients:'));started=tonumber(redis.call('HGET',key,'connected_at') or '0')
       local stop_media=media;if typ=='timeshift' then stop_media=redis.call('HGET',key,'programme_vid') or media end
       stop=typ..':channel:'..stop_media..':client:'..client..':stop'
      end
     end
     if media and client and started then table.insert(clients,{key=key,media=media,client=client,started=started,typ=typ,stop=stop}) end
    end
   end
  end
 until cursor=='0'
 return true
end
-- The source always terminates prior positions before checking caps, even when
-- the user is unlimited. Preserve its stats-id/programme-id comparison literally.
if not scan('timeshift:channel:*:clients:*','timeshift') then return {'SCAN_LIMIT'} end
if limit>0 and (not scan('live:channel:*:clients:*','live') or not scan('vod_persistent_connection:*','vod')) then return {'SCAN_LIMIT'} end
for _,client in ipairs(clients) do
 local prefix=channel..'_'
 local hop=client.typ=='timeshift' and client.client~=sid and string.sub(client.media,1,string.len(prefix))==prefix and client.media~=media_id and string.sub(client.media,1,string.len(media_id)+1)~=media_id..'_'
 if hop then
  client.reason='hop';table.insert(hops,client)
 else
  table.insert(active,client);counts[client.media]=(counts[client.media] or 0)+1
  if not unique[client.media] then unique[client.media]=true;unique_count=unique_count+1 end
 end
end
local same=false
for _,client in ipairs(active) do
 if policy.ignore_same_channel_connections and client.typ=='live' and client.media==media_id then same=true end
 if client.typ=='timeshift' and client.client==sid then
  if client.media==media_id or string.sub(client.media,1,string.len(media_id)+1)==media_id..'_' or string.match(client.media,'^(%d+)_')==channel then same=true end
 end
end
local count=#active;if policy.ignore_same_channel_connections then count=unique_count end
local allowed=true;local victims={}
if limit>0 and not same and count>=limit then
 if not policy.terminate_on_limit_exceeded then allowed=false
 else
  table.sort(active,function(a,b)
   local am=counts[a.media]>1;local bm=counts[b.media]>1
   local asame=(policy.ignore_same_channel_connections and am) and 1 or 0;local bsame=(policy.ignore_same_channel_connections and bm) and 1 or 0
   if asame~=bsame then return asame<bsame end
   local asingle=(policy.prioritize_single_client_channels and not am) and 0 or 1;local bsingle=(policy.prioritize_single_client_channels and not bm) and 0 or 1
   if asingle~=bsingle then return asingle<bsingle end
   if a.started==b.started then return a.key<b.key end
   if policy.terminate_oldest then return a.started<b.started end return a.started>b.started
  end)
  local target=active[1];if not target then allowed=false else
   for _,client in ipairs(active) do if client.key==target.key or (policy.ignore_same_channel_connections and client.media==target.media) then client.reason='limit';table.insert(victims,client) end end
  end
 end
end
local function stop_targets(client)
 local output={client.stop}
 if client.typ=='timeshift' and client.reason=='limit' then
  local cursor='0';local pattern='timeshift:stream_gen:*:'..client.client;local prefix='timeshift:stream_gen:';local suffix=':'..client.client
  repeat
   examined=examined+1;if examined>maximum then return nil end
   local result=redis.call('SCAN',cursor,'MATCH',pattern,'COUNT',128);cursor=result[1]
   for _,key in ipairs(result[2]) do
    examined=examined+1;if examined>maximum then return nil end
    local generation_media=string.sub(key,string.len(prefix)+1,-string.len(suffix)-1)
    table.insert(output,'timeshift:channel:'..generation_media..':client:'..client.client..':stop')
   end
  until cursor=='0'
 end
 return output
end
for _,group in ipairs({hops,victims}) do
 for _,client in ipairs(group) do
  local targets=stop_targets(client);if not targets then return {'SCAN_LIMIT'} end
  for _,key in ipairs(targets) do local typ=kind(key);if typ~='none' and typ~='string' then return {'CORRUPT'} end;local value=client.reason;local ttl=60000;if client.reason=='limit' and client.typ~='timeshift' then value='true';if client.typ=='live' then ttl=30000 end end;stops[key]={value=value,ttl=ttl} end
  if client.reason=='hop' then
   client.set='timeshift:channel:'..client.media..':clients';client.metadata='timeshift:channel:'..client.media..':metadata'
   local typ=kind(client.set);if typ~='none' and typ~='set' then return {'CORRUPT'} end
   typ=kind(client.metadata);if typ~='none' and typ~='hash' then return {'CORRUPT'} end
  end
 end
end
-- Hops remain observable even if another channel/VOD/live makes the later cap
-- check fail. Validate every write first because Redis Lua does not roll back.
for _,client in ipairs(hops) do
 redis.call('SREM',client.set,client.client);redis.call('DEL',client.key)
 if redis.call('SCARD',client.set)==0 then redis.call('DEL',client.set,client.metadata) end
end
for key,stop in pairs(stops) do redis.call('SET',key,stop.value,'PX',stop.ttl) end
for _,client in ipairs(victims) do redis.call('HSET',client.key,'rust_preempted','1') end
if not allowed then return {'LIMIT'} end
if ARGV[9]=='create' then
 for key,value in pairs(data) do redis.call('HSET',KEYS[1],key,tostring(value)) end
 redis.call('PEXPIRE',KEYS[1],ARGV[4])
end
return {'OK',tostring(#hops),tostring(#victims)}
"#;
