// Ported live client/user-limit semantics from apps/proxy/utils.py and
// live_proxy/client_manager.py. Original project credit: ATTRIBUTION.md.
pub const REGISTER: &str = r#"
local function kind(key)
  local result=redis.call('TYPE',key)
  if type(result)=='table' then return result.ok end
  return result
end
if kind(KEYS[1])~='none' or (kind(KEYS[2])~='none' and kind(KEYS[2])~='set') then
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
      if client.typ=='live' and client.media==data.channel_id then same=true end
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
for key,value in pairs(data) do redis.call('HSET',KEYS[1],key,tostring(value)) end
redis.call('PEXPIRE',KEYS[1],ttl)
redis.call('SADD',KEYS[2],ARGV[6])
redis.call('PEXPIRE',KEYS[2],ttl)
return {'OK',tostring(#stop_targets)}
"#;

pub const HEARTBEAT: &str = r#"
if redis.call('HGET',KEYS[1],'rust_token')~=ARGV[1] then return 0 end
if redis.call('EXISTS',KEYS[3])==1 or redis.call('HGET',KEYS[1],'rust_preempted')=='1' then return -1 end
local old=redis.call('HGET',KEYS[1],'last_active')
if old and tonumber(ARGV[2])-tonumber(old)>tonumber(ARGV[7]) then return -1 end
if ARGV[10]~='hls' then
  redis.call('HSET',KEYS[1],'bytes_sent',ARGV[3],'current_rate_KBps',ARGV[4],
    'chunks_sent',ARGV[8],'avg_rate_KBps',ARGV[9],'stats_updated_at',ARGV[2])
end
if ARGV[5]=='1' then redis.call('HSET',KEYS[1],'last_active',ARGV[2]) end
redis.call('PEXPIRE',KEYS[1],ARGV[6])
redis.call('PEXPIRE',KEYS[2],ARGV[6])
return 1
"#;

pub const RELEASE: &str = r#"
if redis.call('HGET',KEYS[1],'rust_token')~=ARGV[1] then return 0 end
local token=redis.call('HGET',KEYS[1],'hls_token')
if token then redis.call('DEL','live:hls:session:'..token) end
redis.call('DEL',KEYS[1])
redis.call('SREM',KEYS[2],ARGV[2])
redis.call('DEL',KEYS[3])
return 1
"#;
