// Redis TIME is authoritative; client wall clocks never decide lease expiry.
pub const REAP: &str = r#"
local clock = redis.call('TIME')
local now = tonumber(clock[1]) * 1000 + math.floor(tonumber(clock[2]) / 1000)
local function counter_value(key)
    local raw = redis.call('GET', key) or '0'
    if raw ~= '0' and not string.match(raw, '^[1-9]%d*$') then
        error('Invalid reservation counter')
    end
    return tonumber(raw)
end
local function clear_provider(ledger)
    local profile = redis.call('HGET', ledger, 'profile')
    local credential = redis.call('HGET', ledger, 'credential')
    local release_key = redis.call('HGET', ledger, 'release_key')
    local reserved = redis.call('HGET', ledger, 'reserved') == '1'
    if not profile or not credential or not release_key then
        error('Invalid reservation ledger')
    end
    -- A borrowed Source slot is claimed through the shared metadata token,
    -- never treated as a second private allocation. An external Source parent
    -- may already have consumed it while this raw owner still serves media.
    local borrowed=redis.call('HGET',ledger,'source_slot_shared')=='1' or redis.call('HGET',ledger,'source_borrowed')=='1'
    local metadata=redis.call('HGET',ledger,'source_metadata')
    local claim_metadata=false
    if borrowed then
        local token=redis.call('HGET',ledger,'token')
        local meta_kind=metadata and redis.call('TYPE',metadata).ok or 'none'
        local meta_profile=metadata and redis.call('HGET',metadata,'m3u_profile') or false
        local expected=string.match(profile,'^profile_connections:(%d+)$')
        local meta_owner=metadata and redis.call('HGET',metadata,'owner') or false
        local alias=redis.call('HGET',ledger,'alias_channel')
        local stream=redis.call('HGET',ledger,'alias_sid')
        local preview=redis.call('HGET',ledger,'preview_key')
        local preview_profile=redis.call('HGET',ledger,'preview_profile')
        local alias_live=(alias and stream and redis.call('GET',alias)==stream) or (preview and preview_profile and redis.call('GET',preview)==preview_profile)
        claim_metadata=meta_profile==expected and (not meta_owner or meta_owner==token)
        reserved=reserved and (claim_metadata or (not meta_profile and alias_live))
    end
    -- Read and validate every possibly failing value before mutation: Lua
    -- errors do not roll back writes already performed by the script.
    local count = reserved and counter_value(profile) or 0
    local credential_count = reserved and credential ~= '' and counter_value(credential) or 0
    local remembered = reserved and credential ~= '' and redis.call('GET', release_key) or false
    local alias_channel=redis.call('HGET',ledger,'alias_channel')
    local alias_stream=redis.call('HGET',ledger,'alias_stream')
    local channel_receipt=redis.call('HGET',ledger,'alias_channel_receipt')
    local stream_receipt=redis.call('HGET',ledger,'alias_stream_receipt')
    local alias_sid=redis.call('HGET',ledger,'alias_sid')
    local alias_pid=redis.call('HGET',ledger,'alias_pid')
    local alias_token=redis.call('HGET',ledger,'token')
    local current_owner=redis.call('GET',redis.call('HGET',ledger,'owner'))
    local channel_receipt_owned=channel_receipt and redis.call('HGET',channel_receipt,'token')==alias_token
    local stream_receipt_owned=stream_receipt and redis.call('HGET',stream_receipt,'token')==alias_token
    local channel_owned=alias_channel and channel_receipt_owned and (not current_owner or current_owner==alias_token)
        and redis.call('GET',alias_channel)==alias_sid
    local stream_owned=alias_stream and stream_receipt_owned and (not current_owner or current_owner==alias_token)
        and redis.call('GET',alias_stream)==alias_pid
    local preview=redis.call('HGET',ledger,'preview_key')
    local preview_profile=redis.call('HGET',ledger,'preview_profile')
    if preview and (not current_owner or current_owner==alias_token) and redis.call('GET',preview)==preview_profile then redis.call('DEL',preview) end
    redis.call('HDEL',ledger,'preview_key','preview_profile')
    if borrowed and metadata and claim_metadata then redis.call('HDEL',metadata,'m3u_profile','stream_id') end
    if channel_owned then redis.call('DEL',alias_channel) end
    if stream_owned then redis.call('DEL',alias_stream) end
    if channel_receipt_owned then redis.call('DEL',channel_receipt) end
    if stream_receipt_owned then redis.call('DEL',stream_receipt) end
    redis.call('HDEL',ledger,'alias_channel','alias_stream','alias_channel_receipt','alias_stream_receipt','alias_sid','alias_pid')
    if reserved then
        if count > 0 then redis.call('DECR', profile); count = count - 1 end
        if credential ~= '' then
            if credential_count > 0 then redis.call('DECR', credential) end
            if count <= 0 and remembered == credential then
                redis.call('DEL', release_key)
            end
        end
    end
    redis.call('HSET', ledger, 'reserved', '0', 'credential', '', 'source_borrowed','0','source_parent_token','','source_slot_shared','0','source_metadata','')
    return reserved and 1 or 0
end
local function release_record(ledger, index)
    local token = redis.call('HGET', ledger, 'token')
    if not token then redis.call('ZREM', index, ledger); return 0 end
    local owner = redis.call('HGET', ledger, 'owner')
    if not owner then error('Invalid reservation ledger') end
    local current_owner = redis.call('GET', owner)
    clear_provider(ledger)
    if current_owner == token then redis.call('DEL', owner) end
    redis.call('DEL', ledger)
    redis.call('ZREM', index, ledger)
    return 1
end
local function reap(index, limit)
    local records = redis.call('ZRANGEBYSCORE', index, '-inf', now, 'LIMIT', 0, limit)
    local released = 0
    for _, ledger in ipairs(records) do released = released + release_record(ledger, index) end
    return released
end
"#;

pub const ACQUIRE: &str = r#"
reap(KEYS[3], tonumber(ARGV[5]))
if redis.call('EXISTS', KEYS[1]) == 1 then return {-1, 0} end
local maximum = tonumber(ARGV[3])
local credential_maximum = tonumber(ARGV[4])
local count = counter_value(KEYS[4])
if maximum > 0 and count >= maximum then return {-2, 0} end
if maximum > 0 and credential_maximum > 0 then
    local credential_count = counter_value(KEYS[5])
    if credential_count >= credential_maximum then return {-3, 0} end
    local remembered = redis.call('GET', KEYS[6])
    if count > 0 and remembered and remembered ~= KEYS[5] then return {-4, 0} end
end
local fence = redis.call('INCR', KEYS[7])
redis.call('SET', KEYS[1], ARGV[1], 'PX', ARGV[2])
local credential = ''
if maximum > 0 then
    redis.call('INCR', KEYS[4])
    if credential_maximum > 0 then
        credential = KEYS[5]
        redis.call('INCR', credential)
        redis.call('SET', KEYS[6], credential)
    end
end
redis.call('HSET', KEYS[2], 'token', ARGV[1], 'owner', KEYS[1],
    'profile', KEYS[4], 'credential', credential, 'release_key', KEYS[6],
    'reserved', maximum > 0 and '1' or '0', 'fence', fence)
redis.call('ZADD', KEYS[3], now + tonumber(ARGV[2]), KEYS[2])
return {1, fence}
"#;

pub const HEARTBEAT: &str = r#"
if redis.call('GET', KEYS[1]) ~= ARGV[1]
    or redis.call('HGET', KEYS[2], 'token') ~= ARGV[1] then return 0 end
redis.call('PEXPIRE', KEYS[1], ARGV[2])
redis.call('ZADD', KEYS[3], now + tonumber(ARGV[2]), KEYS[2])
return 1
"#;

pub const RELEASE: &str = r#"
if redis.call('HGET', KEYS[2], 'token') ~= ARGV[1] then return 0 end
return release_record(KEYS[2], KEYS[3])
"#;

pub const REAP_ONLY: &str = "return reap(KEYS[1], tonumber(ARGV[1]))";

pub const CLEAR_PROVIDER: &str = r#"
if redis.call('GET', KEYS[1]) ~= ARGV[1]
    or redis.call('HGET', KEYS[2], 'token') ~= ARGV[1] then return -1 end
return clear_provider(KEYS[2])
"#;

pub const RESERVE_PROVIDER: &str = r#"
if redis.call('GET', KEYS[1]) ~= ARGV[1]
    or redis.call('HGET', KEYS[2], 'token') ~= ARGV[1] then return -1 end
if redis.call('HGET', KEYS[2], 'reserved') == '1' then return -5 end
if ARGV[4]=='1' and redis.call('EXISTS',KEYS[7],KEYS[8],KEYS[9],KEYS[10])~=0 then return -6 end
local maximum = tonumber(ARGV[2])
local credential_maximum = tonumber(ARGV[3])
local count = counter_value(KEYS[4])
if maximum > 0 and count >= maximum then return -2 end
if maximum > 0 and credential_maximum > 0 then
    local credential_count = counter_value(KEYS[5])
    if credential_count >= credential_maximum then return -3 end
    local remembered = redis.call('GET', KEYS[6])
    if count > 0 and remembered and remembered ~= KEYS[5] then return -4 end
end
local credential = ''
if maximum > 0 then
    redis.call('INCR', KEYS[4])
    if credential_maximum > 0 then
        credential = KEYS[5]
        redis.call('INCR', credential)
        redis.call('SET', KEYS[6], credential)
    end
end
redis.call('HSET', KEYS[2], 'profile', KEYS[4], 'credential', credential,
    'release_key', KEYS[6], 'reserved', maximum > 0 and '1' or '0')
if ARGV[4]=='1' then
 redis.call('HSET',KEYS[2],'recovery_channel_alias',KEYS[7],'recovery_stream_alias',KEYS[8])
end
return 1
"#;

/// Publish aliases ONLY under the actual existing raw owner and its current
/// provider reservation. Never increments a profile/credential counter.
pub const SOURCE_ASSIGNMENT: &str = r#"
if redis.call('GET',KEYS[1])~=ARGV[1] or redis.call('HGET',KEYS[2],'token')~=ARGV[1] then return -1 end
local profile=redis.call('HGET',KEYS[2],'profile') or ''
local pid=string.match(profile,'^profile_connections:(%d+)$')
if not pid or tonumber(pid)<=0 then return -1 end
for i=4,5 do local kind=redis.call('TYPE',KEYS[i]).ok;if kind~='none' and kind~='string' then return redis.error_reply('Invalid source alias type') end end
for i=6,7 do local kind=redis.call('TYPE',KEYS[i]).ok;if kind~='none' and kind~='hash' then return redis.error_reply('Invalid source alias receipt') end end
if ARGV[4]=='1' then
 if redis.call('HGET',KEYS[2],'recovery_channel_alias')~=KEYS[4]
    or redis.call('HGET',KEYS[2],'recovery_stream_alias')~=KEYS[5] then return -1 end
 if redis.call('EXISTS',KEYS[4],KEYS[5],KEYS[6],KEYS[7])~=0 then return -2 end
end
local borrowed_check=redis.call('HGET',KEYS[2],'source_borrowed')=='1'
if borrowed_check and (redis.call('GET',KEYS[4])~=ARGV[2] or redis.call('GET',KEYS[5])~=pid) then return -1 end
local ttl=tonumber(ARGV[3] or '0')
local metadata=string.gsub(KEYS[1],':owner$',':metadata')
if ttl>0 then
 local kind=redis.call('TYPE',metadata).ok;if kind~='none' and kind~='hash' then return redis.error_reply('Invalid source slot metadata') end
end
redis.call('SET',KEYS[4],ARGV[2]);redis.call('SET',KEYS[5],pid)
if ttl>0 then
 redis.call('HSET',metadata,'m3u_profile',pid,'stream_id',ARGV[2],'owner',ARGV[1],'rust_token',ARGV[1],'state','initializing')
 redis.call('PEXPIRE',metadata,ttl)
 redis.call('HSET',KEYS[2],'source_metadata',metadata,'source_slot_shared','1')
end
local borrowed=redis.call('HGET',KEYS[2],'source_borrowed')=='1'
local parent=borrowed and redis.call('HGET',KEYS[2],'source_parent_token') or ''
redis.call('HSET',KEYS[6],'token',ARGV[1],'scope',borrowed and 'live_borrowed' or 'live','ledger',KEYS[2],'stream',ARGV[2],'profile',pid,
 'borrowed_from',parent or '', 'reserved',redis.call('HGET',KEYS[2],'reserved') or '0')
redis.call('HSET',KEYS[7],'token',ARGV[1],'scope',borrowed and 'live_borrowed' or 'live','ledger',KEYS[2])
redis.call('HSET',KEYS[2],'alias_channel',KEYS[4],'alias_stream',KEYS[5],
 'alias_channel_receipt',KEYS[6],'alias_stream_receipt',KEYS[7],'alias_sid',ARGV[2],'alias_pid',pid)
if ARGV[4]=='1' then redis.call('HDEL',KEYS[2],'recovery_channel_alias','recovery_stream_alias') end
return 1
"#;

/// Switch under the existing fenced alias. A cross-profile command on two
/// verified unlimited direct profiles atomically moves count and ownership.
pub const SWITCH_ACTIVE_SOURCE: &str = r#"
local owner,ledger=KEYS[1],KEYS[2]
local token,stream,pid,nextpid=ARGV[1],ARGV[2],ARGV[3],ARGV[4]
local same_unlimited_profile=ARGV[5]=='1'
if redis.call('GET',owner)~=token or redis.call('HGET',ledger,'token')~=token then return 0 end
local reserved=redis.call('HGET',ledger,'reserved')
if (reserved~='1' and (reserved~='0' or (nextpid==pid and not same_unlimited_profile)))
    or (same_unlimited_profile and (pid~=nextpid or redis.call('HGET',ledger,'credential')~=''))
    or redis.call('HGET',ledger,'source_slot_shared')~='1'
    or redis.call('HGET',ledger,'source_borrowed')=='1'
    or redis.call('HGET',ledger,'profile')~='profile_connections:'..pid then return 0 end
local metadata=redis.call('HGET',ledger,'source_metadata')
if not metadata or metadata~=string.gsub(owner,':owner$',':metadata')
    or redis.call('HGET',metadata,'owner')~=token or redis.call('HGET',metadata,'rust_token')~=token
    or redis.call('HGET',metadata,'m3u_profile')~=pid then return 0 end
local state=redis.call('HGET',metadata,'state')
if state~='active' and state~='buffering' and state~='waiting_for_clients' then return 0 end
local channel_alias,stream_alias=unpack(redis.call('HMGET',ledger,'alias_channel','alias_stream'))
local channel_receipt,stream_receipt=unpack(redis.call('HMGET',ledger,'alias_channel_receipt','alias_stream_receipt'))
local alias_sid,alias_pid=unpack(redis.call('HMGET',ledger,'alias_sid','alias_pid'))
if not channel_alias or not stream_alias or not channel_receipt or not stream_receipt
    or redis.call('GET',channel_alias)~=alias_sid or redis.call('GET',stream_alias)~=pid or alias_pid~=pid
    or redis.call('HGET',channel_receipt,'token')~=token or redis.call('HGET',stream_receipt,'token')~=token
    or redis.call('HGET',channel_receipt,'scope')~='live' or redis.call('HGET',stream_receipt,'scope')~='live'
    or redis.call('HGET',channel_receipt,'ledger')~=ledger or redis.call('HGET',stream_receipt,'ledger')~=ledger
    or redis.call('HGET',channel_receipt,'stream')~=alias_sid or redis.call('HGET',channel_receipt,'profile')~=pid then return 0 end
local previous_key='profile_connections:'..pid
local previous_count
if reserved=='1' or nextpid~=pid then previous_count=counter_value(previous_key) end
if reserved=='1' and previous_count<1 then return 0 end
if nextpid~=pid then
    -- A fresh unlimited Native reservation carries no counter. Refuse to
    -- debit another owner's count if this ledger has never owned one.
    if reserved=='0' and previous_count~=0 then return 0 end
    if redis.call('HGET',ledger,'credential')~='' then return 0 end
    local previous_release='profile_credential_release:'..pid
    local next_release='profile_credential_release:'..nextpid
    if redis.call('HGET',ledger,'release_key')~=previous_release
        or redis.call('EXISTS',previous_release,next_release)~=0 then return 0 end
    local next_key='profile_connections:'..nextpid
    -- Validate the destination before any writes. Redis scripts do not undo
    -- an already applied write if a later command fails.
    if counter_value(next_key)>=9007199254740991 then return 0 end
    if reserved=='1' then redis.call('DECR',previous_key) end
    redis.call('INCR',next_key)
    redis.call('SET',stream_alias,nextpid)
    redis.call('HSET',channel_receipt,'profile',nextpid,'reserved','1')
    redis.call('HSET',ledger,'profile',next_key,'alias_pid',nextpid,'release_key',next_release,'reserved','1')
    redis.call('HSET',metadata,'m3u_profile',nextpid)
end
redis.call('HSET',metadata,'stream_id',stream)
return 1
"#;

/// Adopt the real public allocation without incrementing profile/credential
/// counters. A sealed observation is rechecked under this actual raw owner.
pub const ADOPT_SOURCE_ASSIGNMENT: &str = r#"
if redis.call('GET',KEYS[1])~=ARGV[1] or redis.call('HGET',KEYS[2],'token')~=ARGV[1] then return 0 end
if redis.call('HGET',KEYS[2],'reserved')=='1' then return -1 end
if redis.call('GET',KEYS[4])~=ARGV[2] or redis.call('GET',KEYS[5])~=ARGV[3] then return 0 end
local metadata=string.gsub(KEYS[1],':owner$',':metadata')
local kind=redis.call('TYPE',metadata).ok
if kind~='none' and kind~='hash' then return redis.error_reply('Invalid source assignment metadata') end
local state=redis.call('HGET',metadata,'state')
if kind~='none' and not (state=='active' or state=='waiting_for_clients' or state=='buffering' or state=='initializing' or state=='connecting') then return 0 end
local receipt_kind=redis.call('TYPE',KEYS[6]).ok
if receipt_kind~='none' and receipt_kind~='hash' then return redis.error_reply('Invalid source assignment receipt') end
local scope=redis.call('HGET',KEYS[6],'scope')
-- Adoption from an expired native raw owner needs a separate owner-loss
-- characterization; do not relabel its private allocation as a fresh slot.
if scope=='live' or scope=='live_borrowed' then return -2 end
local parent=scope=='redirect' and redis.call('HGET',KEYS[6],'token') or ''
local profile='profile_connections:'..ARGV[3]
local count=counter_value(profile)
local release_key='profile_credential_release:'..ARGV[3]
local group=redis.call('GET',release_key) or ''
if string.len(group)>256 or (group~='' and string.sub(group,1,25)~='server_group_connections:') then return redis.error_reply('Invalid source assignment credential') end
if group~='' then counter_value(group) end
redis.call('HSET',KEYS[2],'profile',profile,'credential',group,'release_key',release_key,
 'reserved',count>0 and '1' or '0','source_borrowed','1','source_metadata',metadata,'source_parent_token',parent or '')
if parent and parent~='' then redis.call('HSET',KEYS[6],'borrower_ledger',KEYS[2],'borrower_token',ARGV[1]) end
return 1
"#;

/// KEYS use the existing actual owner/ledger/index and one preview alias only.
/// Prepare may retire Source-defined inactive metadata; publish never reserves.
pub const PREVIEW_ASSIGNMENT: &str = r#"
if redis.call('GET',KEYS[1])~=ARGV[1] or redis.call('HGET',KEYS[2],'token')~=ARGV[1] then return -1 end
local pid=ARGV[2];local ttl=tonumber(ARGV[3]);local metadata=string.gsub(KEYS[1],':owner$',':metadata')
local kind=redis.call('TYPE',metadata).ok;if kind~='none' and kind~='hash' then return redis.error_reply('Invalid preview metadata type') end
local assigned=redis.call('GET',KEYS[4])
local profile='profile_connections:'..pid
if ttl==0 then
 if redis.call('HGET',KEYS[2],'reserved')=='1' then return -2 end
 if not assigned then return 0 end
 local state=redis.call('HGET',metadata,'state')
 local reusable=kind=='none' or state=='active' or state=='waiting_for_clients' or state=='buffering' or state=='initializing' or state=='connecting'
 if reusable then
  if assigned~=pid then return -2 end
  local count=counter_value(profile);local release='profile_credential_release:'..pid;local group=redis.call('GET',release) or ''
  if string.len(group)>256 or (group~='' and string.sub(group,1,25)~='server_group_connections:') then return redis.error_reply('Invalid preview credential') end
  if group~='' then counter_value(group) end
  redis.call('HSET',KEYS[2],'profile',profile,'credential',group,'release_key',release,'reserved',count>0 and '1' or '0','source_borrowed','1','source_metadata',metadata,'preview_key',KEYS[4],'preview_profile',pid)
  return 1
 end
 -- Only actual existing inactive metadata makes a preview assignment stale.
 local old=tonumber(assigned);if not old or old<=0 or old%1~=0 then return redis.error_reply('Invalid preview profile') end
 local key='profile_connections:'..assigned;local release='profile_credential_release:'..assigned;local group=redis.call('GET',release) or ''
 if string.len(group)>256 or (group~='' and string.sub(group,1,25)~='server_group_connections:') then return redis.error_reply('Invalid preview credential') end
 local count=counter_value(key);local shared=group~='' and counter_value(group) or 0
 redis.call('DEL',KEYS[4]);redis.call('HDEL',metadata,'m3u_profile','stream_id')
 if count>0 then redis.call('DECR',key);count=count-1 end
 if shared>0 then redis.call('DECR',group) end
 if count<=0 and group~='' then redis.call('DEL',release) end
 return 0
end
if redis.call('HGET',KEYS[2],'profile')~=profile then return -1 end
if redis.call('HGET',KEYS[2],'source_borrowed')=='1' and assigned~=pid then return -1 end
if assigned and assigned~=pid then return -2 end
redis.call('SET',KEYS[4],pid)
redis.call('HSET',metadata,'m3u_profile',pid,'stream_id',ARGV[4],'owner',ARGV[1],'rust_token',ARGV[1],'state','initializing')
redis.call('PEXPIRE',metadata,ttl)
redis.call('HSET',KEYS[2],'source_metadata',metadata,'source_slot_shared','1','preview_key',KEYS[4],'preview_profile',pid)
return 1
"#;
