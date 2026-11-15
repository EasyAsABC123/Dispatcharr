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
    local channel_owned=alias_channel and channel_receipt_owned and current_owner==alias_token
        and redis.call('GET',alias_channel)==alias_sid
    local stream_owned=alias_stream and stream_receipt_owned and current_owner==alias_token
        and redis.call('GET',alias_stream)==alias_pid
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
    redis.call('HSET', ledger, 'reserved', '0', 'credential', '')
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
redis.call('SET',KEYS[4],ARGV[2]);redis.call('SET',KEYS[5],pid)
redis.call('HSET',KEYS[6],'token',ARGV[1],'scope','live','ledger',KEYS[2],'stream',ARGV[2],'profile',pid)
redis.call('HSET',KEYS[7],'token',ARGV[1],'scope','live','ledger',KEYS[2])
redis.call('HSET',KEYS[2],'alias_channel',KEYS[4],'alias_stream',KEYS[5],
 'alias_channel_receipt',KEYS[6],'alias_stream_receipt',KEYS[7],'alias_sid',ARGV[2],'alias_pid',pid)
return 1
"#;
