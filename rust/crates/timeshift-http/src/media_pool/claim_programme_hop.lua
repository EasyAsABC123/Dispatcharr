-- Atomically install the successor of a physically drained local archive.
-- The caller retains and transfers the existing provider lease. This script
-- verifies its owner and only writes archive metadata.

local function kind(key)
    local value = redis.call('TYPE', key)
    return type(value) == 'table' and value.ok or value
end

local function bounded(value, limit, allow_empty)
    return type(value) == 'string' and #value <= limit and (allow_empty or #value > 0)
end

local function positive_integer(value, max_digits)
    return bounded(value, max_digits) and string.match(value, '^[1-9][0-9]*$') ~= nil
end

local function uuid_token(value)
    return bounded(value, 36) and #value == 36
        and string.match(value, '^[0-9a-f]+%-[0-9a-f]+%-[0-9a-f]+%-[0-9a-f]+%-[0-9a-f]+$') ~= nil
end

if #KEYS ~= 10 or #ARGV ~= 20 then return 0 end
local old_token, new_token, lease_token, generation = ARGV[1], ARGV[2], ARGV[3], ARGV[4]
local old_vid, new_vid, sid, old_media, new_media = ARGV[5], ARGV[6], ARGV[7], ARGV[8], ARGV[9]
local provider_time, timezone, duration, now = ARGV[10], ARGV[11], ARGV[12], ARGV[13]
local user, account, profile, dispatcharr_stream, stream = ARGV[14], ARGV[15], ARGV[16], ARGV[17], ARGV[18]
local ip, agent = ARGV[19], ARGV[20]

if not uuid_token(old_token) or not uuid_token(new_token) or old_token == new_token
    or not bounded(lease_token, 128) or not positive_integer(generation, 19)
    or not bounded(sid, 128) or not string.match(sid, '^[a-zA-Z0-9_%-]+$')
    or not bounded(old_vid, 512) or not bounded(new_vid, 512)
    or string.find(old_vid, ':', 1, true) or string.find(new_vid, ':', 1, true)
    or not bounded(old_media, 512) or not bounded(new_media, 512) or old_media == new_media
    or not bounded(stream, 512) or old_vid ~= old_media .. '_' .. stream or new_vid ~= new_media .. '_' .. stream
    or not bounded(provider_time, 128) or not bounded(timezone, 128, true)
    or not positive_integer(duration, 19) or not bounded(now, 32) or not string.match(now, '^[0-9]+%.?[0-9]*$')
    or not positive_integer(user, 19) or not positive_integer(account, 19)
    or not positive_integer(profile, 19) or not positive_integer(dispatcharr_stream, 19)
    or not bounded(ip, 128, true) or not bounded(agent, 8192, true) then return 0 end

local seen = {}
for _, key in ipairs(KEYS) do
    if seen[key] then return 0 end
    seen[key] = true
end

local pool, client, members, stats, old_gen, old_stop, next_gen, next_stop, lease, grace = unpack(KEYS)
local channel = string.match(old_media, '^([1-9][0-9]*)_')
if not channel or string.match(new_media, '^([1-9][0-9]*)_') ~= channel then return 0 end
local stats_id = channel .. '_' .. sid
local stats_prefix = 'timeshift:channel:' .. stats_id
local lease_suffix = ':timeshift:session:' .. sid .. ':owner'
if client ~= stats_prefix .. ':clients:' .. sid or members ~= stats_prefix .. ':clients'
    or stats ~= stats_prefix .. ':metadata' or grace ~= 'timeshift:grace:' .. stats_id .. ':' .. sid
    or #lease <= #lease_suffix or string.sub(lease, -#lease_suffix) ~= lease_suffix
    or old_gen ~= 'timeshift:stream_gen:' .. old_vid .. ':' .. sid
    or old_stop ~= 'timeshift:channel:' .. old_vid .. ':client:' .. sid .. ':stop'
    or next_gen ~= 'timeshift:stream_gen:' .. new_vid .. ':' .. sid
    or next_stop ~= 'timeshift:channel:' .. new_vid .. ':client:' .. sid .. ':stop'
    or pool ~= 'timeshift:pool:' .. sid then return 0 end

for index, expected in ipairs({'hash', 'hash', 'set', 'hash', 'string', 'string', 'none', 'none', 'string', 'none'}) do
    if kind(KEYS[index]) ~= expected then return 0 end
end

local prior = redis.call('HMGET', pool, 'rust_owner_token', 'busy', 'rust_programme_hop_ready',
    'user_id', 'account_id', 'profile_id', 'dispatcharr_stream_id', 'stream_id', 'media_id', 'provider_timestamp')
if prior[1] ~= old_token or prior[2] ~= '1' or prior[3] ~= new_token
    or prior[4] ~= user or prior[5] ~= account or prior[6] ~= profile or prior[7] ~= dispatcharr_stream
    or prior[8] ~= stream or prior[9] ~= old_media or not prior[10] or prior[10] == ''
    or prior[10] == provider_time then return 0 end
if redis.call('GET', lease) ~= lease_token or redis.call('GET', old_gen) ~= generation
    or redis.call('GET', old_stop) ~= generation then return 0 end
local admission = redis.call('HMGET', client, 'rust_owner_token', 'user_id', 'programme_vid', 'rust_preempted')
if admission[1] ~= old_token or admission[2] ~= user or admission[3] ~= old_vid or admission[4] == '1'
    or redis.call('HGET', stats, 'rust_owner_token') ~= old_token
    or redis.call('SISMEMBER', members, sid) ~= 1 then return 0 end

-- All key types and input shapes are checked before the first write. Redis
-- script errors do not roll back earlier writes.
redis.call('HSET', pool, 'rust_owner_token', new_token, 'media_id', new_media,
    'provider_timestamp', provider_time, 'provider_tz_name', timezone, 'rust_duration_secs', duration,
    'client_ip', ip, 'client_user_agent', agent, 'last_activity', now)
redis.call('HDEL', pool, 'rust_programme_hop_ready', 'rust_idle_ready', 'final_url', 'content_length',
    'serving_range', 'archive_anchor_ts')
redis.call('HSET', client, 'rust_owner_token', new_token, 'programme_vid', new_vid, 'rust_pending_media_id', new_media)
redis.call('HDEL', client, 'programme_start', 'last_active', 'position_anchor_at', 'playback_base_secs',
    'paused', 'rust_total_bytes', 'stats_grace_deadline', 'stats_grace_disconnected_at')
redis.call('HSET', stats, 'rust_owner_token', new_token)
redis.call('DEL', old_gen, old_stop)
redis.call('EXPIRE', pool, 600)
redis.call('EXPIRE', client, 60)
redis.call('EXPIRE', stats, 60)
return 1
