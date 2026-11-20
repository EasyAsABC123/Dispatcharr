-- Choose one exact generation for the existing fenced archive CLEANUP.
-- Inputs are generated from the immutable old and new entries in one owner.
local keys, args = KEYS, ARGV
local function kind(key)
    local result = redis.call('TYPE', key)
    return type(result) == 'table' and result.ok or result
end
for index, expected in ipairs({'hash','hash','set','hash','hash','string','string','string','string','string'}) do
    local actual = kind(keys[index])
    if actual ~= 'none' and actual ~= expected then return -1 end
end
local token = redis.call('HGET',keys[1],'rust_owner_token')
    or redis.call('HGET',keys[2],'rust_owner_token')
local old = token == args[1]
if not old and token ~= args[2] then return 0 end
local generation = old and keys[6] or keys[9]
local stop = old and keys[7] or keys[10]
local grace_token = old and args[5] or args[6]
local KEYS = {keys[1],keys[2],keys[3],keys[4],keys[5],generation,stop,keys[8]}
local ARGV = {token,'0',args[3],args[4],grace_token}
