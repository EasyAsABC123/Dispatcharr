
local t=redis.call('TYPE',KEYS[1]).ok
if t=='none'then return{0,''}end
if t~='string'or redis.call('STRLEN',KEYS[1])>262144 then return{-1,''}end
return{1,redis.call('GET',KEYS[1])}
