
local q=redis.call('TYPE',KEYS[1]).ok;local b=redis.call('TYPE',KEYS[2]).ok
if(q~='none'and q~='list')or(b~='none'and b~='set')then return -3 end
if #ARGV[1]>65536 then return -1 end
if redis.call('LLEN',KEYS[1])>=tonumber(ARGV[2])then return -2 end
redis.call('SADD',KEYS[2],'celery'..string.char(6,22)..string.char(6,22)..'celery')
return redis.call('LPUSH',KEYS[1],ARGV[1])
