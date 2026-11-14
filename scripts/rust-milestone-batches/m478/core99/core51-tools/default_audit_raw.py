"""Read-only default SQL62/59 +RedisDB7 audit; no Django application import."""
import json,sys
from pathlib import Path
from native_state import default
from redis_channels import inventory
import redis
schema=json.loads(Path('/core51-root/inputs/schema62-authority.json').read_bytes());client=redis.Redis(host='127.0.0.1',port=6379,password='playback-fixture-only',db=7,socket_connect_timeout=2,socket_timeout=2)
try:x={'SQL':default('dispatcharr',schema),'RedisDB7':inventory(client,7)}
finally:client.close()
p=Path('/evidence/core51')/('default-'+sys.argv[1]+'.json');p.write_text(json.dumps(x,indent=2)+'\n')
