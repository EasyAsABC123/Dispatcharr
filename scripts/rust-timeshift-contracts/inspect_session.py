"""Read only the disposable fixture's requested Timeshift metadata."""
import json
import os
import sys
if os.environ.get('POSTGRES_PASSWORD') != 'playback-fixture-only':
    raise RuntimeError('Restricted to disposable playback fixture')
sys.path.insert(0,'/app')
os.environ.setdefault('DJANGO_SETTINGS_MODULE','dispatcharr.settings')
import django
django.setup()
from core.utils import RedisClient
request = json.load(sys.stdin)
sid = request['session_id']
if not isinstance(sid,str) or not sid.replace('_','').replace('-','').isalnum() or len(sid)>100:
    raise ValueError('invalid fixture session')
r = RedisClient.get_client()
pool = f'timeshift:pool:{sid}'
api = f'timeshift:session:{sid}'
clients = list(r.scan_iter(match=f'timeshift:channel:*:clients:{sid}',count=100))
busy = r.hget(pool,'busy')
print(json.dumps({'api_exists':bool(r.exists(api)), 'api_ttl':r.ttl(api),'pool_exists':bool(r.exists(pool)),
    'pool_ttl':r.ttl(pool),'pool_busy':busy.decode() if isinstance(busy,bytes) else busy,
    'profile_connections':int(r.get('profile_connections:'+str(request['profile_id'])) or 0),
    'stats_client_count':len(clients)}))
