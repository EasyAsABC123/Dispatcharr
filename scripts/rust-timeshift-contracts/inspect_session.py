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
result={'api_exists':bool(r.exists(api)), 'api_ttl':r.ttl(api),'pool_exists':bool(r.exists(pool)),
    'pool_ttl':r.ttl(pool),'pool_busy':busy.decode() if isinstance(busy,bytes) else busy,
    'profile_connections':int(r.get('profile_connections:'+str(request['profile_id'])) or 0),
    'stats_client_count':len(clients)}
if request.get('details'):
    def selected(key,names):
        return {name:(value.decode() if isinstance(value,bytes) else value) for name in names if (value:=r.hget(key,name)) is not None}
    result['pool_fields']=selected(pool,['busy','profile_id','content_length','serving_range','media_id','stream_id','user_id'])
    result['cached_final_url_present']=bool(r.hget(pool,'final_url'))
    result['clients']=[selected(key,['connected_at','init_time','total_bytes','playback_base_secs','position_anchor_at','media_id','paused']) for key in clients]
    result['metadata']=[selected(key.rsplit(b':clients:',1)[0]+b':metadata' if isinstance(key,bytes) else key.rsplit(':clients:',1)[0]+':metadata',['init_time','total_bytes','state']) for key in clients]
print(json.dumps(result))
