"""Pinned Original read methods and actual Django cache, without startup side effects.
Original Dispatcharr authors retain full application/algorithm credit.
Constructors/heartbeat/event publishers are deliberately outside this read-only gate.
"""
import ast,hashlib,json,logging,os,sys,time
from pathlib import Path
from types import SimpleNamespace
assert os.environ.get('REDIS_RESTART_DISPOSABLE_FIXTURE')=='playback-fixture-only'
os.environ.setdefault('DJANGO_SETTINGS_MODULE','dispatcharr.settings');sys.path.insert(0,'/app');sys.argv.append('shell')
import core.apps
from core.utils import RedisClient
from django.core.cache import cache
from django.apps import apps
channel=os.environ['RESTART_PROBE_TASK'];assert channel.startswith('owned_redis_restart_')
paths=['apps/proxy/live_proxy/client_manager.py','apps/proxy/vod_proxy/multi_worker_connection_manager.py','dispatcharr/settings.py','core/utils.py','core/redis_connection.py']
def method(path,cls,name):
 tree=ast.parse(Path('/app',path).read_bytes());parent=next(n for n in tree.body if isinstance(n,ast.ClassDef)and n.name==cls);node=next(n for n in parent.body if isinstance(n,ast.FunctionDef)and n.name==name)
 scope={'logger':logging.getLogger('owned-restart')};exec(compile(ast.Module(body=[node],type_ignores=[]),'<pinned-original-read>', 'exec'),scope);return scope[name]
live_read=method(paths[0],'ClientManager','get_total_client_count');vod_read=method(paths[1],'RedisBackedVODConnection','has_active_streams');client=None;identity=None;cache_identity=None
for line in iter(lambda:sys.stdin.readline(16385),''):
 if len(line)>16384:raise RuntimeError('input limit')
 command=json.loads(line);action=command['action'];record={'action':action,'pid':os.getpid()}
 try:
  if action=='init':
   client=RedisClient.get_client();assert client is not None;identity=id(client);cache.get('owned_restart_readiness');cache_identity=id(cache._connections[cache._alias]);record.update(ready=True,source_sha256={p:hashlib.sha256(Path('/app',p).read_bytes()).hexdigest()for p in paths},cache_backend=type(cache._connections[cache._alias]).__module__)
  elif action=='seed':
   generation=int(command['generation']);assert generation in(1,2)
   client.sadd('live:channel:'+channel+':clients','owned-client');client.hset('live:channel:'+channel+':clients:owned-client',mapping={'ip_address':'127.0.0.1','user_agent':'owned-restart'})
   client.hset('vod_persistent_connection:'+channel,mapping={'active_streams':2})
   cache.set('coresettings:group:system_settings',{'owned_restart':channel,'generation':generation},timeout=300);record['result']=True
  elif action=='read':
   record['live']=live_read(SimpleNamespace(redis_client=client,clients=set(),client_set_key='live:channel:'+channel+':clients'))
   record['vod']=vod_read(SimpleNamespace(redis_client=client,connection_key='vod_persistent_connection:'+channel,session_id=channel))
   record['cache']=cache.get('coresettings:group:system_settings')
  elif action=='inspect_native':
   native_id=command['id'];assert isinstance(native_id,str)and native_id.startswith('client_')and len(native_id)<80;record['bytes']=client.hget('live:channel:'+channel+':clients:'+native_id,'bytes_sent');record['ttl']=client.ttl('live:channel:'+channel+':clients:'+native_id)
  elif action=='shutdown':record['done']=True
  else:raise ValueError('owned action only')
 except Exception as error:record['error']={'module':type(error).__module__,'type':type(error).__name__,'args':list(error.args)}
 record.update(same_client=identity is not None and id(RedisClient._client)==identity,same_cache=cache_identity is not None and id(cache._connections[cache._alias])==cache_identity,apps_ready=apps.ready)
 assert not apps.ready,'No provider/application initialization'
 encoded=json.dumps(record);assert len(encoded)<32768;print(encoded,flush=True)
 if action=='shutdown':break
