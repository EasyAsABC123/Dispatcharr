"""Persistent actual Original cached-client/lock helper process; no Django.setup.
Original Dispatcharr authors retain all application/algorithm credit.
"""
import hashlib,json,os,sys,time
from pathlib import Path
if os.environ.get('REDIS_RESTART_DISPOSABLE_FIXTURE')!='playback-fixture-only':raise SystemExit(2)
os.environ.setdefault('DJANGO_SETTINGS_MODULE','dispatcharr.settings');sys.path.insert(0,'/app');sys.argv.append('shell')
import core.apps  # Actual Original TRACE bootstrap only; never invoke AppConfig.ready.
from core.utils import RedisClient,acquire_task_lock,release_task_lock
from django.apps import apps
import redis
name=os.environ['RESTART_PROBE_TASK'];assert name.startswith('owned_redis_restart_')and name.replace('_','').isalnum();identity=None
for line in iter(lambda:sys.stdin.readline(16385),''):
 if len(line)>16384:raise SystemExit(2)
 command=json.loads(line);action=command['action'];begin=time.monotonic();record={'action':action,'pid':os.getpid()}
 try:
  if action=='init':
   client=RedisClient.get_client();assert client is not None;identity=id(client)
   record.update(ready=True,redis_version=redis.__version__,source_sha256={path:hashlib.sha256(Path(path).read_bytes()).hexdigest()for path in ['/app/core/utils.py','/app/core/redis_connection.py','/app/core/apps.py']})
  elif action=='acquire':record['result']=bool(acquire_task_lock(name,int(command['id'])))
  elif action=='release':record['result']=release_task_lock(name,int(command['id']))
  elif action=='read':record['result']=RedisClient.get_client().get('task_lock_'+command.get('task',name)+'_'+str(int(command['id'])))
  elif action=='ping':record['result']=RedisClient.get_client().ping()
  elif action=='shutdown':record['done']=True
  else:raise ValueError('Owned action only')
 except Exception as error:record['error']={'module':type(error).__module__,'type':type(error).__name__,'args':list(error.args)}
 record.update(elapsed_seconds=time.monotonic()-begin,same_client=identity is not None and id(RedisClient._client)==identity,apps_ready=apps.ready)
 assert not apps.ready,'No provider/application initialization permitted'
 encoded=json.dumps(record)
 if len(encoded)>32768:raise RuntimeError('Bounded Source observation required')
 print(encoded,flush=True)
 if action=='shutdown':break
