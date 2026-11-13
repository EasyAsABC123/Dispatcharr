"""Passive actual Original task/SQL/publication observer; only scanner clock controlled.
Original Dispatcharr/Celery/Django authors retain code and licensing credit.
"""
import base64,hashlib,json,os,fcntl
from pathlib import Path
from celery.signals import task_prerun,task_postrun,before_task_publish,task_received
from dispatcharr.celery import app
assert os.environ.get('JOB_DISPOSABLE_FIXTURE')=='playback-fixture-only'
assert os.environ['POSTGRES_DB'].startswith('rust_probe_backup_jobs_')
active={}
def append(kind,value):
 data=(json.dumps(value,default=str)+'\n').encode();assert len(data)<=1048576
 fd=os.open('/evidence/periodic-worker-'+kind+'.jsonl',os.O_WRONLY|os.O_CREAT|os.O_APPEND,0o600)
 try:
  fcntl.flock(fd,fcntl.LOCK_EX)
  offset=0
  while offset<len(data):
   wrote=os.write(fd,data[offset:])
   if wrote<=0:raise OSError('ObserverShortWrite')
   offset+=wrote
  os.fsync(fd)
 finally:os.close(fd)
@task_received.connect
def received(request=None,**extra):
 append('received',{'task':request.name,'id':request.id,'root_id':request.root_id,'parent_id':request.parent_id,'delivery_info':request.delivery_info,'delivery_tag':request.message.delivery_tag,'args':request.args,'kwargs':request.kwargs})
@task_prerun.connect
def before(task_id=None,task=None,args=None,kwargs=None,**extra):
 from django.db import connection
 import core.tasks as source
 prior_time=None;prior_m3u_time=None
 if task.name=='core.tasks.scan_and_process_files':
  config=json.loads(Path('/evidence/owned-scanner-config.json').read_text());assert config['task_id']==task_id
  prior_time=source.time
  class OwnedClock:
   @staticmethod
   def time():return config['now']
  source.time=OwnedClock
 if task.name in ['apps.m3u.tasks.refresh_m3u_groups','apps.m3u.tasks.refresh_single_m3u_account']:
  import apps.m3u.tasks as mt
  prior_m3u_time=mt.time
  class ElapsedClock:
   def time(self):return 1000.125
   def __getattr__(self,name):return getattr(prior_m3u_time,name)
  mt.time=ElapsedClock()
 queries=[]
 def observed(execute,sql,params,many,context):
  assert len(queries)<8192 and len(sql.encode())<=65536
  record={'sql':sql,'param_types':[type(v).__name__ for v in(params or [])],'atomic':connection.in_atomic_block};queries.append(record)
  try:return execute(sql,params,many,context)
  except Exception as error:record['error_class']=type(error).__module__+'.'+type(error).__name__;raise
 guard=connection.execute_wrapper(observed);guard.__enter__();active[task_id]=(prior_time,prior_m3u_time,guard,queries)
 import redis
 broker=redis.Redis.from_url(os.environ['CELERY_BROKER_URL'])
 attrs={name:getattr(task,name,None)for name in ['acks_late','ignore_result','track_started','time_limit','soft_time_limit','max_retries','reject_on_worker_lost']}
 append('prerun',{'task':task.name,'id':task_id,'root_id':task.request.root_id,'parent_id':task.request.parent_id,'args':args,'kwargs':kwargs,'uid':os.geteuid(),'attrs':attrs,'unacked_tags':[v.decode()for v in broker.hkeys('unacked')],'unacked_task_ids':[json.loads(v)[0]['headers']['id']for v in broker.hvals('unacked')],'queue_length':broker.llen('celery')})
@before_task_publish.connect
def publication(sender=None,body=None,headers=None,properties=None,exchange=None,routing_key=None,**extra):
 if active:append('children',{'task':sender,'headers':headers,'body':body,'properties':properties,'exchange':exchange,'routing_key':routing_key})
@task_postrun.connect
def after(task_id=None,task=None,state=None,retval=None,**extra):
 import core.tasks as source
 value=active.pop(task_id,None)
 if value:
  prior_time,prior_m3u_time,guard,queries=value;guard.__exit__(None,None,None)
  if prior_time is not None:source.time=prior_time
  if prior_m3u_time is not None:
   import apps.m3u.tasks as mt
   mt.time=prior_m3u_time
  append('sql',{'id':task_id,'queries':queries})
 files=[]
 for root in ['/app/media/cached_m3u','/app/media/cached_epg']:
  for p in Path(root).rglob('*'):
   if p.is_file():
    assert p.stat().st_size<=1048576
    raw=p.read_bytes();files.append({'path':str(p),'size':len(raw),'sha256':hashlib.sha256(raw).hexdigest(),'raw_base64':base64.b64encode(raw).decode()})
 append('filepost',{'id':task_id,'files':files})
 if task.name.startswith('apps.m3u.tasks.'):
  from apps.plugins.loader import PluginManager
  from apps.plugins.models import PluginConfig
  pm=PluginManager.get();append('hooks',{'id':task_id,'actions':{kind:list(pm.iter_actions_for_event(kind))for kind in ['m3u_refresh','m3u_error']},'enabled_plugin_keys':list(PluginConfig.objects.filter(enabled=True).values_list('key',flat=True))})
 append('postrun',{'task':task.name,'id':task_id,'state':state,'result':retval})
