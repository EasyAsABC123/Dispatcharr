"""Owned real scanner dependency clock/Redis ACL controls and passive observer.
Original task/helper/ORM/child routing untouched. Authors retain code/credit.
"""
import json,os
from pathlib import Path
from celery.signals import task_prerun,task_postrun,before_task_publish
from dispatcharr.celery import app
assert os.environ.get('JOB_DISPOSABLE_FIXTURE')=='playback-fixture-only'
assert os.environ['POSTGRES_DB'].startswith('rust_probe_backup_jobs_')
active={}
def append(kind,value):
 data=(json.dumps(value,default=str)+'\n').encode();assert len(data)<=1048576
 fd=os.open('/evidence/periodic-worker-'+kind+'.jsonl',os.O_WRONLY|os.O_CREAT|os.O_APPEND,0o600)
 try:os.write(fd,data)
 finally:os.close(fd)
@task_prerun.connect
def before(task_id=None,task=None,args=None,kwargs=None,**extra):
 assert task.name=='core.tasks.scan_and_process_files'
 config=json.loads(Path('/evidence/owned-scanner-config.json').read_text());assert config['task_id']==task_id
 import core.tasks as source
 from core.utils import RedisClient
 from django.db import connection
 prior_time=source.time;prior_client=RedisClient._client;replacement=None
 class OwnedClock:
  @staticmethod
  def time():return config['now']
 source.time=OwnedClock
 if config.get('marker_acl_user'):
  import redis
  replacement=redis.Redis(host='localhost',port=6379,password='playback-fixture-only',username=config['marker_acl_user'],db=0,decode_responses=True);RedisClient._client=replacement
 queries=[]
 def observed(execute,sql,params,many,context):
  assert len(queries)<256 and len(sql.encode())<=32768
  record={'sql':sql,'param_types':[type(v).__name__ for v in(params or [])],'atomic':connection.in_atomic_block};queries.append(record)
  try:return execute(sql,params,many,context)
  except Exception as error:record['error_class']=type(error).__module__+'.'+type(error).__name__;raise
 guard=connection.execute_wrapper(observed);guard.__enter__();active[task_id]=(prior_time,prior_client,replacement,guard,queries)
 append('prerun',{'task':task.name,'id':task_id,'root_id':task.request.root_id,'parent_id':task.request.parent_id,'args':args,'kwargs':kwargs,'first_scan_before':source._first_scan_completed,'clock':config['now'],'uid':os.geteuid(),'marker_acl':bool(replacement)})
@before_task_publish.connect
def publication(sender=None,body=None,headers=None,properties=None,exchange=None,routing_key=None,**extra):
 if headers and headers.get('parent_id')in active:
  append('children',{'task':sender,'headers':headers,'body':body,'properties':properties,'exchange':exchange,'routing_key':routing_key})
@task_postrun.connect
def after(task_id=None,task=None,state=None,retval=None,**extra):
 import core.tasks as source
 from core.utils import RedisClient
 value=active.pop(task_id,None)
 if value:
  prior_time,prior_client,replacement,guard,queries=value;guard.__exit__(None,None,None);source.time=prior_time
  if replacement:RedisClient._client=prior_client;replacement.close()
  append('sql',{'id':task_id,'queries':queries})
 append('postrun',{'task':task.name,'id':task_id,'state':state,'result':retval,'first_scan_after':source._first_scan_completed})
