"""Fixture-only owned task clock control and passive SQL/task observer.
Task code, result backend, Channels and ORM remain pinned Original implementations.
Original Dispatcharr/Celery/Django authors retain credit. No handler replacement.
"""
import datetime,json,os
from unittest.mock import patch
from pathlib import Path
from celery.signals import task_prerun,task_postrun
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
def before(task_id=None,task=None,args=None,kwargs=None,**kw):
 assert task.name=='apps.m3u.tasks.check_account_expirations'
 config=json.loads(Path('/evidence/owned-active-clock.json').read_text());assert task_id==config['task_id']
 clock=datetime.datetime.fromisoformat(config['now']);assert clock.tzinfo
 from django.db import connection
 from django.apps import apps
 proxy=apps.get_app_config('proxy')
 managers={n:len(getattr(getattr(proxy,n,None),'stream_managers',{}))for n in ['hls_proxy','live_proxy']}
 rows=[]
 def observed(execute,sql,params,many,context):
  assert len(rows)<512 and len(sql.encode())<=32768
  row={'sql':sql,'param_types':[type(v).__name__ for v in(params or [])],'atomic':connection.in_atomic_block};rows.append(row)
  try:return execute(sql,params,many,context)
  except Exception as e:row['error_class']=type(e).__module__+'.'+type(e).__name__;raise
 guard=connection.execute_wrapper(observed);guard.__enter__();clock_guard=patch('django.utils.timezone.now',return_value=clock);clock_guard.start()
 active[task_id]=(guard,clock_guard,rows)
 append('prerun',{'task':task.name,'id':task_id,'root_id':task.request.root_id,'parent_id':task.request.parent_id,'args':args,'kwargs':kwargs,'controlled_now':clock,'process_local_managers':managers})
@task_postrun.connect
def after(task_id=None,task=None,state=None,retval=None,**kw):
 value=active.pop(task_id,None)
 if value:
  guard,clock_guard,rows=value;clock_guard.stop();guard.__exit__(None,None,None);append('sql',{'id':task_id,'queries':rows})
 append('postrun',{'task':task.name,'id':task_id,'state':state,'result':retval})
