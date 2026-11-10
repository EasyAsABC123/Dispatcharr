"""Actual Original missing-source refresh SQL observer; fixture only.
Original Dispatcharr/Django/Celery authors retain credit/licenses.
No application/task/helper replacement. Other families route to an owned queue.
"""
import json,os
from pathlib import Path
if os.environ.get('JOB_DISPOSABLE_FIXTURE')!='playback-fixture-only'or not os.environ.get('POSTGRES_DB','').startswith('rust_probe_backup_jobs_'):raise RuntimeError('Copied fixture required')
from dispatcharr.celery import app
from celery.signals import task_prerun,task_postrun
from django.db import connection
from django.test.utils import CaptureQueriesContext
TARGET='apps.epg.tasks.refresh_epg_data';contexts={}
def route(name,args,kwargs,options,task=None,**unused):
 if name!=TARGET:return {'queue':'owned-orphan-unconsumed'}
app.conf.task_routes=(route,)
def append(name,value):
 data=(json.dumps(value,default=str)+'\n').encode()
 if len(data)>524288:raise RuntimeError('Owned observer cap')
 fd=os.open(Path('/evidence')/name,os.O_CREAT|os.O_APPEND|os.O_WRONLY,0o600)
 try:os.write(fd,data)
 finally:os.close(fd)
@task_prerun.connect
def before(task_id=None,task=None,**unused):
 if task.name!=TARGET:append('orphan-unexpected-tasks.jsonl',{'id':task_id,'task':task.name});return
 capture=CaptureQueriesContext(connection);capture.__enter__();contexts[task_id]=capture
@task_postrun.connect
def after(task_id=None,task=None,retval=None,state=None,**unused):
 if task.name!=TARGET:return
 capture=contexts.pop(task_id);capture.__exit__(None,None,None);queries=list(capture.captured_queries)
 if len(queries)>128 or sum(len(x['sql'].encode())for x in queries)>262144:raise RuntimeError('Owned SQL observer cap')
 append('orphan-worker-postrun.jsonl',{'id':task_id,'state':state,'result':retval,'sql':queries,'pid':os.getpid()})
