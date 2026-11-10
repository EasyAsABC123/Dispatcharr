"""Read-only three-family observer; Original Dispatcharr/Celery authors retain credit.
No task replacement or descendant routing override.
"""
import json,os
from pathlib import Path
from celery.signals import task_prerun,task_postrun,after_task_publish
from dispatcharr.celery import app
if os.environ.get('JOB_DISPOSABLE_FIXTURE')!='playback-fixture-only':raise RuntimeError('Owned fixture required')
ALLOWED={'apps.epg.tasks.refresh_epg_data','apps.epg.tasks.build_programme_index_task','apps.channels.tasks.evaluate_series_rules'}
def append(kind,value):
 data=(json.dumps(value,default=str)+'\n').encode()
 if len(data)>262144:raise RuntimeError('Observer bound')
 fd=os.open('/evidence/workflow-'+kind+'.jsonl',os.O_WRONLY|os.O_CREAT|os.O_APPEND,0o600)
 try:os.write(fd,data)
 finally:os.close(fd)
@task_prerun.connect
def before(task_id=None,task=None,args=None,kwargs=None,**kw):
 append('prerun',{'task':task.name,'id':task_id,'args':args,'kwargs':kwargs,'root_id':task.request.root_id,'parent_id':task.request.parent_id,'uid':os.getuid(),'cwd':os.getcwd()})
 if task.name not in ALLOWED:raise RuntimeError('Unrelated fixture task')
@task_postrun.connect
def after(task_id=None,task=None,state=None,retval=None,**kw):append('postrun',{'task':task.name,'id':task_id,'state':state,'result':retval})
@after_task_publish.connect
def published(sender=None,headers=None,body=None,**kw):append('published',{'task':sender,'headers':headers,'body':body})
