"""Read-only actual Original periodic worker observer; authors retain credit.
No task implementation, task route or result writer is replaced.
"""
import json,os
from celery.signals import task_prerun,task_postrun
from dispatcharr.celery import app
if os.environ.get('JOB_DISPOSABLE_FIXTURE')!='playback-fixture-only':raise RuntimeError('Owned fixture required')
def append(kind,value):
 data=(json.dumps(value,default=str)+'\n').encode()
 if len(data)>65536:raise RuntimeError('Observer bound')
 fd=os.open('/evidence/periodic-worker-'+kind+'.jsonl',os.O_WRONLY|os.O_CREAT|os.O_APPEND,0o600)
 try:os.write(fd,data)
 finally:os.close(fd)
@task_prerun.connect
def before(task_id=None,task=None,args=None,kwargs=None,**kw):append('prerun',{'task':task.name,'id':task_id,'headers':task.request.headers,'root_id':task.request.root_id,'parent_id':task.request.parent_id,'args':args,'kwargs':kwargs})
@task_postrun.connect
def after(task_id=None,task=None,state=None,retval=None,**kw):append('postrun',{'task':task.name,'id':task_id,'state':state,'result':retval})
