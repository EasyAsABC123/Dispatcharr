"""UNEXECUTED actual Source worker observer; no task/function is replaced.
Parent queue isolation is explicit; real descendant frames remain untouched.
"""
import datetime,fcntl,json,os,sys,time
from pathlib import Path
if os.environ.get('JOB_DISPOSABLE_FIXTURE')!='playback-fixture-only' or not os.environ.get('POSTGRES_DB','').startswith('rust_probe_backup_jobs_'):raise ValueError('OwnedCopiedFixture')
from dispatcharr.celery import app
from celery.signals import before_task_publish,task_prerun,task_postrun
from django.db import connection
sys.path.insert(0,'/job-tools')
from ledger_io import append_receipt
ROOT=Path('/evidence');contexts={}
KNOWN={'apps.channels.tasks.evaluate_series_rules','apps.channels.tasks.maintain_recurring_recordings'}
def append(kind,value):
 with (ROOT/'configured-ledger.lock').open('a+b')as lock:
  fcntl.flock(lock.fileno(),fcntl.LOCK_EX);receipt=append_receipt(ROOT/('configured-'+kind+'.jsonl'),value)
 if not receipt['full_record_retained']:raise ValueError('ObservedReceiptCapacityRefusal')
@before_task_publish.connect
def published(sender=None,headers=None,body=None,properties=None,exchange=None,routing_key=None,**unused):
 append('publications',{'task':sender,'headers':headers,'body':body,'properties':properties,'exchange':exchange,'routing_key':routing_key,'pid':os.getpid(),'utc':datetime.datetime.now(datetime.timezone.utc).isoformat()})
@task_prerun.connect
def prerun(task_id=None,task=None,args=None,kwargs=None,**unused):
 append('prerun',{'id':task_id,'task':task.name,'args':args,'kwargs':kwargs,'pid':os.getpid(),'uid':os.getuid(),'cwd':os.getcwd(),'root_id':task.request.root_id,'parent_id':task.request.parent_id,'delivery':task.request.delivery_info,'attrs':{k:getattr(task,k)for k in ('acks_late','ignore_result','track_started','reject_on_worker_lost')},'utc':datetime.datetime.now(datetime.timezone.utc).isoformat()})
 if task.name not in KNOWN:return # Collector fails unknown; signal raise would NOT prevent execution.
 def sql(execute,query,params,many,context):
  row={'id':task_id,'sql':query,'many':many,'began_epoch':time.time()}
  try:return execute(query,params,many,context)
  except BaseException as error:
   row.update(exception_type=type(error).__module__+'.'+type(error).__name__,exception_text=str(error),SQLSTATE=getattr(error.__cause__,'pgcode',None));raise
  finally:row['finished_epoch']=time.time();append('sql',row)
 manager=connection.execute_wrapper(sql);manager.__enter__();contexts[task_id]=manager
@task_postrun.connect
def postrun(task_id=None,task=None,retval=None,state=None,**unused):
 manager=contexts.pop(task_id,None)
 if manager:manager.__exit__(None,None,None)
 append('postrun',{'id':task_id,'task':task.name,'result':retval,'state':state,'pid':os.getpid(),'utc':datetime.datetime.now(datetime.timezone.utc).isoformat()})
