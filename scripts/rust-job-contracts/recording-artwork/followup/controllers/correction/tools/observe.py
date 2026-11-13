"""Source-only observation and dormant deny-HTTP dependency guard.
No application task/model/signal function is replaced. HTTP attempts invalidate
this local-VOD-logo corpus even if Original catches the dependency exception.
"""
import datetime,fcntl,hashlib,os,sys,time
from pathlib import Path
from urllib.parse import urlsplit,parse_qsl
if os.environ.get('JOB_DISPOSABLE_FIXTURE')!='playback-fixture-only'or not os.environ.get('POSTGRES_DB','').startswith('rust_probe_backup_jobs_'):raise ValueError('OwnedCopiedFixture')
sys.path.insert(0,'/job-tools')
from ledger_io import append_receipt
from seed_spec import ROUTES,REFRESH,SERIES,ARTWORK
ROOT=Path('/evidence');contexts={};KNOWN={REFRESH,SERIES,ARTWORK}

def append(kind,value):
 with(ROOT/'parented-ledger.lock').open('a+b')as lock:
  fcntl.flock(lock.fileno(),fcntl.LOCK_EX);receipt=append_receipt(ROOT/('parented-'+kind+'.jsonl'),value)
 if not receipt['full_record_retained']:raise ValueError('ObservationCapacityRefusal')

# External HTTP is a dependency boundary, not an application helper patch.
# No request is sent to a provider; successful authority REQUIRES zero calls.
import requests
_original_request=requests.sessions.Session.request
def forbidden_http(session,method,url,*args,**kwargs):
 parsed=urlsplit(str(url));append('HTTP-attempts',{'method':str(method),'scheme':parsed.scheme,'host':parsed.hostname,'path':parsed.path,'query_names':[k for k,v in parse_qsl(parsed.query)],'full_URL_sha256':hashlib.sha256(str(url).encode()).hexdigest(),'actual_request_forwarded':False,'pid':os.getpid(),'utc':datetime.datetime.now(datetime.timezone.utc).isoformat()});raise RuntimeError('OwnedLocalArtworkUnexpectedHTTPDependency')
requests.sessions.Session.request=forbidden_http

from dispatcharr.celery import app
from celery.signals import before_task_publish,task_prerun,task_postrun
from django.db import connection
app.conf.task_routes=ROUTES

@before_task_publish.connect
def published(sender=None,headers=None,body=None,properties=None,exchange=None,routing_key=None,**unused):
 append('publications',{'task':sender,'headers':headers,'body':body,'properties':properties,'exchange':exchange,'routing_key':routing_key,'pid':os.getpid(),'utc':datetime.datetime.now(datetime.timezone.utc).isoformat()})

@task_prerun.connect
def prerun(task_id=None,task=None,args=None,kwargs=None,**unused):
 append('prerun',{'id':task_id,'task':task.name,'args':args,'kwargs':kwargs,'pid':os.getpid(),'uid':os.getuid(),'cwd':os.getcwd(),'root_id':task.request.root_id,'parent_id':task.request.parent_id,'delivery':task.request.delivery_info,'attrs':{k:getattr(task,k)for k in('acks_late','ignore_result','track_started','reject_on_worker_lost')},'utc':datetime.datetime.now(datetime.timezone.utc).isoformat()})
 if task.name not in KNOWN:return # Signals cannot enforce dispatch; corpus rejects it.
 def sql(execute,query,params,many,context):
  row={'id':task_id,'sql':query,'params':params,'many':many,'began_epoch':time.time()}
  try:
   actual_database=getattr(getattr(context['connection'].connection,'info',None),'dbname',None);row['actual_database']=actual_database
   if actual_database!=os.environ['POSTGRES_DB']:row['ownership_refused']=True;raise RuntimeError('ActualCopiedWorkerDatabaseRequiredBeforeSQL')
   return execute(query,params,many,context)
  except BaseException as error:
   cause=error.__cause__;row.update(exception_type=type(error).__module__+'.'+type(error).__name__,exception_text=str(error),SQLSTATE=getattr(cause,'sqlstate',None)or getattr(cause,'pgcode',None)or getattr(error,'sqlstate',None));raise
  finally:row['finished_epoch']=time.time();append('sql',row)
 manager=connection.execute_wrapper(sql);manager.__enter__();contexts[task_id]=manager

@task_postrun.connect
def postrun(task_id=None,task=None,retval=None,state=None,**unused):
 manager=contexts.pop(task_id,None)
 if manager:manager.__exit__(None,None,None)
 append('postrun',{'id':task_id,'task':task.name,'result':retval,'state':state,'pid':os.getpid(),'root_id':task.request.root_id,'parent_id':task.request.parent_id,'utc':datetime.datetime.now(datetime.timezone.utc).isoformat()})
