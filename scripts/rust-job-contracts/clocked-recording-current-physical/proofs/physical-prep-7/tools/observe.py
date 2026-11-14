"""Observe real Source signals/all target-DB threads; no model/task replacement.

Python HTTP dependency calls are denied and retained. FFmpeg's only permitted
network is the separately guarded owned origin inside an internal network.
"""
import datetime,fcntl,hashlib,os,sys,threading,time
from pathlib import Path
from urllib.parse import urlsplit,parse_qsl
if os.environ.get('JOB_DISPOSABLE_FIXTURE')!='playback-fixture-only'or not os.environ.get('POSTGRES_DB','').startswith('rust_probe_backup_jobs_'):raise ValueError('OwnedCopiedFixture')
sys.path.insert(0,'/job-tools')
from ledger_io import append_receipt
from seed_spec import EXPECTED_ROUTES
ROOT=Path('/evidence')
def append(kind,value):
 with(ROOT/'clocked-ledger.lock').open('a+b')as lock:
  fcntl.flock(lock.fileno(),fcntl.LOCK_EX);receipt=append_receipt(ROOT/('clocked-'+kind+'.jsonl'),value)
 if not receipt['full_record_retained']:raise ValueError('ClockedObservationCapacity')
import requests
def forbidden_http(session,method,url,*args,**kwargs):
 parsed=urlsplit(str(url));append('HTTP-attempts',{'method':str(method),'scheme':parsed.scheme,'host':parsed.hostname,'path':parsed.path,'query_names':[k for k,v in parse_qsl(parsed.query)],'URL_sha256':hashlib.sha256(str(url).encode()).hexdigest(),'actual_request_forwarded':False,'pid':os.getpid(),'thread':threading.get_ident(),'utc':datetime.datetime.now(datetime.timezone.utc).isoformat()});raise RuntimeError('OwnedClockedUnexpectedPythonHTTPDependency')
requests.sessions.Session.request=forbidden_http
from dispatcharr.celery import app
from celery import current_task
from celery.signals import before_task_publish,task_prerun,task_postrun
from django.db.backends.signals import connection_created
from process_observer import Observer,starttime
# Verify, never override, the actual pinned Source dedicated dvr routing.
if app.conf.task_routes!=EXPECTED_ROUTES:raise ValueError('ActualPinnedSourceDvrRoute')
def actual_task_id():return getattr(getattr(current_task,'request',None),'id',None)
process_observer=Observer(append,actual_task_id);process_observer.install()
def sql_observer(execute,query,params,many,context):
 task=getattr(current_task,'request',None);ident=getattr(task,'id',None)
 row={'actual_thread_task_id':ident,'task_context_available':ident is not None,'sql':query,'params':params,'many':many,'pid':os.getpid(),'thread':threading.get_ident(),'thread_name':threading.current_thread().name,'began_epoch':time.time()}
 try:
  actual=getattr(getattr(context['connection'].connection,'info',None),'dbname',None);row['actual_database']=actual
  if actual!=os.environ['POSTGRES_DB']:row['ownership_refused']=True;raise RuntimeError('ActualCopiedDatabaseBeforeSourceSQL')
  return execute(query,params,many,context)
 except BaseException as error:
  cause=error.__cause__;row.update(exception_type=type(error).__module__+'.'+type(error).__name__,exception_text=str(error),SQLSTATE=getattr(cause,'sqlstate',None)or getattr(cause,'pgcode',None)or getattr(error,'sqlstate',None));raise
 finally:row['finished_epoch']=time.time();append('sql',row)
def opened(sender=None,connection=None,**unused):
 if sql_observer not in connection.execute_wrappers:connection.execute_wrappers.append(sql_observer)
connection_created.connect(opened,weak=False)
@before_task_publish.connect(weak=False)
def published(sender=None,headers=None,body=None,properties=None,exchange=None,routing_key=None,**unused):
 append('publications',{'task':sender,'headers':headers,'body':body,'properties':properties,'exchange':exchange,'routing_key':routing_key,'pid':os.getpid(),'thread':threading.get_ident(),'utc':datetime.datetime.now(datetime.timezone.utc).isoformat()})
@task_prerun.connect(weak=False)
def prerun(task_id=None,task=None,args=None,kwargs=None,**unused):
 append('prerun',{'id':task_id,'task':task.name,'actual_process_starttime':starttime(os.getpid()),'args':args,'kwargs':kwargs,'pid':os.getpid(),'thread':threading.get_ident(),'uid':os.getuid(),'cwd':os.getcwd(),'root_id':task.request.root_id,'parent_id':task.request.parent_id,'delivery':task.request.delivery_info,'attrs':{k:getattr(task,k)for k in('acks_late','ignore_result','track_started','reject_on_worker_lost')},'utc':datetime.datetime.now(datetime.timezone.utc).isoformat()})
@task_postrun.connect(weak=False)
def postrun(task_id=None,task=None,retval=None,state=None,**unused):
 append('postrun',{'id':task_id,'task':task.name,'actual_process_starttime':starttime(os.getpid()),'result':retval,'state':state,'pid':os.getpid(),'thread':threading.get_ident(),'root_id':task.request.root_id,'parent_id':task.request.parent_id,'utc':datetime.datetime.now(datetime.timezone.utc).isoformat()})
 append('subprocess-postrun',{'task_id':task_id,'pid':os.getpid(),'observation':process_observer.snapshot(),'utc':datetime.datetime.now(datetime.timezone.utc).isoformat()})
