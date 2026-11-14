"""Actual Source signal/SQL/HTTP observer for one owned M6 batch worker.
Import only from the explicitly admitted Source process; task methods unchanged.
"""
import datetime,hashlib,json,os,threading,time
from pathlib import Path
from urllib.parse import urlsplit
if os.environ.get('JOB_DISPOSABLE_FIXTURE')!='playback-fixture-only'or not os.environ.get('POSTGRES_DB','').startswith('rust_probe_backup_jobs_'):raise ValueError('OwnedSourceWorkerOnly')
from dispatcharr.celery import app
from celery import current_task
from celery.signals import before_task_publish,task_prerun,task_postrun,worker_process_init
from django.db.backends.signals import connection_created
ROOT=Path(os.environ['M6_SOURCE_EVIDENCE'])
if not ROOT.is_absolute()or not str(ROOT).startswith('/evidence/'):raise ValueError('OwnedEvidenceRoot')
def record(kind,value):
 # Reuse the already qualified full-write/fsync/capacity-refusal ledger.
 from ledger_io import append_receipt
 import fcntl
 with (ROOT/'m6-ledger.lock').open('a+b')as lock:
  fcntl.flock(lock.fileno(),fcntl.LOCK_EX)
  result=append_receipt(ROOT/('m6-'+kind+'.jsonl'),value)
 if not result['full_record_retained']:raise ValueError('ActualM6LedgerCapacity')
def identity():
 return {'pid':os.getpid(),'starttime':Path('/proc/self/stat').read_text().rsplit(')',1)[1].split()[19],'thread':threading.get_ident(),'PID_namespace':os.readlink('/proc/self/ns/pid'),'utc':datetime.datetime.now(datetime.timezone.utc).isoformat()}
def sql(execute,query,params,many,context):
 row={**identity(),'task_id':getattr(getattr(current_task,'request',None),'id',None),'sql':query,'params':params,'many':many,'began_epoch':time.time()}
 try:
  actual=getattr(getattr(context['connection'].connection,'info',None),'dbname',None);row['actual_database']=actual
  if actual!=os.environ['POSTGRES_DB']:raise RuntimeError('ActualCopiedDatabaseBeforeSQL')
  return execute(query,params,many,context)
 except BaseException as error:
  row.update(exception_type=type(error).__module__+'.'+type(error).__name__,exception_text=str(error),actual_SQLSTATE=getattr(error.__cause__,'sqlstate',None)or getattr(error.__cause__,'pgcode',None)or getattr(error,'sqlstate',None));raise
 finally:row['finished_epoch']=time.time();record('sql',row)
def opened(sender=None,connection=None,**unused):
 if sql not in connection.execute_wrappers:connection.execute_wrappers.append(sql)
connection_created.connect(opened,weak=False)
@before_task_publish.connect(weak=False)
def published(sender=None,headers=None,body=None,properties=None,exchange=None,routing_key=None,**unused):record('publication',{**identity(),'task':sender,'headers':headers,'body':body,'properties':properties,'exchange':exchange,'routing_key':routing_key})
@task_prerun.connect(weak=False)
def prerun(task_id=None,task=None,args=None,kwargs=None,**unused):record('prerun',{**identity(),'id':task_id,'task':task.name,'args':args,'kwargs':kwargs,'root_id':task.request.root_id,'parent_id':task.request.parent_id,'delivery':task.request.delivery_info})
@task_postrun.connect(weak=False)
def postrun(task_id=None,task=None,retval=None,state=None,**unused):record('postrun',{**identity(),'id':task_id,'task':task.name,'result':retval,'result_type':type(retval).__module__+'.'+type(retval).__name__,'state':state,'root_id':task.request.root_id,'parent_id':task.request.parent_id})
@worker_process_init.connect(weak=False)
def http_dependency(**unused):
 import requests.sessions
 origin=os.environ['DEFAULT_TASK_HTTP_ORIGIN'];target=urlsplit(origin)
 if target.scheme!='http'or target.hostname!='127.0.0.1'or not target.port:raise ValueError('OwnedVersionOrigin')
 original=requests.sessions.Session.request
 def request(session,method,url,*args,**kwargs):
  address=urlsplit(str(url));allowed=address.scheme=='https'and(address.hostname,address.path)in [('hub.docker.com','/v2/repositories/dispatcharr/dispatcharr/tags/dev'),('api.github.com','/repos/Dispatcharr/Dispatcharr/releases/latest')]
  record('HTTP',{**identity(),'method':method,'original_host':address.hostname,'original_path':address.path,'URL_sha256':hashlib.sha256(str(url).encode()).hexdigest(),'owned_dependency_route':allowed})
  if not allowed:raise RuntimeError('UnknownSourceHTTPDependencyRefused')
  return original(session,method,origin+'/owned-version',*args,**kwargs)
 requests.sessions.Session.request=request
