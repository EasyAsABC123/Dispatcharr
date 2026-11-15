"""Loaded-plugin callable provenance for the shared native Jobs bridge.
This is not prefix-based task admission and never executes Source backend tasks.
Native registration must independently verify the actual Config/loaded-files and
return its opaque sealed witness before any broker publication/execution.
"""
import hashlib,os
from pathlib import Path
from .wire import current,CompatibilityUnavailable
MAX_TASKS=1024

def registered():
 from apps.plugins.loader import PluginManager
 from dispatcharr.celery import app
 manager=PluginManager.get()
 with manager._lock:loaded=list(manager._registry.values())
 result={}
 for plugin in loaded:
  if not plugin.loaded or not plugin.path:continue
  root=Path(plugin.path).resolve(strict=True)
  for name in manager._registered_task_names(plugin.key):
   task=app.tasks.get(name)
   if task is None:continue
   source=manager._task_source_path(task)
   if not source:continue
   path=Path(source).resolve(strict=True)
   if not path.is_relative_to(root):raise CompatibilityUnavailable('Loaded plugin task source escaped its actual root')
   if name in result:raise CompatibilityUnavailable('Ambiguous loaded plugin task ownership')
   if len(result)>=MAX_TASKS:raise CompatibilityUnavailable('Plugin task registry capacity')
   with path.open('rb')as stream:
    digest=hashlib.sha256();total=0
    while True:
     chunk=stream.read(65536)
     if not chunk:break
     total+=len(chunk)
     if total>1024*1024:raise CompatibilityUnavailable('Loaded plugin callable source capacity')
     digest.update(chunk)
   if task.time_limit is not None or task.soft_time_limit is not None:raise CompatibilityUnavailable('Plugin task time limits require an owned worker signal adapter')
   from celery.backends.redis import RedisBackend
   if not isinstance(task.backend,RedisBackend):raise CompatibilityUnavailable('Registered task custom backend requires an owned native adapter before publication')
   result[name]={'name':name,'plugin_key':plugin.key,'root':str(root),'source':str(path),'sha256':digest.hexdigest(),'actual_pid':os.getpid(),'policy':{'acks_late':bool(task.acks_late),'acks_on_failure_or_timeout':bool(task.acks_on_failure_or_timeout),'reject_on_worker_lost':bool(task.reject_on_worker_lost),'ignore_result':bool(task.ignore_result),'track_started':bool(task.track_started)}}
 return result

def seal():
 records=registered()
 reply=current().call('jobs','register_loaded_tasks',{'tasks':list(records.values())})
 witness=reply.get('witness')if isinstance(reply,dict)else None
 if not isinstance(witness,str)or not witness or len(witness)>512:raise CompatibilityUnavailable('Actual native task registration witness missing')
 return witness,records

def execute(value):
 # Entire request must be an already claimed native task invocation with an
 # exact loaded-module witness. No task.run()/apply() or eager fallback exists.
 import base64
 from celery.app.trace import trace_task_ret
 from dispatcharr.celery import app
 witness,records=seal();name=value['name']
 if name not in records:raise CompatibilityUnavailable('Task callable is not owned by an actual loaded plugin')
 permit=current().call('jobs','validate_trace_invocation',{'witness':witness,'name':name,'task_id':value['id'],'invocation':value['invocation']})
 if permit is not True:raise CompatibilityUnavailable('Actual claimed task invocation ownership missing')
 from celery.backends.redis import RedisBackend
 task=app.tasks[name]
 if not isinstance(task.backend,RedisBackend):raise CompatibilityUnavailable('Registered task custom backend requires an owned native adapter')
 raw=base64.b64decode(value['body_base64'],validate=True)
 if len(raw)>1024*1024:raise CompatibilityUnavailable('Plugin task body capacity')
 request=value['request'];request['hostname']=__import__('celery.utils.nodenames',fromlist=['default_nodename']).default_nodename(None)
 if not isinstance(request,dict)or request.get('id')!=value['id']or request.get('task')!=name:raise CompatibilityUnavailable('Plugin task request identity mismatch')
 # Actual Celery serializer, request/Context, signals, before/after hooks,
 # callbacks/chains and backend/error processing remain its pinned code.
 import socket
 failed,_actual_result,elapsed=trace_task_ret(name,value['id'],request,raw,value['content_type'],value['content_encoding'],app=app,hostname=__import__('celery.utils.nodenames',fromlist=['default_nodename']).default_nodename(None))
 # The result is NOT reconstructed here. Native must settle/read its actual
 # backend publication(s), including legitimate ignore_result/Retry/Ignore.
 key=task.backend.get_key_for_task(value['id']);actual=task.backend.client.get(key)
 if actual is not None and not isinstance(actual,bytes):raise CompatibilityUnavailable('Actual backend result must retain raw bytes')
 observed=None if actual is None else task.backend.decode_result(actual)
 exception=getattr(_actual_result,'exception',None)if failed else None
 if exception is not None:exception=getattr(exception,'exc',exception)
 from celery.exceptions import Ignore,Reject,Retry
 kind='ignore'if isinstance(exception,Ignore)else'reject'if isinstance(exception,Reject)else'retry'if isinstance(exception,Retry)else'failure'if failed else'success'
 return {'trace_reported_failure':bool(failed),'elapsed':elapsed,'task_id':value['id'],'witness':witness,'actual_pid':os.getpid(),'ignore_result':bool(task.ignore_result or request.get('ignore_result')),'actual_backend_key_base64':base64.b64encode(key).decode('ascii'),'actual_backend_raw_base64':None if actual is None else base64.b64encode(actual).decode('ascii'),'actual_backend_status':None if observed is None else observed.get('status'),'actual_backend_task_id':None if observed is None else observed.get('task_id'),'actual_trace_kind':kind,'actual_reject_requeue':bool(exception.requeue)if isinstance(exception,Reject)else None,'actual_acks_late_after':bool(task.acks_late),'actual_acks_on_failure_after':bool(task.acks_on_failure_or_timeout)}


def announce_revoked(value):
 from dispatcharr.celery import app
 from celery.app.task import Context
 from celery.backends.redis import RedisBackend
 witness,records=seal();name=value['name']
 if name not in records:raise CompatibilityUnavailable('Revoked task callable is not registered')
 permit=current().call('jobs','validate_trace_invocation',{'witness':witness,'name':name,'task_id':value['id'],'invocation':value['invocation']})
 if permit is not True:raise CompatibilityUnavailable('Revoked task actual admission missing')
 task=app.tasks[name]
 if not isinstance(task.backend,RedisBackend):raise CompatibilityUnavailable('Custom backend revocation adapter unavailable')
 request=revoked_context(value)
 reason='expired'if value['expired']else'revoked'
 # Source Request.store_errors: not ignore_result OR store_errors_even_if_ignored.
 ignore=request.get('ignore_result')
 if ignore is None:ignore=task.ignore_result
 store=not ignore or task.store_errors_even_if_ignored
 task.backend.mark_as_revoked(value['id'],reason,request=request,store_result=store)
 # Celery sends revoked signal AFTER acknowledge. Rust emits that signal only
 # via the subsequent actual-ACK callback, never before backend settlement.
 key=task.backend.get_key_for_task(value['id']);actual=task.backend.client.get(key)
 return {'task_id':value['id'],'actual_backend_stored':actual is not None,'store_errors':bool(store),'actual_backend_raw_base64':None if actual is None else __import__('base64').b64encode(actual).decode('ascii'),'reason':reason}


def signal_revoked(value):
 from dispatcharr.celery import app
 from celery.app.task import Context
 from celery.signals import task_revoked
 witness,records=seal();name=value['name']
 if name not in records:raise CompatibilityUnavailable('Revoked signal callable is not registered')
 permit=current().call('jobs','validate_trace_invocation',{'witness':witness,'name':name,'task_id':value['id'],'invocation':value['invocation']})
 if permit is not True:raise CompatibilityUnavailable('Actual post-ACK revocation admission missing')
 request=revoked_context(value)
 task_revoked.send(app.tasks[name],request=request,terminated=False,signum=None,expired=bool(value['expired']))
 return {'task_id':value['id'],'actual_revoked_signal_sent':True}


def revoked_context(value):
 import base64,socket
 from celery.app.task import Context
 from kombu.serialization import loads
 raw=base64.b64decode(value['body_base64'],validate=True)
 args,kwargs,embed=loads(raw,'application/json','utf-8',accept={'application/json'})
 request=dict(value['request']);request.update(args=args,kwargs=kwargs,hostname=__import__('celery.utils.nodenames',fromlist=['default_nodename']).default_nodename(None));request.update(embed or {})
 return Context(request)
