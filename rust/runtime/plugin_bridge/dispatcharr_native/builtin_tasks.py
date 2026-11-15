"""Fence dependency Task.run from executing Original backend task bodies.
Registered user-plugin callables remain actual Python. Builtin direct/eager
invocation requires a real prepared native service; an absent adapter raises
before the Source backend body, with no Python/function fallback.
Original Celery/Dispatcharr authors retain full algorithm credit.
"""
import base64
from pathlib import Path
from kombu.serialization import dumps,loads
from .wire import current,CompatibilityUnavailable

def install(plugin_root):
 from dispatcharr.celery import app
 configured=Path(plugin_root).resolve(strict=True)
 for name,task in list(app.tasks.items()):
  if getattr(task,'_native_builtin_guard',False):continue
  # Runtime task provenance is independently verified again by Rust. This
  # local distinction only prevents accidental execution of backend bodies.
  import inspect
  try:path=Path(inspect.getsourcefile(task.run)).resolve(strict=True)
  except (TypeError,OSError):path=None
  if path is not None and path.is_relative_to(configured):continue
  from .execution_guard import forbid_backend_callable
  forbid_backend_callable(task.run)
  def invoke(*args,_task_name=name,**kwargs):
   content_type,encoding,payload=dumps([args,kwargs],serializer='json')
   raw=payload.encode(encoding)if isinstance(payload,str)else payload
   if len(raw)>65536:raise CompatibilityUnavailable('Native inline builtin task argument capacity')
   actual=current().call('jobs','inline_builtin',{'task':_task_name,'content_type':content_type,'content_encoding':encoding,'body_base64':base64.b64encode(raw).decode('ascii')})
   if not isinstance(actual,dict)or actual.get('actual_native_execution')is not True:raise CompatibilityUnavailable('Native inline builtin execution is unavailable')
   if actual.get('raised')is not None:raise CompatibilityUnavailable('Native inline builtin raised an unmapped exception')
   body=base64.b64decode(actual['body_base64'],validate=True)
   if len(body)>4*1024*1024:raise CompatibilityUnavailable('Native inline builtin result capacity')
   return loads(body,actual['content_type'],actual['content_encoding'],accept={'application/json'})
  task.run=invoke;task._native_builtin_guard=True
