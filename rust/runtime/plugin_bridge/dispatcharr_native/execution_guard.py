"""Ordinary-entry guard and bounded actual Source execution-origin audit.
This is a compatibility boundary, not an adversarial-code sandbox. No argument,
SQL, credential, user return value or fabricated stack is recorded.
"""
import os,sys,threading
from pathlib import Path
from .wire import CompatibilityUnavailable
_root=None
_backend_codes=set()
_calls={}
_blocked={}
_enabled=False
MAX_ORIGINS=4096

def forbid_backend_callable(function):
 import inspect
 seen=set()
 while function is not None and id(function)not in seen:
  seen.add(id(function));code=getattr(function,'__code__',None)
  if code is not None:_backend_codes.add(code)
  function=getattr(function,'__wrapped__',None)

def _record(target,code):
 key=(code.co_filename,code.co_qualname,code.co_firstlineno)
 if key not in target and len(target)>=MAX_ORIGINS:
  os.write(2,b'Original execution-origin audit capacity: owned host retired\n');os._exit(79)
 target[key]=target.get(key,0)+1

def profile(frame,event,arg):
 if event!='call':return
 code=frame.f_code;filename=code.co_filename
 if code in _backend_codes:
  _record(_blocked,code)
  # Raising from a profiler disables it in CPython. Fail the actual owned
  # process closed instead, never permit a catch-and-retry to run that body.
  os.write(2,b'Original backend task body guard: owned host retired before body\n');os._exit(78)
 if _root is None or not filename.startswith(_root):return
 relative=filename[len(_root):];parts=relative.split('/');name=parts[-1]
 if name in('api_views.py','views.py','api_urls.py','urls.py','wsgi.py','asgi.py','manage.py')or'management'in parts or'views'in parts:
  _record(_blocked,code)
  os.write(2,b'Original HTTP/management entry guard: owned host retired before body\n');os._exit(78)
 if _enabled:_record(_calls,code)

def install(root):
 global _root,_enabled
 if _root is not None:raise CompatibilityUnavailable('Original entrypoint guard already installed')
 _root=str(Path(root).resolve())+os.sep;_enabled=os.environ.get('RUST_PLUGIN_EXECUTION_AUDIT')=='1'
 # Cover the current initializer plus subsequently created action/task threads.
 sys.setprofile(profile);threading.setprofile(profile)
 # These entrypoints are not needed for model/compiler/plugin compatibility.
 import django.core.management
 def forbidden(*args,**kwargs):raise CompatibilityUnavailable('Original management entrypoint requires an explicit native operation')
 django.core.management.call_command=forbidden
 django.core.management.execute_from_command_line=forbidden

def snapshot():
 def rows(values):
  return[{'path':str(Path(filename).relative_to(_root))if filename.startswith(_root)else filename,'qualified_name':name,'line':line,'actual_calls':count}for(filename,name,line),count in sorted(values.items())]
 return{'actual_pid':os.getpid(),'execution_audit_enabled':_enabled,'entrypoint_guard_installed':_root is not None,'registered_backend_code_guards':len(_backend_codes),'actual_source_calls':rows(_calls),'actual_blocked_calls':rows(_blocked),'security_sandbox_claimed':False}
