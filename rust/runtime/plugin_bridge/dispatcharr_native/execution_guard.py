"""Ordinary-entry guard and bounded actual Source execution-origin audit.
This is a compatibility boundary, not an adversarial-code sandbox. No argument,
SQL, credential, user return value or fabricated stack is recorded.
"""
import ast,os,sys,threading
from pathlib import Path
from .wire import CompatibilityUnavailable
_root=None
_backend_codes=set()
_source_task_sites=set()
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
 if code in _backend_codes or(filename,code.co_name,code.co_firstlineno)in _source_task_sites:
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
 # Index immutable Original task definitions BEFORE importing any application
 # module. Late imports cannot introduce an unfenced backend body between user
 # callback entry and the next registry/discovery refresh. No module is executed.
 def decorator_name(node):
  if isinstance(node,ast.Call):return decorator_name(node.func)
  return node.id if isinstance(node,ast.Name)else node.attr if isinstance(node,ast.Attribute)else ''
 files=sorted(Path(_root).rglob('*.py'))
 if len(files)>1211:raise CompatibilityUnavailable('Original task-body index file capacity')
 for path in files:
  if '__pycache__'in path.parts:continue
  data=path.read_bytes()
  if len(data)>4*1024*1024:raise CompatibilityUnavailable('Original task-body index source capacity')
  module=ast.parse(data,filename=str(path))
  for node in ast.walk(module):
   if isinstance(node,(ast.FunctionDef,ast.AsyncFunctionDef))and any(decorator_name(d)in('shared_task','task')for d in node.decorator_list):
    first=min([node.lineno,*[d.lineno for d in node.decorator_list]])
    _source_task_sites.add((str(path),node.name,first))
    if len(_source_task_sites)>256:raise CompatibilityUnavailable('Original task-body index capacity')
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
 return{'actual_pid':os.getpid(),'execution_audit_enabled':_enabled,'entrypoint_guard_installed':_root is not None,'registered_backend_code_guards':len(_backend_codes),'immutable_source_task_body_guards':len(_source_task_sites),'actual_source_calls':rows(_calls),'actual_blocked_calls':rows(_blocked),'security_sandbox_claimed':False}
