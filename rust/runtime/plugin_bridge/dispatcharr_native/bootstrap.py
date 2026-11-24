"""Lazy actual user-plugin execution, never a Source HTTP/Celery backend loop.
Action invocations multiplex within the existing four-writer admission; task
interpreters execute one claimed task each. All SQL/cache/jobs/events use owned
native ports. Original Dispatcharr/CPython/dependency authors retain full credit.
"""
import argparse,base64,os,sys,threading
from pathlib import Path

def main():
 parser=argparse.ArgumentParser();parser.add_argument('--source',required=True);parser.add_argument('--plugins',required=True);parser.add_argument('--mode',choices=('plugin','regex'),default='plugin');parser.add_argument('--role',choices=('actions','tasks'),default='actions');args=parser.parse_args()
 reader=os.fdopen(os.dup(0),'rb',buffering=0);writer=os.fdopen(os.dup(1),'wb',buffering=0)
 null=os.open(os.devnull,os.O_RDONLY);os.dup2(null,0);os.close(null);os.dup2(2,1)
 sys.stdin=open(os.devnull);sys.stdout=sys.stderr;sys.executable='/usr/local/bin/dispatcharr-python'
 sys.path.insert(0,str(Path(__file__).resolve().parent.parent))
 from dispatcharr_native.wire import Channel,install,operation,CompatibilityUnavailable
 channel=Channel(reader,writer);install(channel)
 from dispatcharr_native.plugin_lifecycle import nested as lifecycle_nested
 from dispatcharr_native.plugin_events import nested as event_nested
 channel.reentry=lambda method,value:event_nested(method,value)if method.startswith('event_')else lifecycle_nested(method,value)
 if args.mode=='regex':
  from dispatcharr_native.regex_only import serve
  serve(channel);return
 sys.path.insert(0,str(Path(args.source).resolve()))
 if os.environ.get('DISPATCHARR_NATIVE_PORTS_READY')!='sql,cache,jobs,events,process':raise CompatibilityUnavailable('Native production adapters are unavailable')
 from dispatcharr_native.execution_guard import install as install_execution_guard
 install_execution_guard(args.source)
 os.environ['DJANGO_SETTINGS_MODULE']='dispatcharr_native.settings';os.environ['DISPATCHARR_PLUGINS_DIR']=str(Path(args.plugins).resolve());os.environ['DISPATCHARR_SKIP_PLUGIN_AUTODISCOVERY']='1';sys.argv=['dispatcharr-plugin-host','shell']
 from dispatcharr_native.transports import install_all
 install_all()
 from celery._state import _set_task_join_will_block
 _set_task_join_will_block(args.role=='tasks')
 import django
 django.setup()
 from django.db import connections
 from apps.plugins import loader as plugin_loader
 from apps.plugins.loader import PluginManager
 if args.role=='actions':
  from dispatcharr_native import loaded_module_witness
  loaded_module_witness.instrument(plugin_loader)
 from dispatcharr_native.builtin_tasks import install as guard_builtin_tasks
 guard_builtin_tasks(args.plugins)
 # Startup callbacks borrow the actual admitted startup session. A worker
 # thread subsequently opens its own logical wrapper on its own invocation.
 connections.close_all()
 channel.send({'kind':'ready','actual_pid':os.getpid(),'version':sys.version,'application_server_started':False})
 def invoke(request):
  ident=request['id'];token=operation.set(ident)
  try:
   channel.send({'kind':'operation_started','id':ident,'actual_tid':threading.get_native_id()})
   method=request['method'];value=request['args']
   if method=='run_action':
    if args.role!='actions':raise CompatibilityUnavailable('Task host cannot execute web action')
    result=run_action(value)
   elif method=='observe_taskless_module':
    if args.role!='actions':raise CompatibilityUnavailable('Only the existing action host can attest a loaded module')
    result=loaded_module_witness.observe(PluginManager.get(),value['key'])
   elif method=='execute_plugin_task':
    if args.role!='tasks':raise CompatibilityUnavailable('Action host cannot consume its queued task')
    from dispatcharr_native.task_registry import execute
    result=execute(value)
   elif method=='announce_plugin_revoked':
    if args.role!='tasks':raise CompatibilityUnavailable('Action host cannot announce queued revocation')
    from dispatcharr_native.task_registry import announce_revoked
    result=announce_revoked(value)
   elif method=='signal_plugin_revoked':
    if args.role!='tasks':raise CompatibilityUnavailable('Action host cannot signal queued revocation')
    from dispatcharr_native.task_registry import signal_revoked
    result=signal_revoked(value)
   elif method=='change_enabled':
    if args.role!='actions':raise CompatibilityUnavailable('Task host cannot mutate plugin enabled lifecycle')
    from dispatcharr_native.plugin_lifecycle import change_enabled
    result=change_enabled(value)
   elif method=='delete_enabled':
    if args.role!='actions':raise CompatibilityUnavailable('Task host cannot own enabled deletion')
    from dispatcharr_native.plugin_lifecycle import delete_enabled
    result=delete_enabled(value)
   elif method=='plugin_logo':
    if args.role!='actions':raise CompatibilityUnavailable('Task host cannot serve web plugin logo')
    from dispatcharr_native.plugin_lifecycle import plugin_logo
    result=plugin_logo(value)
   elif method=='list_plugins':
    from dispatcharr_native.plugin_lifecycle import list_plugins
    result=list_plugins(value)
   elif method=='stop_all_plugins':
    if args.role!='actions':raise CompatibilityUnavailable('Task host cannot stop action plugins')
    from dispatcharr_native.plugin_lifecycle import stop_all
    result=stop_all(value)
   elif method=='execution_audit':
    from dispatcharr_native.execution_guard import snapshot
    result=snapshot()
   elif method in('begin_native_task','append_native_trail','dispatch_plugin_event','snapshot_native_task','finish_native_task','render_connect_template','begin_plugin_event','end_plugin_event','plugin_event_payload','abort_native_task'):
    if args.role!='tasks':raise CompatibilityUnavailable('Action host cannot impersonate native task context')
    from dispatcharr_native import plugin_events
    if method=='begin_native_task':result=plugin_events.begin(value)
    elif method=='append_native_trail':result=plugin_events.append_native(value)
    elif method=='dispatch_plugin_event':result=plugin_events.dispatch(value)
    elif method=='abort_native_task':result=plugin_events.abort(value)
    elif method=='begin_plugin_event':result=plugin_events.begin_event(value)
    elif method=='end_plugin_event':result=plugin_events.end_event()
    elif method=='plugin_event_payload':result=plugin_events.event_payload()
    elif method=='render_connect_template':result=plugin_events.render_template(value)
    elif method=='snapshot_native_task':result=plugin_events.snapshot()
    else:result=plugin_events.finish()
   elif method=='stop_plugin':result=PluginManager.get().stop_plugin(value['key'],reason=value['reason'])
   elif method=='discover':
    sync_db=bool(value.get('sync_db',False))
    if args.role=='tasks'and sync_db:raise CompatibilityUnavailable('Task process cannot repeat migration registry sync')
    manager=PluginManager.get();manager.discover_plugins(sync_db=sync_db,force_reload=bool(value.get('force_reload',True)));guard_builtin_tasks(args.plugins)
    from dispatcharr_native.task_registry import seal
    seal();result=manager.list_plugins()
   elif method=='shutdown':result=None
   else:raise CompatibilityUnavailable('Unknown owned plugin method')
   connections.close_all()
   channel.send({'kind':'result','id':ident,'result':result,'error':None})
  except BaseException as error:
   # Actual class/message from the real child, never fabricated Python frames.
   channel.send({'kind':'result','id':ident,'result':None,'error':{'actual_class':type(error).__name__,'message':str(error)}})
  finally:
   connections.close_all();operation.reset(token)
 pool=None
 try:
  if args.role=='actions':
   from concurrent.futures import ThreadPoolExecutor
   pool=ThreadPoolExecutor(max_workers=4,thread_name_prefix='native-plugin-action')
   active=set()
   while True:
    request=channel.calls.get()
    if request is None:break
    active={f for f in active if not f.done()}
    if request['method']=='shutdown':
     for future in active:future.result()
     invoke(request);break
    if len(active)>=4:raise CompatibilityUnavailable('Native action admission exceeds existing Auth4')
    active.add(pool.submit(invoke,request))
  else:
   while True:
    request=channel.calls.get()
    if request is None:break
    invoke(request)
    if request['method']=='shutdown':break
 finally:
  if pool is not None:pool.shutdown(wait=True,cancel_futures=False)
  connections.close_all();channel.close_writer();channel.thread.join(30)
  if channel.thread.is_alive():raise CompatibilityUnavailable('Native RPC reader has not observed actual EOF')

def run_action(value):
 from apps.plugins.loader import PluginManager
 from rest_framework.renderers import JSONRenderer
 try:actual=PluginManager.get().run_action(value['key'],value['action'],value.get('params',{}))
 except PermissionError as error:status=403;body={'success':False,'error':str(error)}
 except Exception as error:status=500;body={'success':False,'error':str(error)}
 else:status=200;body={'success':True,'result':actual}
 try:rendered=JSONRenderer().render(body)
 except Exception as error:return{'source_rendering_failed':True,'actual_exception_class':type(error).__name__}
 return{'status':status,'body_base64':base64.b64encode(rendered).decode('ascii')}


if __name__ == "__main__":
 main()
