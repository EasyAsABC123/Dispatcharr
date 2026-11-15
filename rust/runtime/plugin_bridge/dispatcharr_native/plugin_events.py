"""Scoped native-task context plus actual plugin hook framework semantics.
Connect subscriptions run before this port in native orchestration. No Original
Connect dispatcher/backend task body is invoked here. Authors retain full credit.
"""
import base64,logging,socket
from .wire import current,CompatibilityUnavailable
_active=None

def begin(value):
 global _active
 if _active is not None:raise CompatibilityUnavailable('Native task hook context already active')
 from dispatcharr.celery import app
 from celery.app.task import Context
 from celery._state import push_current_task
 from kombu.serialization import loads
 frame=value['frame'];name=frame['headers']['task'];task=app.tasks[name]
 args,kwargs,embed=loads(base64.b64decode(frame['body'],validate=True),'application/json','utf-8',accept={'application/json'})
 request=dict(frame['headers']);properties=frame['properties'];delivery=properties.get('delivery_info')or{}
 request.update(properties=properties,reply_to=properties.get('reply_to'),correlation_id=properties.get('correlation_id'),hostname=__import__('celery.utils.nodenames',fromlist=['default_nodename']).default_nodename(None),args=args,kwargs=kwargs,called_directly=False,is_eager=False,delivery_info={'exchange':delivery.get('exchange'),'routing_key':delivery.get('routing_key'),'priority':properties.get('priority'),'redelivered':delivery.get('redelivered',False)})
 request.update(embed or {});context=Context(request)
 push_current_task(task);task.request_stack.push(context)
 _active={'task':task,'context':context,'params':None,'event':None,'payload':None}
 return{'task_id':context.id,'task_name':name}

def append_native(value):
 if _active is None:raise CompatibilityUnavailable('Native task hook context missing')
 from celery.result import result_from_tuple
 from dispatcharr.celery import app
 for item in value['children']:_active['context'].children.append(result_from_tuple(item,app=app))
 return None

def begin_event(value):
 if _active is None or _active['event']is not None:raise CompatibilityUnavailable('Native event context unavailable or already active')
 if value['event']not in('epg_error','epg_refresh'):raise CompatibilityUnavailable('Native event outside EPG hook slice')
 _active['event']=value['event'];_active['payload']=value['payload'];_active['params']=None;_active['plugin_dispatched']=False
 return None

def event_payload():
 if _active is None or _active['event']is None:raise CompatibilityUnavailable('Native event context missing')
 return _active['payload']

def dispatch(value):
 if _active is None or _active['event']is None:raise CompatibilityUnavailable('Native event context missing')
 if _active.get('plugin_dispatched'):raise CompatibilityUnavailable('Actual event plugin phase already dispatched')
 _active['plugin_dispatched']=True
 # Source creates this AFTER Connect. Payload is the SAME Python object that
 # Template(Context(payload)) may already have mutated, including SafeString.
 _active['params']={'event':_active['event'],'payload':_active['payload']}
 return current().call('plugin_events','dispatch',{})

def end_event():
 if _active is None or _active['event']is None:raise CompatibilityUnavailable('Native event context missing')
 _active['event']=None;_active['payload']=None;_active['params']=None
 return None

def nested(method,args):
 if _active is None or _active['event']is None:raise CompatibilityUnavailable('Actual event dispatch context missing')
 from apps.plugins.loader import PluginManager
 pm=PluginManager.get()
 if method=='event_plan':
  from apps.plugins.models import PluginConfig
  pm.discover_plugins(sync_db=False,use_cache=True,release_connections=False)
  from .task_registry import seal
  seal()
  handlers=list(pm.iter_actions_for_event(_active['event']))
  keys={key for key,_ in handlers}
  enabled=set(PluginConfig.objects.filter(enabled=True,key__in=keys).values_list('key',flat=True))
  return[{'key':key,'action':action,'enabled':key in enabled}for key,action in handlers]
 if method=='event_invoke_caught':
  try:pm.run_action(args['key'],args['action'],_active['params'])
  except Exception:
   logging.getLogger(__name__).exception('Plugin action failed for event %s on %s action %s',_active['event'],args['key'],args['action'])
   return{'actual_completed':False,'actual_exception_caught':True}
  return{'actual_completed':True,'actual_exception_caught':False}
 raise CompatibilityUnavailable('Unknown event reentry')

def snapshot():
 if _active is None:raise CompatibilityUnavailable('Native task hook context missing')
 return{'task_id':_active['context'].id,'children':[r.as_tuple()for r in _active['context'].children]}

def finish():
 global _active
 if _active is None:raise CompatibilityUnavailable('Native task hook context missing')
 from celery._state import pop_current_task
 try:return snapshot()
 finally:
  _active['task'].request_stack.pop();pop_current_task();_active=None

def render_template(value):
 if _active is None or _active['event']is None:raise CompatibilityUnavailable('Native event context missing for Connect template')
 from django.template import Template,Context
 try:rendered=Template(value['template']).render(Context(_active['payload'])).strip()
 except Exception as error:return{'actual_render_succeeded':False,'actual_exception_class':type(error).__name__,'message':str(error),'current_payload':_active['payload'],'source_json_loads_succeeded':None}
 if len(rendered.encode('utf8'))>262144:raise CompatibilityUnavailable('Native Connect rendered-template response capacity')
 import json
 try:json.loads(rendered)
 except Exception:parsed=False
 else:parsed=True
 return{'actual_render_succeeded':True,'rendered':rendered,'current_payload':_active['payload'],'source_json_loads_succeeded':parsed}

def abort(value):
 global _active
 if _active is None:return{'aborted':True,'actual_context_existed':False}
 if _active['context'].id!=value['task_id']:raise CompatibilityUnavailable('Actual native task context mismatch on abort')
 from celery._state import pop_current_task
 task=_active['task'];context=_active['context'];_active=None
 popped=task.request_stack.pop();current=pop_current_task()
 if popped is not context or current is not task:raise CompatibilityUnavailable('Actual native task stack unsettled on abort')
 return{'aborted':True,'actual_context_existed':True}
