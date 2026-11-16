"""Owned USER plugin for physical compatibility acceptance; no backend delegation.
Original Dispatcharr/Celery/Django authors retain application/framework credit.
"""
import time
from celery import shared_task
from celery.exceptions import TimeoutError as ResultTimeout
from django.db import connection,transaction

def mark(label,phase,payload):
 import json
 with connection.cursor()as cursor:
  cursor.execute('INSERT INTO owned_m8_probe(label,phase,backend_pid,backend_start,payload) SELECT %s,%s,pg_backend_pid(),backend_start,%s::jsonb FROM pg_stat_activity WHERE pid=pg_backend_pid()',[label,phase,json.dumps(payload)])

def audit():
 import sys
 guard=sys.modules.get('dispatcharr_native.execution_guard');return guard.snapshot()if guard else None

def identity():
 with connection.cursor()as cursor:
  cursor.execute('SELECT pg_backend_pid(),backend_start::text FROM pg_stat_activity WHERE pid=pg_backend_pid()');row=cursor.fetchone()
 return{'pid':row[0],'backend_start':row[1]}

@shared_task(bind=True,name='owned_m8_actions.physical_child')
def physical_child(self,label,mode):
 actual=identity();mark(label,'child_started',{'identity':actual,'task_id':self.request.id,'parent_id':self.request.parent_id,'root_id':self.request.root_id})
 if mode=='locked':
  with connection.cursor()as cursor:
   cursor.execute("SET lock_timeout='400ms'")
   cursor.execute('UPDATE owned_m8_lock SET value=value+1 WHERE id=1')
 else:
  deadline=time.monotonic()+20
  while True:
   with connection.cursor()as cursor:
    cursor.execute('SELECT released FROM owned_m8_barrier WHERE label=%s',[label]);row=cursor.fetchone()
   if row and row[0]:break
   if time.monotonic()>=deadline:raise TimeoutError('OwnedSQLBarrierDeadline')
   time.sleep(.025)
 mark(label,'child_finished',{'identity':actual});return{**actual,'native_execution_audit':audit()}

class Plugin:
 name='owned_m8_actions';version='1';description='Owned physical regression';author='Original authors retain all application credit'
 actions=[{'id':name,'label':name}for name in('wait','timeout_continue','transaction_rollback','builtin_refresh','inline_characterize')]
 def run(self,action,params,context):
  label=params['label'];parent=identity();mark(label,'parent_started',{'identity':parent,'action':action})
  if action=='inline_characterize':return characterize_inline()
  if action=='builtin_refresh':
   from celery import current_app
   # Real producer/AsyncResult; never import/call the Original task body.
   with connection.cursor()as cursor:
    cursor.execute('SELECT count(*) FROM plugins_pluginrepo WHERE enabled');enabled=cursor.fetchone()[0]
   if enabled:raise AssertionError('Owned disabled repository fence not established')
   result=current_app.send_task('apps.plugins.tasks.refresh_plugin_repos',args=[],kwargs={},add_to_parent=False)
   mark(label,'builtin_published',{'id':result.id,'parent':parent})
   value=result.get(timeout=15)
   after=identity();mark(label,'builtin_parent_continued',{'id':result.id,'after':after,'returned_none':value is None})
   return{'id':result.id,'result':value,'parent':parent,'after':after,'native_execution_audit':audit()}
  if action=='wait':
   result=physical_child.delay(label,'barrier');mark(label,'published',{'id':result.id});value=result.get(timeout=15)
   mark(label,'parent_finished',{'child':value});return{'parent':parent,'child':value,'id':result.id,'native_execution_audit':audit()}
  if action=='timeout_continue':
   result=physical_child.delay(label,'barrier');mark(label,'published',{'id':result.id})
   deadline=time.monotonic()+5
   while True:
    with connection.cursor()as cursor:
     cursor.execute("SELECT EXISTS(SELECT 1 FROM owned_m8_probe WHERE label=%s AND phase='child_started')",[label]);started=cursor.fetchone()[0]
    if started:break
    if time.monotonic()>=deadline:raise TimeoutError('ActualChildDidNotStartBeforeGetTimeoutCase')
    time.sleep(.025)
   if result.ready():raise AssertionError('Actual child already terminal before controlled timeout')
   try:result.get(timeout=.3)
   except ResultTimeout:
    if result.ready():raise AssertionError('Actual child became terminal before parent continuation')
    # Actual same parent SQL MUST resume before blocked child completes.
    mark(label,'actual_get_timeout',{'id':result.id})
    with connection.cursor()as cursor:cursor.execute('UPDATE owned_m8_barrier SET released=true WHERE label=%s',[label])
    mark(label,'parent_released_child',{'id':result.id})
   else:raise AssertionError('Child unexpectedly completed before controlled release')
   value=result.get(timeout=15);return{'parent':parent,'child':value,'id':result.id,'timeout_continuation':True,'native_execution_audit':audit()}
  if action=='transaction_rollback':
   with transaction.atomic():
    with connection.cursor()as cursor:cursor.execute('UPDATE owned_m8_lock SET value=value+100 WHERE id=1')
    result=physical_child.delay(label,'locked')
    try:result.get(timeout=5)
    except Exception as error:
     failure={'class':type(error).__name__,'message':str(error)[:4096]};mark(label,'actual_child_failure',{**failure,'id':result.id})
     transaction.set_rollback(True)
    else:raise AssertionError('Actual parent row lock was bypassed')
   with connection.cursor()as cursor:cursor.execute('SELECT value FROM owned_m8_lock WHERE id=1');value=cursor.fetchone()[0]
   mark(label,'parent_after_rollback',{'value':value});return{'parent':parent,'id':result.id,'value':value,'failure':failure}
  raise ValueError('Unknown owned test action')

"""Owned user-code observation, never a Native Original-body fallback."""
def characterize_inline():
 import sys
 if 'dispatcharr_native.execution_guard'in sys.modules:
  raise RuntimeError('OriginalOnlyUnmeasuredAuthorityCell')
 from apps.plugins.tasks import refresh_plugin_repos as task
 from celery import current_task,signals
 from django.db import connection,connections,transaction
 def state():
  raw=connection.connection;info=getattr(raw,'info',None)
  top=current_task
  return{'connection_is_none':raw is None,'raw_closed':None if raw is None else raw.closed,'autocommit_field':connection.autocommit,'in_atomic_block':connection.in_atomic_block,'closed_in_transaction':connection.closed_in_transaction,'needs_rollback':connection.needs_rollback,'driver_transaction_status':None if info is None else str(info.transaction_status),'current_task':None if not top else{'name':top.name,'id':top.request.id,'is_eager':top.request.is_eager,'called_directly':top.request.called_directly,'parent_id':top.request.parent_id,'root_id':top.request.root_id}}
 def identity():
  try:
   with connection.cursor()as c:
    c.execute('SELECT pg_backend_pid(),backend_start::text,pg_current_xact_id_if_assigned()::text FROM pg_stat_activity WHERE pid=pg_backend_pid()');row=c.fetchone()
   return{'pid':row[0],'backend_start':row[1],'assigned_xid':row[2]}
  except Exception as e:return{'class':type(e).__name__,'message':str(e)[:4096]}
 def outcome(call):
  try:
   value=call()
   if hasattr(value,'state')and hasattr(value,'result'):
    return{'kind':'EagerResult','id':value.id,'state':value.state,'returned_none':value.result is None,'result_class':type(value.result).__name__,'result_message':None if value.result is None else str(value.result)[:4096]}
   return{'kind':'return','returned_none':value is None}
  except Exception as e:return{'kind':'raised','class':type(e).__name__,'message':str(e)[:4096]}
 signals_seen=[]
 def prerun(sender=None,**_):
  if sender.name==task.name:signals_seen.append({'signal':'prerun','state':state()})
 def postrun(sender=None,state=None,**_):
  if sender.name==task.name:signals_seen.append({'signal':'postrun','task_state':state,'connection_state':globals_state()})
 globals_state=state
 signals.task_prerun.connect(prerun,weak=False);signals.task_postrun.connect(postrun,weak=False)
 cases=[]
 try:
  # Each call below is a genuinely unmeasured direct/eager invocation. No prior
  # async task-function fixture is replayed and no task methods are patched.
  for name,call in [('direct-call',lambda:task()),('raw-run',lambda:task.run()),('eager-throw',lambda:task.apply(throw=True)),('eager-caught',lambda:task.apply(throw=False)),('wrong-positional',lambda:task(1)),('wrong-keyword',lambda:task(owned_bad=1))]:
   before=identity();row={'name':name,'before_identity':before,'before':state(),'outcome':outcome(call)}
   row['immediately_after']=state();row['continued_identity']=identity();row['after_continuation']=state();cases.append(row)
  # Actual app task_prerun/postrun may close/reset the parent's transaction.
  # Capture its real branch, exception and recovery; do not assert a guessed
  # InternalError or pretend Source preserves/rolls back its pool connection.
  for name,call in [('atomic-direct',lambda:task()),('atomic-eager-throw',lambda:task.apply(throw=True)),('atomic-eager-caught',lambda:task.apply(throw=False))]:
   row={'name':name,'before_identity':identity()}
   try:
    with transaction.atomic():
     row['atomic_identity']=identity();row['atomic_before']=state();row['outcome']=outcome(call);row['atomic_after']=state();row['inside_continuation']=identity()
     transaction.set_rollback(True)
    row['after_atomic']=state()
   except Exception as e:row['outer_error']={'class':type(e).__name__,'message':str(e)[:4096]}
   row['after_continuation']=identity();row['final']=state();cases.append(row)
  return{'characterization_completed':True,'backend_engine':connection.settings_dict['ENGINE'],'actual_wrapper_class':type(connections['default']).__module__,'conn_max_age':connection.settings_dict['CONN_MAX_AGE'],'cases':cases,'signals':signals_seen,'Native_parity_qualified':False}
 finally:
  signals.task_prerun.disconnect(prerun);signals.task_postrun.disconnect(postrun)
