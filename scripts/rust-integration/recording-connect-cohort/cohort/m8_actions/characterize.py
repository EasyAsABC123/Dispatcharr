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
