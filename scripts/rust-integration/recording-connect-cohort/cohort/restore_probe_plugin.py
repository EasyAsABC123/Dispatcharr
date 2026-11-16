"""Owned USER plugin observing real restore task discovery/child SQL behavior.
Original Dispatcharr/Celery/Django authors retain their application/framework credit.
"""
import json,os,uuid
from celery import shared_task,current_task
from celery.signals import worker_process_init,task_prerun,task_postrun
_MODULE_IMPORT_NONCE=uuid.uuid4().hex
from django.db import connection

def identity():
 with connection.cursor()as c:
  c.execute('SELECT pg_backend_pid(),backend_start::text FROM pg_stat_activity WHERE pid=pg_backend_pid()');row=c.fetchone()
 return {'pid':row[0],'backend_start':row[1],'autocommit':connection.get_autocommit(),'in_atomic_block':connection.in_atomic_block}
def context():
 task=current_task
 if not task:return None
 r=task.request
 return {'name':task.name,'id':r.id,'parent_id':r.parent_id,'root_id':r.root_id,'is_eager':r.is_eager,'called_directly':r.called_directly,'children':[x.as_tuple()for x in r.children]}
def mark(phase,payload):
 value={'SQL':identity(),'current_task':context(),'worker_pid':os.getpid(),'module_nonce':_MODULE_IMPORT_NONCE,'module_name':__name__,**payload}
 with connection.cursor()as c:c.execute('INSERT INTO owned_restore_probe(phase,payload)VALUES(%s,%s::jsonb)',[phase,json.dumps(value)])
 return value
@shared_task(bind=True,name='owned_restore_probe.physical_child')
def physical_child(self,parent):
 return mark('physical_child',{'expected_parent':parent})
class Plugin:
 name='Owned restore probe';version='1';description='Actual restore discovery characterization';author='Original authors retain application credit';actions=[]
 def __init__(self):
  mark('constructor_seen',{'restore_scope':bool(current_task and current_task.name=='apps.backups.tasks.restore_backup_task')})
  if not current_task or current_task.name!='apps.backups.tasks.restore_backup_task':return
  before=mark('constructor_begin',{})
  result=physical_child.delay(before['current_task']['id'])
  mark('constructor_child_published',{'actual_child_id':result.id})
  try:
   value=result.get(timeout=8,disable_sync_subtasks=False)
  except Exception as error:
   mark('constructor_child_get_caught',{'actual_child_id':result.id,'actual_exception_class':type(error).__name__})
  else:mark('constructor_child_joined',{'actual_child_id':result.id,'actual_result':value})
  mark('constructor_end',{})

@worker_process_init.connect(weak=False)
def observed_worker_process_init(**_):
 # Ordinary user signal receiver runs after pinned Source's already-registered
 # init receiver; no private manager state or Source function is replaced.
 mark('worker_process_init_seen',{})
# Observation only: retain booleans/metadata across the destructive restore,
# not receiver functions, so the probe does not make a weak receiver live.
_RESTORE_LIVENESS_BEFORE={}
def post_migrate_receiver_liveness():
 import weakref
 from django.conf import settings
 from django.db.models.signals import post_migrate
 matches=[]
 for row in list(post_migrate.receivers):
  if row[0][0]!='apps.plugins.post_migrate_discover':continue
  reference=row[1];weak=isinstance(reference,weakref.ReferenceType)
  matches.append({'weak_reference':weak,'alive':reference()is not None if weak else True})
 return{'dispatch_uid':'apps.plugins.post_migrate_discover','Django_DEBUG':bool(settings.DEBUG),'matching_entries':matches,'receiver_alive':any(x['alive']for x in matches),'receiver_function_retained':False,'private_cache_cleared':False,'signal_dispatched_by_probe':False}

@task_prerun.connect(weak=False,dispatch_uid='owned_restore_probe.liveness_before')
def observed_restore_prerun(task_id=None,task=None,**_):
 if task is None or task.name!='apps.backups.tasks.restore_backup_task':return
 if len(_RESTORE_LIVENESS_BEFORE)>=8:raise RuntimeError('OwnedRestoreLivenessCapacity')
 _RESTORE_LIVENESS_BEFORE[task_id]=post_migrate_receiver_liveness()

@task_postrun.connect(weak=False,dispatch_uid='owned_restore_probe.liveness_after')
def observed_restore_postrun(task_id=None,task=None,**_):
 if task is None or task.name!='apps.backups.tasks.restore_backup_task':return
 before=_RESTORE_LIVENESS_BEFORE.pop(task_id,None)
 from apps.plugins.models import PluginConfig
 cfg=PluginConfig.objects.filter(key='owned_restore_probe').values('enabled','ever_enabled').first()
 mark('post_migrate_receiver_liveness',{'observed_task_id':task_id,'before_restore_body':before,'after_restore_body':post_migrate_receiver_liveness(),'restored_plugin_config':cfg})

mark('module_import_seen',{})
