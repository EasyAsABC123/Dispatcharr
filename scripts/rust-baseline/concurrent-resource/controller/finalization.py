"""Independent cleanup attempts with terminal proof even after earlier failures."""
import time
from supervision import terminal

def finish(report,output,window,registry,audit,logs,down,inventory,limits):
 registry.close_admission();started=time.monotonic()
 def stop_window():
  if window is None:return {'started':False,'drained':True}
  value=window.close();report['resource_window']=value
  if not value['observer_threads_drained']:raise RuntimeError('ObserverUndrained')
  return value
 def drain():
  with registry.lock:items=[x for x in registry.active.values()if x is not None]
  for item in items:registry.retire(item)
  with registry.lock:pending=bool(registry.active)
  value=registry.drain(limits['final_join_seconds'],escalate=pending);report['physical_process_cleanup']=value
  if not value['drained']:raise RuntimeError('OwnedProcessesUndrained')
  return value
 def default():
  value=audit();report['default_database_unchanged']=value
  if value is None:raise RuntimeError('DefaultDatabaseAuditNotRun')
  if value is not True:raise RuntimeError('DefaultDatabaseChanged')
  return value
 def check_inventory(kind):
  value=inventory(kind);report.setdefault('cleanup',{})[kind]=value
  if value:raise RuntimeError('OwnedObjectsRemain:'+kind)
  return value
 actions=[('observer_stop_join',stop_window),('physical_process_drain',drain),('default_database_audit',default),('logs',logs),('compose_down',down)]
 actions.extend((kind,lambda kind=kind:check_inventory(kind))for kind in ['containers','networks','volumes'])
 value=terminal(report,output,actions)
 value['cleanup_wall_seconds']=time.monotonic()-started
 value['cleanup_wall_quality_ceiling_seconds']=limits['cleanup_wall_quality_ceiling_seconds']
 if value['cleanup_wall_seconds']>limits['cleanup_wall_quality_ceiling_seconds']:value['passed']=False;value['cleanup_deadline_exceeded']=True
 # A final atomic checkpoint includes post-action duration and persistence status.
 terminal(value,output,[])
 return value
