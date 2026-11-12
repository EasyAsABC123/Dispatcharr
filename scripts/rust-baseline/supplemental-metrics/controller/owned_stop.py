"""Stop/verify only generated project; no CLI or Docker action on import.
Responses are fixed-format IDs/state tuples, never arbitrary logs/inspection.
"""
import subprocess,time

def stop_project(compose,environment,project,seconds,run=subprocess.run,clock=time.monotonic):
 if not project.startswith('dispatcharr-m0-concurrent-')or len(project)!=len('dispatcharr-m0-concurrent-')+10 or any(c not in'0123456789abcdef'for c in project[-10:]):raise ValueError('OwnedStopBinding')
 until=clock()+seconds
 def command(argv):
  left=until-clock()
  if left<=0:raise TimeoutError('OwnedStopDeadline')
  value=run(argv,env=environment,stdin=subprocess.DEVNULL,stdout=subprocess.PIPE,stderr=subprocess.DEVNULL,timeout=left,check=True).stdout
  if len(value)>8192:raise ValueError('OwnedStopReplyCapacity')
  return value
 def identifiers():
  ids=command(compose+['ps','--all','-q']).decode().split()
  if len(ids)>4 or len(set(ids))!=len(ids)or any(len(cid)!=64 or any(c not in'0123456789abcdef'for c in cid)for cid in ids):raise ValueError('OwnedStopIdentity')
  return ids
 before=identifiers();command(compose+['kill']);after=identifiers()
 for cid in set(before+after):
  state=command(['docker','inspect','--format','{{index .Config.Labels "com.docker.compose.project"}} {{.State.Running}} {{.State.Pid}}',cid]).decode().strip().split()
  if state!=[project,'false','0']:raise RuntimeError('OwnedStopUnacknowledged')
 return {'container_count':len(set(before+after)),'all_stopped_PID_zero':True,'inspection_scope':'exact generated project IDs before and after kill; fixed state format'}
