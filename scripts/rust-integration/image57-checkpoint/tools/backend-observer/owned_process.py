"""Exact container child PID/starttime/nonce ownership, no app methods replaced."""
import json,os,re,signal,sys,time
from pathlib import Path
if os.environ.get('SOURCE_CAPTURE_FIXTURE')!='playback-fixture-only':raise RuntimeError('DisposableOnly')
action,role=sys.argv[1:3];nonce=os.environ['CAPTURE_NONCE']
if role not in ['source-redis','native-1','native-2','native-3','native-4','native-5','native-6','native-7','native-8','native-9','native-10','native-11','native-http']:raise ValueError('FixedOwnedRole')
p=Path('/evidence/owned-'+role+'.json')
def stat(pid):
 raw=Path('/proc',str(pid),'stat').read_text();parts=raw.rsplit(')',1)[1].split();return parts[19],parts[0]
if action=='start':
 if p.exists():raise ValueError('FreshOwnedPIDRecord')
 p.write_text(json.dumps({'pid':os.getpid(),'starttime':stat(os.getpid())[0],'role':role,'nonce':nonce}));os.execvp(sys.argv[3],sys.argv[3:])
elif action=='stop':
 v=json.loads(p.read_text());pid=v['pid']
 if type(pid)is not int or pid<=1 or v['role']!=role or v['nonce']!=nonce:raise ValueError('OwnedPIDIdentity')
 try:
  start,state=stat(pid)
  if start!=v['starttime']:raise ValueError('ReusedPIDNotOwned')
  os.kill(pid,signal.SIGTERM)
  deadline=time.monotonic()+10
  while time.monotonic()<deadline:
   try:
    current,state=stat(pid)
    if current!=start:raise ValueError('PIDReusedBeforeAcknowledgment')
    if state=='Z':break
   except FileNotFoundError:state='absent';break
   time.sleep(.05)
  else:raise TimeoutError('OwnedChildStopUnacknowledged')
 except FileNotFoundError:state='absent'
 print(json.dumps({'role':role,'pid':pid,'same_owned_starttime':v['starttime'],'terminal_state':state,'physical_process_stop_observed':True}))
else:raise ValueError('FixedOwnedAction')
