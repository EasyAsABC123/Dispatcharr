"""Finite host-output/command admission, deadlines and independent terminal proof.
No activity on import. Payload accounting excludes immutable archived inputs and
is not a universal Python RSS cap. Original/dependency authors retain credit.
"""
import json,os,subprocess,threading,time
from pathlib import Path

class Capacity(RuntimeError):pass
class Deadline(RuntimeError):pass
class PayloadBudget:
 def __init__(self,total,reserve=0):self.total=total;self.reserve=reserve;self.used=0;self.lock=threading.Lock()
 def charge(self,size,cleanup=False):
  if type(size)is not int or size<0:raise ValueError('PayloadCharge')
  with self.lock:
   maximum=self.total if cleanup else self.total-self.reserve
   if self.used+size>maximum:raise Capacity('HostPayloadCapacity')
   self.used+=size
 def snapshot(self):
  with self.lock:return {'used_bytes':self.used,'maximum_bytes':self.total,'cleanup_reserved_bytes':self.reserve}

class WorkDeadline:
 def __init__(self,seconds,clock=time.monotonic):self.clock=clock;self.started=clock();self.deadline=self.started+seconds
 def remaining(self,requested):
  left=self.deadline-self.clock()
  if left<=0:raise Deadline('FixtureWorkDeadline')
  return min(requested,left)
 def check(self):self.remaining(1)

class Registry:
 def __init__(self,collector,environment,output,stop_owned,limits,budget,deadline=None):
  self.collector=collector;self.env=environment;self.output=Path(output);self.stop_owned=stop_owned;self.limits=limits;self.budget=budget;self.deadline=deadline
  self.lock=threading.Lock();self.stop_lock=threading.Lock();self.index=0;self.active={};self.closing=False;self.stop_ack=False;self.stop_error=None;self.stop_attempted=False;self.retired=[]
 def stop(self):
  with self.stop_lock:
   if self.stop_ack:return
   if self.stop_attempted:raise RuntimeError('OwnedStopPreviouslyUnacknowledged')
   self.stop_attempted=True
   try:self.stop_owned();self.stop_ack=True
   except BaseException as error:self.stop_error=type(error).__name__;raise
 def stop_control(self,argv,**kwargs):
  # Fixed-format emergency Docker state commands share the same command index,
  # count and cleanup payload reservation. No unbounded log reply is requested.
  with self.lock:
   if self.index>=self.limits['maximum_commands']+self.limits['cleanup_reserved_commands']or len(self.active)>=self.limits['maximum_live_commands']+self.limits['cleanup_reserved_live_commands']:raise Capacity('HostCommandCapacity')
   self.index+=1;number=self.index;self.active[number]=None
  state={'command':number,'fixed_format_stop_control':True,'drained':False,'operation_successful':False}
  try:
   result=subprocess.run(argv,**kwargs)
   if len(result.stdout)>8192:raise Capacity('OwnedStopReplyCapacity')
   self.budget.charge(len(result.stdout),True);state.update(drained=True,operation_successful=True,output_bytes=len(result.stdout));return result
  except subprocess.CalledProcessError:
   state['drained']=True;raise
  finally:
   with self.lock:self.active.pop(number,None);self.retired.append(state)
 def launch(self,argv,body=None,limit=8388608,stdout_path=None,stderr_path=None,cleanup=False):
  if len(argv)>256 or sum(len(os.fsencode(arg))for arg in argv)>65536:raise Capacity('HostArgvCapacity')
  if body is not None:
   if len(body)>1048576:raise Capacity('HostInputCapacity')
  with self.lock:
   cap=self.limits['maximum_commands']+(self.limits['cleanup_reserved_commands']if cleanup else 0)
   slots=self.limits['maximum_live_commands']+(self.limits['cleanup_reserved_live_commands']if cleanup else 0)
   if(self.closing and not cleanup)or self.index>=cap or len(self.active)>=slots:raise Capacity('HostCommandCapacity')
   if body is not None:self.budget.charge(len(body),cleanup)
   self.index+=1;number=self.index;self.active[number]=None
  try:
   if self.deadline is not None and not cleanup:self.deadline.check()
   charge=lambda size:self.budget.charge(size,cleanup)
   with self.stop_lock:
    # A concurrent stop cannot acknowledge an empty project between admission
    # and publication of a newly started CLI owner.
    item=self.collector(argv,self.env,limit,self.stop,stdout_path=stdout_path,stderr_path=stderr_path or self.output/('command-'+str(number)+'.stderr'),body=body,retain=charge)
    with self.lock:self.active[number]=item
   return item
  except BaseException:
   with self.lock:self.active.pop(number,None)
   raise
 def retire(self,item):
  state=item.settlement()
  if state['drained']:
   item._close_files()
   with self.lock:
    for number,value in list(self.active.items()):
     if value is item:self.active.pop(number);self.retired.append({'command':number,**state})
  return state
 def run(self,argv,body=None,timeout=180,cleanup=False):
  if self.deadline is not None and not cleanup:timeout=self.deadline.remaining(timeout)
  item=self.launch(argv,body=body,cleanup=cleanup)
  try:return item.wait(timeout)
  finally:self.retire(item)
 def close_admission(self):
  with self.lock:self.closing=True
 def drain(self,seconds,escalate=True):
  self.close_admission();deadline=time.monotonic()+seconds
  if escalate:
   try:self.stop()  # Acknowledged exact-owned stop precedes CLI termination/join.
   except BaseException:pass  # Persist explicit undrained status; never fake ACK.
   if self.stop_ack:
    with self.lock:items=[v for v in self.active.values()if v is not None]
    for item in items:
     try:item.abort('TerminalCleanup')
     except BaseException:pass
  while True:
   with self.lock:items=list(self.active.values())
   for item in items:
    if item is not None:self.retire(item)
   with self.lock:remaining=list(self.active.items())
   if not remaining or time.monotonic()>=deadline:break
   time.sleep(.01)
  states=[{'command':n,'starting':v is None,**(v.settlement()if v is not None else{'drained':False})}for n,v in remaining]
  return {'drained':not states and (not escalate or self.stop_ack),'host_clients_drained':not states,'remaining':states,'owned_stop_acknowledged':self.stop_ack,'owned_stop_error':self.stop_error,'commands_started':self.index}

def atomic_json(path,value):
 path=Path(path);data=(json.dumps(value,indent=2,default=str)+'\n').encode();temp=path.with_name(path.name+'.tmp')
 with temp.open('wb')as stream:stream.write(data);stream.flush();os.fsync(stream.fileno())
 os.replace(temp,path)

def terminal(report,output,actions,writer=atomic_json):
 """Every cleanup action independently attempted; primary/fallback proof persisted."""
 output=Path(output);outcomes={};failed=False
 for name,action in actions:
  try:outcomes[name]={'ok':True,'value':action()}
  except BaseException as error:failed=True;outcomes[name]={'ok':False,'error_type':type(error).__name__,'message':str(error)[:1024]}
  report['terminal_actions']=dict(outcomes)
  if failed:report['passed']=False
  # Persist each completed attempt; later failures never erase earlier evidence.
  try:writer(output/'cleanup-checkpoint.json',report)
  except BaseException as error:report['cleanup_checkpoint_error']=type(error).__name__
 if failed:report['passed']=False
 try:writer(output/'integration.json',report);report['terminal_persistence']='integration.json'
 except BaseException as error:
  report['passed']=False;report['terminal_persistence_error']=type(error).__name__
  try:writer(output/'terminal-fallback.json',report);report['terminal_persistence']='terminal-fallback.json'
  except BaseException as fallback:report['terminal_persistence']='UNPERSISTED';report['terminal_fallback_error']=type(fallback).__name__
 return report
