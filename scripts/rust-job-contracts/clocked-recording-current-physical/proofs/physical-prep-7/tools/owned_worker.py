"""Bounded exact-role prefork process owner. No process is launched on import."""
import os,signal,subprocess,threading,time,datetime
from pathlib import Path
from seed_spec import actual_argv

def identity(pid):
 try:return Path('/proc',str(pid),'stat').read_text().rsplit(')',1)[1].split()[19]
 except FileNotFoundError:return None
class OwnedJob:
 def __init__(self,role,root,environment):
  tag=role
  if role not in ('artwork','dvr','beat'):raise ValueError('OnlyThreeOwnedRoles')
  self.tag=tag;self.role=role;self.limit=2097152;self.used=0;self.invalid=None;self.lock=threading.Lock();self.closed=None;self.stop_requested=False
  self.argv=actual_argv(role)
  self.began_utc=datetime.datetime.now(datetime.timezone.utc).isoformat()
  self.files={key:(root/('worker-'+tag+'-'+key+'.log')).open('xb')for key in('stdout','stderr')};
  try:self.process=subprocess.Popen(self.argv,env=dict(environment,C_FORCE_ROOT='true',PYTHONPATH='/job-tools:/app'),stdin=subprocess.DEVNULL,stdout=subprocess.PIPE,stderr=subprocess.PIPE,start_new_session=True)
  except BaseException:
   for file in self.files.values():file.close()
   raise
  self.pid=self.process.pid;self.starttime=identity(self.pid);self.pgid=self.pid
  try:self.actual_initial_pgid=os.getpgid(self.pid)
  except ProcessLookupError:self.actual_initial_pgid=None
  if self.actual_initial_pgid!=self.pid:self.invalid='ActualOwnedSessionNotObserved'
  if type(self.starttime)is not str or not self.starttime.isdigit()or int(self.starttime)<=0:self.invalid='ActualOwnedStarttimeUnknown'
  self.threads=[]
  for key in('stdout','stderr'):
   thread=threading.Thread(target=self._read,args=(key,getattr(self.process,key)),daemon=True);self.threads.append(thread);thread.start()
 def request_stop(self):
  self.stop_requested=True
  # Exact originally-created process/session only; no broad process search/kill.
  if self.process.poll()is None and type(self.starttime)is str and identity(self.pid)==self.starttime:
   try:
    if os.getpgid(self.pid)==self.pgid:os.killpg(self.pgid,signal.SIGTERM)
    else:self.invalid=self.invalid or'OriginalOwnedProcessGroupChanged'
   except ProcessLookupError:pass
 def _read(self,key,stream):
  try:
   while True:
    chunk=stream.read1(65536)
    if not chunk:break
    with self.lock:
     take=min(len(chunk),max(0,self.limit-self.used));self.files[key].write(chunk[:take]);self.used+=take
     if take!=len(chunk):self.invalid=self.invalid or'WorkerLogCapacity'
    if take!=len(chunk):self.request_stop()
  except BaseException as error:self.invalid='WorkerRead'+type(error).__name__;self.request_stop()
  finally:stream.close()
 def close(self):
  if self.closed is not None:return self.closed
  owned=[]
  for path in Path('/proc').glob('[0-9]*/stat'):
   try:parts=path.read_text().rsplit(')',1)[1].split()
   except (FileNotFoundError,PermissionError,ProcessLookupError):continue
   if int(parts[2])==self.pgid:owned.append({'pid':int(path.parent.name),'starttime':parts[19]})
   if len(owned)>256:raise ValueError('OwnedSessionPIDCapacity')
  self.request_stop();forced=False
  try:code=self.process.wait(timeout=12)
  except subprocess.TimeoutExpired:
   forced=True
   if type(self.starttime)is str and identity(self.pid)==self.starttime:
    try:os.killpg(self.pgid,signal.SIGKILL)
    except ProcessLookupError:pass
   code=self.process.wait(timeout=5)
  deadline=time.monotonic()+10
  for thread in self.threads:thread.join(max(0,deadline-time.monotonic()))
  alive=any(t.is_alive()for t in self.threads)
  if not alive:
   for file in self.files.values():file.flush();os.fsync(file.fileno());file.close()
  absence=[{**row,'original_process_absent':identity(row['pid'])in(None,)or identity(row['pid'])!=row['starttime']}for row in owned]
  self.closed={'case':self.tag,'role':self.role,'began_utc':self.began_utc,'finished_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'argv':self.argv,'pid':self.pid,'starttime':self.starttime,'pgid':self.pgid,'actual_initial_pgid':self.actual_initial_pgid,'owned_PID_observations':absence,'all_observed_owned_PIDs_absent':bool(absence)and all(v['original_process_absent']for v in absence),'actual_exit':code,'actual_wait_completed':True,'actual_poll_exit':self.process.poll(),'forced':forced,'pipes_settled':not alive,'invalid':self.invalid,'captured_log_bytes':self.used,'metadata_is_not_descendant_process_absence_authority':True}
  return self.closed
