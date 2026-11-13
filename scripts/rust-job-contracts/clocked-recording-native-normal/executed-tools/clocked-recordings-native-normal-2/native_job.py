"""Owned bounded Native DVR test process. No process starts on import.

An acknowledged host/container teardown is still required after any invalid
collection; killing the observer never proves the Rust operation was settled.
"""
import datetime,json,os,signal,subprocess,threading,time
from pathlib import Path
def identity(pid):
 try:return Path('/proc',str(pid),'stat').read_text().rsplit(')',1)[1].split()[19]
 except FileNotFoundError:return None
class NativeJob:
 def __init__(self,config,root,environment):
  body=(json.dumps(config,allow_nan=False)+'\n').encode()
  if len(body)>16384:raise ValueError('NativeConfig16KiB')
  self.root=Path(root);self.used=0;self.limit=2097152;self.invalid=None;self.closed=None;self.lock=threading.Lock();self.stop_requested=False
  self.argv=['/native-peer/native-job-tests','recording_dvr_peer::actual_source_beat_normal_dvr_peer','--exact','--ignored','--nocapture','--test-threads=1']
  self.began_utc=datetime.datetime.now(datetime.timezone.utc).isoformat()
  self.files={name:(self.root/('worker-native-dvr-'+name+'.log')).open('xb')for name in('stdout','stderr')}
  env=dict(environment,JOB_DISPOSABLE_FIXTURE='playback-fixture-only',DVR_NATIVE_MODE='natural-Beat-normal-once')
  try:self.process=subprocess.Popen(self.argv,env=env,stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE,start_new_session=True)
  except BaseException:
   for file in self.files.values():file.close()
   raise
  self.pid=self.process.pid;self.starttime=identity(self.pid);self.pgid=self.pid
  try:self.actual_initial_pgid=os.getpgid(self.pid)
  except ProcessLookupError:self.actual_initial_pgid=None
  if type(self.starttime)is not str or not self.starttime.isdigit()or int(self.starttime)<=0 or self.actual_initial_pgid!=self.pid:self.invalid='OriginalNativeIdentityUnknown'
  self.threads=[]
  for name in('stdout','stderr'):
   thread=threading.Thread(target=self._read,args=(name,getattr(self.process,name)),daemon=True);thread.start();self.threads.append(thread)
  def writer():
   try:self.process.stdin.write(body);self.process.stdin.close()
   except BaseException as error:self.invalid=self.invalid or'NativeInput'+type(error).__name__;self.request_stop()
  self.writer=threading.Thread(target=writer,daemon=True);self.writer.start()
 def _read(self,name,pipe):
  try:
   while True:
    data=pipe.read1(65536)
    if not data:break
    with self.lock:
     count=min(len(data),max(0,self.limit-self.used));self.files[name].write(data[:count]);self.used+=count
     if count!=len(data):self.invalid=self.invalid or'NativeOutput2MiBCapacity'
    if count!=len(data):self.request_stop()
  except BaseException as error:self.invalid=self.invalid or'NativePipe'+type(error).__name__;self.request_stop()
  finally:pipe.close()
 def request_stop(self):
  self.stop_requested=True
  if self.process.poll()is None and type(self.starttime)is str and identity(self.pid)==self.starttime:
   try:
    if os.getpgid(self.pid)==self.pgid:os.killpg(self.pgid,signal.SIGTERM)
    else:self.invalid=self.invalid or'NativeProcessGroupChanged'
   except ProcessLookupError:pass
 def close(self):
  if self.closed is not None:return self.closed
  self.request_stop();return self._collect(12)
 def wait(self,seconds,monitor=None):
  if self.closed is not None:return self.closed
  return self._collect(seconds,monitor)
 def _collect(self,seconds,monitor=None):
  forced=False
  deadline=time.monotonic()+seconds;next_observation=time.monotonic()
  try:
   while monitor is not None and self.process.poll()is None and time.monotonic()<deadline:
    if time.monotonic()>=next_observation:monitor();next_observation=time.monotonic()+1
    time.sleep(.05)
   # Always perform actual wait, including when poll already reports an exit.
   code=self.process.wait(timeout=max(.01,deadline-time.monotonic()))
  except subprocess.TimeoutExpired:
   self.invalid=self.invalid or'NativeTaskDeadline';self.request_stop()
   try:code=self.process.wait(timeout=12)
   except subprocess.TimeoutExpired:
    forced=True
    if identity(self.pid)==self.starttime:
     try:os.killpg(self.pgid,signal.SIGKILL)
     except ProcessLookupError:pass
    code=self.process.wait(timeout=5)
  deadline=time.monotonic()+10
  for thread in self.threads+[self.writer]:thread.join(max(0,deadline-time.monotonic()))
  settled=not any(thread.is_alive()for thread in self.threads+[self.writer])
  if settled:
   for stream in self.files.values():stream.flush();os.fsync(stream.fileno());stream.close()
  self.closed={'role':'native-dvr','argv':self.argv,'pid':self.pid,'starttime':self.starttime,'pgid':self.pgid,'actual_initial_pgid':self.actual_initial_pgid,'began_utc':self.began_utc,'finished_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'actual_exit':code,'actual_poll_exit':self.process.poll(),'actual_wait_completed':True,'forced':forced,'pipes_settled':settled,'writer_settled':not self.writer.is_alive(),'invalid':self.invalid,'captured_log_bytes':self.used,'original_process_absent':identity(self.pid)!=self.starttime,'observer_receipt_is_not_descendant_absence_authority':True,'requires_owned_host_container_cleanup_after_invalid':True}
  return self.closed
