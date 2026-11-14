"""One common Source worker/origin, reused across M6 missing-domain cases.
Explicit construction only. Root's outer harness owns Docker/default audit and
stops the exact service if this guest cannot acknowledge all descendant waits.
"""
import base64,datetime,hashlib,http.server,json,os,signal,subprocess,threading,time
from pathlib import Path

def starttime(pid):
 try:return Path('/proc',str(pid),'stat').read_text().rsplit(')',1)[1].split()[19]
 except FileNotFoundError:return None
class Observer:
 def __init__(self,root):self.root=Path(root)
 def snapshot(self):
  import fcntl
  output={};receipts=[]
  with (self.root/'m6-ledger.lock').open('a+b')as lock:
   fcntl.flock(lock.fileno(),fcntl.LOCK_SH)
   for name in ['prerun','postrun','publication','sql','HTTP']:
    p=self.root/('m6-'+name+'.jsonl');data=p.read_bytes()if p.exists()else b''
    if len(data)>16*1024*1024:raise ValueError('ActualObserverLedger16MiB')
    output[name]=[json.loads(line)for line in data.splitlines()if line]
    receipts.append({'path':p.name,'bytes':len(data),'sha256':hashlib.sha256(data).hexdigest()})
  output['raw_ledger_receipts']=receipts;return output
class VersionOrigin:
 def __init__(self,root):
  self.root=Path(root);self.lock=threading.Lock();self.response=(503,b'owned dependency not configured');self.rows=[];owner=self
  class Handler(http.server.BaseHTTPRequestHandler):
   def setup(self):
    super().setup();self.connection.settimeout(5)
   def do_GET(self):
    with owner.lock:status,body=owner.response
    row={'began_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'method':'GET','path':self.path,'headers':list(self.headers.items()),'status':status,'response_bytes':len(body),'response_sha256':hashlib.sha256(body).hexdigest()}
    if self.path!='/owned-version':status,body=400,b'unexpected owned path';row['invalid']='path'
    row['status']=status
    self.send_response(status);self.send_header('Content-Type','application/json');self.send_header('Content-Length',str(len(body)));self.end_headers();self.wfile.write(body);self.wfile.flush();row['finished_utc']=datetime.datetime.now(datetime.timezone.utc).isoformat()
    with owner.lock:
     owner.rows.append(row)
     if len(owner.rows)>32:raise ValueError('OwnedVersion32Requests')
   def log_message(self,*args):pass
  self.server=http.server.ThreadingHTTPServer(('127.0.0.1',0),Handler);self.server.daemon_threads=False;self.thread=threading.Thread(target=self.server.serve_forever);self.thread.start();self.url='http://127.0.0.1:'+str(self.server.server_port)
 def set_response(self,status,body):
  if status!=200 or len(body)>8192:raise ValueError('OwnedResponsePlan')
  with self.lock:self.response=(status,body)
 def snapshot(self):
  with self.lock:return {'url':self.url,'requests':json.loads(json.dumps(self.rows)),'observer_thread_alive':self.thread.is_alive()}
 def close(self):
  self.server.shutdown();self.server.server_close();self.thread.join(5)
  return {'thread_settled':not self.thread.is_alive(),'all_request_records':self.snapshot()['requests']}
class WorkerFactory:
 def __init__(self,root,tools):self.root=Path(root);self.tools=Path(tools);self.jobs=[]
 def start(self,*,queues,observer_module,owned_version_origin):
  if queues!=['celery']or observer_module!='m6_source_observer'or len(self.jobs)!=0:raise ValueError('OneDefaultWorkerCohort')
  env=dict(os.environ,C_FORCE_ROOT='true',PYTHONPATH=str(self.tools)+':/app',DEFAULT_TASK_HTTP_ORIGIN=owned_version_origin,M6_SOURCE_EVIDENCE=str(self.root))
  argv=['/dispatcharrpy/bin/celery','-A','m6_source_observer:app','worker','--pool=prefork','--concurrency=1','--queues=celery','--without-gossip','--without-mingle','--without-heartbeat','--loglevel=WARNING','--hostname=m6-whole-source@fixture']
  job=Worker(argv,env,self.root);self.jobs.append(job)
  if job.invalid is not None:raise ValueError('ActualOriginalWorkerIdentity:'+job.invalid)
  return job
 def poll(self,job):return job.process.poll()
 def stop_and_collect(self,job):return job.close()
class Worker:
 def __init__(self,argv,env,root):
  self.argv=argv;self.root=root;self.files={k:(root/('original-worker-'+k+'.log')).open('xb')for k in ['stdout','stderr']};self.used=0;self.lock=threading.Lock();self.invalid=None;self.closed=None
  self.process=subprocess.Popen(argv,env=env,stdin=subprocess.DEVNULL,stdout=subprocess.PIPE,stderr=subprocess.PIPE,start_new_session=True);self.pid=self.process.pid;self.start=starttime(self.pid);self.parent_pid_namespace=os.readlink('/proc/self/ns/pid')
  try:self.worker_pid_namespace=os.readlink('/proc/'+str(self.pid)+'/ns/pid')
  except OSError:self.worker_pid_namespace=None
  if self.start is None or os.getpgid(self.pid)!=self.pid:self.invalid='ActualOriginalIdentityUnknown'
  if self.worker_pid_namespace!=self.parent_pid_namespace:self.invalid='ActualWorkerPIDNamespaceUnknownOrDifferent'
  self.threads=[]
  for kind in self.files:
   t=threading.Thread(target=self.read,args=(kind,getattr(self.process,kind)));self.threads.append(t);t.start()
 def stop(self,sig=signal.SIGTERM):
  if self.process.poll()is None and self.start is not None and starttime(self.pid)==self.start:
   try:
    if os.getpgid(self.pid)!=self.pid:self.invalid=self.invalid or'OwnedPGIDChanged';return
    os.killpg(self.pid,sig)
   except ProcessLookupError:pass
 def read(self,kind,pipe):
  try:
   while True:
    data=pipe.read1(65536)
    if not data:break
    with self.lock:
     count=min(len(data),max(0,2097152-self.used));self.files[kind].write(data[:count]);self.used+=count
     if count!=len(data):self.invalid=self.invalid or'OwnedWorker2MiB'
    if count!=len(data):self.stop()
  except BaseException as error:self.invalid=self.invalid or type(error).__name__;self.stop()
  finally:pipe.close()
 def close(self):
  if self.closed is not None:return self.closed
  known=[]
  for p in Path('/proc').glob('[0-9]*/stat'):
   try:fields=p.read_text().rsplit(')',1)[1].split()
   except(FileNotFoundError,PermissionError,ProcessLookupError):continue
   if int(fields[2])==self.pid:known.append({'pid':int(p.parent.name),'starttime':fields[19]})
   if len(known)>256:raise ValueError('OwnedPID256')
  self.stop();forced=False
  try:code=self.process.wait(12)
  except subprocess.TimeoutExpired:forced=True;self.stop(signal.SIGKILL);code=self.process.wait(5)
  deadline=time.monotonic()+10
  for t in self.threads:t.join(max(0,deadline-time.monotonic()))
  settled=not any(t.is_alive()for t in self.threads)
  if settled:
   for f in self.files.values():f.flush();os.fsync(f.fileno());f.close()
  self.closed={'argv':self.argv,'pid':self.pid,'starttime':self.start,'actual_exit':code,'actual_wait_completed':True,'pipes_settled':settled,'forced':forced,'invalid':self.invalid,'parent_PID_namespace':self.parent_pid_namespace,'worker_PID_namespace':self.worker_pid_namespace,'same_PID_namespace_as_observer':self.parent_pid_namespace==self.worker_pid_namespace,'known_PID_generations':[{**row,'original_absent':starttime(row['pid'])!=row['starttime']}for row in known]};return self.closed
