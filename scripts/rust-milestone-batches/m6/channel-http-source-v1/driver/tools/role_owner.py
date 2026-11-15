"""Actual Source role owner, preserving master-only TERM and real wait/pipes.
Only fixture process ownership; no Source function or producer replacement.
"""
import argparse,datetime,json,os,signal,socket,socketserver,subprocess,sys,threading,time,uuid
from pathlib import Path
ROOT=Path('/tmp/normal-dvr-roles')
def identity(pid):
 try:return Path('/proc',str(pid),'stat').read_text().rsplit(')',1)[1].split()[19]
 except (FileNotFoundError,ProcessLookupError):return None
def processes():
 rows=[]
 for p in Path('/proc').glob('[0-9]*/cmdline'):
  try:
   text=p.read_bytes().replace(b'\0',b' ').decode('utf8',errors='replace');pid=int(p.parent.name)
   if any(k in text for k in ['celery','uwsgi','ffmpeg','role_owner']):rows.append({'pid':pid,'starttime':identity(pid),'argv_text':text})
  except (FileNotFoundError,PermissionError,ProcessLookupError):continue
 return rows
def rpc(record,action):
 with socket.socket(socket.AF_UNIX)as c:
  c.settimeout(80);c.connect(record['socket']);c.sendall((action+'\n').encode());data=b''
  while not data.endswith(b'\n'):
   part=c.recv(65536)
   if not part:break
   data+=part
   if len(data)>1048576:raise ValueError('RoleOwnerReplyBudget')
 return json.loads(data)
def start(name,args,environment):
 ROOT.mkdir(mode=0o700,exist_ok=True);nonce=uuid.uuid4().hex;config=ROOT/(nonce+'.json');sock=ROOT/(nonce+'.sock')
 config.write_text(json.dumps({'name':name,'args':args,'environment':environment,'socket':str(sock),'nonce':nonce}));config.chmod(0o600)
 p=subprocess.Popen([sys.executable,__file__,'serve',str(config)],stdin=subprocess.DEVNULL,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,start_new_session=True)
 deadline=time.monotonic()+15
 while time.monotonic()<deadline:
  if p.poll()is not None:raise RuntimeError('ActualSourceRoleOwnerExited')
  if sock.exists():return rpc({'socket':str(sock)},'status')
  time.sleep(.05)
 raise TimeoutError('SourceRoleOwnerStartup')
def stop(record):return rpc(record,'stop')
def begin_stop(record):return rpc(record,'begin_stop')
def crash(record):return rpc(record,'crash')
def serve(path):
 import ctypes
 libc=ctypes.CDLL(None,use_errno=True)
 if libc.prctl(36,1,0,0,0)!=0:raise OSError(ctypes.get_errno(),'actual role child-subreaper setup')
 config=json.loads(Path(path).read_text());root=ROOT/config['nonce'];root.mkdir(mode=0o700)
 env=dict(os.environ,**config['environment']);env['PYTHONDONTWRITEBYTECODE']='1'
 def user():os.setgid(65532);os.setuid(65532)
 child=subprocess.Popen(config['args'],cwd='/app',env=env,stdin=subprocess.DEVNULL,stdout=subprocess.PIPE,stderr=subprocess.PIPE,start_new_session=True,preexec_fn=user)
 pid=child.pid;starttime=identity(pid);streams={};failures=[];used=[0];lock=threading.Lock()
 def drain(kind,pipe):
  with(root/(kind+'.log')).open('xb')as f:
   while True:
    data=pipe.read1(65536)
    if not data:break
    with lock:
     take=min(len(data),max(0,4*1024*1024-used[0]));used[0]+=take;f.write(data[:take])
     if take<len(data):failures.append('ActualSourceRoleOutputBudget');os.kill(pid,signal.SIGTERM)
  pipe.close()
 threads=[threading.Thread(target=drain,args=(k,getattr(child,k)),daemon=True)for k in ['stdout','stderr']]
 for t in threads:t.start()
 stopped=[None];shutdown_lock=threading.Lock();shutdown_started=threading.Event();signal_settled=threading.Event();shutdown_settled=threading.Event();term_sent=[False]
 def status():return {'role':config['name'],'socket':config['socket'],'owner_pid':os.getpid(),'owner_starttime':identity(os.getpid()),'pid':pid,'starttime':starttime,'actual_pgid':pid,'process_group_authority':'successful Popen(start_new_session=True)','actual_argv':config['args'],'log_paths':{kind:str(root/(kind+'.log'))for kind in ('stdout','stderr')},'shutdown_started':shutdown_started.is_set(),'signal_settled':signal_settled.is_set(),'actual_master_SIGTERM_sent':term_sent[0],'shutdown_settled':shutdown_settled.is_set(),'running':child.poll()is None,'actual_exit':child.poll()}
 def shutdown():
  with shutdown_lock:
   if stopped[0]is not None:return stopped[0]
   shutdown_started.set()
   owned=[]
   for p in Path('/proc').glob('[0-9]*/stat'):
    try:v=p.read_text().rsplit(')',1)[1].split()
    except(FileNotFoundError,PermissionError,ProcessLookupError):continue
    if int(v[2])==pid:owned.append({'pid':int(p.parent.name),'starttime':v[19]})
   forced=False
   if child.poll()is None and identity(pid)==starttime:
    try:os.kill(pid,signal.SIGTERM);term_sent[0]=True
    except ProcessLookupError:failures.append('ActualMasterExitedBeforeSIGTERM')
   signal_settled.set()
   try:code=child.wait(timeout=55)
   except subprocess.TimeoutExpired:forced=True;os.killpg(pid,signal.SIGKILL);code=child.wait(timeout=5)
   for t in threads:t.join(10)
   reaped=[];until=time.monotonic()+5
   while time.monotonic()<until:
    try:
     adopted,wait_status=os.waitpid(-pid,os.WNOHANG)
     if adopted:reaped.append({'pid':adopted,'actual_wait_status':wait_status});continue
    except ChildProcessError:break
    time.sleep(.05)
   absent=[{**r,'absent':identity(r['pid'])!=r['starttime']}for r in owned]
   shutdown_settled.set()
   stopped[0]={**status(),'actual_exit':code,'actual_wait_completed':True,'forced':forced,'stdout_EOF':not threads[0].is_alive(),'stderr_EOF':not threads[1].is_alive(),'failures':failures,'actual_reaped_adopted_children':reaped,'owned_generations':absent,'all_original_process_generations_absent':bool(absent)and all(r['absent']for r in absent)}
   return stopped[0]
 class Handler(socketserver.StreamRequestHandler):
  def handle(self):
   action=self.rfile.readline(32).decode().strip()
   if action=='begin_stop':
    if not shutdown_started.is_set():
     shutdown_started.set();threading.Thread(target=shutdown,daemon=True).start()
    if not signal_settled.wait(5):raise TimeoutError('ActualSourceMasterSIGTERMSignalSettlement')
    result=status()
   elif action=='crash':
    if child.poll()is None:os.killpg(pid,signal.SIGKILL)
    result=shutdown();result['declared_crash']=True
   else:result=shutdown()if action=='stop'else status()
   self.wfile.write(json.dumps(result).encode()+b'\n')
   if action in('stop','crash'):threading.Thread(target=self.server.shutdown,daemon=True).start()
 try:
  with socketserver.UnixStreamServer(config['socket'],Handler)as server:server.serve_forever(poll_interval=.1)
 finally:shutdown();Path(config['socket']).unlink(missing_ok=True);Path(path).unlink(missing_ok=True)
if __name__=='__main__':
 p=argparse.ArgumentParser();p.add_argument('mode',choices=['serve']);p.add_argument('config');a=p.parse_args();serve(a.config)
