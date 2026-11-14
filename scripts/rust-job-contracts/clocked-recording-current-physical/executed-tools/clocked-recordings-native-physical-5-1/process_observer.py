"""Observe real Popen identity/argv without replacing task execution or wait.

Returns the exact real Popen object. Never drains application pipes or calls
wait/poll; the Source task retains its original process/pipe responsibilities.
"""
import datetime,os,subprocess,threading,time
from pathlib import Path
def starttime(pid):
 try:return Path('/proc',str(pid),'stat').read_text().rsplit(')',1)[1].split()[19]
 except FileNotFoundError:return None
class Observer:
 def __init__(self,append,current_task_id):
  self.append=append;self.current_task_id=current_task_id;self.real=subprocess.Popen;self.lock=threading.Lock();self.rows=[];self.errors=[];self.installed=False
 def install(self):
  if self.installed:raise ValueError('ProcessObserverInstallOnce')
  self.installed=True;subprocess.Popen=self.create
 def create(self,*args,**kwargs):
  argv=args[0]if args else kwargs.get('args')
  if type(argv)not in(list,tuple)or not argv or len(argv)>128 or any(type(v)is not str or len(v.encode())>8192 for v in argv):raise ValueError('BoundedActualSubprocessArgv')
  with self.lock:
   if len(self.rows)>=128:raise ValueError('ActualSubprocessCount128')
   slot={'argv':list(argv),'actual_thread_task_id':self.current_task_id(),'pid_namespace':os.readlink('/proc/self/ns/pid'),'parent_pid':os.getpid(),'thread':threading.get_ident(),'began_epoch':time.time(),'began_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'spawn_state':'constructor_not_returned'};self.rows.append(slot)
  try:child=self.real(*args,**kwargs)
  except BaseException as error:
   slot.update(spawn_state='actual_constructor_error',error_type=type(error).__name__,finished_epoch=time.time());self.append('subprocess',self.serializable(slot));raise
  slot.update(spawn_state='actual_child_started',pid=child.pid,starttime=starttime(child.pid),process=child)
  if type(slot['starttime'])is not str or not slot['starttime'].isdigit():self.errors.append('ActualChildStarttimeUnknown')
  try:self.append('subprocess',self.serializable(slot))
  except BaseException as error:
   # An observation failure must not orphan a process by making the application
   # believe its real constructor failed. Keep the object/negative receipt.
   self.errors.append('LedgerAfterActualSpawn:'+type(error).__name__)
  return child
 def serializable(self,row):
  value={k:v for k,v in row.items()if k!='process'};process=row.get('process')
  if process is not None:
   value['actual_returncode_attribute']=process.returncode
   value['original_process_absent']=starttime(row['pid'])!=row['starttime']
   value['application_stdout_closed']=process.stdout is None or process.stdout.closed
   value['application_stderr_closed']=process.stderr is None or process.stderr.closed
  return value
 def snapshot(self):
  with self.lock:return {'rows':[self.serializable(row)for row in self.rows],'errors':list(self.errors),'observer_never_waits_or_reaps':True}
