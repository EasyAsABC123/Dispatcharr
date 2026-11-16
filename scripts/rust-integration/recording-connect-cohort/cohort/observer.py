"""Owned metadata-only reader observer; no application or media process changes.
Original Dispatcharr and dependency authors retain all application/algorithm credit.
"""
import hashlib,json,os,time
from pathlib import Path
ROOT=Path('/observer');config={}
def generation(pid):
 try:return Path('/proc',str(pid),'stat').read_text().rsplit(')',1)[1].split()[19]
 except(FileNotFoundError,ProcessLookupError):return None

def selected():
 if config['kind']=='Native':return [1] if generation(1)==config['target_generation']else[]
 result=[]
 for path in Path('/proc').glob('[0-9]*/stat'):
  try:fields=path.read_text().rsplit(')',1)[1].split()
  except(FileNotFoundError,ProcessLookupError,PermissionError):continue
  if int(fields[2])==config['target_pgid']:result.append(int(path.parent.name))
 return result

def snapshot():
 pids=selected();fds=[];errors=[]
 for pid in pids:
  prefix=Path('/proc',str(pid),'fd')
  try:numbers=list(prefix.iterdir())
  except(FileNotFoundError,ProcessLookupError):continue
  except PermissionError:errors.append({'pid':pid,'phase':'fd-list','error':'PermissionError'});continue
  for path in numbers:
   try:
    metadata=path.stat()
    if metadata.st_ino!=config['file_inode'] or metadata.st_size!=config['file_bytes']:continue
    link=os.readlink(path);info=dict(line.split(':',1)for line in Path('/proc',str(pid),'fdinfo',path.name).read_text().splitlines()if ':'in line)
    fds.append({'pid':pid,'generation':generation(pid),'fd':int(path.name),'inode':metadata.st_ino,'device':metadata.st_dev,'bytes':metadata.st_size,'position':int(info['pos'].strip()),'link':link})
   except(FileNotFoundError,ProcessLookupError):continue
   except PermissionError:errors.append({'pid':pid,'fd':path.name,'phase':'fd-stat','error':'PermissionError'})
 return {'epoch':time.time(),'monotonic':time.monotonic(),'target_pids':pids,'fds':fds,'errors':errors}

receipt={'passed':False,'actual_UID':os.getuid(),'rows':0,'bytes':0,'row_limit':2000,'byte_limit':2097152,'period_seconds':.05,'wall_limit_seconds':90,'writer_closed':False,'stop_marker_observed':False}
try:
 config=json.loads((ROOT/'config.json').read_bytes())
 target=config.get('target_pid',1 if config['kind']=='Native'else config['target_pgid'])
 receipt.update(observer_pid_namespace=os.readlink('/proc/self/ns/pid'),target_pid_namespace=os.readlink('/proc/'+str(target)+'/ns/pid'),target_pid=target,target_generation=generation(target),config_sha256=hashlib.sha256((ROOT/'config.json').read_bytes()).hexdigest(),source_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest())
 if receipt['target_generation']!=config['target_generation']:raise ValueError('ActualTargetGenerationChanged')
 if os.getuid()!=65532 or receipt['observer_pid_namespace']!=receipt['target_pid_namespace']:raise ValueError('ActualSameUIDAndPIDNamespaceRequired')
 receipt['masked_app_mode']=oct(Path('/app').stat().st_mode&0o777)
 try:list(Path('/app').iterdir());receipt['masked_app_read_refused']=False
 except PermissionError:receipt['masked_app_read_refused']=True
 if receipt['masked_app_mode']!='0o0' or not receipt['masked_app_read_refused']:raise ValueError('ObserverOriginalAppMustBeMasked')
 (ROOT/'ready.json').write_text(json.dumps(receipt)+'\n');began=time.monotonic()
 with(ROOT/'samples.jsonl').open('xb')as output:
  while True:
   row=snapshot();encoded=(json.dumps(row,separators=(',',':'))+'\n').encode()
   if receipt['rows']>=2000 or receipt['bytes']+len(encoded)>2097152:raise ValueError('ActualObserverFiniteBudget')
   output.write(encoded);output.flush();receipt['rows']+=1;receipt['bytes']+=len(encoded)
   if(ROOT/'stop').exists():receipt['stop_marker_observed']=True;break
   if time.monotonic()-began>=90:raise TimeoutError('ActualObserver90Seconds')
   time.sleep(.05)
  output.flush();os.fsync(output.fileno())
 receipt['writer_closed']=True;receipt['passed']=True
except BaseException as error:receipt['error']={'type':type(error).__name__,'message':str(error)}
finally:
 receipt['completed_epoch']=time.time();pending=ROOT/'receipt.pending';pending.write_text(json.dumps(receipt,indent=2)+'\n');os.replace(pending,ROOT/'receipt.json')
raise SystemExit(0 if receipt['passed']else 1)
