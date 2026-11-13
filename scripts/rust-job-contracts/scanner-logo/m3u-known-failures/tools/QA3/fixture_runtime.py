"""Owned actual Source producer/native peer helpers; no import-time app/startup.
Only called inside the sealed copied-DB fixture. Original authors retain credit.
"""
import base64,json,os,select,signal,subprocess,sys,time,uuid
from datetime import datetime,timezone
from pathlib import Path

def utc():return datetime.now(timezone.utc).isoformat()
def wait(predicate,seconds=30):
 end=time.monotonic()+seconds
 while time.monotonic()<end:
  value=predicate()
  if value:return value
  time.sleep(.01)
 raise TimeoutError('OwnedFixedDeadline')

class Peer:
 def __init__(self,parent,account,label,**overrides):
  self.log=Path('/evidence/native-'+label+'.log').open('xb')
  try:self.process=subprocess.Popen(['/native-m3u-peer','scanner_m3u_peer::native_scanner_m3u_peer','--exact','--ignored','--nocapture','--test-threads=1'],stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=self.log,text=True,bufsize=1)
  except BaseException:self.log.close();raise
  self.commands=[];self.closed=False;self.buffer=bytearray()
  try:
   reply=self.command({'parent_id':parent,'account_id':account,**overrides})
   if reply!={'prepared':True}:raise AssertionError('ActualProductionConstructor')
  except BaseException:self.abort_cleanup();raise
 def command(self,value):
  began=utc();self.process.stdin.write(json.dumps(value)+'\n');self.process.stdin.flush();end=time.monotonic()+35
  while time.monotonic()<end:
   while b'\n'in self.buffer:
    line,_,tail=self.buffer.partition(b'\n');self.buffer=bytearray(tail)
    if b'NATIVE_SCANNER_M3U_JSON='in line:
     answer=json.loads(line.split(b'NATIVE_SCANNER_M3U_JSON=',1)[1]);self.commands.append({'action':value.get('action','configure'),'began_utc':began,'finished_utc':utc(),'reply':answer});return answer
   if select.select([self.process.stdout],[],[],.1)[0]:
    data=os.read(self.process.stdout.fileno(),65536)
    if not data and self.process.poll()is not None:raise RuntimeError('NativePeerEOFBeforeReply')
    self.log.write(data);self.log.flush();self.buffer.extend(data)
    if len(self.buffer)>1048576:raise ValueError('OwnedPeerLineCapacity')
   if self.process.poll()is not None and not self.buffer:raise RuntimeError('NativePeerExitedBeforeReply')
  raise TimeoutError('UnchangedPeerResponse35s')
 def close(self):
  answer=self.command({'action':'shutdown'})
  if answer!={'drained':True}:raise AssertionError('ActualNativeDrain')
  self.process.stdin.close();code=self.process.wait(timeout=10);self.drain_stdout();self.process.stdout.close();self.log.close();self.closed=True
  if code!=0:raise AssertionError('ActualNativeExit0')
  return {'actual_exit':code,'process_settled':True,'commands':self.commands}
 def drain_stdout(self):
  total=0
  while True:
   data=os.read(self.process.stdout.fileno(),65536)
   if not data:break
   total+=len(data)
   if total>1048576:raise ValueError('OwnedFinalStdoutCapacity')
   self.log.write(data)
  self.log.flush()
 def abort_cleanup(self):
  if self.process.poll()is None:self.process.kill()
  code=self.process.wait(timeout=5)
  try:self.drain_stdout()
  finally:
   for stream in (self.process.stdin,self.process.stdout,self.log):
    if stream and not stream.closed:stream.close()
   self.closed=True
  return {'actual_exit':code,'process_settled':True,'forced':True,'commands':self.commands}

class Fixture:
 def __init__(self):
  if os.environ.get('JOB_DISPOSABLE_FIXTURE')!='playback-fixture-only'or not os.environ.get('POSTGRES_DB','').startswith('rust_probe_backup_jobs_'):raise ValueError('OwnedCopiedDBGuard')
  sys.path[:0]=['/app','/pair-tools'];sys.argv.append('shell');os.environ.setdefault('DJANGO_SETTINGS_MODULE','dispatcharr.settings')
  import django;django.setup()
  import redis
  from dispatcharr.celery import app
  from core.models import CoreSettings,SYSTEM_SETTINGS_KEY
  from apps.m3u.models import M3UAccount
  from apps.epg.models import EPGSource
  from django_celery_beat.models import PeriodicTask
  self.app=app;self.account_model=M3UAccount;self.worker=None;self.paths=[];self.parent_exits=[]
  self.broker=redis.Redis.from_url(os.environ['CELERY_BROKER_URL']);self.backend=redis.Redis.from_url(os.environ['CELERY_RESULT_BACKEND']);self.cache=redis.Redis(host='localhost',port=6379,password='playback-fixture-only',db=0)
  if self.broker.connection_pool.connection_kwargs['db']!=8 or self.backend.connection_pool.connection_kwargs['db']!=9 or self.broker.dbsize()or self.backend.dbsize():raise ValueError('OwnedRedis8And9Empty')
  for key in ['dvr:recover_dispatch_lock','core:version_check_dispatch_lock']:self.cache.set(key,'1',ex=3600)
  app.loader.import_default_modules();CoreSettings._update_group(SYSTEM_SETTINGS_KEY,'System Settings',{'auto_import_mapped_files':True});EPGSource.objects.all().update(is_active=False);PeriodicTask.objects.all().update(enabled=False)
  for name in ['m3us','epgs','logos']:
   path=Path('/data')/name;path.mkdir(exist_ok=True)
   if list(path.iterdir()):raise ValueError('OwnedEmptyRoots')
 def stop(self):
  if self.worker and self.worker.poll()is None:
   os.killpg(self.worker.pid,signal.SIGTERM)
   try:
    code=self.worker.wait(timeout=15);self.parent_exits.append({'pid':self.worker.pid,'actual_exit':code,'process_settled':True,'finished_utc':utc()})
   except subprocess.TimeoutExpired:os.killpg(self.worker.pid,signal.SIGKILL);self.worker.wait(timeout=5);raise RuntimeError('SourceParentForcedStop')
  self.worker=None
 def produce(self,label,text):
  self.stop()
  if self.broker.llen('celery')or self.broker.hlen('unacked'):raise ValueError('PriorChildrenNotConsumed')
  for path in self.paths:
   if path.exists():path.unlink()
  self.paths=[];self.account_model.objects.all().update(is_active=False)
  path=Path('/data/m3us')/('rust-owned-children-failure-'+label+'.m3u');path.write_text(text);os.utime(path,(1699999993,1699999993));self.paths.append(path)
  ident=str(uuid.uuid4());Path('/evidence/owned-scanner-config.json').write_text(json.dumps({'task_id':ident,'now':1700000000.125}))
  log=Path('/evidence/source-parents.log').open('ab')
  try:self.worker=subprocess.Popen(['/dispatcharrpy/bin/celery','-A','periodic_worker_observer:app','worker','--pool=prefork','--concurrency=1','-Q','rust-owned-scanner-parents','--without-gossip','--without-mingle','--without-heartbeat','--loglevel=WARNING'],env=dict(os.environ,C_FORCE_ROOT='true',PYTHONPATH='/pair-tools:/app'),stdout=log,stderr=log,start_new_session=True)
  finally:log.close()
  result=self.app.tasks['core.tasks.scan_and_process_files'].apply_async(args=[],kwargs={},task_id=ident,queue='rust-owned-scanner-parents')
  def done():
   p=Path('/evidence/periodic-worker-postrun.jsonl')
   if not p.exists():return False
   import fcntl
   with p.open()as stream:
    fcntl.flock(stream.fileno(),fcntl.LOCK_SH)
    return any(json.loads(line)['id']==ident for line in stream.read().splitlines())
  wait(done,30);self.stop()
  if result.get(timeout=2)is not None:raise AssertionError('OriginalParentNull')
  account=self.account_model.objects.get(file_path=str(path));frames=[json.loads(v)for v in reversed(self.broker.lrange('celery',0,-1))]
  if len(frames)!=2 or [v['headers']['task']for v in frames]!=['apps.m3u.tasks.refresh_m3u_groups','apps.m3u.tasks.refresh_single_m3u_account']:raise AssertionError('ActualTwoChildrenInOrder')
  if any(v['headers']['root_id']!=ident or v['headers']['parent_id']!=ident for v in frames):raise AssertionError('ActualParentRoot')
  return ident,account,frames,path
 def snapshot(self,account):
  # Small owned-fixture diagnostic only; not a production SQL/Redis byte cap.
  from apps.m3u.models import M3UAccountProfile
  from apps.channels.models import Stream,ChannelGroup,ChannelGroupM3UAccount
  from core.models import SystemEvent
  import hashlib
  files=[]
  for root in ('/data/m3us','/app/media/cached_m3u'):
   for path in Path(root).glob('*'):
    if path.is_file():
     stat=path.stat();raw=path.read_bytes()
     if len(raw)>65536 or len(files)>=32:raise ValueError('OwnedPhysicalFileDiagnosticCapacity')
     files.append({'path':str(path),'bytes':len(raw),'mtime_ns':stat.st_mtime_ns,'sha256':hashlib.sha256(raw).hexdigest(),'raw_base64':base64.b64encode(raw).decode()})
  locks={}
  for key in self.cache.scan_iter('task_lock_*'):
   value=self.cache.get(key)
   if len(locks)>=64 or value is None or len(value)>8192:raise ValueError('OwnedPhysicalLockDiagnosticCapacity')
   locks[key.decode()]={'raw_base64':base64.b64encode(value).decode(),'ttl':self.cache.ttl(key)}
  value={'accounts':list(self.account_model.objects.filter(id=account).values()),'profiles':list(M3UAccountProfile.objects.filter(m3u_account_id=account).values()),'streams':list(Stream.objects.filter(m3u_account_id=account).values()),'groups':list(ChannelGroup.objects.values()),'group_links':list(ChannelGroupM3UAccount.objects.filter(m3u_account_id=account).values()),'system_events':list(SystemEvent.objects.filter(details__account_name__startswith='rust-owned-children-').order_by('id').values()),'files':files,'locks':locks,'queued_frames_raw_base64':[base64.b64encode(v).decode()for v in self.broker.lrange('celery',0,-1)],'unacked_raw_base64':{base64.b64encode(k).decode():base64.b64encode(v).decode()for k,v in self.broker.hgetall('unacked').items()}}
  return json.loads(json.dumps(value,default=str))
 def raw_result(self,ident):
  key=self.app.backend.get_key_for_task(ident);raw=self.backend.get(key)
  return {'raw_base64':None if raw is None else base64.b64encode(raw).decode(),'metadata':None if raw is None else json.loads(raw),'ttl':self.backend.ttl(key)}
