"""Actual retained native spool service against separately captured Original259 files.
Original Dispatcharr authors retain traversal/algorithm/code/license credit.
No native task claim or Original worker execution in this comparison.
"""
import os,sys,json,time,hashlib,subprocess
from pathlib import Path
assert os.environ.get('JOB_DISPOSABLE_FIXTURE')=='playback-fixture-only'
assert os.environ['POSTGRES_DB'].startswith('rust_probe_backup_jobs_')
sys.path.insert(0,'/app');sys.argv.append('shell');os.environ.setdefault('DJANGO_SETTINGS_MODULE','dispatcharr.settings')
import django;django.setup()
import redis
from asgiref.sync import async_to_sync
from channels.layers import get_channel_layer
from django.db import connection
from apps.channels.models import Logo
from apps.epg.models import EPGSource
from dispatcharr.celery import app
from core.tasks import scan_and_process_files as original_task
reference=json.loads(Path('/evidence/reference.json').read_text());assert reference['passed']and len(reference['checks'])==85
NOW=reference['fixed_scanner_clock'];ROOT=Path('/data/logos');cache=redis.Redis(host='localhost',port=6379,password='playback-fixture-only',db=0);broker=redis.Redis.from_url(os.environ['CELERY_BROKER_URL']);backend=redis.Redis.from_url(os.environ['CELERY_RESULT_BACKEND']);assert broker.connection_pool.connection_kwargs['db']==8 and backend.connection_pool.connection_kwargs['db']==9
for k in ['dvr:recover_dispatch_lock','core:version_check_dispatch_lock']:cache.set(k,'1',ex=3600)
TASK_MODE=os.environ.get('SPOOL_TASK_MODE')=='1'
report={'task_roundtrip':TASK_MODE,'passed':False,'checks':[],'cases':[],'scope':'Source259-file reference vs normal native explicit spool scanner service; exact rows/markers/event/createorder and untouched files; no task/Main/ACK/Originalworker/performance claim'};peer=None;log=None;made=[]
layer=get_channel_layer();channel='specific.rust_owned_scanner!receiver';channelkey=layer.prefix+channel.split('!')[0]+'!'
def check(name,value):
 report['checks'].append({'name':name,'passed':bool(value)})
 if not value:raise AssertionError(name)
def response():
 while True:
  raw=peer.stdout.readline()
  if not raw:raise RuntimeError('Native spool EOF')
  if raw.startswith('NATIVE_LOGO_TASK_JSON='if TASK_MODE else'NATIVE_LOGO_JSON='):return json.loads(raw.split('=',1)[1])
def ask(value):peer.stdin.write(json.dumps(value)+'\n');peer.stdin.flush();return response()
def scratch_clean():
 entries=list(Path('/data/scanner-work').iterdir())
 if len(entries)!=1 or entries[0].name!='.scanner-owner-v2':return False
 value=entries[0].read_bytes()
 return len(value)==136 and value[:8]==b'LSPOOL02' and entries[0].stat().st_mode&0o777==0o600

def rows():return list(Logo.objects.order_by('id').values('id','name','url'))
def stat(p):return {'path':str(p),'bytes':p.stat().st_size,'mtime_ns':p.stat().st_mtime_ns,'sha256':hashlib.sha256(p.read_bytes()).hexdigest()}
def marker_snapshot(keys):
 return {k:{'value_ascii':None if cache.get(k)is None else cache.get(k).decode(),'ttl':cache.ttl(k)}for k in keys}
def reset():
 for path in made:
  if path.exists():path.unlink()
  cache.delete('processed_file:'+str(path))
 made.clear();Logo.objects.filter(url__startswith=str(ROOT)+'/rust-owned-large-').delete();Logo.objects.filter(url__startswith=str(ROOT)+'/sub1/rust-owned-large-').delete();Logo.objects.filter(url__startswith=str(ROOT)+'/sub2/rust-owned-large-').delete();cache.delete(channelkey);async_to_sync(layer.group_add)('updates',channel)
 with connection.cursor()as c:c.execute('DROP TRIGGER IF EXISTS rust_owned_logo_fail ON dispatcharr_channels_logo');c.execute('DROP FUNCTION IF EXISTS rust_owned_logo_fail()')
try:
 for path in ['/data/m3us','/data/epgs','/data/logos']:Path(path).mkdir(exist_ok=True);check('owned-empty-'+path,not list(Path(path).iterdir()))
 EPGSource.objects.all().update(is_active=False);log=open('/evidence/native-spool-peer-private.log','w');peer=subprocess.Popen(['/native-task-peer','logo_task_peer::native_logo_task_peer'if TASK_MODE else'logo_spool_peer::native_logo_spool_peer','--ignored','--nocapture'],stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=log,text=True);
 if TASK_MODE:peer.stdin.write(json.dumps({'now':NOW,'spooled':True})+'\n');peer.stdin.flush();check('actual-spooled-task-prepared',response()['state']=='Prepared');ask({'action':'start'});report['scope']='Actual Source producer→normal native spooled LogoApplication→Original AsyncResult plus Source259-file effects; no Main/complete scanner/ACK timing/performance claim'
 else:check('real-native-spool-peer-ready',response()=={'ready':True})
 for label in ['259-files-walk','259-files-one-caught-SQL-error']:
  expected=next(v for v in reference['cases']if v['name']==label);reset();by_path={v['path']:v for v in expected['before']['files']}
  creation=[str(ROOT/((('sub%d/'%(i%3))if i%3 else'')+'rust-owned-large-%03d.png'%i))for i in range(257)]+[str(ROOT/'rust-owned-large-ignore.txt'),str(ROOT/'sub1/rust-owned-large-ignore.dat')]
  assert set(creation)==set(reference['large_pre_execution_walk'])
  for text in creation:
   p=Path(text);assert p.is_relative_to(ROOT);p.parent.mkdir(parents=True,exist_ok=True);p.write_bytes(b'owned synthetic metadata-only logo fixture\n');os.utime(p,ns=(by_path[text]['mtime_ns'],by_path[text]['mtime_ns']));check(label+'-input-file-'+p.name,stat(p)==by_path[text]);made.append(p)
  actual_walk=[str(Path(r)/name)for r,dirs,names in os.walk(ROOT)for name in names];report['input_walk_'+label]=actual_walk;check(label+'-Source-input-walk-before-native-exact',actual_walk==reference['large_pre_execution_walk'])
  if 'caught'in label:
   with connection.cursor()as c:c.execute("CREATE FUNCTION rust_owned_logo_fail() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN IF NEW.name='rust-owned-large-128' THEN RAISE EXCEPTION 'rust-owned per-file large failure' USING ERRCODE='P0001'; END IF; RETURN NEW; END$$");c.execute('CREATE TRIGGER rust_owned_logo_fail BEFORE INSERT ON dispatcharr_channels_logo FOR EACH ROW EXECUTE FUNCTION rust_owned_logo_fail()')
  before=[stat(p)for p in made]
  if TASK_MODE:
   produced=original_task.apply_async(headers={'periodic_task_name':'scan-files'});end=time.monotonic()+60
   while not produced.ready():
    if time.monotonic()>end:raise TimeoutError('Spooled task result')
    time.sleep(.025)
   metadata=json.loads(backend.get(app.backend.get_key_for_task(produced.id)));check(label+'-Source-consumer-null',produced.get(timeout=2)is None and metadata['status']=='SUCCESS'and metadata['result']is None and metadata['children']==[]);actual={'task_metadata':metadata,'state':ask({'action':'state'})};check(label+'-first-complete-after-cleanup',actual['state']['first']is True)
  else:actual=ask({'now':NOW})
  events=[layer.deserialize(v)for v in cache.zrange(channelkey,0,31)];current_rows=rows();keys=expected['after']['markers'];markers=marker_snapshot(keys)
  report['cases'].append({'name':label,'native':actual,'rows':current_rows,'events':events,'markers':markers,'files':before,'source_parent_id':expected['parent_id']})
  summary=expected['events'][0]['data']
  if TASK_MODE:check(label+'-actual-task-event-counts',len(events)==1 and all(events[0]['data'][n]==summary[n]for n in ['processed','skipped','errors','total_files']))
  else:check(label+'-normal-service-return',actual['ok']is True);check(label+'-actual-service-counts',all(actual[n]==summary[n]for n in ['processed','skipped','errors','total_files']))
  check(label+'-exact-rows-including-IDs-and-order',current_rows==expected['after']['rows']);check(label+'-exact-Channels-event',events==expected['events']);check(label+'-all-input-files-untouched',[stat(p)for p in made]==before);check(label+'-every-marker-byte-exact',all(v['value_ascii']==expected['after']['markers'][k]['value_ascii']for k,v in markers.items()));check(label+'-marker-TTL-profile',all((259100<=v['ttl']<=259200)if v['value_ascii']is not None else v['ttl']==-2 for v in markers.values()));check(label+'-no-private-spool-residue',scratch_clean())
 if TASK_MODE:
  before_rows=rows();bad=ROOT/'rust-owned-spool-invalid.png';bad.write_bytes(b'owned-invalid-marker');os.utime(bad,ns=(1699999993125000000,1699999993125000000));cache.set('processed_file:'+str(bad),'owned-invalid-float');failed=original_task.delay();end=time.monotonic()+60
  while not failed.ready():
   if time.monotonic()>end:raise TimeoutError('Spooled failure result')
   time.sleep(.025)
  value=json.loads(backend.get(app.backend.get_key_for_task(failed.id)));check('known-invalid-marker-task-failure',value['status']=='FAILURE'and value['result']=={'exc_type':'ValueError','exc_module':'builtins','exc_message':["could not convert string to float: 'owned-invalid-float'"]});check('known-failure-only-after-spool-cleanup',scratch_clean()and rows()==before_rows)
  try:failed.get(timeout=2)
  except ValueError as error:check('actual-Source-ValueError-consumer',error.args==("could not convert string to float: 'owned-invalid-float'",))
  else:raise AssertionError('Source consumer did not raise known ValueError')
  report['failure_metadata']=value
 if TASK_MODE:
  # Actual earlyACK process loss while a real PostgreSQL reply is blocked.
  bad.unlink();cache.delete('processed_file:'+str(bad));reset()
  import signal
  lost_file=ROOT/'rust-owned-large-hardloss.png';lost_file.write_bytes(b'owned hardloss fixture');os.utime(lost_file,ns=(1699999993125000000,1699999993125000000));made.append(lost_file)
  blocker=connection.copy(alias='rust_owned_scanner_blocker');blocker.set_autocommit(False);cur=blocker.cursor();cur.execute('LOCK TABLE dispatcharr_channels_logo IN ACCESS EXCLUSIVE MODE')
  lost=original_task.delay();end=time.monotonic()+15
  while True:
   cur.execute("SELECT pg_stat_clear_snapshot()");cur.execute("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname=current_database() AND pid<>pg_backend_pid() AND wait_event_type='Lock' AND query LIKE '%dispatcharr_channels_logo%')")
   if cur.fetchone()[0]:break
   if time.monotonic()>end:raise TimeoutError('Actual native SQL lock witness')
   time.sleep(.02)
  check('hardloss-earlyACK-queue-empty',broker.llen('celery')==0 and broker.hlen('unacked')==0 and broker.zcard('unacked_index')==0);check('hardloss-no-terminal-before-kill',backend.get(app.backend.get_key_for_task(lost.id))is None)
  scratch=list(Path('/data/scanner-work').iterdir());check('hardloss-real-spool-before-kill',len(scratch)==2 and any(p.name.startswith('.rust-owned-logo-spool-v2-')and p.stat().st_size>8 for p in scratch))
  peer.kill();check('actual-SIGKILL-exit',peer.wait(timeout=5)==-signal.SIGKILL);peer=None;blocker.rollback();blocker.close();time.sleep(.2)
  check('hardloss-residual-recorded-file-retained',len(list(Path('/data/scanner-work').iterdir()))==2);check('hardloss-no-guessed-terminal',lost.state=='PENDING' and backend.get(app.backend.get_key_for_task(lost.id))is None)
  # New process and packaged service UID use the same normal production path.
  assert os.geteuid()==0
  for p in [Path('/data/scanner-work'),*Path('/data/scanner-work').iterdir()]:os.chown(p,65532,65532)
  def unprivileged():os.setgid(65532);os.setuid(65532)
  peer=subprocess.Popen(['/native-task-peer','logo_task_peer::native_logo_task_peer','--ignored','--nocapture'],stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=log,text=True,preexec_fn=unprivileged)
  peer.stdin.write(json.dumps({'now':NOW,'spooled':True})+'\n');peer.stdin.flush();check('UID65532-fresh-worker-prepared',response()['state']=='Prepared');ask({'action':'start'})
  fresh=original_task.delay();end=time.monotonic()+15
  while not fresh.ready():
   if time.monotonic()>end:raise TimeoutError('Fresh task after scratch recovery')
   time.sleep(.02)
  check('fresh-task-Source-consumer-null',fresh.get(timeout=2)is None);check('fresh-worker-recovered-exact-scratch',scratch_clean());check('fresh-worker-real-logo-row',Logo.objects.filter(url=str(lost_file)).count()==1);check('fresh-worker-real-marker',cache.get('processed_file:'+str(lost_file))==b'1699999993.125');check('lost-task-not-replayed-bounded',lost.state=='PENDING' and broker.llen('celery')==0);check('recovered-control-UID65532',Path('/data/scanner-work/.scanner-owner-v2').stat().st_uid==65532)
  report['hardloss']={'lost_task_id':lost.id,'fresh_task_id':fresh.id,'bounded_nonreplay_seconds':.2,'fresh_metadata':json.loads(backend.get(app.backend.get_key_for_task(fresh.id))),'uid':65532,'scope':'Actual normal spooled task, real held SQL then SIGKILL; no visibility-expiry or power-loss claim'}
 check('actual-owner-shutdown',ask({'action':'shutdown'})['drained'if TASK_MODE else'shutdown']is True);peer.stdin.close();check('native-process-exit0',peer.wait(timeout=10)==0);peer=None
 if not TASK_MODE:
  reset();physical=subprocess.run(['/native-task-peer','logo_spool_resources::','--ignored','--nocapture','--test-threads=1'],text=True,capture_output=True,timeout=120);Path('/evidence/physical-private.log').write_text(physical.stdout+physical.stderr);check('physical-resource-exit0',physical.returncode==0);check('all-four-spool-ownership-controls','4 passed; 0 failed'in physical.stdout)
 report['physical_tests']=0 if TASK_MODE else 4;report['passed']=True
finally:
 if peer and peer.poll()is None:peer.kill();peer.wait(timeout=5)
 if log:log.close()
 Path('/evidence/source-native-spool-private.json').write_text(json.dumps(report,indent=2,default=str)+'\n')
print(json.dumps({'passed':report['passed'],'checks':len(report['checks'])}))
