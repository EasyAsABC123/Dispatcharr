"""Owned actual Original scanner -> unmodified default worker descendant capture.
No task routing overrides/quarantine. Synthetic local M3U/XML only, no providers.
Original Dispatcharr/Django/Celery/Redis/Channels authors retain credit/licenses.
"""
import base64,hashlib,inspect,json,os,signal,subprocess,sys,time,uuid,select,copy,threading,fcntl
from datetime import datetime,timezone as utc_timezone
from compare_full import compare_case,Clocks
from owned_pg_gate_proxy import Proxy
from ledger_io import append_receipt
from pathlib import Path
assert os.environ.get('JOB_DISPOSABLE_FIXTURE')=='playback-fixture-only'
assert os.environ['POSTGRES_DB'].startswith('rust_probe_backup_jobs_')
sys.path[:0]=['/app','/pair-tools'];sys.argv.append('shell');os.environ.setdefault('DJANGO_SETTINGS_MODULE','dispatcharr.settings')
import django;django.setup()
import redis
from asgiref.sync import async_to_sync
from channels.layers import get_channel_layer
from dispatcharr.celery import app
from django.db import connection
from django.core.cache import cache as django_cache
from apps.m3u.models import M3UAccount,M3UAccountProfile
from apps.epg.models import EPGSource,EPGSourceIndex,EPGData,ProgramData
from core.models import CoreSettings,SYSTEM_SETTINGS_KEY,SystemEvent
from django_celery_beat.models import PeriodicTask
import core.tasks as scanner
from celery.signals import before_task_publish
TASK='core.tasks.scan_and_process_files';NOW=1700000000.125
broker=redis.Redis.from_url(os.environ['CELERY_BROKER_URL']);backend=redis.Redis.from_url(os.environ['CELERY_RESULT_BACKEND']);cache=redis.Redis(host='localhost',port=6379,password='playback-fixture-only',db=0)
assert broker.connection_pool.connection_kwargs['db']==8 and backend.connection_pool.connection_kwargs['db']==9
assert broker.dbsize()==backend.dbsize()==0
app.loader.import_default_modules()
for key in ['dvr:recover_dispatch_lock','core:version_check_dispatch_lock']:cache.set(key,'1',ex=3600)
layer=get_channel_layer();channel='specific.rust_owned_scanner_children!receiver';channel_key=layer.prefix+channel.split('!')[0]+'!';group_key=layer._group_key('updates')
report={'passed':False,'native_verified':False,'checks':[],'cases':[],'predeclared_cases':['m3u-inactive','m3u-empty-active','m3u-one-stream-active','m3u-missing-local-file','m3u-groups-lock-busy','m3u-single-lock-busy','m3u-group-SQL-P0001','m3u-stream-SQL-P0001'],'scope':'Actual pinned Original scanner and real same-worker default-queue children; no route quarantine or native claim; synthetic local files only'}
report['controlled_elapsed_clock']=1000.125
worker=None;made=[];publications={};native=None;pg=None;injector=None;known_ids=set();clocks=Clocks()
QA=json.loads(Path('/pair-tools/qa-policy.json').read_text())
REFERENCE=json.loads(Path('/pair-tools/reference.json').read_text())
RESIDUAL_KEYS={'task_lock_refresh_m3u_account_groups_'+str(c['after']['accounts'][0]['id'])for c in REFERENCE['cases']if c['name']in ['m3u-missing-local-file','m3u-group-SQL-P0001']}
report['scope']='Actual Original scanner producer -> native ALL8 groups/single domains incl2known failures -> OriginalAsyncResult; full temporal fields and raw effects retained; no Source fallback/Main/performance'
report['source_command_intervals']=None;report['qa_policy_sha256']=hashlib.sha256(Path('/pair-tools/qa-policy.json').read_bytes()).hexdigest()
def utc():return datetime.now(utc_timezone.utc).isoformat()
def save():Path('/evidence/source-logo-scanner-private.json').write_text(json.dumps(report,indent=2,default=str)+'\n')
def ledger(stage):
 entry={'stage':stage,'epoch':time.time(),'utc':utc(),'queue':[],'backend':{},'unacked':{}}
 frames=broker.lrange('celery',0,-1)
 if len(frames)>64 or sum(map(len,frames))>2097152:
  append_receipt('/evidence/transport-ledger.jsonl',{'stage':stage,'kind':'frame-capacity-refusal','count':len(frames),'bytes':sum(map(len,frames))});raise RuntimeError('OwnedFrameLedgerCapacity')
 for frame in frames:
  decoded=json.loads(frame);known_ids.add(decoded['headers']['id']);entry['queue'].append({'raw_base64':base64.b64encode(frame).decode(),'decoded':decoded})
 unacked=broker.hgetall('unacked')
 if len(unacked)>64:
  append_receipt('/evidence/transport-ledger.jsonl',{'stage':stage,'kind':'unacked-count-refusal','count':len(unacked)});raise RuntimeError('OwnedUnackedLedgerCapacity')
 for tag,value in unacked.items():
  if len(value)>262144:
   append_receipt('/evidence/transport-ledger.jsonl',{'stage':stage,'kind':'unacked-value-refusal','bytes':len(value),'sha256':hashlib.sha256(value).hexdigest()});raise RuntimeError('OwnedUnackedBytes')
  entry['unacked'][tag.decode()]={'raw_base64':base64.b64encode(value).decode(),'index_score':broker.zscore('unacked_index',tag)}
 for ident in sorted(known_ids):
  key=app.backend.get_key_for_task(ident);raw=backend.get(key)
  if raw is not None and len(raw)>262144:
   append_receipt('/evidence/transport-ledger.jsonl',{'stage':stage,'kind':'backend-value-refusal','task_id':ident,'bytes':len(raw),'sha256':hashlib.sha256(raw).hexdigest()});raise RuntimeError('OwnedBackendLedgerCapacity')
  entry['backend'][ident]={'key':key.decode(),'raw_base64':None if raw is None else base64.b64encode(raw).decode(),'ttl':backend.ttl(key)}
 receipt=append_receipt('/evidence/transport-ledger.jsonl',entry)
 if not receipt['full_record_retained']:raise RuntimeError('RetainedLedgerCapacityRefusal')
@before_task_publish.connect
def publication(sender=None,headers=None,body=None,properties=None,exchange=None,routing_key=None,**extra):
 if sender==TASK:
  known_ids.add(headers['id']);publications[headers['id']]={'headers':headers,'body':body,'properties':properties,'exchange':exchange,'routing_key':routing_key}
def check(name,value):
 ledger('before-check:'+name)
 report['checks'].append({'name':name,'passed':bool(value)})
 save()
 if not value:raise AssertionError(name)
def records(kind):
 p=Path('/evidence/periodic-worker-'+kind+'.jsonl')
 if not p.exists():return []
 with p.open()as stream:
  fcntl.flock(stream.fileno(),fcntl.LOCK_SH)
  return [json.loads(v)for v in stream.read().splitlines()]
def stop():
 global worker
 if worker and worker.poll()is None:
  os.killpg(worker.pid,signal.SIGTERM)
  try:worker.wait(timeout=15)
  except subprocess.TimeoutExpired:
   os.killpg(worker.pid,signal.SIGKILL);worker.wait(timeout=5);report['forced_cleanup']=True;raise RuntimeError('SourceParentForcedCleanup')
 worker=None
def start(queue):
 if queue!='rust-owned-scanner-parents':raise ValueError('NoOriginalChildConsumerAuthorized')
 log=open('/evidence/scanner-worker-private.log','ab')
 try:p=subprocess.Popen(['/dispatcharrpy/bin/celery','-A','periodic_worker_observer:app','worker','--pool=prefork','--concurrency=1','-Q',queue,'--without-gossip','--without-mingle','--without-heartbeat','--loglevel=WARNING','--hostname=rust-scanner-children@fixture'],env=dict(os.environ,C_FORCE_ROOT='true',PYTHONPATH='/pair-tools:/app'),stdout=log,stderr=log,start_new_session=True)
 finally:log.close()
 return p
def raw_snapshot(m,e):
 a=list(M3UAccount.objects.filter(file_path=str(m)).values())if m else[];s=list(EPGSource.objects.filter(file_path=str(e)).values())if e else[]
 aids=[x['id']for x in a];sids=[x['id']for x in s]
 from apps.channels.models import Stream,ChannelGroup,ChannelGroupM3UAccount
 return {'accounts':a,'profiles':list(M3UAccountProfile.objects.filter(m3u_account_id__in=aids).values()),'sources':s,'indices':list(EPGSourceIndex.objects.filter(source_id__in=sids).values()),'epg_data':list(EPGData.objects.filter(epg_source_id__in=sids).values()),'programmes':list(ProgramData.objects.filter(epg__epg_source_id__in=sids).values()),'streams':list(Stream.objects.filter(m3u_account_id__in=aids).values()),'all_owned_streams':list(Stream.objects.filter(name__startswith='Owned ').values()),'group_links':list(ChannelGroupM3UAccount.objects.filter(m3u_account_id__in=aids).values()),'groups':list(ChannelGroup.objects.values()),'system_events':list(SystemEvent.objects.filter(details__account_name__startswith='rust-owned-children-').order_by('id').values()),'output_cache':{'m3u':{name:django_cache.get(name)for name in ['m3u_content:rust-owned-a','m3u_content:rust-owned-b']},'unrelated':django_cache.get('rust-owned-cache-preserve'),'generation_ascii':cache.get('epg_content_generation').decode(),'generation_ttl':cache.ttl('epg_content_generation'),'retired_chunk_base64':[base64.b64encode(v).decode()for v in cache.lrange('rust-owned-scanner-epg:g=700:chunks',0,3)],'retired_chunk_ttl':cache.ttl('rust-owned-scanner-epg:g=700:chunks')},'events':[layer.deserialize(v)for v in cache.zrange(channel_key,0,255)],'locks':{k.decode():{'raw_base64':base64.b64encode(cache.get(k)).decode(),'ttl':cache.ttl(k)}for k in cache.scan_iter('task_lock_*')},'files':[{'path':str(p),'size':p.stat().st_size,'mtime_ns':p.stat().st_mtime_ns,'sha256':hashlib.sha256(p.read_bytes()).hexdigest()}for p in made if p.exists()],'cache_files':[{'path':str(p),'size':p.stat().st_size,'sha256':hashlib.sha256(p.read_bytes()).hexdigest(),'raw_base64':base64.b64encode(p.read_bytes()).decode()if p.stat().st_size<=65536 else None}for root in ['/app/media/cached_m3u','/app/media/cached_epg','/data/epgs']for p in Path(root).rglob('*')if p.is_file()and p not in made]}
def snapshot(m,e):return json.loads(json.dumps(raw_snapshot(m,e),default=str))
def case(label,active,mtext=None,etext=None):
 fault=label not in ['m3u-inactive','m3u-empty-active','m3u-one-stream-active']
 global worker,made,native,pg,injector
 case_began=utc();command_windows={};active_native_id=None;injector_errors=[]
 stop();check(label+'-prior-default-queue-empty',broker.llen('celery')==broker.hlen('unacked')==0)
 for p in made:
  if p.exists():p.unlink()
 made=[];M3UAccount.objects.all().update(is_active=False);EPGSource.objects.all().update(is_active=False)
 CoreSettings._update_group(SYSTEM_SETTINGS_KEY,'System Settings',{'auto_import_mapped_files':active})
 cache.delete(channel_key,group_key);async_to_sync(layer.group_add)('updates',channel)
 m=e=None
 for root,text,ext in [('/data/m3us',mtext,'.m3u'),('/data/epgs',etext,'.xml')]:
  if text is not None:
   p=Path(root)/('rust-owned-children-'+label+ext);p.write_text(text);os.utime(p,(NOW-7.125,NOW-7.125));made.append(p)
   if ext=='.m3u':m=p
   else:e=p
 django_cache.set('m3u_content:rust-owned-a','owned-a',timeout=600);django_cache.set('m3u_content:rust-owned-b','owned-b',timeout=600);django_cache.set('rust-owned-cache-preserve','preserve',timeout=600);cache.set('epg_content_generation','700',ex=600);cache.delete('rust-owned-scanner-epg:g=700:chunks');cache.rpush('rust-owned-scanner-epg:g=700:chunks','owned retired XMLTV');cache.expire('rust-owned-scanner-epg:g=700:chunks',600)
 before=snapshot(m,e);prior_ids={v['id']for v in records('postrun')};tid=str(uuid.uuid4());Path('/evidence/owned-scanner-config.json').write_text(json.dumps({'task_id':tid,'now':NOW}));worker=start('rust-owned-scanner-parents');result=app.tasks[TASK].apply_async(args=[],kwargs={},task_id=tid,queue='rust-owned-scanner-parents')
 entry={'name':label,'parent_id':tid,'before':before,'parent_publication':publications[tid]};report['cases'].append(entry)
 end=time.monotonic()+30
 while not any(v['id']==tid for v in records('postrun')):
  assert time.monotonic()<end;time.sleep(.025)
 stop();entry['producer_window']={'began_utc':case_began,'finished_utc':utc()};account=M3UAccount.objects.get(file_path=str(m));entry['effective_hash_keys']=account.get_effective_hash_keys();entry['m3u_hash_setting']=CoreSettings.get_m3u_hash_key()
 if label=='m3u-missing-local-file':m.unlink()
 if label in ['m3u-groups-lock-busy','m3u-single-lock-busy']:
  key=('task_lock_refresh_m3u_account_groups_'if label=='m3u-groups-lock-busy'else'task_lock_refresh_single_m3u_account_')+str(account.id);cache.set(key,'rust-owned-foreign-token',ex=300);entry['owned_busy_key']=key
 if label in ['m3u-group-SQL-P0001','m3u-stream-SQL-P0001']:
  table='dispatcharr_channels_channelgroup'if label=='m3u-group-SQL-P0001'else'dispatcharr_channels_stream'
  with connection.cursor()as c:
   c.execute('CREATE SEQUENCE rust_owned_m3u_fail_count')
   c.execute("CREATE OR REPLACE FUNCTION rust_owned_m3u_worker_fail() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN PERFORM nextval('rust_owned_m3u_fail_count'); RAISE EXCEPTION 'rust-owned M3U worker failure' USING ERRCODE='P0001'; END$$")
   if label!='m3u-group-SQL-P0001':c.execute('CREATE TRIGGER rust_owned_m3u_worker_fail BEFORE INSERT OR UPDATE ON '+table+' FOR EACH ROW EXECUTE FUNCTION rust_owned_m3u_worker_fail()')
 pg=Proxy(QA,hold_preflight=label=='m3u-group-SQL-P0001')
 log=open('/evidence/native-m3u-'+label+'.log','wb')
 try:native=subprocess.Popen(['/native-m3u-peer','scanner_m3u_peer::native_scanner_m3u_peer','--exact','--ignored','--nocapture','--test-threads=1'],stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=log,text=True,bufsize=1)
 except BaseException:log.close();entry['native_pg']=pg.close();pg=None;raise
 receive_buffer=bytearray()
 def receive():
  nonlocal receive_buffer
  end=time.monotonic()+35
  while time.monotonic()<end:
   while b'\n'in receive_buffer:
    line,_,tail=receive_buffer.partition(b'\n');receive_buffer=bytearray(tail)
    if b'NATIVE_SCANNER_M3U_JSON='in line:return json.loads(line.split(b'NATIVE_SCANNER_M3U_JSON=',1)[1])
   if select.select([native.stdout],[],[],.1)[0]:
    data=os.read(native.stdout.fileno(),65536);log.write(data);log.flush();receive_buffer.extend(data)
    if len(receive_buffer)>1048576:raise ValueError('OwnedPeerLineCapacity')
    if not data and native.poll()is not None:raise RuntimeError('NativeEOFBeforeReply')
   if native.poll()is not None and not receive_buffer:raise RuntimeError('NativeExitedBeforeReply')
  raise TimeoutError('Native M3U response deadline')
 def command(v):
  began=utc();native.stdin.write(json.dumps(v)+'\n');native.stdin.flush();answer=receive();finished=utc()
  if v.get('action')=='run':command_windows[active_native_id]={'began_utc':began,'finished_utc':finished}
  return answer
 nativeids=[];outputs=[];fileposts=[]
 entry.update(native_ids=nativeids,native_outputs=outputs,native_fileposts=fileposts,source_fallback=False,command_windows=command_windows,source_command_intervals=None)
 try:
  check(label+'-native-prepared',command({'parent_id':tid,'account_id':account.id,'database_url':'postgres://dispatch:playback-fixture-only@127.0.0.1:61651/'+os.environ['POSTGRES_DB']})=={'prepared':True})
  first=broker.lindex('celery',-1);refused=command({'action':'foreign'});check(label+'-foreign-scope-retains-actual-frame',refused['run']=='Err(Transport(UnsupportedTask))'and broker.lindex('celery',-1)==first)
  while broker.llen('celery'):
   assert len(nativeids)<2
   frame=json.loads(broker.lindex('celery',-1));ident=frame['headers']['id'];active_native_id=ident
   known_ids.add(ident);family=frame['headers']['task']
   check(label+'-known-original-child-'+str(len(nativeids)),family in ['apps.m3u.tasks.refresh_m3u_groups','apps.m3u.tasks.refresh_single_m3u_account'])
   if label=='m3u-group-SQL-P0001'and family=='apps.m3u.tasks.refresh_m3u_groups':
    def inject():
     try:
      if not pg.preflight_held.wait(25):raise TimeoutError('ActualGuardReadyNotObserved')
      with connection.cursor()as cursor:
       cursor.execute('SELECT current_database()');assert cursor.fetchone()[0]==os.environ['POSTGRES_DB']
       cursor.execute('CREATE TRIGGER rust_owned_m3u_worker_fail BEFORE INSERT OR UPDATE ON dispatcharr_channels_channelgroup FOR EACH ROW EXECUTE FUNCTION rust_owned_m3u_worker_fail()')
     except Exception as error:injector_errors.append(type(error).__name__+':'+str(error)[:256])
     finally:connection.close();pg.preflight_release.set()
    injector=threading.Thread(target=inject);injector.start()
   answer=command({'action':'run'});outputs.append(answer)
   if injector:injector.join(2);check(label+'-actual-owned-injection-settled',not injector.is_alive()and not injector_errors);injector=None
   check(label+'-native-completed-'+str(len(nativeids)),answer['run'].startswith('Ok(Completed(')and answer['slots']==1);nativeids.append(ident)
   fileposts.append({'id':ident,'files':snapshot(m,e)['cache_files']})
   entry.setdefault('native_intermediate',[]).append({'id':ident,'task':family,'snapshot':snapshot(m,e),'utc':utc()});save()
   if label=='m3u-group-SQL-P0001'and family=='apps.m3u.tasks.refresh_m3u_groups':
    with connection.cursor()as cursor:cursor.execute('DROP TRIGGER rust_owned_m3u_worker_fail ON dispatcharr_channels_channelgroup')
  check(label+'-native-drained',command({'action':'shutdown'})=={'drained':True});native.stdin.close();native.wait(timeout=10);check(label+'-native-exit0',native.returncode==0)
 finally:
  if pg:pg.preflight_release.set();pg.error_release.set()
  if native.poll()is None:native.kill();native.wait(timeout=5)
  final_stdout_bytes=0
  while True:
   tail=os.read(native.stdout.fileno(),65536)
   final_stdout_bytes+=len(tail)
   if final_stdout_bytes>1048576:raise ValueError('OwnedFinalStdoutCapacity')
   if not tail:break
   log.write(tail)
  native.stdout.close()
  if injector:injector.join(2);injector=None
  if pg:entry['native_pg']=pg.close();pg=None
  log.close()
 entry.update(native_ids=nativeids,native_outputs=outputs,native_fileposts=fileposts,source_fallback=False,command_windows=command_windows,source_command_intervals=None)
 check(label+'-actual-PG-relay-closed',entry['native_pg']['physical_close_verified']and not entry['native_pg']['errors'])

 end=time.monotonic()+65;stable=None
 while time.monotonic()<end:
  posts=[v for v in records('postrun')if v['id']not in prior_ids];pubs=[v for v in records('children')if v['headers'].get('root_id')==tid];expected={tid}|{v['headers']['id']for v in pubs};done={v['id']for v in posts}|set(nativeids)
  idle=expected<=done and broker.llen('celery')==broker.hlen('unacked')==0
  if idle:
   if stable is None:stable=time.monotonic()
   if time.monotonic()-stable>=.5:break
  else:stable=None
  if worker and worker.poll()is not None:raise RuntimeError('Owned original worker exited')
  time.sleep(.025)
 else:raise TimeoutError('Actual descendants did not drain in fixed65s')
 entry.update(fileposts=[v for v in records('filepost')if v['id']in expected],after=snapshot(m,e),publications=pubs,postruns=posts,preruns=[v for v in records('prerun')if v['id']in expected],received=[v for v in records('received')if v['id']in expected],sql=[v for v in records('sql')if v['id']in expected],results={})
 for taskid in expected:
  key=app.backend.get_key_for_task(taskid);raw=backend.get(key)
  entry['results'][taskid]={'metadata':None if raw is None else json.loads(raw),'raw_base64':None if raw is None else base64.b64encode(raw).decode(),'ttl':backend.ttl(key)}
 check(label+'-parent-null-success',entry['results'][tid]['metadata']['status']=='SUCCESS'and result.get(timeout=2)is None)
 check(label+'-all-real-children-completed',expected==done and len(entry['preruns'])+len(nativeids)==len(expected))
 check(label+'-all-metadata-retained',all(v['metadata']is not None and 3500<=v['ttl']<=3600 for v in entry['results'].values()))
 check(label+'-lineage-root-parent-preserved',all(v['headers']['root_id']==tid and v['headers']['parent_id']in expected for v in pubs))
 check(label+'-all-default-route',all(v['exchange']=='' and v['routing_key']=='celery'for v in pubs) and all(v['delivery_info'].get('routing_key')=='celery'for v in entry['received']if v['id']!=tid))
 if label!='m3u-missing-local-file':check(label+'-full-input-bytes-unchanged',before['files']==entry['after']['files'])
 check(label+'-prerun-reservations-recorded',all(isinstance(v['unacked_task_ids'],list)for v in entry['preruns']))
 check(label+'-known-task-attributes',all(v['attrs']['acks_late']is False and v['attrs']['ignore_result']is False and v['attrs']['track_started']is False for v in entry['preruns']))
 if not fault:check(label+'-all-normal-success',all(v['state']=='SUCCESS'for v in posts))
 if not fault:check(label+'-actual-parent-and-descendant-metadata',all(v['metadata']['task_id']==k and v['metadata']['status']=='SUCCESS'and v['metadata']['traceback']is None and set(v['metadata'])>=set(['status','result','traceback','children','date_done','task_id'])for k,v in entry['results'].items()))
 if not fault:check(label+'-all-owned-task-locks-released',not entry['after']['locks'])
 check(label+'-no-unrelated-families',all(v['task']in [TASK,'apps.epg.tasks.refresh_epg_data','apps.epg.tasks.build_programme_index_task','apps.channels.tasks.evaluate_series_rules','apps.m3u.tasks.refresh_m3u_groups','apps.m3u.tasks.refresh_single_m3u_account']for v in posts))
 if label in ['m3u-group-SQL-P0001','m3u-stream-SQL-P0001']:
  with connection.cursor()as c:c.execute('SELECT last_value,is_called FROM rust_owned_m3u_fail_count');last,called=c.fetchone()
  entry['trigger_calls']=last if called else 0;check(label+'-actual-server-fault-executed',entry['trigger_calls']>=1)
 for ident,row in entry['results'].items():
  value=app.AsyncResult(ident).get(timeout=2,propagate=False)
  row['Original_get_type']=type(value).__module__+'.'+type(value).__name__
  if isinstance(value,BaseException):row['Original_exception_args']=json.loads(json.dumps(value.args))
  else:row['Original_get_value']=json.loads(json.dumps(value,default=str))
 entry['observed_hooks']=[v for v in records('hooks')if v['id']in expected]
 check(label+'-unrelated-and-retired-cache-retained',entry['after']['output_cache']['unrelated']=='preserve'and entry['after']['output_cache']['retired_chunk_base64']==before['output_cache']['retired_chunk_base64']and 500<=entry['after']['output_cache']['retired_chunk_ttl']<=600)
 check(label+'-postrun-states-match-backend',all(entry['results'][v['id']]['metadata']['status']==v['state']for v in posts))
 if label=='m3u-group-SQL-P0001':check(label+'-actual-group-handler-raised',next(entry['results'][v['headers']['id']]['metadata']for v in pubs if v['task']=='apps.m3u.tasks.refresh_m3u_groups')['status']=='FAILURE')
 reference=next(v for v in json.loads(Path('/pair-tools/reference.json').read_text())['cases']if v['name']==label)
 entry['case_window']={'began_utc':case_began,'finished_utc':utc()}
 entry['Source_comparison']=compare_case(reference,entry,clocks,RESIDUAL_KEYS)
 check(label+'-full-Source-effect-and-temporal-contract',True)
 stop()
 with connection.cursor()as c:
  for table in ['dispatcharr_channels_channelgroup','dispatcharr_channels_stream']:c.execute('DROP TRIGGER IF EXISTS rust_owned_m3u_worker_fail ON '+table)
  c.execute('DROP FUNCTION IF EXISTS rust_owned_m3u_worker_fail()');c.execute('DROP SEQUENCE IF EXISTS rust_owned_m3u_fail_count')
try:
 for root in ['/data/logos','/data/m3us','/data/epgs']:Path(root).mkdir(exist_ok=True);check('fresh-empty-'+root,not list(Path(root).iterdir()))
 import apps.m3u.tasks as mt,apps.epg.tasks as et
 for name,obj in [('scanner',scanner.scan_and_process_files.run),('groups',mt.refresh_m3u_groups.run),('single',mt.refresh_single_m3u_account.run),('epg',et.refresh_epg_data.run)]:Path('/evidence/original-'+name+'.py').write_text(inspect.getsource(obj))
 case('m3u-inactive',False,mtext='#EXTM3U\n')
 case('m3u-empty-active',True,mtext='#EXTM3U\n')
 for label in ['m3u-one-stream-active','m3u-missing-local-file','m3u-groups-lock-busy','m3u-single-lock-busy','m3u-group-SQL-P0001','m3u-stream-SQL-P0001']:
  text='#EXTM3U\n#EXTINF:-1 tvg-id="owned.channel" group-title="Owned '+label+'",Owned stream\nhttp://127.0.0.1:9/owned-never-played.ts\n'
  # Positive reference retains exactly its original group to compare captured semantics.
  if label=='m3u-one-stream-active':text=text.replace('Owned '+label,'Owned Group')
  case(label,True,mtext=text)
 report['passed']=True;report['native_verified']=True;save()
finally:
 cleanup_errors=[]
 for role,closer in [('Source',stop),('PG',lambda:pg.close()if pg else None),('native',lambda:(native.kill(),native.wait(timeout=5))if native and native.poll()is None else None)]:
  try:
   value=closer();report.setdefault('finally_closers',{})[role]=value
   if role=='PG'and value and not value['physical_close_verified']:cleanup_errors.append('PG:unsettled')
  except BaseException as error:cleanup_errors.append(role+':'+type(error).__name__)
 if injector:injector.join(2);cleanup_errors+=['injection-thread-unsettled']if injector.is_alive()else[]
 report['cleanup_errors']=cleanup_errors;report['partial_temporal_bindings']=list(clocks.raw)
 if cleanup_errors:report['passed']=False
 save()
print(json.dumps({'passed':report['passed'],'checks':len(report['checks']),'cases':len(report['cases'])}))

if not report['passed']:raise SystemExit(1)
