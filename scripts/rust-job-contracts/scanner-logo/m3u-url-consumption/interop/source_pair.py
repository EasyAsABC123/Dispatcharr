"""Actual pinned Original scanner/default-child URL-hash corpus; no native execution.
Owned local STD files/SQL/Redis/Channels only. Original Dispatcharr/dependency
contributors retain application code, algorithms and licenses. Explicit observer
clocks label scanner input and elapsed0.0; Django datetime/renewer remain real.
"""
import base64,hashlib,inspect,json,os,select,signal,subprocess,sys,time,uuid
from pathlib import Path
from datetime import timedelta
assert os.environ.get('JOB_DISPOSABLE_FIXTURE')=='playback-fixture-only'
assert os.environ['POSTGRES_DB'].startswith('rust_probe_backup_jobs_')
sys.path[:0]=['/app','/pair-tools'];sys.argv.append('shell');os.environ.setdefault('DJANGO_SETTINGS_MODULE','dispatcharr.settings')
from ledger_io import append_receipt
from compare_trace import compare,instant,capture_snapshot
import django;django.setup()
import redis
from asgiref.sync import async_to_sync
from channels.layers import get_channel_layer
from django.core.cache import cache as django_cache
from django.db import connection
from django.utils import timezone
from core.models import CoreSettings,STREAM_SETTINGS_KEY,SYSTEM_SETTINGS_KEY,SystemEvent,StreamProfile
from apps.m3u.models import M3UAccount,M3UAccountProfile
from apps.epg.models import EPGSource
from apps.channels.models import Stream,Channel,ChannelStream,ChannelGroup,ChannelGroupM3UAccount
from dispatcharr.celery import app
from celery.signals import before_task_publish
plan=json.loads(Path('/pair-tools/Source-plan.json').read_text());report={'passed':False,'native_verified':True,'performance_verified':False,'scope':__doc__,'plan_sha256':hashlib.sha256(Path('/pair-tools/Source-plan.json').read_bytes()).hexdigest(),'cases':[],'checks':[],'deferred_native_groups':[13,14],'transport_ledger':'transport-ledger.jsonl','status':'UNEXECUTED_REDESIGN_ROOT_REVIEW_REQUIRED'};worker=None;publications={};native=None;known_task_ids=set()
broker=redis.Redis.from_url(os.environ['CELERY_BROKER_URL']);backend=redis.Redis.from_url(os.environ['CELERY_RESULT_BACKEND']);raw=redis.Redis(host='localhost',port=6379,password='playback-fixture-only',db=0)
assert broker.connection_pool.connection_kwargs['db']==8 and backend.connection_pool.connection_kwargs['db']==9 and broker.dbsize()==backend.dbsize()==0
for key in ['dvr:recover_dispatch_lock','core:version_check_dispatch_lock']:raw.set(key,'1',ex=3600)
app.loader.import_default_modules();layer=get_channel_layer();channel='specific.rust_owned_url_hash!receiver';ckey=layer.prefix+channel.split('!')[0]+'!';gkey=layer._group_key('updates');GROUP=CoreSettings.group_cache_key(STREAM_SETTINGS_KEY)
@before_task_publish.connect
def publish(sender=None,headers=None,body=None,properties=None,exchange=None,routing_key=None,**extra):
 if sender=='core.tasks.scan_and_process_files':
  known_task_ids.add(headers['id']);publications[headers['id']]={'headers':headers,'body':body,'properties':properties,'exchange':exchange,'routing_key':routing_key}
def check(name,value):
 ledger('before-check:'+name)
 report['checks'].append({'name':name,'passed':bool(value)})
 if not value:raise AssertionError(name)
def records(kind):
 p=Path('/evidence/periodic-worker-'+kind+'.jsonl');return [json.loads(v)for v in p.read_text().splitlines()]if p.exists()else[]
def stop():
 global worker
 if worker and worker.poll()is None:
  os.killpg(worker.pid,signal.SIGTERM)
  try:worker.wait(timeout=15)
  except subprocess.TimeoutExpired:os.killpg(worker.pid,signal.SIGKILL);worker.wait(timeout=5);report['forced_cleanup']=True
 worker=None
def start(queue):
 log=open('/evidence/scanner-worker-private.log','ab');p=subprocess.Popen(['/dispatcharrpy/bin/celery','-A','periodic_worker_observer:app','worker','--pool=prefork','--concurrency=1','-Q',queue,'--without-gossip','--without-mingle','--without-heartbeat','--loglevel=WARNING','--hostname=rust-url-hash@fixture'],env=dict(os.environ,C_FORCE_ROOT='true',PYTHONPATH='/pair-tools:/app'),stdout=log,stderr=log,start_new_session=True);log.close();return p
def wait(fn,seconds=65):
 end=time.monotonic()+seconds
 while time.monotonic()<end:
  v=fn()
  if v:return v
  if worker and worker.poll()is not None:raise RuntimeError('Original owned worker exited')
  time.sleep(.025)
 raise TimeoutError('Sealed per-step drain budget')
def configure(item):
 CoreSettings.objects.update_or_create(key=STREAM_SETTINGS_KEY,defaults={'name':'Stream Settings','value':{'m3u_hash_key':item['global_raw']}})
 if 'cached'in item:django_cache.set(GROUP,item['cached'],timeout=300)
 else:django_cache.delete(GROUP)
def cache_snapshot():
 keys=sorted(raw.keys('*'));assert len(keys)<=1024;result={};total=0
 for k in keys:
  value=raw.dump(k);total+=len(value or b'');assert total<=4194304
  result[k.decode()]={'type':raw.type(k).decode(),'dump_base64':None if value is None else base64.b64encode(value).decode(),'ttl':raw.ttl(k)}
 return result
def snapshot():
 accounts=M3UAccount.objects.filter(name__startswith='rust-owned-children-url-');ids=list(accounts.values_list('id',flat=True))
 return {'stream_settings_rows':list(CoreSettings.objects.filter(key=STREAM_SETTINGS_KEY).values()),'accounts':list(accounts.order_by('id').values()),'profiles':list(M3UAccountProfile.objects.filter(m3u_account_id__in=ids).order_by('id').values()),'streams':list(Stream.objects.filter(name__startswith='Owned ').order_by('id').values()),'groups':list(ChannelGroup.objects.order_by('id').values()),'group_links':list(ChannelGroupM3UAccount.objects.filter(m3u_account_id__in=ids).order_by('id').values()),'channels':list(Channel.objects.filter(name__startswith='rust-owned-url-').order_by('id').values()),'channel_streams':list(ChannelStream.objects.filter(channel__name__startswith='rust-owned-url-').order_by('id').values()),'system_events':list(SystemEvent.objects.filter(details__account_name__startswith='rust-owned-children-url-').order_by('id').values()),'events':[layer.deserialize(v)for v in raw.zrange(ckey,0,1023)],'cache':cache_snapshot(),'files':[{'path':str(p),'bytes':p.stat().st_size,'mtime_ns':p.stat().st_mtime_ns,'sha256':hashlib.sha256(p.read_bytes()).hexdigest()}for p in sorted(Path('/data/m3us').glob('rust-owned-children-url-*.m3u'))]}
raw_snapshot=snapshot
def snapshot():return capture_snapshot(raw_snapshot())
active_label='setup'
def ledger_decode(value):
 try:return json.loads(value)
 except (ValueError,UnicodeError)as error:return {'decode_error':type(error).__name__}
def ledger(stage):
 # Fsynced owned-controller evidence precedes assertions; never ACK/pop/delete.
 # KEYS/TYPE/LLEN/LRANGE/HGETALL/STRLEN/GET are separate observations.
 # Their races are not strict transport byte bounds or atomic snapshots.
 # Only the small, owned, no-external-writer fixture domain is covered.
 entry={'stage':stage,'case':active_label,'epoch':time.time(),'queues':{},'unacked':{},'backend':{},'capacity_refusals':[]}
 keys=sorted(broker.keys('*'))
 if len(keys)>512:entry['capacity_refusals'].append('broker-key-count');keys=[]
 total=0
 for key in keys:
  if broker.type(key)!=b'list':continue
  count=broker.llen(key)
  if count>64:entry['capacity_refusals'].append('queue-count:'+repr(key));continue
  values=broker.lrange(key,0,-1);frames=[]
  for index,value in enumerate(values):
   total+=len(value)
   if len(value)>131072 or total>2097152:entry['capacity_refusals'].append('frame-bytes');break
   frames.append({'LRANGE_index':index,'raw_base64':base64.b64encode(value).decode(),'sha256':hashlib.sha256(value).hexdigest(),'decoded':ledger_decode(value)})
  entry['queues'][key.decode()]={'length':count,'LRANGE_order':frames,'delivery_end':'RPOP; raw ordering is retained, not normalized'}
 if broker.hlen('unacked')>64:entry['capacity_refusals'].append('unacked-count')
 else:
  for tag,value in broker.hgetall('unacked').items():
   if len(value)>262144:entry['capacity_refusals'].append('unacked-bytes');continue
   entry['unacked'][tag.decode()]={'raw_base64':base64.b64encode(value).decode(),'decoded':ledger_decode(value),'index_score':broker.zscore('unacked_index',tag)}
 keys=sorted(app.backend.get_key_for_task(ident)for ident in known_task_ids)
 if len(keys)>64:entry['capacity_refusals'].append('backend-count');keys=[]
 for key in keys:
  size=backend.strlen(key)
  if size>131072:entry['capacity_refusals'].append('backend-bytes');continue
  value=backend.get(key)
  entry['backend'][key.decode()]={'raw_base64':None if value is None else base64.b64encode(value).decode(),'decoded':None if value is None else ledger_decode(value),'ttl':backend.ttl(key)}
 receipt=append_receipt('/evidence/transport-ledger.jsonl',entry)
 if not receipt['full_record_retained'] or entry['capacity_refusals']:raise RuntimeError('Owned ledger capacity refusal retained')

def save():Path('/evidence/source-url-hash-private.json').write_text(json.dumps(report,indent=2,default=str)+'\n')
def peer_start(parent,account,label):
 global native
 log=open('/evidence/native-'+label+'.log','wb');native=subprocess.Popen(['/native-m3u-peer','scanner_m3u_peer::native_scanner_m3u_peer','--exact','--ignored','--nocapture','--test-threads=1'],stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=log,text=True,bufsize=1);native._owned_log=log
 def response():
  end=time.monotonic()+35
  while time.monotonic()<end:
   if select.select([native.stdout],[],[],.1)[0]:
    line=native.stdout.readline();log.write(line.encode());log.flush()
    if 'NATIVE_SCANNER_M3U_JSON='in line:return json.loads(line.split('NATIVE_SCANNER_M3U_JSON=',1)[1])
   if native.poll()is not None:raise RuntimeError('Native peer exited before response')
  raise TimeoutError('Sealed native response35s')
 def cmd(v):native.stdin.write(json.dumps(v)+'\n');native.stdin.flush();return response()
 check(label+'-normal-native-constructor',cmd({'parent_id':parent,'account_id':account})=={'prepared':True});return cmd
def peer_stop(cmd,label):
 global native
 check(label+'-actual-native-drain',cmd({'action':'shutdown'})=={'drained':True});native.stdin.close();native.wait(timeout=10);check(label+'-native-exit0',native.returncode==0);native._owned_log.close();native=None
SOURCE=json.loads(Path('/pair-tools/reference.json').read_text());SW=json.loads(Path('/pair-tools/Source-command-windows.json').read_text())['windows'];QA=json.loads(Path('/pair-tools/predeclared-qa-plan.json').read_text());refcases={v['label']:v for v in SOURCE['cases']}
plan=json.loads(Path('/pair-tools/Source-plan.json').read_text());report['scope']='Actual Original scanner producer and AsyncResult around normal native URL/Empty M3U worker; Source17 authority, explicit mapped/normalization refusals with real Original fallback; no Main/performance/fullhash authority';report['reference_sha256']=hashlib.sha256(Path('/pair-tools/reference.json').read_bytes()).hexdigest();report['execution_input_receipt']=json.loads(Path('/pair-tools/execution-input-receipt-1.json').read_text());report['Source_cohort_passed']=False;report['known_backend_keys']='Explicit app.backend.get_key_for_task only; historical wildcard gap not reused'
def mapped_meta(value,mapping):
 if isinstance(value,str):return mapping.get(value,value)
 if isinstance(value,list):return [mapped_meta(v,mapping)for v in value]
 if isinstance(value,dict):return {k:mapped_meta(v,mapping)for k,v in value.items()}
 return value
try:
 for d in ['/data/m3us','/data/epgs','/data/logos']:Path(d).mkdir(exist_ok=True);check('fresh-root-'+d,not list(Path(d).iterdir()))
 CoreSettings._update_group(SYSTEM_SETTINGS_KEY,'System Settings',{'auto_import_mapped_files':True});EPGSource.objects.all().update(is_active=False)
 for item,policy in zip(plan['actual_worker_steps'][:17],QA['Source_derived_case_order']):
  label=item['label'];active_label=label;check(label+'-sealed-case-order',label==policy['label']);stop();check(label+'-prior-queue-empty',broker.llen('celery')==broker.hlen('unacked')==0);M3UAccount.objects.all().update(is_active=False)
  path=Path('/data/m3us/rust-owned-children-url-'+item['cohort']+'.m3u');account=M3UAccount.objects.filter(file_path=str(path)).first()
  if account:M3UAccount.objects.filter(pk=account.id).update(is_active=True)
  path.write_bytes(item['playlist'].encode());os.utime(path,(item['mtime'],item['mtime']));configure(item);raw.delete(ckey,gkey);async_to_sync(layer.group_add)('updates',channel)
  if item.get('map_from'):
   a=M3UAccount.objects.get(file_path='/data/m3us/rust-owned-children-url-'+item['map_from']+'.m3u');target=Stream.objects.get(m3u_account=a,url='http://127.0.0.1:9/owned-url-mapped.ts');source_channel=next(v for v in refcases[label]['before']['channels']if v['name']=='rust-owned-url-map');ch=Channel.objects.create(name='rust-owned-url-map',channel_number=9911,uuid=source_channel['uuid']);ChannelStream.objects.create(channel=ch,stream=target,order=0)
  before=snapshot();tid=str(uuid.uuid4());known_task_ids.add(tid);Path('/evidence/owned-scanner-config.json').write_text(json.dumps({'task_id':tid,'now':item['scanner_now']}));parent=app.tasks['core.tasks.scan_and_process_files'].apply_async(args=[],task_id=tid,queue='rust-owned-scanner-parents');parent_start=time.time();ledger('parent-published-before-worker-start');worker=start('rust-owned-scanner-parents');wait(lambda:any(v['id']==tid for v in records('postrun')));parent_end=time.time();check(label+'-actual-parent-success-null',parent.get(timeout=2)is None);stop();account=M3UAccount.objects.get(file_path=str(path));M3UAccount.objects.filter(pk=account.id).update(name='rust-owned-children-url-'+item['cohort'],custom_properties=item.get('account_custom',{}));account.refresh_from_db();configure(item)
  frames=[json.loads(v)for v in reversed(broker.lrange('celery',0,-1))];known_task_ids.update(v['headers']['id']for v in frames);ledger('children-before-native');check(label+'-actual-created-existing-frames',[v['headers']['task']for v in frames]==item['expected_direct_child_families']);head=broker.lindex('celery',-1);before_native=snapshot();child_start=time.time();cmd=peer_start(tid,account.id,label);outputs=[];refused=policy['native_expectation'].startswith('preACK')
  for number in range(1 if refused else len(frames)):
   output=cmd({'action':'run'});outputs.append(output)
   if refused:check(label+'-exact-retained-refusal',output['run']=='Err(Domain(Unsupported))'and broker.lindex('celery',-1)==head)
   else:check(label+'-native-completed-'+str(number),output['run'].startswith('Ok(Completed(') and output['slots']==1)
  peer_stop(cmd,label)
  if refused:
   retained=snapshot();check(label+'-all-model-effects-beforeACK-retained',all(retained[t]==before_native[t]for t in ['accounts','profiles','streams','groups','group_links','channels','channel_streams','system_events','events','files','stream_settings_rows']));check(label+'-no-child-result-on-refusal',all(backend.get(app.backend.get_key_for_task(v['headers']['id']))is None for v in frames));worker=start('celery');wait(lambda:broker.llen('celery')==broker.hlen('unacked')==0 and all(backend.get(app.backend.get_key_for_task(v['headers']['id']))is not None for v in frames));stop()
  child_end=time.time();check(label+'-queue-unacked-drained',broker.llen('celery')==broker.hlen('unacked')==0);after=snapshot();reference=refcases[label];windows={'parent':[parent_start,parent_end],'child':[child_start,child_end]};compared=compare(reference,before,after,SW[label],windows);entry={'label':label,'Source_case_index':policy['source_case_index'],'native_refusal':refused,'native_outputs':outputs,'before':before,'before_native':before_native,'after':after,'actual_command_windows':windows,'Source_command_windows':SW[label],'comparison':compared,'frames':frames,'parent_id':tid,'results':{}};report['cases'].append(entry);save();check(label+'-Source-row-event-file-and-temporal-comparison',compared['passed'])
  mapping={reference['parent_id']:tid};mapping.update({a['headers']['id']:b['headers']['id']for a,b in zip(reference['queue_frames'],frames)})
  for ident in [tid]+[v['headers']['id']for v in frames]:
   key=app.backend.get_key_for_task(ident);value=backend.get(key);meta=json.loads(value);decoded=app.AsyncResult(ident).get(timeout=2,propagate=False);entry['results'][ident]={'metadata':meta,'raw_base64':base64.b64encode(value).decode(),'ttl':backend.ttl(key),'Original_get_type':type(decoded).__module__+'.'+type(decoded).__name__,'Original_get_value':decoded};source_id=next(k for k,v in mapping.items()if v==ident);expected=mapped_meta(reference['results'][source_id]['metadata'],mapping);check(label+'-result-field-presence-'+ident,set(meta)==set(expected));check(label+'-typed-result-and-lineage-'+ident,{k:v for k,v in meta.items()if k!='date_done'}=={k:v for k,v in expected.items()if k!='date_done'});phase='parent'if ident==tid else'child';check(label+'-retained-date-done-window-'+ident,windows[phase][0]<=instant(meta['date_done'])<=windows[phase][1]and SW[label][phase][0]<=instant(expected['date_done'])<=SW[label][phase][1]);check(label+'-actual-result-TTL-'+ident,3500<=entry['results'][ident]['ttl']<=3600)
  group_key=django_cache.make_key(GROUP);expected_group=before_native['cache'].get(group_key);check(label+'-native-readonly-cached-authority',after['cache'].get(group_key)==expected_group if not refused else True);entry['cache_scope']='All raw values retained; existing cached group bytes unchanged on Native path; Source miss-fill difference explicitly outside readonly scope, no full-cache parity claim'
  if item.get('preserve_cross_fields'):
   target=Stream.objects.get(m3u_account=account,url='http://127.0.0.1:9/owned-url-cross.ts');profile=StreamProfile.objects.order_by('id').first();Stream.objects.filter(pk=target.id).update(local_file='owned-local.ts',current_viewers=3,stream_stats={'owned':'preserve'},stream_profile_id=profile.id if profile else None);stale=Stream.objects.get(m3u_account=account,url='http://127.0.0.1:9/owned-url-a-stale.ts');Stream.objects.filter(pk=stale.id).update(last_seen=timezone.now()-timedelta(days=30),is_stale=True)
  save();print(json.dumps({'label':label,'passed':True,'refused':refused}),flush=True)
 check('exact-sealed17-case-order',[v['label']for v in report['cases']]==[v['label']for v in QA['Source_derived_case_order']]);report['passed']=True
finally:
 if native and native.poll()is None:native.kill();native.wait(timeout=5)
 stop();save()
print(json.dumps({'passed':report['passed'],'checks':len(report['checks']),'cases':len(report['cases'])}))
