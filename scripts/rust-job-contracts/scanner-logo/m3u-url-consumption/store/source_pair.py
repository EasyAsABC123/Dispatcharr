"""Actual native Store projection and scanner refusal controls with Original producer.
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
from owned_pg_proxy import Proxy
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
 accounts=M3UAccount.objects.filter(name__startswith='rust-owned-children-');ids=list(accounts.values_list('id',flat=True))
 return {'stream_settings_rows':list(CoreSettings.objects.filter(key=STREAM_SETTINGS_KEY).values()),'accounts':list(accounts.order_by('id').values()),'profiles':list(M3UAccountProfile.objects.filter(m3u_account_id__in=ids).order_by('id').values()),'streams':list(Stream.objects.filter(name__startswith='Owned ').order_by('id').values()),'groups':list(ChannelGroup.objects.order_by('id').values()),'group_links':list(ChannelGroupM3UAccount.objects.filter(m3u_account_id__in=ids).order_by('id').values()),'channels':list(Channel.objects.filter(name__startswith='rust-owned-url-').order_by('id').values()),'channel_streams':list(ChannelStream.objects.filter(channel__name__startswith='rust-owned-url-').order_by('id').values()),'system_events':list(SystemEvent.objects.filter(details__account_name__startswith='rust-owned-children-').order_by('id').values()),'events':[layer.deserialize(v)for v in raw.zrange(ckey,0,1023)],'cache':cache_snapshot(),'files':[{'path':str(p),'bytes':p.stat().st_size,'mtime_ns':p.stat().st_mtime_ns,'sha256':hashlib.sha256(p.read_bytes()).hexdigest()}for p in sorted(Path('/data/m3us').glob('rust-owned-children-*.m3u'))]}
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
def peer_start(parent,account,label,database_url=None):
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
 cfg={'parent_id':parent,'account_id':account}
 if database_url:cfg['database_url']=database_url
 check(label+'-normal-native-constructor',cmd(cfg)=={'prepared':True});return cmd
def peer_stop(cmd,label):
 global native
 check(label+'-actual-native-drain',cmd({'action':'shutdown'})=={'drained':True});native.stdin.close();native.wait(timeout=10);check(label+'-native-exit0',native.returncode==0);native._owned_log.close();native=None
QA=json.loads(Path('/pair-tools/predeclared-qa-plan.json').read_text());report['scope']='Same normal bounded StoreAPI and scanner preACK/actualSELECT refusal controls; Source producer/decoder only, no new Source oracle or Main/performance claim';proxy=None

def produce(now):
 global worker
 path=Path('/data/m3us/rust-owned-children-caps.m3u');path.write_text('#EXTM3U\n#EXTINF:-1 group-title="Owned Caps Group",Owned caps\nhttp://127.0.0.1:9/owned-url-component.ts\n');os.utime(path,(now-7.125,now-7.125));tid=str(uuid.uuid4());known_task_ids.add(tid);Path('/evidence/owned-scanner-config.json').write_text(json.dumps({'task_id':tid,'now':now}));parent=app.tasks['core.tasks.scan_and_process_files'].apply_async(args=[],task_id=tid,queue='rust-owned-scanner-parents');worker=start('rust-owned-scanner-parents');wait(lambda:any(v['id']==tid for v in records('postrun')));check('parent-'+tid+'-actual-success-null',parent.get(timeout=2)is None);stop();account=M3UAccount.objects.get(file_path=str(path));frames=[json.loads(v)for v in reversed(broker.lrange('celery',0,-1))];known_task_ids.update(v['headers']['id']for v in frames);return tid,account,frames

def stream_row(ident):return dict(Stream.objects.filter(pk=ident).values().get())
def update_field(ident,field,kind,size=None):
 assert field in ['logo_url','custom_properties']
 with connection.cursor()as c:
  if kind=='SQLNULL':c.execute('UPDATE dispatcharr_channels_stream SET '+field+'=NULL WHERE id=%s',[ident])
  elif kind=='JSONnull':c.execute("UPDATE dispatcharr_channels_stream SET custom_properties='null'::jsonb WHERE id=%s",[ident])
  elif field=='logo_url':c.execute('UPDATE dispatcharr_channels_stream SET logo_url=repeat(%s,%s) WHERE id=%s',['l',size,ident])
  else:c.execute("UPDATE dispatcharr_channels_stream SET custom_properties=jsonb_build_object('x',repeat('p',%s)) WHERE id=%s",[size-9,ident])
 with connection.cursor()as c:c.execute('SELECT octet_length('+field+('::text'if field=='custom_properties'else'')+'),pg_column_size('+field+') FROM dispatcharr_channels_stream WHERE id=%s',[ident]);return c.fetchone()
def reset_row(ident):
 with connection.cursor()as c:c.execute("UPDATE dispatcharr_channels_stream SET name='Owned capped original',logo_url='',custom_properties='{}'::jsonb,is_stale=false WHERE id=%s",[ident])

def retained_models(before,after):
 tables=['accounts','profiles','streams','groups','group_links','channels','channel_streams','system_events','events','files','stream_settings_rows']
 if not all(before[t]==after[t]for t in tables):return False
 def cache_values(v):
  result={}
  for key,value in v.items():
   parts=key.split(':')
   if len(parts)==5 and parts[:2]==['live','worker']and parts[3].isdigit()and parts[4]=='heartbeat':continue
   result[key]={'type':value['type'],'dump_base64':value['dump_base64']}
  return result
 return cache_values(before['cache'])==cache_values(after['cache'])

def proxy_stats():return {'rows':proxy.data_rows,'max_data_row_bytes':proxy.max_data_row_bytes,'target_rows':proxy.target_rows,'snapshot_queries':proxy.snapshot_queries,'mutations':proxy.mutations,'backend_pids':list(proxy.backend_pids)}
try:
 for d in ['/data/m3us','/data/epgs','/data/logos']:Path(d).mkdir(exist_ok=True);check('fresh-root-'+d,not list(Path(d).iterdir()))
 M3UAccount.objects.all().update(is_active=False);EPGSource.objects.all().update(is_active=False);CoreSettings._update_group(SYSTEM_SETTINGS_KEY,'System Settings',{'auto_import_mapped_files':True});CoreSettings._update_group(STREAM_SETTINGS_KEY,'Stream Settings',{'m3u_hash_key':'url'});tid,account,frames=produce(1700000000.125);check('initial-real-groups-single',[v['headers']['task']for v in frames]==['apps.m3u.tasks.refresh_m3u_groups','apps.m3u.tasks.refresh_single_m3u_account'])
 foreign=M3UAccount.objects.exclude(pk=account.id).order_by('id').first();check('copied-foreign-account',foreign is not None);template=dict(Stream.objects.order_by('id').values().first());columns=[f.column for f in Stream._meta.concrete_fields if not f.primary_key];url='http://127.0.0.1:9/owned-url-component.ts';digest=Stream.generate_hash_key('Owned caps',url,'',['url'],m3u_id=account.id,account_type='STD');group=ChannelGroup.objects.create(name='Owned Caps Group');template.update(channel_group_id=group.id,name='Owned capped original',url=url,stream_hash=digest,m3u_account_id=foreign.id,logo_url='',custom_properties={},local_file='owned-protect.ts',stream_stats={'owned':'preserve'},current_viewers=3)
 with connection.cursor()as c:c.execute('INSERT INTO dispatcharr_channels_stream('+','.join(columns)+') SELECT '+','.join(columns)+' FROM jsonb_populate_record(NULL::dispatcharr_channels_stream,%s::jsonb) RETURNING id',[json.dumps(template,default=str)]);ident=c.fetchone()[0]
 row={'stream_hash':digest,'name':'Owned capped replacement','url':url,'logo_url':'','tvg_id':'','channel_group_id':template['channel_group_id'],'custom_properties':{},'is_adult':False,'stream_id':None,'stream_chno':None,'is_catchup':False,'catchup_days':0,'is_radio':False};proxy=Proxy();cmd=peer_start(tid,account.id,'store-caps','postgres://dispatch:playback-fixture-only@127.0.0.1:61651/'+os.environ['POSTGRES_DB']+'?sslmode=disable')
 for item in QA['Store_projection_and_refusal_order']:
  label=item['label'];active_label=label
  if label.startswith('postadmission-'):
   # Run the already-queued Groups preparation once; each failed Single then
   # uses a new real scanner parent on the existing account, not a fake frame.
   if label=='postadmission-logo-enlargement':
    reset_row(ident);check('post-admission-real-group-preparation',cmd({'action':'run'})['run'].startswith('Ok(Completed('))
   else:
    reset_row(ident);peer_stop(cmd,'prior-post-case');tid,account,frames=produce(1700000010.125);check('repeat-real-single-only',[v['headers']['task']for v in frames]==['apps.m3u.tasks.refresh_single_m3u_account']);cmd=peer_start(tid,account.id,'post-properties','postgres://dispatch:playback-fixture-only@127.0.0.1:61651/'+os.environ['POSTGRES_DB']+'?sslmode=disable')
   reset_row(ident);proxy.reset();proxy.record_data=True;proxy.gate=True;check(label+'-start',cmd({'action':'start'})=={'started':True});check(label+'-actual-SELECT-before-server-gate',proxy.held.wait(10));single=next(v for v in frames if v['headers']['task']=='apps.m3u.tasks.refresh_single_m3u_account');check(label+'-ACK-before-read',broker.llen('celery')==0 and backend.get(app.backend.get_key_for_task(single['headers']['id']))is None);size=item.get('bytes',item.get('serialized_text_bytes'));length,stored=update_field(ident,item['field'],'sized',size);before=stream_row(ident);aborted=cmd({'action':'abort'});check(label+'-caller-owned-admission',aborted['caller_retained']is True and aborted['slots']==0 and aborted['drain']=='Err(DrainTimeout)');proxy.release.set();settled=cmd({'action':'settled'});check(label+'-actual-owner-release',settled=={'caller_released':True,'slots':1});proxy.record_data=False;check(label+'-bounded-actual-response',proxy.max_data_row_bytes<=2048 and len(proxy.target_rows)==1);check(label+'-no-mutation-retry',proxy.snapshot_queries==1 and proxy.mutations==0);check(label+'-foreign-row-exact',stream_row(ident)==before);check(label+'-no-fake-result',backend.get(app.backend.get_key_for_task(single['headers']['id']))is None);entry={'label':label,'kind':'postACK-worker-operational-refusal','field':item['field'],'actual_text_bytes':length,'compressed_storage_bytes':stored,'before':before,'after':stream_row(ident),'abort':aborted,'settled':settled,'proxy':proxy_stats(),'Source_parity_claim':False};proxy.gate=False
  elif label.startswith('preACK-'):
   reset_row(ident);size=item.get('bytes',item.get('serialized_text_bytes'));length,stored=update_field(ident,item['field'],'sized',size);before=stream_row(ident);model_before=snapshot();head=broker.lindex('celery',-1);proxy.reset();proxy.record_data=True;result=cmd({'action':'run'});proxy.record_data=False;check(label+'-retained-before-ACK',result['run']=='Err(Domain(Unsupported))'and broker.lindex('celery',-1)==head);check(label+'-no-Store-read-or-mutation',proxy.snapshot_queries==proxy.mutations==0);check(label+'-no-large-response',proxy.max_data_row_bytes<=2048);check(label+'-all-model-effects-retained',retained_models(model_before,snapshot()));check(label+'-no-child-result',all(backend.get(app.backend.get_key_for_task(v['headers']['id']))is None for v in frames));entry={'label':label,'field':item['field'],'actual_text_bytes':length,'before':before,'after':stream_row(ident),'result':result,'proxy':proxy_stats(),'Source_parity_claim':False}
  else:
   reset_row(ident);legacy=item.get('legacy',False)
   if legacy:field='logo_url';length,stored=update_field(ident,field,'sized',262144)
   else:field=item['field'];length,stored=update_field(ident,field,item.get('value','sized'),item.get('bytes',item.get('serialized_text_bytes')))
   before=stream_row(ident);proxy.reset();proxy.record_data=True;result=cmd({'action':'batch','legacy':legacy,'rows':[row]});proxy.record_data=False;after=stream_row(ident);accept=item.get('accept',True);check(label+'-normal-API-result',result['driver_fenced']is True and result['batch']['ok']is accept)
   if not accept:check(label+'-typed-unsupported',result['batch']['stage']=='ExistingSnapshotUnsupported');check(label+'-no-mask-create-update',after==before and proxy.mutations==0)
   else:check(label+'-original-owner-fields-preserved',all(after[k]==before[k]for k in ['m3u_account_id','local_file','current_viewers','stream_stats','stream_profile_id']));check(label+'-existing-id-updated-not-created',result['batch']['created']==0 and result['batch']['updated']==1)
   if not legacy:
    check(label+'-one-actual-bounded-snapshot',proxy.snapshot_queries==1 and len(proxy.target_rows)==1);check(label+'-bounded-response',proxy.max_data_row_bytes<10000)
    if length==8192:check(label+'-exact-boundary-really-transferred',proxy.max_data_row_bytes>=8192)
    if not accept:check(label+'-masked-response-small',proxy.max_data_row_bytes<=2048)
   else:check(label+'-legacy-behavior-not-capped',proxy.snapshot_queries==0 and proxy.max_data_row_bytes>=262144)
   entry={'label':label,'kind':'legacy-public-API'if legacy else'normal-bounded-Store-API','field':field,'actual_text_bytes':length,'compressed_storage_bytes':stored,'before':before,'after':after,'result':result,'proxy':proxy_stats(),'Source_parity_claim':False}
  report['cases'].append(entry);save();print(json.dumps({'label':label,'passed':True}),flush=True)
 peer_stop(cmd,'final-store');proxy.close();check('proxy-no-errors',not proxy.errors);proxy=None;check('exact14-store-vector-order',[v['label']for v in report['cases']]==[v['label']for v in QA['Store_projection_and_refusal_order']]);report['passed']=True
finally:
 if proxy:proxy.release.set();proxy.close()
 if native and native.poll()is None:native.kill();native.wait(timeout=5)
 stop();save()
print(json.dumps({'passed':report['passed'],'checks':len(report['checks']),'cases':len(report['cases'])}))
