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
from owned_pg_physical_proxy import Proxy
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
def peer_start(parent,account,label,database_url=None,cache_url=None):
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
 if cache_url:cfg['cache_url']=cache_url
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
from owned_cache_reply_proxy import Proxy as CacheProxy
from phase_expectations import NORMAL_SINGLE,BATCH_FAILED,exact_success
CURRENT_URL='';CURRENT_FILE=''
# Each phase gets a new actual scanner-created account and parent lineage.
def produce_phase(index,label):
 global worker
 for p in Path('/data/m3us').glob('rust-owned-children-*.m3u'):p.unlink()
 M3UAccount.objects.all().update(is_active=False)
 path=Path('/data/m3us/rust-owned-children-physical-'+str(index)+'.m3u');url='http://127.0.0.1:9/owned-physical-'+str(index)+'.ts';path.write_text('#EXTM3U\n#EXTINF:-1 group-title="Owned Physical Group",Owned physical\n'+url+'\n');now=1700000000.125+index*10;os.utime(path,(now-7.125,now-7.125));tid=str(uuid.uuid4());known_task_ids.add(tid);Path('/evidence/owned-scanner-config.json').write_text(json.dumps({'task_id':tid,'now':now}));worker=start('rust-owned-scanner-parents');parent=app.tasks['core.tasks.scan_and_process_files'].apply_async(args=[],task_id=tid,queue='rust-owned-scanner-parents');wait(lambda:any(v['id']==tid for v in records('postrun')));check(label+'-real-parent-success-null',parent.get(timeout=2)is None);stop();account=M3UAccount.objects.get(file_path=str(path));frames=[json.loads(v)for v in reversed(broker.lrange('celery',0,-1))];known_task_ids.update(v['headers']['id']for v in frames);check(label+'-actual-created-groups-single',[v['headers']['task']for v in frames]==['apps.m3u.tasks.refresh_m3u_groups','apps.m3u.tasks.refresh_single_m3u_account']);return tid,account,frames,url
role=None
try:
 for d in ['/data/m3us','/data/epgs','/data/logos']:Path(d).mkdir(exist_ok=True);check('fresh-root-'+d,not list(Path(d).iterdir()))
 CoreSettings._update_group(SYSTEM_SETTINGS_KEY,'System Settings',{'auto_import_mapped_files':True})
 for index,item in enumerate(QA['physical_order']):
  label=item['label'];active_label=label;check(label+'-prior-queue-empty',broker.llen('celery')==broker.hlen('unacked')==0);CoreSettings._update_group(STREAM_SETTINGS_KEY,'Stream Settings',{'m3u_hash_key':'url'});django_cache.delete(GROUP);tid,account,frames,url=produce_phase(index,label);head=broker.lindex('celery',-1);target_id=frames[0]['headers']['id'];cfgurl=None;cacheurl=None
  if index>=2:
   # Normal Groups execution remains identical; fault authority is Single only
   # except the explicit pre-ACK support query phase.
   if index!=2:
    cmd=peer_start(tid,account.id,label+'-groups');check(label+'-Groups-completes',cmd({'action':'run'})['run'].startswith('Ok(Completed('));peer_stop(cmd,label+'-Groups');head=broker.lindex('celery',-1);target_id=frames[1]['headers']['id']
   foreign=M3UAccount.objects.exclude(pk=account.id).order_by('id').first();template=dict(Stream.objects.order_by('id').values().first());columns=[f.column for f in Stream._meta.concrete_fields if not f.primary_key];group,_=ChannelGroup.objects.get_or_create(name='Owned Physical Group');digest=Stream.generate_hash_key('Owned physical',url,'',['url'],m3u_id=account.id,account_type='STD');template.update(name='Owned physical original',url=url,stream_hash=digest,m3u_account_id=foreign.id,channel_group_id=group.id,logo_url='',custom_properties={},local_file='owned-protect.ts',stream_stats={'owned':'preserve'},current_viewers=3)
   with connection.cursor()as c:c.execute('INSERT INTO dispatcharr_channels_stream('+','.join(columns)+') SELECT '+','.join(columns)+' FROM jsonb_populate_record(NULL::dispatcharr_channels_stream,%s::jsonb) RETURNING id',[json.dumps(template,default=str)]);ident=c.fetchone()[0]
  if index<2:
   django_cache.set(GROUP,{'m3u_hash_key':item['setting']},timeout=300);proxy=CacheProxy(django_cache.make_key(GROUP));cacheurl='redis://:playback-fixture-only@127.0.0.1:61611/0'
  else:
   proxy=Proxy(label);cfgurl='postgres://dispatch:playback-fixture-only@127.0.0.1:61651/'+os.environ['POSTGRES_DB']+'?sslmode=disable'
   if index in [2,3]:
    role='rust_m3u_preflight_'+str(account.id);quoted=connection.ops.quote_name(role)
    marker='SELECT NOT EXISTS(SELECT 1 FROM dispatcharr_channels_stream'if index==2 else'AS logo_bounded'
    with connection.cursor()as c:
     c.execute('CREATE ROLE '+quoted+" LOGIN PASSWORD 'playback-fixture-only'");c.execute('GRANT USAGE ON SCHEMA public TO '+quoted);c.execute('GRANT ALL ON ALL TABLES IN SCHEMA public TO '+quoted);c.execute('GRANT USAGE,SELECT ON ALL SEQUENCES IN SCHEMA public TO '+quoted);c.execute('CREATE SEQUENCE rust_owned_url_fault_calls');c.execute('GRANT USAGE,SELECT ON SEQUENCE rust_owned_url_fault_calls TO '+quoted)
     c.execute("CREATE FUNCTION rust_owned_url_read_guard() RETURNS boolean LANGUAGE plpgsql AS $$BEGIN IF strpos(current_query(),%s)>0 THEN PERFORM nextval('rust_owned_url_fault_calls');RAISE EXCEPTION 'rust-owned URL snapshot failure' USING ERRCODE='P0001';END IF;RETURN true;END$$",[marker]);c.execute('ALTER TABLE dispatcharr_channels_stream ENABLE ROW LEVEL SECURITY');c.execute('CREATE POLICY rust_owned_url_read ON dispatcharr_channels_stream FOR SELECT TO '+quoted+' USING(rust_owned_url_read_guard())')
    cfgurl='postgres://'+role+':playback-fixture-only@127.0.0.1:61651/'+os.environ['POSTGRES_DB']+'?sslmode=disable'
   if index in [6,7,8]:
    with connection.cursor()as c:
     c.execute('CREATE SEQUENCE rust_owned_url_fault_calls');c.execute("CREATE FUNCTION rust_owned_url_mutation_guard() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN PERFORM nextval('rust_owned_url_fault_calls');RAISE EXCEPTION 'rust-owned URL mutation failure' USING ERRCODE='P0001';END$$")
     if index==8:c.execute('CREATE CONSTRAINT TRIGGER rust_owned_url_mutation AFTER INSERT OR UPDATE ON dispatcharr_channels_stream DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION rust_owned_url_mutation_guard()')
     else:c.execute('CREATE TRIGGER rust_owned_url_mutation BEFORE INSERT OR UPDATE ON dispatcharr_channels_stream FOR EACH ROW EXECUTE FUNCTION rust_owned_url_mutation_guard()')
  before=snapshot();before_row=stream_row(ident)if index>=2 else None;cmd=peer_start(tid,account.id,label,cfgurl,cacheurl);check(label+'-start',cmd({'action':'start'})=={'started':True});check(label+'-real-physical-gate',proxy.held.wait(15))
  if index>=2 and index not in [4,5,7]:check(label+'-actual-P0001',proxy.error.is_set()and proxy.error_codes==['P0001'])
  if index==4:check(label+'-actual-DataRow-CommandComplete',proxy.data_rows==1 and proxy.command_complete==1)
  if index>=2 and index not in [5,7]:time.sleep(6.2)
  answer=cmd({'action':'abort'});check(label+'-physical-caller-admission-drain-retained',answer=={'caller_retained':True,'slots':0,'admission':'Err(Capacity)','drain':'Err(DrainTimeout)'})
  if index<2:
   time.sleep(2.3);check(label+'-beyond-cache-logical-timeout',cmd({'action':'observe'})=={'caller_retained':True,'slots':0,'drain':'Err(DrainTimeout)'})
  preack=index in [0,1,2];check(label+'-ACK-boundary',broker.lindex('celery',-1)==head if preack else broker.llen('celery')==0);check(label+'-no-premature-result',backend.get(app.backend.get_key_for_task(target_id))is None)
  if preack:check(label+'-preACK-model-effects-retained',retained_models(before,snapshot()))
  proxy.release.set();check(label+'-actual-settlement-releases-owner',cmd({'action':'settled'})=={'caller_released':True,'slots':1});metadata=backend.get(app.backend.get_key_for_task(target_id));after=snapshot()
  if index in [0,2,3,5,7,8]:check(label+'-operational-no-fabricated-terminal',metadata is None)
  if index==0:check(label+'-unsupported-head-retained',broker.lindex('celery',-1)==head)
  if index==4:check(label+'-success-after-physical-ready',exact_success(metadata,NORMAL_SINGLE) and app.AsyncResult(target_id).get(timeout=2)==NORMAL_SINGLE)
  if index==6:check(label+'-caught-mutation-failure-normal-result',exact_success(metadata,BATCH_FAILED) and app.AsyncResult(target_id).get(timeout=2)==BATCH_FAILED)
  if index in [2,3]:
   with connection.cursor()as c:c.execute('SELECT last_value,is_called FROM rust_owned_url_fault_calls');called=c.fetchone()
   check(label+'-no-read-retry',called==(1,True))
  if index in [6,7,8]:
   with connection.cursor()as c:c.execute('SELECT last_value,is_called FROM rust_owned_url_fault_calls');called=c.fetchone()
   check(label+'-exact-mutation-attempts',called==((2,True)if index==6 else(1,True)))
  if index>=2:
   qs=proxy.queries;expected_selects=2 if index==6 else(0 if index==2 else 1);check(label+'-actual-snapshot-attempts',sum('AS logo_bounded'in q for q in qs)==expected_selects)
   if index==6:
    snapshots=[i for i,q in enumerate(qs)if 'AS logo_bounded'in q];rollbacks=[i for i,q in enumerate(qs)if q.strip().upper()=='ROLLBACK'];check(label+'-retry-only-after-actual-rollback',len(rollbacks)==2 and snapshots[0]<rollbacks[0]<snapshots[1]<rollbacks[1])
   if index not in [4]:check(label+'-foreign-row-protected',stream_row(ident)==before_row)
  else:qs=[]
  entry={'label':label,'retention':answer,'metadata':None if metadata is None else json.loads(metadata),'Original_decoded':None if metadata is None else str(app.AsyncResult(target_id).get(timeout=2,propagate=False)),'before':before,'after':after,'queries':qs,'error_codes':getattr(proxy,'error_codes',[]),'timeline':getattr(proxy,'timeline',[]),'actual_matching_DataRows':getattr(proxy,'data_rows',None),'actual_matching_CommandComplete':getattr(proxy,'command_complete',None),'no_new_Source_reference':True};peer_stop(cmd,label);proxy.close();check(label+'-proxy-clean',not proxy.errors);proxy=None
  if role:
   with connection.cursor()as c:c.execute('DROP POLICY rust_owned_url_read ON dispatcharr_channels_stream');c.execute('ALTER TABLE dispatcharr_channels_stream DISABLE ROW LEVEL SECURITY');c.execute('DROP FUNCTION rust_owned_url_read_guard()');c.execute('DROP SEQUENCE rust_owned_url_fault_calls');c.execute('DROP OWNED BY '+quoted);c.execute('DROP ROLE '+quoted)
   role=None
  if index in [6,7,8]:
   with connection.cursor()as c:c.execute('DROP TRIGGER rust_owned_url_mutation ON dispatcharr_channels_stream');c.execute('DROP FUNCTION rust_owned_url_mutation_guard()');c.execute('DROP SEQUENCE rust_owned_url_fault_calls')
  # Retained queue work uses actual pinned worker only after native observations.
  if broker.llen('celery'):
   worker=start('celery');wait(lambda:broker.llen('celery')==broker.hlen('unacked')==0 and all(backend.get(app.backend.get_key_for_task(v['headers']['id']))is not None for v in frames));stop();entry['Source_fallback']=True
  report['cases'].append(entry);save();print(json.dumps({'label':label,'passed':True}),flush=True)
 report['passed']=True
finally:
 if proxy:
  report['partial_physical_diagnostics']={k:getattr(proxy,k,None)for k in ['mode','queries','error_codes','data_rows','command_complete','backend_pids','timeline','disconnections','errors']};report['partial_physical_diagnostics']['held']=proxy.held.is_set();save();proxy.release.set();proxy.close()
 if native and native.poll()is None:native.kill();native.wait(timeout=5)
 report['finally_diagnostics_retained']=True;stop();save()
print(json.dumps({'passed':report['passed'],'checks':len(report['checks']),'cases':len(report['cases'])}))
