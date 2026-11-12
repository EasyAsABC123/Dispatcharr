"""Actual pinned Original scanner/default-child URL-hash corpus; no native execution.
Owned local STD files/SQL/Redis/Channels only. Original Dispatcharr/dependency
contributors retain application code, algorithms and licenses. Explicit observer
clocks label scanner input and elapsed0.0; Django datetime/renewer remain real.
"""
import base64,hashlib,inspect,json,os,signal,subprocess,sys,time,uuid
from pathlib import Path
from datetime import timedelta
assert os.environ.get('JOB_DISPOSABLE_FIXTURE')=='playback-fixture-only'
assert os.environ['POSTGRES_DB'].startswith('rust_probe_backup_jobs_')
sys.path[:0]=['/app','/pair-tools'];sys.argv.append('shell');os.environ.setdefault('DJANGO_SETTINGS_MODULE','dispatcharr.settings')
from ledger_io import append_receipt
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
plan=json.loads(Path('/pair-tools/predeclared-plan.json').read_text());report={'passed':False,'native_verified':False,'performance_verified':False,'scope':__doc__,'plan_sha256':hashlib.sha256(Path('/pair-tools/predeclared-plan.json').read_bytes()).hexdigest(),'cases':[],'checks':[],'deferred_native_groups':[13,14],'transport_ledger':'transport-ledger.jsonl','status':'UNEXECUTED_REDESIGN_ROOT_REVIEW_REQUIRED'};worker=None;publications={}
broker=redis.Redis.from_url(os.environ['CELERY_BROKER_URL']);backend=redis.Redis.from_url(os.environ['CELERY_RESULT_BACKEND']);raw=redis.Redis(host='localhost',port=6379,password='playback-fixture-only',db=0)
assert broker.connection_pool.connection_kwargs['db']==8 and backend.connection_pool.connection_kwargs['db']==9 and broker.dbsize()==backend.dbsize()==0
for key in ['dvr:recover_dispatch_lock','core:version_check_dispatch_lock']:raw.set(key,'1',ex=3600)
app.loader.import_default_modules();layer=get_channel_layer();channel='specific.rust_owned_url_hash!receiver';ckey=layer.prefix+channel.split('!')[0]+'!';gkey=layer._group_key('updates');GROUP=CoreSettings.group_cache_key(STREAM_SETTINGS_KEY)
@before_task_publish.connect
def publish(sender=None,headers=None,body=None,properties=None,exchange=None,routing_key=None,**extra):
 if sender=='core.tasks.scan_and_process_files':publications[headers['id']]={'headers':headers,'body':body,'properties':properties,'exchange':exchange,'routing_key':routing_key}
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
 keys=sorted(backend.keys('celery-task-meta-*'))
 if len(keys)>64:entry['capacity_refusals'].append('backend-count');keys=[]
 for key in keys:
  size=backend.strlen(key)
  if size>131072:entry['capacity_refusals'].append('backend-bytes');continue
  value=backend.get(key)
  entry['backend'][key.decode()]={'raw_base64':None if value is None else base64.b64encode(value).decode(),'decoded':None if value is None else ledger_decode(value),'ttl':backend.ttl(key)}
 receipt=append_receipt('/evidence/transport-ledger.jsonl',entry)
 if not receipt['full_record_retained'] or entry['capacity_refusals']:raise RuntimeError('Owned ledger capacity refusal retained')

def save():Path('/evidence/source-url-hash-private.json').write_text(json.dumps(report,indent=2,default=str)+'\n')
try:
 import importlib.metadata
 report['provenance']={'revision':plan['Source_revision'],'packages':{n:importlib.metadata.version(n)for n in ['Django','django-redis','celery','kombu']},'files':{n:hashlib.sha256(Path(n).read_bytes()).hexdigest()for n in ['/app/core/tasks.py','/app/core/models.py','/app/apps/m3u/tasks.py','/app/apps/m3u/models.py','/app/apps/m3u/utils.py','/app/apps/channels/models.py']}}
 import apps.m3u.tasks as mt
 for name,obj in [('batch',mt.process_m3u_batch_direct),('single',mt.refresh_single_m3u_account.run),('hash',Stream.generate_hash_key),('effective',M3UAccount.get_effective_hash_keys)]:Path('/evidence/original-url-'+name+'.py').write_text(inspect.getsource(obj))
 for root in ['/data/m3us','/data/epgs','/data/logos']:
  Path(root).mkdir(exist_ok=True);check('fresh-owned-root-'+root,not list(Path(root).iterdir()))
 CoreSettings._update_group(SYSTEM_SETTINGS_KEY,'System Settings',{'auto_import_mapped_files':True});EPGSource.objects.all().update(is_active=False)
 for index,item in enumerate(plan['actual_worker_steps']):
  label=item['label'];active_label=label;stop();check(label+'-prior-queue-and-unacked-empty',broker.llen('celery')==broker.hlen('unacked')==0);M3UAccount.objects.all().update(is_active=False)
  path=Path('/data/m3us/rust-owned-children-url-'+item['cohort']+'.m3u');account=M3UAccount.objects.filter(file_path=str(path)).first()
  if account:M3UAccount.objects.filter(pk=account.id).update(is_active=True)
  path.write_bytes(item['playlist'].encode());os.utime(path,(item['mtime'],item['mtime']));check(label+'-sealed-input-bytes',hashlib.sha256(path.read_bytes()).hexdigest()==item['playlist_sha256']);configure(item)
  raw.delete(ckey,gkey);async_to_sync(layer.group_add)('updates',channel)
  if item.get('map_from'):
   owner=M3UAccount.objects.get(file_path='/data/m3us/rust-owned-children-url-'+item['map_from']+'.m3u');target=Stream.objects.get(m3u_account=owner,url='http://127.0.0.1:9/owned-url-mapped.ts');ch=Channel.objects.create(name='rust-owned-url-map',channel_number=9911);ChannelStream.objects.create(channel=ch,stream=target,order=0)
  before=snapshot();tid=str(uuid.uuid4());Path('/evidence/owned-scanner-config.json').write_text(json.dumps({'task_id':tid,'now':item['scanner_now']}));prior={x['id']for x in records('postrun')};parent=app.tasks['core.tasks.scan_and_process_files'].apply_async(args=[],task_id=tid,queue='rust-owned-scanner-parents');ledger('parent-published-before-worker-start');worker=start('rust-owned-scanner-parents');wait(lambda:any(x['id']==tid for x in records('postrun')));ledger('parent-postrun-before-result-assertion');check(label+'-actual-parent-normal-null',parent.get(timeout=2)is None);stop();account=M3UAccount.objects.get(file_path=str(path));M3UAccount.objects.filter(pk=account.id).update(name='rust-owned-children-url-'+item['cohort'],custom_properties=item.get('account_custom',{}));account.refresh_from_db();configure(item)
  frames=[json.loads(v)for v in reversed(broker.lrange('celery',0,-1))];ledger('default-children-before-consumer-start-and-shape-assertion');check(label+'-actual-created-or-existing-child-families',[v['headers']['task']for v in frames]==item['expected_direct_child_families']);check(label+'-direct-parent-root-lineage',all(v['headers']['parent_id']==tid and v['headers']['root_id']==tid for v in frames));worker=start('celery');stable=[None]
  def drained():
   posts=[x for x in records('postrun')if x['id']not in prior];pubs=[x for x in records('children')if x['headers'].get('root_id')==tid];ids={tid}|{x['headers']['id']for x in pubs};idle=ids<={x['id']for x in posts}and broker.llen('celery')==broker.hlen('unacked')==0
   if idle:
    if stable[0]is None:stable[0]=time.monotonic()
    if time.monotonic()-stable[0]>=.5:return posts,pubs,ids
   else:stable[0]=None
   return None
  posts,pubs,ids=wait(drained,plan['fixed_per_step_drain_seconds']);stop();results={}
  for ident in sorted(ids):
   key=app.backend.get_key_for_task(ident);value=backend.get(key);meta=None if value is None else json.loads(value);decoded=app.AsyncResult(ident).get(timeout=2,propagate=False);results[ident]={'metadata':meta,'raw_base64':None if value is None else base64.b64encode(value).decode(),'ttl':backend.ttl(key),'Original_get_type':type(decoded).__module__+'.'+type(decoded).__name__,'Original_get_value':repr(decoded)if isinstance(decoded,BaseException)else decoded}
  after=snapshot();check(label+'-all-descendants-actually-complete',ids=={x['id']for x in posts});check(label+'-real-default-child-route',all(x['routing_key']=='celery'and x['exchange']==''for x in pubs));check(label+'-result-ID-and-TTL',all(v['metadata']is not None and v['metadata']['task_id']==k and 3500<=v['ttl']<=3600 for k,v in results.items()));single=next(x for x in posts if x['task']=='apps.m3u.tasks.refresh_single_m3u_account');check(label+'-expected-Single-state',single['state']==('FAILURE'if item.get('expect_single_failure')else'SUCCESS'));check(label+'-no-lost-input-file',hashlib.sha256(path.read_bytes()).hexdigest()==item['playlist_sha256'])
  entry={'group':item['group'],'label':label,'sealed_input':item,'parent_id':tid,'account_id':account.id,'parent_publication':publications[tid],'queue_frames':frames,'publications':pubs,'postruns':posts,'preruns':[x for x in records('prerun')if x['id']in ids],'received':[x for x in records('received')if x['id']in ids],'sql':[x for x in records('sql')if x['id']in ids],'fileposts':[x for x in records('filepost')if x['id']in ids],'results':results,'before':before,'after':after}
  if item.get('preserve_cross_fields'):
   target=Stream.objects.get(m3u_account=account,url='http://127.0.0.1:9/owned-url-cross.ts');profile=StreamProfile.objects.order_by('id').first();Stream.objects.filter(pk=target.id).update(local_file='owned-local.ts',current_viewers=3,stream_stats={'owned':'preserve'},stream_profile_id=profile.id if profile else None);entry['fixture_post_step_mutation']={'purpose':'Seed non-import fields before cross-account observations; not a task effect','stream_id':target.id,'after':list(Stream.objects.filter(pk=target.id).values())};stale=Stream.objects.get(m3u_account=account,url='http://127.0.0.1:9/owned-url-a-stale.ts');Stream.objects.filter(pk=stale.id).update(last_seen=timezone.now()-timedelta(days=30),is_stale=True)
  report['cases'].append(entry);save();print(json.dumps({'step':index+1,'label':label,'tasks':len(ids),'single_state':single['state']}),flush=True)
 check('exact-sealed-step-order',[x['label']for x in report['cases']]==[x['label']for x in plan['actual_worker_steps']]);report['passed']=True
finally:
 try:ledger('finally-before-worker-stop')
 finally:
  stop();save()
 ledger('finally-after-worker-stop')
print(json.dumps({'passed':report['passed'],'checks':len(report['checks']),'cases':len(report['cases'])}))
