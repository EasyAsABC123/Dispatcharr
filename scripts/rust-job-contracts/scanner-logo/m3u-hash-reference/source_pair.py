"""Actual pinned Source grouped-setting lookup oracle and real scanner children.
Model/SQL/Redis factory probes are labeled separately from two actual workers.
Original Dispatcharr/dependency authors retain code, algorithm and license credit.
"""
import base64,copy,concurrent.futures,hashlib,inspect,json,os,signal,subprocess,sys,time,uuid
from pathlib import Path
assert os.environ.get('JOB_DISPOSABLE_FIXTURE')=='playback-fixture-only'
assert os.environ['POSTGRES_DB'].startswith('rust_probe_backup_jobs_')
sys.path[:0]=['/app','/pair-tools'];sys.argv.append('shell');os.environ.setdefault('DJANGO_SETTINGS_MODULE','dispatcharr.settings')
import django;django.setup()
import redis
from django.db import connection
from django.core.cache import cache as django_cache
from django.conf import settings
from django_redis.cache import RedisCache
from core.models import CoreSettings,STREAM_SETTINGS_KEY,SYSTEM_SETTINGS_KEY
import core.models as core_models
from apps.m3u.models import M3UAccount
from apps.m3u.utils import parse_m3u_hash_keys
from apps.channels.models import Stream
from dispatcharr.celery import app
from owned_cache_reply_proxy import Proxy
raw_cache=redis.Redis(host='localhost',port=6379,password='playback-fixture-only',db=0);broker=redis.Redis.from_url(os.environ['CELERY_BROKER_URL']);backend=redis.Redis.from_url(os.environ['CELERY_RESULT_BACKEND'])
assert broker.connection_pool.connection_kwargs['db']==8 and backend.connection_pool.connection_kwargs['db']==9 and broker.dbsize()==backend.dbsize()==0
for key in ['dvr:recover_dispatch_lock','core:version_check_dispatch_lock']:raw_cache.set(key,'1',ex=3600)
app.loader.import_default_modules()
report={'passed':False,'native_verified':False,'scope':__doc__,'checks':[],'cases':[],'actual_worker_cases':2,'factory_cases':22,'held_cache_cases':1};worker=None;relay=None
GROUP=CoreSettings.group_cache_key(STREAM_SETTINGS_KEY);VERSION=CoreSettings.group_cache_ver_key(STREAM_SETTINGS_KEY);RAW=django_cache.make_key(GROUP)
def check(name,value):
 report['checks'].append({'name':name,'passed':bool(value)})
 if not value:raise AssertionError(name)
def cache_state():
 return {key:{'raw_base64':None if raw_cache.get(django_cache.make_key(key))is None else base64.b64encode(raw_cache.get(django_cache.make_key(key))).decode(),'ttl':raw_cache.ttl(django_cache.make_key(key))}for key in [GROUP,VERSION]}
def configure(group,present=True,cached=None,cache_present=False):
 CoreSettings.objects.filter(key=STREAM_SETTINGS_KEY).delete()
 if present:CoreSettings.objects.create(key=STREAM_SETTINGS_KEY,name='Stream Settings',value=group)
 if cache_present:django_cache.set(GROUP,cached,timeout=300)
 else:django_cache.delete(GROUP)
def records(kind):
 p=Path('/evidence/periodic-worker-'+kind+'.jsonl');return [json.loads(v)for v in p.read_text().splitlines()]if p.exists()else[]
def wait(fn,seconds=40):
 end=time.monotonic()+seconds
 while time.monotonic()<end:
  result=fn()
  if result:return result
  time.sleep(.02)
 raise TimeoutError('Owned hash fixture deadline')
def stop():
 global worker
 if worker and worker.poll()is None:
  os.killpg(worker.pid,signal.SIGTERM)
  try:worker.wait(timeout=15)
  except subprocess.TimeoutExpired:os.killpg(worker.pid,signal.SIGKILL);worker.wait(timeout=5);report['forced_cleanup']=True
 worker=None
def start(queue):
 log=open('/evidence/scanner-worker-private.log','ab');result=subprocess.Popen(['/dispatcharrpy/bin/celery','-A','periodic_worker_observer:app','worker','--pool=prefork','--concurrency=1','-Q',queue,'--without-gossip','--without-mingle','--without-heartbeat','--loglevel=WARNING'],env=dict(os.environ,C_FORCE_ROOT='true',PYTHONPATH='/pair-tools:/app'),stdout=log,stderr=log,start_new_session=True);log.close();return result
FACTORY=[
 {'name':'missing-group','present':False,'group':{},'effective':[]},
 {'name':'empty-group','group':{},'effective':[]},
 {'name':'nonobject-group','group':'owned-nonobject','effective':[]},
 {'name':'empty-array-group','group':[],'effective':[]},
 {'name':'missing-key','group':{'default_output_format':'mpegts'},'effective':[]},
 {'name':'null-key','group':{'m3u_hash_key':None},'effective':[]},
 {'name':'empty-key','group':{'m3u_hash_key':''},'effective':[]},
 {'name':'blank-csv','group':{'m3u_hash_key':' , \t '},'effective':[]},
 {'name':'url-key','group':{'m3u_hash_key':'url'},'effective':['url']},
 {'name':'ordered-dedup-csv','group':{'m3u_hash_key':' name,url,name '},'effective':['name','url']},
 {'name':'unknown-token','group':{'m3u_hash_key':'tvg-id'},'effective':[]},
 {'name':'false-key','group':{'m3u_hash_key':False},'effective':[]},
 {'name':'zero-key','group':{'m3u_hash_key':0},'effective':[]},
 {'name':'list-key-TypeError','group':{'m3u_hash_key':['url']},'error':'builtins.TypeError'},
 {'name':'cached-url-over-empty-db','group':{'m3u_hash_key':''},'cached':{'m3u_hash_key':'url'},'effective':['url']},
 {'name':'cached-empty-over-url-db','group':{'m3u_hash_key':'url'},'cached':{'m3u_hash_key':''},'effective':[]},
 {'name':'cached-empty-map-over-url-db','group':{'m3u_hash_key':'url'},'cached':{},'effective':[]},
 {'name':'cached-nonobject-loads-url-db','group':{'m3u_hash_key':'url'},'cached':['owned-nonobject'],'effective':['url']},
 {'name':'account-url-override','group':{'m3u_hash_key':''},'custom':{'hash_key':'url'},'effective':['url']},
 {'name':'unknown-override-falls-global','group':{'m3u_hash_key':'url'},'custom':{'hash_key':'unknown'},'effective':['url']},
 {'name':'null-override-falls-global','group':{'m3u_hash_key':'url'},'custom':{'hash_key':None},'effective':['url']},
 {'name':'list-override-TypeError','group':{'m3u_hash_key':'url'},'custom':{'hash_key':['url']},'error':'builtins.TypeError'},
]
try:
 import importlib.metadata
 report['provenance']={'revision':'1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430','packages':{name:importlib.metadata.version(name)for name in ['Django','django-redis','celery','kombu']},'files':{name:hashlib.sha256(Path(name).read_bytes()).hexdigest()for name in ['/app/core/models.py','/app/apps/m3u/models.py','/app/apps/m3u/utils.py']}}
 for name,obj in [('group',CoreSettings._get_group),('load',CoreSettings._load_group_value),('stream',CoreSettings.get_stream_settings),('hash',CoreSettings.get_m3u_hash_key),('effective',M3UAccount.get_effective_hash_keys),('parse',parse_m3u_hash_keys)]:Path('/evidence/original-hash-'+name+'.py').write_text(inspect.getsource(obj))
 for item in FACTORY:
  configure(item['group'],item.get('present',True),item.get('cached'),'cached'in item)
  before=list(CoreSettings.objects.filter(key=STREAM_SETTINGS_KEY).values());state=cache_state();queries=[]
  def observed(execute,sql,params,many,context):queries.append({'sql':sql,'param_types':[type(v).__name__ for v in(params or[])]});return execute(sql,params,many,context)
  account=M3UAccount(name='rust-owned-hash-oracle',custom_properties=item.get('custom',{}));value=None;error=None
  with connection.execute_wrapper(observed):
   try:value=account.get_effective_hash_keys()
   except Exception as exc:error={'type':type(exc).__module__+'.'+type(exc).__name__,'args':list(exc.args)}
  after_cache=cache_state();check(item['name']+'-actual-effective-or-type',value==item.get('effective')and(error['type']if error else None)==item.get('error'))
  check(item['name']+'-lookup-does-not-mutate-row',list(CoreSettings.objects.filter(key=STREAM_SETTINGS_KEY).values())==before)
  report['cases'].append({'name':item['name'],'role':'actual pinned model/SQL/Redis lookup, no task invocation','input':item,'db_before':before,'cache_before':state,'effective':value,'error':error,'queries':queries,'cache_after_account':after_cache,'global_hash_value':CoreSettings.get_m3u_hash_key()})
 configure({'m3u_hash_key':''},cached={'m3u_hash_key':'url'},cache_present=True)
 relay=Proxy(RAW);original_cache=core_models.cache;cfg=copy.deepcopy(settings.CACHES['default']);routed=RedisCache('redis://:playback-fixture-only@127.0.0.1:61611/0',cfg);core_models.cache=routed
 with concurrent.futures.ThreadPoolExecutor(max_workers=1)as pool:
  future=pool.submit(M3UAccount(name='rust-owned-held-hash',custom_properties={}).get_effective_hash_keys);check('held-real-Redis-reply',relay.held.wait(10));time.sleep(2.2);check('Source-cached-nonempty-still-pending-beyond2s',not future.done());time.sleep(2);relay.release.set();value=future.result(timeout=5);check('Source-returned-actual-cached-url-after-release',value==['url'])
 core_models.cache=original_cache;routed.close();relay.close();check('held-proxy-clean',not relay.errors);report['cases'].append({'name':'held-cached-nonempty','role':'actual pinned model+real Redis routed through owned reply proxy; no task invocation','effective':value,'held_before_release_seconds':4.2,'proxy_errors':relay.errors});relay=None
 for root in ['/data/m3us','/data/epgs','/data/logos']:Path(root).mkdir(exist_ok=True);check('fresh-empty-'+root,not list(Path(root).iterdir()))
 CoreSettings._update_group(SYSTEM_SETTINGS_KEY,'System Settings',{'auto_import_mapped_files':True})
 for label,raw in [('empty',''),('url','url')]:
  stop();check(label+'-queue-start-empty',broker.llen('celery')==broker.hlen('unacked')==0)
  for old in Path('/data/m3us').iterdir():old.unlink()
  M3UAccount.objects.all().update(is_active=False);configure({'m3u_hash_key':raw});path=Path('/data/m3us/rust-owned-children-hash-'+label+'.m3u');path.write_text('#EXTM3U\n#EXTINF:-1 tvg-id="owned.channel" group-title="Owned Group",Owned stream\nhttp://127.0.0.1:9/owned-'+label+'.ts\n');os.utime(path,(1699999993,1699999993));tid=str(uuid.uuid4());Path('/evidence/owned-scanner-config.json').write_text(json.dumps({'task_id':tid,'now':1700000000.125}));prior={v['id']for v in records('postrun')};worker=start('rust-owned-scanner-parents');parent=app.tasks['core.tasks.scan_and_process_files'].apply_async(args=[],task_id=tid,queue='rust-owned-scanner-parents');wait(lambda:any(v['id']==tid for v in records('postrun')));check(label+'-real-parent-null',parent.get(timeout=2)is None);stop();account=M3UAccount.objects.get(file_path=str(path));frames=[json.loads(v)for v in reversed(broker.lrange('celery',0,3))];check(label+'-two-real-parented-default-children',[v['headers']['task']for v in frames]==['apps.m3u.tasks.refresh_m3u_groups','apps.m3u.tasks.refresh_single_m3u_account']and all(v['headers']['parent_id']==tid for v in frames));worker=start('celery');wait(lambda:len([v for v in records('postrun')if v['id']not in prior])==3 and broker.llen('celery')==broker.hlen('unacked')==0);stop();posts=[v for v in records('postrun')if v['id']not in prior];rows=list(Stream.objects.filter(m3u_account_id=account.id).values());results={}
  for ident in [tid]+[v['headers']['id']for v in frames]:
   result=app.AsyncResult(ident);check(label+'-actual-result-'+ident,result.successful());value=result.get(timeout=2);meta=json.loads(backend.get(app.backend.get_key_for_task(ident)));results[ident]={'metadata':meta,'Original_get_type':type(value).__module__+'.'+type(value).__name__,'ttl':backend.ttl(app.backend.get_key_for_task(ident))}
  check(label+'-actual-one-stream',len(rows)==1);check(label+'-actual-effective-setting',account.get_effective_hash_keys()==([]if raw==''else['url']));check(label+'-empty-vs-url-real-hash',(rows[0]['stream_hash']=='44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a')==(raw==''))
  report['cases'].append({'name':'worker-'+label,'role':'actual Original scanner+ordinary default-queue groups/single consumers','raw_setting':raw,'effective':account.get_effective_hash_keys(),'parent_id':tid,'frames':frames,'postruns':posts,'results':results,'account':list(M3UAccount.objects.filter(pk=account.id).values()),'streams':rows,'cache_after':cache_state()})
 check('exact-factory-worker-held-case-count',len(report['cases'])==25);report['passed']=True
finally:
 if relay:relay.release.set();relay.close()
 stop();Path('/evidence/source-logo-scanner-private.json').write_text(json.dumps(report,indent=2,default=str)+'\n')
print(json.dumps({'passed':report['passed'],'checks':len(report['checks']),'cases':len(report['cases'])}))
