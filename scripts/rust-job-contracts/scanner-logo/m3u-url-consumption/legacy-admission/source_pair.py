"""Actual Source producer/native normal pre-ACK hash guard with real fallback.
No general hash implementation, no Main/default consumer, no cachefill claim.
Original Dispatcharr/dependency authors retain code/algorithm/license credit.
"""
import base64,hashlib,json,os,select,signal,subprocess,sys,time,uuid
from pathlib import Path
assert os.environ.get('JOB_DISPOSABLE_FIXTURE')=='playback-fixture-only'
assert os.environ['POSTGRES_DB'].startswith('rust_probe_backup_jobs_')
sys.path[:0]=['/app','/pair-tools'];sys.argv.append('shell');os.environ.setdefault('DJANGO_SETTINGS_MODULE','dispatcharr.settings')
import django;django.setup()
import redis
from django.db import connection
from django.core.cache import cache as django_cache
from core.models import CoreSettings,STREAM_SETTINGS_KEY,SYSTEM_SETTINGS_KEY
from apps.m3u.models import M3UAccount
from apps.channels.models import Stream
from dispatcharr.celery import app
from owned_pg_row_proxy import Proxy as RowProxy
raw_cache=redis.Redis(host='localhost',port=6379,password='playback-fixture-only',db=0);broker=redis.Redis.from_url(os.environ['CELERY_BROKER_URL']);backend=redis.Redis.from_url(os.environ['CELERY_RESULT_BACKEND'])
assert broker.connection_pool.connection_kwargs['db']==8 and backend.connection_pool.connection_kwargs['db']==9 and broker.dbsize()==backend.dbsize()==0
for key in ['dvr:recover_dispatch_lock','core:version_check_dispatch_lock']:raw_cache.set(key,'1',ex=3600)
app.loader.import_default_modules();CoreSettings._update_group(SYSTEM_SETTINGS_KEY,'System Settings',{'auto_import_mapped_files':True})
report={'passed':False,'native_verified':True,'scope':__doc__,'checks':[],'cases':[]};worker=None;native=None;row_proxy=None
CASES=[
 {'name':'missing-group','present':False,'group':{},'accept':True},
 {'name':'null-key','group':{'m3u_hash_key':None},'accept':True},
 {'name':'empty-key','group':{'m3u_hash_key':''},'accept':True},
 {'name':'cached-empty-map-over-url-db','group':{'m3u_hash_key':'url'},'cached':{},'accept':True},
 {'name':'db-url','group':{'m3u_hash_key':'url'},'accept':True},
 {'name':'cached-url-over-empty-db','group':{'m3u_hash_key':''},'cached':{'m3u_hash_key':'url'},'accept':True},
 {'name':'list-key','group':{'m3u_hash_key':['url']},'accept':False},
 {'name':'oversized-db-group','group':{'m3u_hash_key':'','owned_padding':'x'*70000},'accept':False},
 {'name':'oversized-cache-group','group':{'m3u_hash_key':''},'cached':{'m3u_hash_key':'x'*70000},'accept':False},
 {'name':'account-url-override','group':{'m3u_hash_key':''},'custom':{'hash_key':'url'},'accept':False},
 {'name':'message-null','group':{'m3u_hash_key':''},'message':None,'accept':True},
 {'name':'message-8192','group':{'m3u_hash_key':''},'message':'m'*8192,'accept':True},
 {'name':'message-8193','group':{'m3u_hash_key':''},'message':'m'*8193,'accept':False},
 {'name':'message-262144','group':{'m3u_hash_key':''},'message':'m'*262144,'accept':False},
]
def check(name,value):
 report['checks'].append({'name':name,'passed':bool(value)})
 if not value:raise AssertionError(name)
def records(kind):
 p=Path('/evidence/periodic-worker-'+kind+'.jsonl');return [json.loads(v)for v in p.read_text().splitlines()]if p.exists()else[]
def wait(fn,seconds=50):
 end=time.monotonic()+seconds
 while time.monotonic()<end:
  result=fn()
  if result:return result
  time.sleep(.02)
 raise TimeoutError('Owned hash gate deadline')
def stop():
 global worker
 if worker and worker.poll()is None:
  os.killpg(worker.pid,signal.SIGTERM)
  try:worker.wait(timeout=15)
  except subprocess.TimeoutExpired:os.killpg(worker.pid,signal.SIGKILL);worker.wait(timeout=5);report['forced_cleanup']=True
 worker=None
def start(queue):
 log=open('/evidence/scanner-worker-private.log','ab');result=subprocess.Popen(['/dispatcharrpy/bin/celery','-A','periodic_worker_observer:app','worker','--pool=prefork','--concurrency=1','-Q',queue,'--without-gossip','--without-mingle','--without-heartbeat','--loglevel=WARNING'],env=dict(os.environ,C_FORCE_ROOT='true',PYTHONPATH='/pair-tools:/app'),stdout=log,stderr=log,start_new_session=True);log.close();return result
GROUP=CoreSettings.group_cache_key(STREAM_SETTINGS_KEY)
def cache_snapshot():
 result={};keys=list(raw_cache.scan_iter('*'));assert len(keys)<=128
 for key in keys:
  kind=raw_cache.type(key);value=raw_cache.dump(key)
  if kind==b'none'or value is None:continue
  assert len(value)<=262144
  result[key.decode()]={'type':kind.decode(),'dump_base64':base64.b64encode(value).decode()}
 return result
def comparable_cache(values):
 def heartbeat(key):
  parts=key.split(':')
  return len(parts)==5 and parts[:2]==['live','worker']and parts[3].isdigit()and parts[4]=='heartbeat'
 return {key:value for key,value in values.items()if not heartbeat(key)}
try:
 for root in ['/data/m3us','/data/epgs','/data/logos']:Path(root).mkdir(exist_ok=True);check('fresh-empty-'+root,not list(Path(root).iterdir()))
 for item in CASES:
  label=item['name'];stop();check(label+'-prior-queue-empty',broker.llen('celery')==broker.hlen('unacked')==0)
  for old in Path('/data/m3us').iterdir():old.unlink()
  M3UAccount.objects.all().update(is_active=False);path=Path('/data/m3us/rust-owned-children-hash-gate-'+label+'.m3u');path.write_text('#EXTM3U\n#EXTINF:-1 group-title="Owned Group",Owned stream\nhttp://127.0.0.1:9/'+label+'.ts\n');os.utime(path,(1699999993,1699999993));tid=str(uuid.uuid4());Path('/evidence/owned-scanner-config.json').write_text(json.dumps({'task_id':tid,'now':1700000000.125}));worker=start('rust-owned-scanner-parents');parent=app.tasks['core.tasks.scan_and_process_files'].apply_async(args=[],task_id=tid,queue='rust-owned-scanner-parents');wait(lambda:any(v['id']==tid for v in records('postrun')));check(label+'-real-parent-null',parent.get(timeout=2)is None);stop();account=M3UAccount.objects.get(file_path=str(path));frames=[json.loads(v)for v in reversed(broker.lrange('celery',0,3))];check(label+'-actual-parented-two-frames',len(frames)==2 and all(v['headers']['parent_id']==tid for v in frames));head=broker.lindex('celery',-1)
  CoreSettings.objects.filter(key=STREAM_SETTINGS_KEY).delete()
  if item.get('present',True):CoreSettings.objects.create(key=STREAM_SETTINGS_KEY,name='Stream Settings',value=item['group'])
  if 'cached'in item:django_cache.set(GROUP,item['cached'],timeout=300)
  else:django_cache.delete(GROUP)
  if 'custom'in item:
   with connection.cursor()as cursor:cursor.execute('UPDATE m3u_m3uaccount SET custom_properties=%s::jsonb WHERE id=%s',[json.dumps(item['custom']),account.id])
  if 'message'in item:
   with connection.cursor()as cursor:cursor.execute('UPDATE m3u_m3uaccount SET last_message=%s WHERE id=%s',[item['message'],account.id])
   row_proxy=RowProxy()
  before={'account':list(M3UAccount.objects.filter(pk=account.id).values()),'cache':cache_snapshot(),'settings':list(CoreSettings.objects.filter(key=STREAM_SETTINGS_KEY).values())}
  log=open('/evidence/native-'+label+'-private.log','wb');native=subprocess.Popen(['/native-m3u-peer','scanner_m3u_peer::native_scanner_m3u_peer','--exact','--ignored','--nocapture','--test-threads=1'],stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=log,text=True,bufsize=1)
  def response():
   end=time.monotonic()+35
   while time.monotonic()<end:
    assert native.poll()is None,'Native hash peer exited'
    if select.select([native.stdout],[],[],.1)[0]:
     line=native.stdout.readline();log.write(line.encode());log.flush()
     if 'NATIVE_SCANNER_M3U_JSON='in line:return json.loads(line.split('NATIVE_SCANNER_M3U_JSON=',1)[1])
   raise TimeoutError('Native hash response')
  def command(value):native.stdin.write(json.dumps(value)+'\n');native.stdin.flush();return response()
  cfg={'parent_id':tid,'account_id':account.id}
  if row_proxy:cfg['database_url']='postgres://dispatch:playback-fixture-only@127.0.0.1:61651/'+os.environ['POSTGRES_DB']+'?sslmode=disable'
  check(label+'-normal-constructor',command(cfg)=={'prepared':True});outputs=[]
  if row_proxy:row_proxy.record_data=True
  for number in range(2 if item['accept']else 1):
   result=command({'action':'run'});outputs.append(result)
   if item['accept']:check(label+'-native-completed-'+str(number),result['run'].startswith('Ok(Completed('))
   else:check(label+'-explicit-refusal-before-ACK',result['run']=='Err(Domain(Unsupported))'and broker.lindex('celery',-1)==head)
  check(label+'-drain',command({'action':'shutdown'})=={'drained':True});native.stdin.close();native.wait(timeout=10);check(label+'-native-exit0',native.returncode==0);native=None;log.close()
  if row_proxy:
   check(label+'-actual-data-rows-observed',row_proxy.data_rows>0)
   check(label+'-projected-response-byte-bound',row_proxy.max_data_row_bytes<=(10000 if label=='message-8192'else 2048))
   if label=='message-8192':check(label+'-exact-boundary-really-transferred',row_proxy.max_data_row_bytes>=8192)
   row_stats={'rows':row_proxy.data_rows,'max_data_row_bytes':row_proxy.max_data_row_bytes};row_proxy.close();check(label+'-row-proxy-clean',not row_proxy.errors);row_proxy=None
  else:row_stats=None
  after={'account':list(M3UAccount.objects.filter(pk=account.id).values()),'cache':cache_snapshot(),'settings':list(CoreSettings.objects.filter(key=STREAM_SETTINGS_KEY).values())};entry={'name':label,'accept':item['accept'],'parent_id':tid,'frames':frames,'before':before,'after_native':after,'native_outputs':outputs,'projected_rows':row_stats};report['cases'].append(entry)
  check(label+'-settings-row-retained',after['settings']==before['settings'])
  groupkey=django_cache.make_key(GROUP);check(label+'-no-stream-cache-fill',after['cache'].get(groupkey)==before['cache'].get(groupkey))
  if not item['accept']:
   check(label+'-account-cache-retained',after['account']==before['account']and comparable_cache(after['cache'])==comparable_cache(before['cache']));check(label+'-no-task-metadata',all(backend.get(app.backend.get_key_for_task(v['headers']['id']))is None for v in frames));worker=start('celery');wait(lambda:broker.llen('celery')==broker.hlen('unacked')==0 and all(backend.get(app.backend.get_key_for_task(v['headers']['id']))is not None for v in frames));stop();entry['Source_fallback']=True
  else:
   check(label+'-queue-consumed',broker.llen('celery')==0);expected_hash=Stream.generate_hash_key('Owned stream','http://127.0.0.1:9/'+label+'.ts','',['url']if label in ['db-url','cached-url-over-empty-db']else[],m3u_id=account.id,account_type='STD');check(label+'-actual-declared-mode-projection',Stream.objects.filter(stream_hash=expected_hash).exists())
  entry['results']={}
  for frame in frames:
   ident=frame['headers']['id'];result=app.AsyncResult(ident);value=result.get(timeout=2,propagate=False);entry['results'][ident]={'metadata':json.loads(backend.get(app.backend.get_key_for_task(ident))),'Original_get_type':type(value).__module__+'.'+type(value).__name__,'ttl':backend.ttl(app.backend.get_key_for_task(ident))}
 report['passed']=True
finally:
 if row_proxy:row_proxy.close()
 if native and native.poll()is None:native.kill();native.wait(timeout=5)
 stop();Path('/evidence/source-logo-scanner-private.json').write_text(json.dumps(report,indent=2,default=str)+'\n')
print(json.dumps({'passed':report['passed'],'checks':len(report['checks']),'cases':len(report['cases'])}))
