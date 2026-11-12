"""Owned Source producer/native pre-ACK hash-read physical ownership controls.
Real Redis response and real PostgreSQL ERROR/Ready; no synthetic server frames.
Original Dispatcharr/dependency authors retain code/algorithm/license credit.
"""
import base64,json,os,select,signal,subprocess,sys,time,uuid
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
from dispatcharr.celery import app
from owned_cache_reply_proxy import Proxy as CacheProxy
from owned_pg_ready_proxy import Proxy as PgProxy
raw_cache=redis.Redis(host='localhost',port=6379,password='playback-fixture-only',db=0);broker=redis.Redis.from_url(os.environ['CELERY_BROKER_URL']);backend=redis.Redis.from_url(os.environ['CELERY_RESULT_BACKEND'])
assert broker.connection_pool.connection_kwargs['db']==8 and backend.connection_pool.connection_kwargs['db']==9 and broker.dbsize()==backend.dbsize()==0
for key in ['dvr:recover_dispatch_lock','core:version_check_dispatch_lock']:raw_cache.set(key,'1',ex=3600)
app.loader.import_default_modules();CoreSettings._update_group(SYSTEM_SETTINGS_KEY,'System Settings',{'auto_import_mapped_files':True})
report={'passed':False,'native_verified':True,'scope':__doc__,'checks':[],'cases':[],'predeclared':{'cases':['cache-reply','SQL-Ready'],'cache_hold_after_abort':2.3,'SQL_hold_before_abort':6.2,'native_cache_logical_timeout':2,'pool_acquire_timeout':5}};worker=None;native=None;proxy=None;role=None
GROUP=CoreSettings.group_cache_key(STREAM_SETTINGS_KEY);RAW=django_cache.make_key(GROUP)
def check(name,value):
 report['checks'].append({'name':name,'passed':bool(value)})
 if not value:raise AssertionError(name)
def records(kind):
 p=Path('/evidence/periodic-worker-'+kind+'.jsonl');return [json.loads(v)for v in p.read_text().splitlines()]if p.exists()else[]
def wait(fn,seconds=40):
 end=time.monotonic()+seconds
 while time.monotonic()<end:
  result=fn()
  if result:return result
  time.sleep(.02)
 raise TimeoutError('Owned physical deadline')
def stop():
 global worker
 if worker and worker.poll()is None:os.killpg(worker.pid,signal.SIGTERM);worker.wait(timeout=15)
 worker=None
def start(queue):
 log=open('/evidence/scanner-worker-private.log','ab');result=subprocess.Popen(['/dispatcharrpy/bin/celery','-A','periodic_worker_observer:app','worker','--pool=prefork','--concurrency=1','-Q',queue,'--without-gossip','--without-mingle','--without-heartbeat','--loglevel=WARNING'],env=dict(os.environ,C_FORCE_ROOT='true',PYTHONPATH='/pair-tools:/app'),stdout=log,stderr=log,start_new_session=True);log.close();return result
try:
 for root in ['/data/m3us','/data/epgs','/data/logos']:Path(root).mkdir(exist_ok=True);check('fresh-empty-'+root,not list(Path(root).iterdir()))
 for mode in ['cache-reply','SQL-Ready']:
  M3UAccount.objects.all().update(is_active=False)
  for old in Path('/data/m3us').iterdir():old.unlink()
  check(mode+'-prior-queue-empty',broker.llen('celery')==broker.hlen('unacked')==0)
  path=Path('/data/m3us/rust-owned-children-hash-physical-'+mode+'.m3u');path.write_text('#EXTM3U\n');os.utime(path,(1699999993,1699999993));tid=str(uuid.uuid4());Path('/evidence/owned-scanner-config.json').write_text(json.dumps({'task_id':tid,'now':1700000000.125}));worker=start('rust-owned-scanner-parents');parent=app.tasks['core.tasks.scan_and_process_files'].apply_async(args=[],task_id=tid,queue='rust-owned-scanner-parents');wait(lambda:any(v['id']==tid for v in records('postrun')));check(mode+'-real-parent-null',parent.get(timeout=2)is None);stop();account=M3UAccount.objects.get(file_path=str(path));head=broker.lindex('celery',-1);ident=json.loads(head)['headers']['id'];CoreSettings._update_group(STREAM_SETTINGS_KEY,'Stream Settings',{'m3u_hash_key':''});django_cache.delete(GROUP)
  cfg={'parent_id':tid,'account_id':account.id}
  if mode=='cache-reply':
   django_cache.set(GROUP,{'m3u_hash_key':'url'},timeout=300);proxy=CacheProxy(RAW);cfg['cache_url']='redis://:playback-fixture-only@127.0.0.1:61611/0'
  else:
   role='rust_m3u_preflight_'+str(account.id);quoted=connection.ops.quote_name(role)
   with connection.cursor()as cursor:
    cursor.execute('CREATE ROLE '+quoted+" LOGIN PASSWORD 'playback-fixture-only'");cursor.execute('GRANT USAGE ON SCHEMA public TO '+quoted);cursor.execute('GRANT SELECT ON ALL TABLES IN SCHEMA public TO '+quoted);cursor.execute('CREATE SEQUENCE rust_owned_hash_read_calls');cursor.execute('GRANT USAGE,SELECT ON SEQUENCE rust_owned_hash_read_calls TO '+quoted)
    cursor.execute("CREATE FUNCTION rust_owned_hash_read_guard(target text) RETURNS boolean LANGUAGE plpgsql AS $$BEGIN IF target='stream_settings' THEN PERFORM nextval('rust_owned_hash_read_calls');RAISE EXCEPTION 'rust-owned hash setting read failure' USING ERRCODE='P0001';END IF;RETURN true;END$$");cursor.execute('ALTER TABLE core_coresettings ENABLE ROW LEVEL SECURITY');cursor.execute('CREATE POLICY rust_owned_hash_read ON core_coresettings FOR SELECT TO '+quoted+' USING(rust_owned_hash_read_guard(key))')
   proxy=PgProxy();cfg['database_url']='postgres://'+role+':playback-fixture-only@127.0.0.1:61651/'+os.environ['POSTGRES_DB']+'?sslmode=disable'
  before=list(M3UAccount.objects.filter(pk=account.id).values());cache_before=raw_cache.get(RAW);log=open('/evidence/native-'+mode+'-private.log','wb');native=subprocess.Popen(['/native-m3u-peer','scanner_m3u_peer::native_scanner_m3u_peer','--exact','--ignored','--nocapture','--test-threads=1'],stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=log,text=True,bufsize=1)
  def response():
   end=time.monotonic()+35
   while time.monotonic()<end:
    assert native.poll()is None,'Native physical peer exited'
    if select.select([native.stdout],[],[],.1)[0]:
     line=native.stdout.readline();log.write(line.encode());log.flush()
     if 'NATIVE_SCANNER_M3U_JSON='in line:return json.loads(line.split('NATIVE_SCANNER_M3U_JSON=',1)[1])
   raise TimeoutError('Native physical response')
  def command(value):native.stdin.write(json.dumps(value)+'\n');native.stdin.flush();return response()
  check(mode+'-normal-path-prepared',command(cfg)=={'prepared':True});check(mode+'-started',command({'action':'start'})=={'started':True});check(mode+'-actual-reply-held',proxy.held.wait(15))
  if mode=='SQL-Ready':check(mode+'-real-P0001-forwarded',proxy.error.is_set()and proxy.error_code=='P0001');time.sleep(6.2)
  answer=command({'action':'abort'});check(mode+'-aborted-caller-slot-shutdown-retained',answer=={'caller_retained':True,'slots':0,'admission':'Err(Capacity)','drain':'Err(DrainTimeout)'})
  if mode=='cache-reply':
   time.sleep(2.3);observed=command({'action':'observe'});check(mode+'-physical-owner-survives-logical-timeout',observed=={'caller_retained':True,'slots':0,'drain':'Err(DrainTimeout)'})
  else:
   with connection.cursor()as cursor:cursor.execute('SELECT last_value,is_called FROM rust_owned_hash_read_calls');last,called=cursor.fetchone();cursor.execute('SELECT state FROM pg_stat_activity WHERE pid=%s',[proxy.backend_pid]);state=cursor.fetchone()[0]
   check(mode+'-one-real-error-no-statement-retry',called and last==1 and state=='idle')
  check(mode+'-no-ACK-or-task-metadata',broker.lindex('celery',-1)==head and backend.get(app.backend.get_key_for_task(ident))is None);check(mode+'-account-cache-unchanged',list(M3UAccount.objects.filter(pk=account.id).values())==before and raw_cache.get(RAW)==cache_before)
  proxy.release.set();check(mode+'-actual-settlement-releases-owner',command({'action':'settled'})=={'caller_released':True,'slots':1});check(mode+'-still-no-ACK-or-result',broker.lindex('celery',-1)==head and backend.get(app.backend.get_key_for_task(ident))is None)
  check(mode+'-drain',command({'action':'shutdown'})=={'drained':True});native.stdin.close();native.wait(timeout=10);check(mode+'-native-exit0',native.returncode==0);native=None;log.close();proxy.close();check(mode+'-proxy-clean',not proxy.errors);proxy=None
  if role:
   with connection.cursor()as cursor:cursor.execute('DROP POLICY rust_owned_hash_read ON core_coresettings');cursor.execute('ALTER TABLE core_coresettings DISABLE ROW LEVEL SECURITY');cursor.execute('DROP FUNCTION rust_owned_hash_read_guard(text)');cursor.execute('DROP SEQUENCE rust_owned_hash_read_calls');cursor.execute('DROP OWNED BY '+quoted);cursor.execute('DROP ROLE '+quoted)
   role=None
  report['cases'].append({'name':mode,'task_id':ident,'retention':answer,'no_ACK':True,'no_result':True,'account_cache_unchanged':True})
  worker=start('celery');wait(lambda:broker.llen('celery')==broker.hlen('unacked')==0 and backend.get(app.backend.get_key_for_task(ident))is not None);stop()
 report['passed']=True
finally:
 if proxy:proxy.close()
 if native and native.poll()is None:native.kill();native.wait(timeout=5)
 stop();Path('/evidence/source-logo-scanner-private.json').write_text(json.dumps(report,indent=2,default=str)+'\n')
print(json.dumps({'passed':report['passed'],'checks':len(report['checks']),'cases':len(report['cases'])}))
