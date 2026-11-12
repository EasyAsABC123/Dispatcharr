"""Native-only actual physical preflight SQL fence; actual Original producer.
Known real server P0001 is forwarded before held ReadyForQuery, with one owned
RLS role/policy in the copied DB. No Original-handler parity/performance claim.
"""
import json,os,signal,subprocess,sys,time,uuid,select
from pathlib import Path
assert os.environ.get('JOB_DISPOSABLE_FIXTURE')=='playback-fixture-only'
assert os.environ['POSTGRES_DB'].startswith('rust_probe_backup_jobs_')
sys.path[:0]=['/app','/pair-tools'];sys.argv.append('shell');os.environ.setdefault('DJANGO_SETTINGS_MODULE','dispatcharr.settings')
import django;django.setup()
import redis
from django.db import connection
from dispatcharr.celery import app
from core.models import CoreSettings,SYSTEM_SETTINGS_KEY
from apps.m3u.models import M3UAccount
from owned_pg_ready_proxy import Proxy
broker=redis.Redis.from_url(os.environ['CELERY_BROKER_URL']);backend=redis.Redis.from_url(os.environ['CELERY_RESULT_BACKEND']);cache=redis.Redis(host='localhost',port=6379,password='playback-fixture-only',db=0)
assert broker.connection_pool.connection_kwargs['db']==8 and backend.connection_pool.connection_kwargs['db']==9 and broker.dbsize()==backend.dbsize()==0
for key in ['dvr:recover_dispatch_lock','core:version_check_dispatch_lock']:cache.set(key,'1',ex=3600)
app.loader.import_default_modules();CoreSettings._update_group(SYSTEM_SETTINGS_KEY,'System Settings',{'auto_import_mapped_files':True});worker=None;native=None;proxy=None;role=None
report={'passed':False,'native_verified':True,'scope':__doc__,'checks':[],'cases':[],'predeclared':{'held_ready_seconds':6.2,'existing_pool_acquire_timeout_seconds':5,'shutdown_probe_seconds':.03,'no_application_SQL_retry':True}}
def check(name,value):
 report['checks'].append({'name':name,'passed':bool(value)})
 if not value:raise AssertionError(name)
def records(kind):
 p=Path('/evidence/periodic-worker-'+kind+'.jsonl');return [json.loads(v)for v in p.read_text().splitlines()]if p.exists()else[]
def wait(fn,seconds=30):
 end=time.monotonic()+seconds
 while time.monotonic()<end:
  v=fn()
  if v:return v
  time.sleep(.01)
 raise TimeoutError('Owned physical control deadline')
try:
 for root in ['/data/m3us','/data/epgs','/data/logos']:Path(root).mkdir(exist_ok=True);check('fresh-empty-'+root,not list(Path(root).iterdir()))
 path=Path('/data/m3us/rust-owned-children-physical-preflight-ready.m3u');path.write_text('#EXTM3U\n');os.utime(path,(1699999993,1699999993));tid=str(uuid.uuid4());Path('/evidence/owned-scanner-config.json').write_text(json.dumps({'task_id':tid,'now':1700000000.125}))
 log=open('/evidence/scanner-worker-private.log','ab');worker=subprocess.Popen(['/dispatcharrpy/bin/celery','-A','periodic_worker_observer:app','worker','--pool=prefork','--concurrency=1','-Q','rust-owned-scanner-parents','--without-gossip','--without-mingle','--without-heartbeat','--loglevel=WARNING'],env=dict(os.environ,C_FORCE_ROOT='true',PYTHONPATH='/pair-tools:/app'),stdout=log,stderr=log,start_new_session=True);log.close();parent=app.tasks['core.tasks.scan_and_process_files'].apply_async(args=[],task_id=tid,queue='rust-owned-scanner-parents');wait(lambda:any(v['id']==tid for v in records('postrun')));check('actual-Source-scanner-parent-completed',parent.get(timeout=2)is None);os.killpg(worker.pid,signal.SIGTERM);worker.wait(timeout=15);worker=None
 account=M3UAccount.objects.get(file_path=str(path));head=broker.lindex('celery',-1);frame=json.loads(head);ident=frame['headers']['id'];check('actual-scanner-parented-group-head',frame['headers']['task']=='apps.m3u.tasks.refresh_m3u_groups'and frame['headers']['parent_id']==tid)
 role='rust_m3u_preflight_'+str(account.id);quoted=connection.ops.quote_name(role)
 with connection.cursor()as c:
  c.execute('CREATE ROLE '+quoted+" LOGIN PASSWORD 'playback-fixture-only'");c.execute('GRANT USAGE ON SCHEMA public TO '+quoted);c.execute('GRANT SELECT ON ALL TABLES IN SCHEMA public TO '+quoted);c.execute('CREATE SEQUENCE rust_owned_preflight_calls');c.execute('GRANT USAGE,SELECT ON SEQUENCE rust_owned_preflight_calls TO '+quoted)
  c.execute("CREATE FUNCTION rust_owned_preflight_guard(target bigint) RETURNS boolean LANGUAGE plpgsql AS $$BEGIN IF target="+str(account.id)+" THEN PERFORM nextval('rust_owned_preflight_calls'); RAISE EXCEPTION 'rust-owned preflight failure' USING ERRCODE='P0001'; END IF; RETURN true; END$$")
  c.execute('ALTER TABLE m3u_m3uaccount ENABLE ROW LEVEL SECURITY');c.execute('CREATE POLICY rust_owned_preflight ON m3u_m3uaccount FOR SELECT TO '+quoted+' USING(rust_owned_preflight_guard(id))')
 proxy=Proxy();stderr=open('/evidence/native-sql-peer-private.log','wb');native=subprocess.Popen(['/native-m3u-peer','scanner_m3u_peer::native_scanner_m3u_peer','--exact','--ignored','--nocapture','--test-threads=1'],stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=stderr,text=True,bufsize=1)
 def response():
  end=time.monotonic()+35
  while time.monotonic()<end:
   assert native.poll()is None,'Native SQL peer exited'
   if select.select([native.stdout],[],[],.1)[0]:
    line=native.stdout.readline();stderr.write(line.encode());stderr.flush()
    if 'NATIVE_SCANNER_M3U_JSON='in line:return json.loads(line.split('NATIVE_SCANNER_M3U_JSON=',1)[1])
  raise TimeoutError('Native physical response')
 def command(v):native.stdin.write(json.dumps(v)+'\n');native.stdin.flush();return response()
 dburl='postgres://'+role+':playback-fixture-only@127.0.0.1:61651/'+os.environ['POSTGRES_DB']+'?sslmode=disable'
 check('normal-path-peer-prepared',command({'parent_id':tid,'account_id':account.id,'database_url':dburl})=={'prepared':True});check('owned-native-run-started',command({'action':'start'})=={'started':True});check('real-server-error-forwarded-and-Ready-held',proxy.error.wait(15)and proxy.held.wait(5)and proxy.error_code=='P0001')
 time.sleep(6.2)
 observed=command({'action':'abort'});report['cases'].append({'name':'actual-P0001-held-Ready-after-pool-timeout','task_id':ident,'observation':observed,'backend_pid':proxy.backend_pid,'error_code':proxy.error_code})
 check('caller-admission-shutdown-retained-past-existing5s',observed=={'caller_retained':True,'slots':0,'admission':'Err(Capacity)','drain':'Err(DrainTimeout)'})
 with connection.cursor()as c:
  c.execute('SELECT last_value,is_called FROM rust_owned_preflight_calls');last,called=c.fetchone();c.execute('SELECT state FROM pg_stat_activity WHERE pid=%s',[proxy.backend_pid]);state=c.fetchone()[0]
 check('actual-server-completed-error-without-application-retry',called and last==1 and state=='idle');check('preflight-error-never-ACKs-or-publishes-result',broker.lindex('celery',-1)==head and backend.get(app.backend.get_key_for_task(ident))is None)
 proxy.release.set();check('physical-ready-close-releases-owner',command({'action':'settled'})=={'caller_released':True,'slots':1});check('failed-preflight-still-retains-exact-frame',broker.lindex('celery',-1)==head and backend.get(app.backend.get_key_for_task(ident))is None)
 check('normal-family-drain',command({'action':'shutdown'})=={'drained':True});native.stdin.close();native.wait(timeout=10);check('native-exit0',native.returncode==0);native=None;stderr.close();proxy.close();check('proxy-no-errors',not proxy.errors);proxy=None
 report['passed']=True
finally:
 if proxy:proxy.close()
 if native and native.poll()is None:native.kill();native.wait(timeout=5)
 if worker and worker.poll()is None:os.killpg(worker.pid,signal.SIGKILL);worker.wait(timeout=5)
 if role:
  with connection.cursor()as c:
   c.execute('DROP POLICY IF EXISTS rust_owned_preflight ON m3u_m3uaccount');c.execute('ALTER TABLE m3u_m3uaccount DISABLE ROW LEVEL SECURITY');c.execute('DROP FUNCTION IF EXISTS rust_owned_preflight_guard(bigint)');c.execute('DROP SEQUENCE IF EXISTS rust_owned_preflight_calls');c.execute('DROP OWNED BY '+connection.ops.quote_name(role));c.execute('DROP ROLE '+connection.ops.quote_name(role))
 Path('/evidence/source-logo-scanner-private.json').write_text(json.dumps(report,indent=2)+'\n')
print(json.dumps({'passed':report['passed'],'checks':len(report['checks'])}))
