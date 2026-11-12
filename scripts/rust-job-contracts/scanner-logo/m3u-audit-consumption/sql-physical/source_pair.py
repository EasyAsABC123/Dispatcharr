"""Actual Source scanner producer; native post-admission audit ERROR/P0001 and unknownP0002 controls hold real ReadyForQuery; no Source general-fault claim.
Original Dispatcharr/dependency authors retain credit and licenses.
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
from owned_reply_proxy import Proxy as ClaimProxy
from core.models import SystemEvent
broker=redis.Redis.from_url(os.environ['CELERY_BROKER_URL']);backend=redis.Redis.from_url(os.environ['CELERY_RESULT_BACKEND']);cache=redis.Redis(host='localhost',port=6379,password='playback-fixture-only',db=0)
assert broker.connection_pool.connection_kwargs['db']==8 and backend.connection_pool.connection_kwargs['db']==9 and broker.dbsize()==backend.dbsize()==0
for key in ['dvr:recover_dispatch_lock','core:version_check_dispatch_lock']:cache.set(key,'1',ex=3600)
app.loader.import_default_modules();CoreSettings._update_group(SYSTEM_SETTINGS_KEY,'System Settings',{'auto_import_mapped_files':True});worker=None;native=None;proxy=None;claim_proxy=None;role=None
report={'passed':False,'native_verified':True,'scope':__doc__,'checks':[],'cases':[],'predeclared':{'held_ready_seconds':6.2,'existing_pool_acquire_timeout_seconds':5,'shutdown_probe_seconds':.03,'no_application_SQL_retry':True,'cases':['INSERT-P0001','DELETE-P0001','INSERT-P0002'],'fault_install':'after admitted real CAS/pop before reply release'}}
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
 for mode,code in [('INSERT','P0001'),('DELETE','P0001'),('INSERT','P0002')]:
  label=mode+'-'+code;M3UAccount.objects.all().update(is_active=False)
  for old in Path('/data/m3us').iterdir():old.unlink()
  check(label+'-prior-default-queue-empty',broker.llen('celery')==broker.hlen('unacked')==0)
  path=Path('/data/m3us/rust-owned-children-audit-held-'+label+'.m3u');path.write_text('#EXTM3U\n#EXTINF:-1 tvg-id="owned.channel" group-title="Owned Group",Owned stream\nhttp://127.0.0.1:9/owned.ts\n');os.utime(path,(1699999993,1699999993));tid=str(uuid.uuid4());Path('/evidence/owned-scanner-config.json').write_text(json.dumps({'task_id':tid,'now':1700000000.125}))
  log=open('/evidence/scanner-worker-private.log','ab');worker=subprocess.Popen(['/dispatcharrpy/bin/celery','-A','periodic_worker_observer:app','worker','--pool=prefork','--concurrency=1','-Q','rust-owned-scanner-parents','--without-gossip','--without-mingle','--without-heartbeat','--loglevel=WARNING'],env=dict(os.environ,C_FORCE_ROOT='true',PYTHONPATH='/pair-tools:/app'),stdout=log,stderr=log,start_new_session=True);log.close();parent=app.tasks['core.tasks.scan_and_process_files'].apply_async(args=[],task_id=tid,queue='rust-owned-scanner-parents');wait(lambda:any(v['id']==tid for v in records('postrun')));check(label+'-actual-Source-parent-null',parent.get(timeout=2)is None);os.killpg(worker.pid,signal.SIGTERM);worker.wait(timeout=15);worker=None
  account=M3UAccount.objects.get(file_path=str(path));CoreSettings._update_group(SYSTEM_SETTINGS_KEY,'System Settings',{'max_system_events':2});SystemEvent.objects.all().delete()
  from datetime import datetime,timedelta,timezone
  for index in range(4):
   row=SystemEvent.objects.create(event_type='m3u_refresh',details={'rust_owned_audit':True,'ordinal':index});SystemEvent.objects.filter(pk=row.pk).update(timestamp=datetime(2010,1,1,tzinfo=timezone.utc)+timedelta(seconds=index))
  proxy=Proxy(code);claim_proxy=ClaimProxy('claim',tid,'unused')
  stderr=open('/evidence/native-'+label+'-private.log','wb');native=subprocess.Popen(['/native-m3u-peer','scanner_m3u_peer::native_scanner_m3u_peer','--exact','--ignored','--nocapture','--test-threads=1'],stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=stderr,text=True,bufsize=1)
  def response():
   end=time.monotonic()+35
   while time.monotonic()<end:
    assert native.poll()is None,'Native peer exited'
    if select.select([native.stdout],[],[],.1)[0]:
     line=native.stdout.readline();stderr.write(line.encode());stderr.flush()
     if 'NATIVE_SCANNER_M3U_JSON='in line:return json.loads(line.split('NATIVE_SCANNER_M3U_JSON=',1)[1])
   raise TimeoutError('Native physical response')
  def command(value):native.stdin.write(json.dumps(value)+'\n');native.stdin.flush();return response()
  cfg={'parent_id':tid,'account_id':account.id,'database_url':'postgres://dispatch:playback-fixture-only@127.0.0.1:61651/'+os.environ['POSTGRES_DB']+'?sslmode=disable','broker_url':'redis://:playback-fixture-only@127.0.0.1:61602/8'}
  check(label+'-same-normal-path-prepared',command(cfg)=={'prepared':True});check(label+'-actual-groups-completed',command({'action':'run'})['run'].startswith('Ok(Completed('))
  head=broker.lindex('celery',-1);frame=json.loads(head);ident=frame['headers']['id'];check(label+'-actual-single-head',frame['headers']['task']=='apps.m3u.tasks.refresh_single_m3u_account')
  claim_proxy.armed=True;check(label+'-single-started',command({'action':'start'})=={'started':True});check(label+'-admitted-real-CAS-held',claim_proxy.held.wait(15));check(label+'-ACK-no-result-before-fault',broker.llen('celery')==0 and backend.get(app.backend.get_key_for_task(ident))is None)
  with connection.cursor()as c:
   c.execute('CREATE SEQUENCE rust_owned_audit_fail_count');guard="NEW.event_type='m3u_refresh'"if mode=='INSERT'else"OLD.details->>'rust_owned_audit'='true'";returned_record='NEW'if mode=='INSERT'else'OLD'
   c.execute("CREATE FUNCTION rust_owned_audit_worker_fail() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN IF "+guard+" THEN PERFORM nextval('rust_owned_audit_fail_count');RAISE EXCEPTION 'rust-owned held audit' USING ERRCODE='"+code+"';END IF;RETURN "+returned_record+";END$$")
   c.execute('CREATE TRIGGER rust_owned_audit_worker_fail BEFORE '+mode+' ON core_systemevent FOR EACH ROW EXECUTE FUNCTION rust_owned_audit_worker_fail()')
  claim_proxy.release.set();check(label+'-real-error-and-Ready-held',proxy.error.wait(15)and proxy.held.wait(5)and proxy.error_code==code and proxy.error_severity=='ERROR');time.sleep(6.2)
  answer=command({'action':'abort'});check(label+'-caller-slot-shutdown-retained',answer=={'caller_retained':True,'slots':0,'admission':'Err(Capacity)','drain':'Err(DrainTimeout)'})
  with connection.cursor()as c:
   c.execute('SELECT last_value,is_called FROM rust_owned_audit_fail_count');last,called=c.fetchone();c.execute('SELECT state FROM pg_stat_activity WHERE pid=%s',[proxy.backend_pid]);state=c.fetchone()[0]
  check(label+'-one-actual-error-no-query-retry',called and last==1 and state=='idle');check(label+'-no-result-before-physical-settlement',backend.get(app.backend.get_key_for_task(ident))is None)
  proxy.release.set();check(label+'-settled-owner-release',command({'action':'settled'})=={'caller_released':True,'slots':1})
  actual=app.AsyncResult(ident)
  if code=='P0001':check(label+'-Original-typed-normal-result',actual.ready()and actual.successful()and actual.get(timeout=2)=='Dispatched jobs complete.')
  else:check(label+'-unknown-code-remains-operational',actual.state=='PENDING'and backend.get(app.backend.get_key_for_task(ident))is None)
  rows=list(SystemEvent.objects.order_by('id').values());check(label+'-exact-partial-audit-row-count',len(rows)==(4 if mode=='INSERT'else 5));check(label+'-filelock-released',not cache.exists('task_lock_refresh_single_m3u_account_'+str(account.id)));check(label+'-cache-file-cleaned',not(Path('/app/media/cached_m3u')/(str(account.id)+'.json')).exists())
  report['cases'].append({'name':label,'task_id':ident,'actual_code':proxy.error_code,'actual_severity':proxy.error_severity,'backend_pid':proxy.backend_pid,'error_calls':last,'observed_retention':answer,'actual_result_state':actual.state,'partial_audit_rows':rows,'fault_timing':'post-admission real CAS held, then owned trigger installed before reply release'})
  check(label+'-normal-drain',command({'action':'shutdown'})=={'drained':True});native.stdin.close();native.wait(timeout=10);check(label+'-native-exit0',native.returncode==0);native=None;stderr.close();proxy.close();claim_proxy.close();check(label+'-proxies-clean',not proxy.errors and not claim_proxy.errors);proxy=None;claim_proxy=None
  with connection.cursor()as c:c.execute('DROP TRIGGER rust_owned_audit_worker_fail ON core_systemevent');c.execute('DROP FUNCTION rust_owned_audit_worker_fail()');c.execute('DROP SEQUENCE rust_owned_audit_fail_count')
 report['passed']=True
finally:
 if claim_proxy:claim_proxy.close()
 if proxy:proxy.close()
 if native and native.poll()is None:native.kill();native.wait(timeout=5)
 if worker and worker.poll()is None:os.killpg(worker.pid,signal.SIGKILL);worker.wait(timeout=5)
 Path('/evidence/source-logo-scanner-private.json').write_text(json.dumps(report,indent=2,default=str)+'\n')
print(json.dumps({'passed':report['passed'],'checks':len(report['checks']),'cases':len(report['cases'])}))
