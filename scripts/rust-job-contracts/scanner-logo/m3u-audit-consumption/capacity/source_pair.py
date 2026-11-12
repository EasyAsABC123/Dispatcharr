"""Actual Source scanner producer -> native bounded-history capacity controls; no Source large-history handler comparison.
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
from core.models import SystemEvent
broker=redis.Redis.from_url(os.environ['CELERY_BROKER_URL']);backend=redis.Redis.from_url(os.environ['CELERY_RESULT_BACKEND']);cache=redis.Redis(host='localhost',port=6379,password='playback-fixture-only',db=0)
assert broker.connection_pool.connection_kwargs['db']==8 and backend.connection_pool.connection_kwargs['db']==9 and broker.dbsize()==backend.dbsize()==0
for key in ['dvr:recover_dispatch_lock','core:version_check_dispatch_lock']:cache.set(key,'1',ex=3600)
app.loader.import_default_modules();CoreSettings._update_group(SYSTEM_SETTINGS_KEY,'System Settings',{'auto_import_mapped_files':True});worker=None;native=None;proxy=None;claim_proxy=None;role=None
report={'passed':False,'native_verified':True,'scope':'Actual Original scanner producer; native bounded-history capacity positive/negative, not Source large-history handler parity','checks':[],'cases':[],'predeclared':{'limit':10000,'cases':[10000,10001],'one_owned_pending_insertion':1,'overflow':'beforeSingleACK'}}
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
 for count in [10000,10001]:
  label=str(count);M3UAccount.objects.all().update(is_active=False)
  for old in Path('/data/m3us').iterdir():old.unlink()
  check(label+'-prior-queue-empty',broker.llen('celery')==broker.hlen('unacked')==0)
  path=Path('/data/m3us/rust-owned-children-audit-cap-'+label+'.m3u');path.write_text('#EXTM3U\n#EXTINF:-1 group-title="Owned Group",Owned stream\nhttp://127.0.0.1:9/owned.ts\n');os.utime(path,(1699999993,1699999993));tid=str(uuid.uuid4());Path('/evidence/owned-scanner-config.json').write_text(json.dumps({'task_id':tid,'now':1700000000.125}))
  log=open('/evidence/scanner-worker-private.log','ab');worker=subprocess.Popen(['/dispatcharrpy/bin/celery','-A','periodic_worker_observer:app','worker','--pool=prefork','--concurrency=1','-Q','rust-owned-scanner-parents','--without-gossip','--without-mingle','--without-heartbeat','--loglevel=WARNING'],env=dict(os.environ,C_FORCE_ROOT='true',PYTHONPATH='/pair-tools:/app'),stdout=log,stderr=log,start_new_session=True);log.close();parent=app.tasks['core.tasks.scan_and_process_files'].apply_async(args=[],task_id=tid,queue='rust-owned-scanner-parents');wait(lambda:any(v['id']==tid for v in records('postrun')));check(label+'-real-parent-null',parent.get(timeout=2)is None);os.killpg(worker.pid,signal.SIGTERM);worker.wait(timeout=15);worker=None
  account=M3UAccount.objects.get(file_path=str(path));CoreSettings._update_group(SYSTEM_SETTINGS_KEY,'System Settings',{'max_system_events':10000})
  with connection.cursor()as c:
   c.execute('DELETE FROM core_systemevent');c.execute("INSERT INTO core_systemevent(event_type,timestamp,details)SELECT 'm3u_refresh','2010-01-01'::timestamptz+g*interval '1 second','{\"rust_owned_audit\":true}'::jsonb FROM generate_series(1,%s)g",[count]);c.execute('SELECT count(*),min(id) FROM core_systemevent');initial_count,oldest=c.fetchone()
  check(label+'-actual-unique-history-count',initial_count==count)
  stderr=open('/evidence/native-cap-'+label+'-private.log','wb');native=subprocess.Popen(['/native-m3u-peer','scanner_m3u_peer::native_scanner_m3u_peer','--exact','--ignored','--nocapture','--test-threads=1'],stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=stderr,text=True,bufsize=1)
  def response():
   end=time.monotonic()+35
   while time.monotonic()<end:
    assert native.poll()is None,'Native capacity peer exited'
    if select.select([native.stdout],[],[],.1)[0]:
     line=native.stdout.readline();stderr.write(line.encode());stderr.flush()
     if 'NATIVE_SCANNER_M3U_JSON='in line:return json.loads(line.split('NATIVE_SCANNER_M3U_JSON=',1)[1])
   raise TimeoutError('Native capacity response')
  def command(value):native.stdin.write(json.dumps(value)+'\n');native.stdin.flush();return response()
  check(label+'-normal-path-prepared',command({'parent_id':tid,'account_id':account.id})=={'prepared':True});check(label+'-group-completed',command({'action':'run'})['run'].startswith('Ok(Completed('))
  head=broker.lindex('celery',-1);ident=json.loads(head)['headers']['id'];cache_path=Path('/app/media/cached_m3u')/(str(account.id)+'.json');cache_before=cache_path.read_bytes();before=list(M3UAccount.objects.filter(pk=account.id).values())
  answer=command({'action':'run'})
  if count==10000:
   result=app.AsyncResult(ident);check(label+'-positive-completion',answer['run'].startswith('Ok(Completed(')and result.ready()and result.get(timeout=2)=='Dispatched jobs complete.');check(label+'-queue-consumed',broker.llen('celery')==0)
  else:
   check(label+'-overflow-before-ACK',answer['run']=='Err(Domain(Unsupported))'and broker.lindex('celery',-1)==head and backend.get(app.backend.get_key_for_task(ident))is None);check(label+'-no-account-or-cache-effects',list(M3UAccount.objects.filter(pk=account.id).values())==before and cache_path.read_bytes()==cache_before)
  with connection.cursor()as c:c.execute('SELECT count(*),min(id) FROM core_systemevent');final_count,final_oldest=c.fetchone()
  check(label+'-exact-history-boundary',final_count==10001 and final_oldest==oldest);check(label+'-no-single-lease-remains',not cache.exists('task_lock_refresh_single_m3u_account_'+str(account.id)))
  report['cases'].append({'name':label,'history_before':initial_count,'history_after':final_count,'oldest_id_unchanged':final_oldest==oldest,'native_observation':answer,'task_id':ident,'scope':'native bounds; no Source large-history handler comparison'})
  check(label+'-drain',command({'action':'shutdown'})=={'drained':True});native.stdin.close();native.wait(timeout=10);check(label+'-native-exit0',native.returncode==0);native=None;stderr.close()
 report['passed']=True
finally:
 if native and native.poll()is None:native.kill();native.wait(timeout=5)
 if worker and worker.poll()is None:os.killpg(worker.pid,signal.SIGKILL);worker.wait(timeout=5)
 Path('/evidence/source-logo-scanner-private.json').write_text(json.dumps(report,indent=2,default=str)+'\n')
print(json.dumps({'passed':report['passed'],'checks':len(report['checks']),'cases':len(report['cases'])}))
