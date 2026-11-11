"""Actual pinned Beat cleanup expiry + real unmodified Original worker.
Original Dispatcharr/Celery/Kombu/django-celery-beat authors retain credit/licenses.
Only copied DB/Redis8+9; no other default family execution or native authority.
"""
import base64,datetime as dt,hashlib,inspect,json,os,signal,subprocess,sys,time,uuid
from pathlib import Path
assert os.environ.get('JOB_DISPOSABLE_FIXTURE')=='playback-fixture-only'
assert os.environ['POSTGRES_DB'].startswith('rust_probe_backup_jobs_')
sys.path[:0]=['/app','/pair-tools'];sys.argv.append('shell');os.environ.setdefault('DJANGO_SETTINGS_MODULE','dispatcharr.settings')
import django;django.setup()
import redis
from django.utils import timezone
from dispatcharr.celery import app
from django_celery_beat.schedulers import DatabaseScheduler
from django_celery_beat.models import PeriodicTask,PeriodicTasks
from celery.worker.request import Request
from celery.exceptions import TaskRevokedError
from celery.backends.base import Backend
broker=redis.Redis.from_url(os.environ['CELERY_BROKER_URL']);backend=redis.Redis.from_url(os.environ['CELERY_RESULT_BACKEND']);cache=redis.Redis(host='localhost',port=6379,password='playback-fixture-only',db=0)
assert broker.connection_pool.connection_kwargs['db']==8 and backend.connection_pool.connection_kwargs['db']==9
broker.flushdb();backend.flushdb()
for key in ['dvr:recover_dispatch_lock','core:version_check_dispatch_lock']:cache.set(key,'1',ex=3600)
app.loader.import_default_modules();worker=None;scheduler=None
report={'passed':False,'scope':'Actual Original/native Beat cleanup-only expiry, REVOKED/normal metadata, local worker restart and ACK interoperability','checks':[],'cases':[]}
def check(name,value):
 report['checks'].append({'name':name,'passed':bool(value)})
 if not value:raise AssertionError(name)
def rows(kind):
 p=Path('/evidence/periodic-worker-'+kind+'.jsonl');return[json.loads(x)for x in p.read_text().splitlines()]if p.exists()else[]
def wait(fn):
 end=time.monotonic()+35
 while time.monotonic()<end:
  value=fn()
  if value:return value
  if worker and worker.poll()is not None:raise RuntimeError('Owned worker exited')
  time.sleep(.025)
 raise TimeoutError('Owned cleanup expiry deadline')
def start():
 global worker
 with Path('/evidence/cleanup-expiry-worker-private.log').open('ab')as log:
  worker=subprocess.Popen(['/dispatcharrpy/bin/celery','-A','periodic_worker_observer:app','worker','--pool=prefork','--concurrency=1','--without-gossip','--without-mingle','--without-heartbeat','--loglevel=WARNING','--hostname=rust-cleanup-expiry@fixture'],env=dict(os.environ,C_FORCE_ROOT='true',PYTHONPATH='/pair-tools:/app'),stdout=log,stderr=log,start_new_session=True)
def stop():
 global worker
 if worker and worker.poll()is None:
  os.killpg(worker.pid,signal.SIGTERM)
  try:worker.wait(timeout=15)
  except subprocess.TimeoutExpired:os.killpg(worker.pid,signal.SIGKILL);worker.wait(timeout=5);report['forced_worker_cleanup']=True
 worker=None
try:
 for name,obj in [('maybe_expire',Request.maybe_expire),('revoked',Request.revoked),('announce_revoked',Request._announce_revoked),('mark_as_revoked',Backend.mark_as_revoked)]:
  data=inspect.getsource(obj).encode();Path('/evidence/original-expiry-'+name+'.py').write_bytes(data)
 PeriodicTask.objects.all().delete();PeriodicTasks.update_changed();scheduler=DatabaseScheduler(app=app,lazy=False,max_interval=5)
 entry=scheduler.schedule['celery.backend_cleanup'];row=PeriodicTask.objects.get(name='celery.backend_cleanup')
 check('real-default-cleanup-expires43200',entry.task=='celery.backend_cleanup'and entry.options.get('expires')==43200 and row.expire_seconds==43200)
 report['default_entry']={'task':entry.task,'options':entry.options,'expire_seconds':row.expire_seconds,'args':entry.args,'kwargs':entry.kwargs}
 # Real scheduler producer path, deliberately force this one known default due.
 before=timezone.now();beat=scheduler.apply_async(entry,advance=True);after=timezone.now();wire=json.loads(broker.lindex('celery',-1));expires=dt.datetime.fromisoformat(wire['headers']['expires'])
 check('Beat-relative-expiry-header-clock-relation',before+dt.timedelta(seconds=43200)<=expires<=after+dt.timedelta(seconds=43200))
 check('Beat-wire-exact-known-shape',wire['headers']['task']=='celery.backend_cleanup'and wire['headers']['periodic_task_name']=='celery.backend_cleanup'and wire['properties']['expiration']=='43200000'and json.loads(base64.b64decode(wire['body']))[:2]==[[],{}])
 start();wait(lambda:any(x['id']==beat.id for x in rows('postrun')));meta=json.loads(backend.get(app.backend.get_key_for_task(beat.id)))
 check('Beat-unexpired-SUCCESS-null',meta['status']=='SUCCESS'and meta['result']is None and beat.get(timeout=2)is None)
 check('Beat-unexpired-expiry-ttl',3580<=backend.ttl(app.backend.get_key_for_task(beat.id))<=3600)
 report['cases'].append({'name':'Beat-default-unexpired','envelope':wire,'metadata':meta})
 # Stop intake before every raw queue witness; no observer-consumption race.
 stop()
 for label,old_success in [('expired',False),('expired-oldSUCCESS',True)]:
  ident=str(uuid.uuid4());key=app.backend.get_key_for_task(ident);initial=None;old_ttl=None
  if old_success:
   seeded=app.tasks['celery.backend_cleanup'].apply_async(args=[],kwargs={},task_id=ident,expires=43200);start();wait(lambda:any(x['id']==ident for x in rows('postrun')));stop();initial=backend.get(key);time.sleep(1.1);old_ttl=backend.ttl(key)
  result=app.tasks['celery.backend_cleanup'].apply_async(args=[],kwargs={},task_id=ident,expires=timezone.now()-dt.timedelta(seconds=60));wire=json.loads(broker.lindex('celery',-1))
  check(label+'-actual-expired-wire',wire['headers']['expires']is not None and wire['properties']['expiration']=='0')
  start();revoked=wait(lambda:next((x for x in rows('revoked')if x['id']==ident),None));wait(lambda:broker.llen('celery')==0 and broker.hlen('unacked')==0);raw=backend.get(key);meta=json.loads(raw)
  check(label+'-no-task-execution',sum(x['id']==ident for x in rows('prerun'))==int(old_success) and sum(x['id']==ident for x in rows('postrun'))==int(old_success))
  check(label+'-actual-expired-revocation',revoked['expired']is True and not revoked['terminated'])
  check(label+'-ACK-and-result-TTL',3580<=backend.ttl(key)<=3600 and broker.hlen('unacked')==broker.llen('celery')==0)
  if old_success:check(label+'-oldSUCCESS-bytes-TTL-preserved',raw==initial and backend.ttl(key)<=old_ttl)
  else:
   check(label+'-typed-REVOKED',meta['status']=='REVOKED'and meta['result']=={'exc_type':'TaskRevokedError','exc_message':['expired'],'exc_module':'celery.exceptions'}and meta['traceback']is None)
   check(label+'-AsyncResult-ready-failed-successful',result.ready()and not result.failed()and not result.successful()and isinstance(result.get(timeout=2,propagate=False),TaskRevokedError))
   try:result.get(timeout=2);raise AssertionError('expected revoked')
   except TaskRevokedError as error:check(label+'-get-raises-exact-args',error.args==('expired',))
  report['cases'].append({'name':label,'envelope':wire,'metadata':meta,'revoked_signal':revoked,'ready':result.ready(),'failed':result.failed(),'successful':result.successful()})
  # Fresh delivery of the same UUID in the same worker: actual revoked-set state.
  fresh=app.tasks['celery.backend_cleanup'].apply_async(args=[],kwargs={},task_id=ident,expires=43200)
  wait(lambda:sum(x['id']==ident for x in rows('revoked'))==2);fresh_meta=json.loads(backend.get(key));check(label+'-fresh-sameUUID-still-no-execution',sum(x['id']==ident for x in rows('prerun'))==int(old_success))
  if old_success:check(label+'-fresh-oldSUCCESS-guard',backend.get(key)==initial)
  else:check(label+'-fresh-revoked-reason',fresh_meta['status']=='REVOKED'and fresh_meta['result']['exc_message']==['revoked'])
  report['cases'].append({'name':label+'-fresh-sameUUID','metadata':fresh_meta,'revoked_signals':[x for x in rows('revoked')if x['id']==ident]})
  stop()
  # Fresh worker has no in-memory revoked UUID (no statedb fixture option).
  restart=app.tasks['celery.backend_cleanup'].apply_async(args=[],kwargs={},task_id=ident,expires=43200);restart_wire=json.loads(broker.lindex('celery',-1));start();wait(lambda:sum(x['id']==ident for x in rows('postrun'))==int(old_success)+1);restart_raw=backend.get(key);restart_meta=json.loads(restart_raw)
  check(label+'-restart-executes-unexpired-sameUUID',sum(x['id']==ident for x in rows('prerun'))==int(old_success)+1 and restart_meta['status']=='SUCCESS'and restart_meta['result']is None)
  if old_success:check(label+'-restart-oldSUCCESS-guard',restart_raw==initial)
  report['cases'].append({'name':label+'-fresh-worker','envelope':restart_wire,'metadata':restart_meta,'scope':'Actual worker restart with no statedb; not production persistence assertion'})
  stop()
 # Real elapsed short TTL: producer unexpired, delivery after expiry, no clock patch.
 delayed=app.tasks['celery.backend_cleanup'].apply_async(args=[],kwargs={},expires=1);delayed_wire=json.loads(broker.lindex('celery',-1));check('short-positive-expiry-wire',delayed_wire['properties']['expiration']=='1000');time.sleep(1.25);start();short_revoked=wait(lambda:next((x for x in rows('revoked')if x['id']==delayed.id),None));short_meta=json.loads(backend.get(app.backend.get_key_for_task(delayed.id)));check('actual-short-delayed-delivery-expired',short_revoked['expired']and short_meta['status']=='REVOKED'and short_meta['result']['exc_message']==['expired']and not any(x['id']==delayed.id for x in rows('prerun')));report['cases'].append({'name':'actual-short-positive-aged-delivery','envelope':delayed_wire,'metadata':short_meta,'revoked_signal':short_revoked});stop()
 check('exact-body-execution-count',len(rows('prerun'))==len(rows('postrun'))==4)
 check('all-owned-queue-reservations-drained',broker.llen('celery')==broker.hlen('unacked')==0)
 # Same regular production paths, first Original producers then actual Original result decoder.
 def native(action,**fields):
  value=subprocess.run(['/evidence/native-default-peer','default_peer::native_default_family_peer','--exact','--ignored','--nocapture'],input=json.dumps({'action':action,**fields}),capture_output=True,text=True,timeout=40)
  with Path('/evidence/native-cleanup-expiry-private.log').open('a')as log:log.write(value.stdout+value.stderr)
  value.check_returncode();lines=[v.split('=',1)[1]for v in value.stdout.splitlines()if v.startswith('NATIVE_DEFAULT_PAIR_JSON=')];assert len(lines)==1
  output=json.loads(lines[0]);check('native-'+action+'-physical-drain',output['drained']and output['jobs_drained']and output['pool_closed']and output['available_slots']==2);check('native-'+action+'-operational-result',output['ok']);return output
 def result_meta(ident,reason=None):
  result=app.AsyncResult(ident);meta=json.loads(backend.get(app.backend.get_key_for_task(ident)))
  if reason:
   check('native-result-typed-revoked-'+reason,meta['status']=='REVOKED'and meta['result']=={'exc_type':'TaskRevokedError','exc_message':[reason],'exc_module':'celery.exceptions'}and result.ready()and not result.failed()and not result.successful())
   caught=result.get(timeout=2,propagate=False);check('Original-decoder-native-revoked-class',type(caught)is TaskRevokedError and caught.args==(reason,))
  else:check('native-result-null-success',meta['status']=='SUCCESS'and result.get(timeout=2)is None and meta['result']is None)
  check('native-result-fields',set(meta)=={'status','result','traceback','children','date_done','task_id'}and meta['children']==[]and meta['traceback']is None and meta['task_id']==ident)
  check('native-result-TTL',3580<=backend.ttl(app.backend.get_key_for_task(ident))<=3600);return meta
 beat2=scheduler.apply_async(entry,advance=True);out=native('run');check('Original-Beat-native-unexpired-body',out['execute_calls']==1 and out['result']['outcome']=='Completed('+beat2.id+')');report['cases'].append({'name':'Original-Beat-native-worker','metadata':result_meta(beat2.id)})
 # Both queued envelopes are consumed by ONE native worker instance.
 ident=str(uuid.uuid4());app.tasks['celery.backend_cleanup'].apply_async(task_id=ident,args=[],kwargs={},expires=timezone.now()-dt.timedelta(seconds=60));app.tasks['celery.backend_cleanup'].apply_async(task_id=ident,args=[],kwargs={},expires=43200);out=native('run_many',count=2)
 check('native-expired-and-revoked-same-worker-skip-body',out['execute_calls']==0 and [v['metadata']['result']['exc_message']for v in out['result']['outcomes']]==[['expired'],['revoked']]);report['cases'].append({'name':'Original-pair-native-single-worker','metadata':result_meta(ident,'revoked'),'native':out})
 app.tasks['celery.backend_cleanup'].apply_async(task_id=ident,args=[],kwargs={},expires=43200);out=native('run');check('fresh-native-worker-executes-after-local-cache-reset',out['execute_calls']==1 and out['result']['outcome']=='Completed('+ident+')');result_meta(ident)
 # Actual Original-worker SUCCESS seeds the old result guard, never a factory result.
 ident=str(uuid.uuid4());seed=app.tasks['celery.backend_cleanup'].apply_async(task_id=ident,args=[],kwargs={});start();wait(lambda:any(x['id']==ident for x in rows('postrun')));stop();key=app.backend.get_key_for_task(ident);old=backend.get(key);time.sleep(1.1);aged_ttl=backend.ttl(key)
 app.tasks['celery.backend_cleanup'].apply_async(task_id=ident,args=[],kwargs={},expires=timezone.now()-dt.timedelta(seconds=60));app.tasks['celery.backend_cleanup'].apply_async(task_id=ident,args=[],kwargs={},expires=43200);out=native('run_many',count=2)
 check('native-revocation-oldSUCCESS-guard-and-no-body',out['execute_calls']==0 and all(x['outcome']=='AlreadySucceeded('+ident+')'for x in out['result']['outcomes'])and backend.get(key)==old and backend.ttl(key)<=aged_ttl)
 app.tasks['celery.backend_cleanup'].apply_async(task_id=ident,args=[],kwargs={},expires=43200);out=native('run');check('fresh-native-worker-executes-oldSUCCESS-byte-TTL-guard',out['execute_calls']==1 and out['result']['outcome']=='AlreadySucceeded('+ident+')'and backend.get(key)==old and backend.ttl(key)<=aged_ttl)
 short=app.tasks['celery.backend_cleanup'].apply_async(args=[],kwargs={},expires=1);time.sleep(1.25);out=native('run');check('native-real-aged-delivery-skips-body',out['execute_calls']==0 and out['result']['outcome']=='Revoked('+short.id+',expired)');report['cases'].append({'name':'Original-aged-native-worker','metadata':result_meta(short.id,'expired')})
 # Native publication, actual Original worker and its AsyncResult decoding.
 for label,seconds in [('future',43200),('past',-60),('short-aged',1)]:
  ident=str(uuid.uuid4());out=native('publish',kind='cleanup',task_id=ident,expiry_seconds=seconds);wire=json.loads(broker.lindex('celery',-1));check('native-'+label+'-cleanup-only-wire',wire['headers']['task']=='celery.backend_cleanup'and wire['properties']['expiration']==str(max(seconds,0)*1000))
  if seconds==1:time.sleep(1.25)
  start()
  if seconds==43200:wait(lambda:any(x['id']==ident for x in rows('postrun')));meta=result_meta(ident)
  else:wait(lambda:any(x['id']==ident for x in rows('revoked')));meta=result_meta(ident,'expired');check('native-published-expired-Original-body-skipped',not any(x['id']==ident for x in rows('prerun')))
  report['cases'].append({'name':'native-publisher-Original-'+label,'envelope':wire,'metadata':meta});stop()
 check('paired-cleanup-final-queue-unacked-empty',broker.llen('celery')==broker.hlen('unacked')==0)
 report['native_verified']=True
 report['passed']=True
finally:
 stop()
 if scheduler is not None:scheduler.close()
 broker.flushdb();backend.flushdb();report['broker_and_result_after_cleanup']=[broker.dbsize(),backend.dbsize()]
 Path('/evidence/source-native-default-pair-private.json').write_text(json.dumps(report,indent=2,default=str)+'\n')
