"""Actual original index producer/worker with native immediate IndexService execution.
Copied database and broker8/result9 only; no app202/generalETA/native scheduler.
Original Dispatcharr authors retain full application/algorithm credit.
"""
import base64,datetime as dt,hashlib,json,os,signal,subprocess,sys,time
from pathlib import Path
if os.environ.get('JOB_DISPOSABLE_FIXTURE')!='playback-fixture-only'or os.environ.get('POSTGRES_PASSWORD')!='playback-fixture-only':raise RuntimeError('Owned fixture required')
db=os.environ.get('POSTGRES_DB','')
if not db.startswith('rust_probe_backup_')or len(db)>63 or not all(c in 'abcdefghijklmnopqrstuvwxyz0123456789_'for c in db):raise RuntimeError('Copied DB required')
sys.path[:0]=['/app','/job-tools'];sys.argv.append('shell');os.environ.setdefault('DJANGO_SETTINGS_MODULE','dispatcharr.settings')
import django;django.setup()
import redis
from dispatcharr.celery import app
from celery.result import AsyncResult
from apps.epg.models import EPGSource,EPGSourceIndex
from apps.epg.tasks import build_programme_index_task as task,build_programme_index
broker=redis.Redis.from_url(os.environ['CELERY_BROKER_URL']);backend=redis.Redis.from_url(os.environ['CELERY_RESULT_BACKEND']);cache=redis.Redis(host='localhost',port=6379,password='playback-fixture-only',db=0)
assert broker.connection_pool.connection_kwargs['db']==8 and backend.connection_pool.connection_kwargs['db']==9
broker.flushdb();backend.flushdb()
for key in ['dvr:recover_dispatch_lock','core:version_check_dispatch_lock']:cache.set(key,'1',ex=3600)
root=Path('/evidence/index-native-files');root.mkdir(exist_ok=True);path=root/'guide.xml';guide=b'<tv><programme channel="fixture&amp;one" start="20260101000000 +0000"><title>Owned</title></programme></tv>';path.write_bytes(guide)
source=EPGSource.objects.create(name='rust-index-native-owned',source_type='xmltv',is_active=False,file_path=str(path));lock='task_lock_epg_source_file_'+str(source.id)
checks=[];worker=None;report={'passed':False,'scope':'Immediate native IndexService worker plus actual original producer/ETA-child worker/result interoperability; no productiondriver/app202/generalETAconsumer','checks':checks,'source_task':'apps.epg.tasks.build_programme_index_task','guide_sha256':hashlib.sha256(guide).hexdigest()}
def check(name,passed,**detail):
 checks.append({'name':name,'passed':bool(passed),**detail})
 if not passed:raise AssertionError(name)
def wait(fn,seconds=30):
 deadline=time.monotonic()+seconds
 while time.monotonic()<deadline:
  v=fn()
  if v:return v
  time.sleep(.025)
 raise TimeoutError('Owned index observation deadline')
def native(action,**fields):
 request={'action':action,'broker_url':os.environ['CELERY_BROKER_URL'],'result_url':os.environ['CELERY_RESULT_BACKEND'],'production_connector':os.environ.get('INDEX_PRODUCTION_CONNECTOR')=='1',**fields}
 p=subprocess.run(['/evidence/native-index-worker'],input=json.dumps(request),text=True,capture_output=True,timeout=30)
 if p.returncode:
  (Path('/evidence')/'native-index-failure-private.log').write_text(p.stderr[-65536:])
  queued=broker.lrange('celery',0,15)
  if len(queued)>16 or any(len(raw)>65536 for raw in queued):raise RuntimeError('Failed queue capture bound')
  descriptor=os.open('/evidence/failed-index-queue-private.json',os.O_WRONLY|os.O_CREAT|os.O_TRUNC,0o600)
  with os.fdopen(descriptor,'w')as file:json.dump([json.loads(raw)for raw in queued],file)
  p.check_returncode()
 return json.loads(p.stdout.splitlines()[-1])
def meta(ident):
 raw=backend.get(app.backend.get_key_for_task(ident))
 if raw and len(raw)>65536:raise RuntimeError('Metadata cap')
 return json.loads(raw)if raw else None
def start_source_worker():
 global worker
 log=open('/evidence/source-index-worker-private.log','ab')
 worker=subprocess.Popen(['/dispatcharrpy/bin/celery','-A','worker_tasks:app','worker','--pool=prefork','--concurrency=1','--without-gossip','--without-mingle','--without-heartbeat','--loglevel=WARNING','--hostname=rust-index-interop@fixture'],env=dict(os.environ,C_FORCE_ROOT='true',PYTHONPATH='/job-tools:/app'),stdout=log,stderr=log,start_new_session=True)
 wait(lambda:broker.exists('_kombu.binding.celery.pidbox'));time.sleep(.3)
def stop_source_worker():
 global worker
 if worker is not None and worker.poll()is None:
  os.killpg(worker.pid,signal.SIGTERM)
  try:worker.wait(timeout=10)
  except subprocess.TimeoutExpired:os.killpg(worker.pid,signal.SIGKILL);worker.wait(timeout=5)
 worker=None
try:
 import uuid
 report['scope']='Actual Original repeated index task UUID execution with existing SUCCESS backend; no native behavior claim'
 report['worker_deduplicate_successful_tasks']=bool(app.conf.worker_deduplicate_successful_tasks)
 report['task_acks_late']=bool(task.acks_late)
 report['task_track_started']=bool(task.track_started)
 ident=str(uuid.uuid4()); previous_meta=None; previous_ttl=None
 start_source_worker()
 for number,label in enumerate(['duplicate-first','duplicate-same-worker','duplicate-restarted-worker']):
  if number==2:stop_source_worker();start_source_worker()
  path.write_bytes(('<tv><programme channel="'+label+'" start="20260101000000 +0000"><title>Owned</title></programme></tv>').encode())
  if number:
   check('prior-success-present-'+str(number),meta(ident)['status']=='SUCCESS')
   time.sleep(1.1)
  queued=task.apply_async(args=[source.id],task_id=ident)
  check('same-explicit-id-'+str(number),queued.id==ident)
  def changed_index():
   row=EPGSourceIndex.objects.filter(source_id=source.id).first()
   return row and row.data=={'channels':{label:[4]},'interleaved_channels':[]}
  wait(changed_index)
  wait(lambda:meta(ident)and meta(ident)['status']=='SUCCESS')
  wait(lambda:not cache.exists(lock));time.sleep(.1)
  current=meta(ident)
  if number:
   check('existing-success-metadata-preserved-'+str(number),current==previous_meta)
   check('existing-success-ttl-not-refreshed-'+str(number),backend.ttl(app.backend.get_key_for_task(ident))<previous_ttl)
  previous_meta=current;previous_ttl=backend.ttl(app.backend.get_key_for_task(ident))
  check('actual-index-reexecuted-'+str(number),changed_index()and current['result']is None and current['children']==[])
  check('result-ttl-'+str(number),3590<=backend.ttl(app.backend.get_key_for_task(ident))<=3600)
 stop_source_worker()
 if os.environ.get('NATIVE_DUPLICATE_PROBE')=='1':
  for kind,label in [('immediate','duplicate-native-immediate'),('eta','duplicate-native-eta')]:
   time.sleep(1.1)
   path.write_bytes(('<tv><programme channel="'+label+'" start="20260101000000 +0000"><title>Owned</title></programme></tv>').encode())
   if kind=='immediate':
    task.apply_async(args=[source.id],task_id=ident);outcome=native('run')['outcome']
    check('native-immediate-existing-success-outcome',outcome.startswith('ResultAlreadySucceeded('))
   else:
    task.apply_async(args=[source.id],task_id=ident,countdown=1)
    request={'action':'execute','broker_url':os.environ['CELERY_BROKER_URL'],'result_url':os.environ['CELERY_RESULT_BACKEND']}
    run=subprocess.run(['/evidence/native-eta-worker'],input=json.dumps(request),text=True,capture_output=True,timeout=30)
    if run.returncode:(Path('/evidence')/'native-duplicate-failure-private.log').write_text(run.stderr[-65536:])
    run.check_returncode();outcome=json.loads(run.stdout.splitlines()[-1])['outcome']
    check('native-eta-existing-success-outcome',outcome.startswith('AlreadySucceeded('))
   row=EPGSourceIndex.objects.get(source_id=source.id)
   check('native-'+kind+'-reexecutes-actual-index',row.data=={'channels':{label:[4]},'interleaved_channels':[]})
   check('native-'+kind+'-preserves-success-metadata',meta(ident)==previous_meta)
   ttl=backend.ttl(app.backend.get_key_for_task(ident));check('native-'+kind+'-preserves-decreasing-ttl',ttl<previous_ttl);previous_ttl=ttl
  report['native_repeated_id_behavior_verified']=True
 check('queue-reservations-and-lock-drained',broker.llen('celery')==0 and broker.hlen('unacked')==0 and broker.zcard('unacked_index')==0 and not cache.exists(lock))
 report.update(passed=all(x['passed']for x in checks),native_worker_behavior_verified=os.environ.get('NATIVE_DUPLICATE_PROBE')=='1')
finally:
 stop_source_worker()
 descriptor=os.open('/evidence/source-index-duplicate-private.json',os.O_WRONLY|os.O_CREAT|os.O_TRUNC,0o600)
 with os.fdopen(descriptor,'w')as file:json.dump(report,file,indent=2);file.write('\n')
