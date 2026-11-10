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
 request={'action':action,'broker_url':os.environ['CELERY_BROKER_URL'],'result_url':os.environ['CELERY_RESULT_BACKEND'],**fields}
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
 # Obtain the exact original SQL/index reference without changing its algorithm.
 build_programme_index(source.id);reference=EPGSourceIndex.objects.get(source_id=source.id).data;EPGSourceIndex.objects.filter(source_id=source.id).delete()
 check('original-reference-index-nonempty',reference=={'channels':{'fixture&one':[4]},'interleaved_channels':[]})
 queued=task.delay(source.id);check('source-producer-queued-before-native',broker.llen('celery')==1 and meta(queued.id)is None)
 native('run');value=AsyncResult(queued.id,app=app).get(timeout=5)
 check('actual-native-index-data-matches-original',EPGSourceIndex.objects.get(source_id=source.id).data==reference and not cache.exists(lock))
 check('native-index-normal-success-null',value is None and meta(queued.id)['status']=='SUCCESS'and meta(queued.id)['children']==[] and 3590<=backend.ttl(app.backend.get_key_for_task(queued.id))<=3600)
 read=native('read',task_id=queued.id);check('native-bounded-source-index-result-reader',read=={'missing':False,'state':'SUCCESS','result':None,'parent_id':None,'children':[]})
 # Missing source/file normal outcomes are null, never backup result dictionaries.
 for label,source_id in [('missing-source',source.id+99999),('missing-file',EPGSource.objects.create(name='rust-index-native-missing',source_type='xmltv',is_active=False,file_path=str(root/'absent.xml')).id)]:
  issued=task.delay(source_id);native('run');check('native-'+label+'-normal-null',AsyncResult(issued.id,app=app).get(timeout=5)is None and meta(issued.id)['status']=='SUCCESS')
 # Native publication remains committed after its short-lived producer exits.
 published=native('publish',source_id=source.id+99998);check('native-index-producer-exit-retains-message',broker.llen('celery')==1 and meta(published['task_id'])is None)
 start_source_worker();wait(lambda:meta(published['task_id']));stop_source_worker();check('original-worker-consumes-native-index-publication',AsyncResult(published['task_id'],app=app).get(timeout=5)is None)
 # A busy native execution must durably enqueue its Source5s child before
 # normal parent SUCCESS; preserve root/parent and Source children tuple.
 cache.set(lock,b'owned-index-native-busy',ex=300);parent=task.delay(source.id);before=dt.datetime.now(dt.UTC);native('run');after=dt.datetime.now(dt.UTC)
 parent_meta=meta(parent.id);children=AsyncResult(parent.id,app=app).children;check('busy-parent-null-with-real-child',parent_meta['status']=='SUCCESS'and parent_meta['result']is None and len(children)==1)
 child=children[0].id;raw=broker.lindex('celery',-1);envelope=json.loads(raw);payload=json.loads(base64.b64decode(envelope['body']));h=envelope['headers'];eta=dt.datetime.fromisoformat(h['eta'])
 check('native-busy-child-source-schema-lineage',h['id']==child and h['task']==task.name and h['root_id']==parent.id and h['parent_id']==parent.id and h['retries']==0 and h['argsrepr']=='['+str(source.id)+']'and h['kwargsrepr']=="{'_defer_retry': 1}"and payload==[[source.id],{'_defer_retry':1},{'callbacks':None,'errbacks':None,'chain':None,'chord':None}])
 check('native-busy-countdown-five',(eta-before).total_seconds()>=4 and(eta-after).total_seconds()<=5.5)
 check('native-reader-preserves-source-child-tuple',native('read',task_id=parent.id)['children']==[child])
 # This first consumer deliberately rejects all ETA/parented heads; it does not
 # pretend to implement Celery's in-memory ETA reservation/recovery semantics.
 request={'action':'run','broker_url':os.environ['CELERY_BROKER_URL'],'result_url':os.environ['CELERY_RESULT_BACKEND']};attempt=subprocess.run(['/evidence/native-index-worker'],input=json.dumps(request),text=True,capture_output=True,timeout=15)
 check('eta-head-explicitly-retained',attempt.returncode!=0 and broker.lindex('celery',-1)==raw and meta(child)is None)
 cache.delete(lock);start_source_worker();wait(lambda:meta(child));stop_source_worker()
 child_meta=meta(child);check('source-worker-executes-native-eta-child',child_meta['status']=='SUCCESS'and child_meta['result']is None and child_meta['parent_id']==parent.id and EPGSourceIndex.objects.get(source_id=source.id).data==reference)
 check('native-reader-source-descendant-parent',native('read',task_id=child)['parent_id']==parent.id)
 # Source's explicit manual defer maximum is independent of retries header3.
 cache.set(lock,b'owned-index-native-max',ex=300);exhausted=task.apply_async(args=[source.id],kwargs={'_defer_retry':480});native('run')
 check('native-defer480-normal-null-no-child',AsyncResult(exhausted.id,app=app).get(timeout=5)is None and meta(exhausted.id)['children']==[]and broker.llen('celery')==0 and cache.get(lock)==b'owned-index-native-max');cache.delete(lock)
 check('native-worker-restart-empty-no-replay',native('run')['outcome']=='Empty')
 report.update(passed=all(x['passed']for x in checks),native_index_fixture_execution_verified=True,native_eta_consumption_verified=False,native_exception_result_writing_verified=False)
finally:
 stop_source_worker();Path('/evidence/native-index-worker-private.json').write_text(json.dumps(report,indent=2)+'\n')
