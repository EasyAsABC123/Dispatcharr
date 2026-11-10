"""Native ETA reservation/timer/warm recovery against pinned actual Source.
Owned copiedPG+broker8/result9 only; no apphook/202/native visibility-loop claim.
Original Dispatcharr/Celery/Kombu authors retain task/transport credit.
"""
import datetime as dt,hashlib,json,os,signal,subprocess,sys,time
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
root=Path('/evidence/eta-native-files');root.mkdir(exist_ok=True);path=root/'guide.xml';guide=b'<tv><programme channel="fixture&amp;one" start="20260101000000 +0000"><title>Owned</title></programme></tv>';path.write_bytes(guide)
source=EPGSource.objects.create(name='rust-eta-native-owned',source_type='xmltv',is_active=False,file_path=str(path));build_programme_index(source.id);reference=EPGSourceIndex.objects.get(source_id=source.id).data;lock='task_lock_epg_source_file_'+str(source.id)
checks=[];processes=[];worker=None;report={'passed':False,'scope':'Native index ETA reservation/timer/warm/hardloss and Original transport/worker interop; no appdriver/202','checks':checks,'actual_visibility_expiry_observed':False,'original_restore_polling_cadence_verified':False}
def check(name,passed,**detail):
 checks.append({'name':name,'passed':bool(passed),**detail})
 if not passed:raise AssertionError(name)
def wait(fn,seconds=30):
 end=time.monotonic()+seconds
 while time.monotonic()<end:
  value=fn()
  if value:return value
  time.sleep(.02)
 raise TimeoutError('Owned ETA observation deadline')
def meta(ident):
 raw=backend.get(app.backend.get_key_for_task(ident));return json.loads(raw)if raw else None
def spawn(action,**fields):
 body={'action':action,'broker_url':os.environ['CELERY_BROKER_URL'],'result_url':os.environ['CELERY_RESULT_BACKEND'],**fields};log=open('/evidence/native-eta-private.log','ab')
 p=subprocess.Popen(['/evidence/native-eta-worker'],stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=log,text=True,start_new_session=True);processes.append(p);p.stdin.write(json.dumps(body));p.stdin.close();return p
def finish(p):
 p.wait(timeout=30);raw=p.stdout.read()
 check('native-eta-process-success',p.returncode==0)
 return json.loads(raw.splitlines()[-1])
def issue(seconds):
 result=task.apply_async(args=[source.id],countdown=seconds);raw=broker.lindex('celery',0);return result.id,json.loads(raw)
def reservation(ident):
 for tag,raw in broker.hgetall('unacked').items():
  if len(raw)>131072:raise RuntimeError('Bounded reservation witness')
  value=json.loads(raw)
  if value[0]['headers']['id']==ident:return tag,value
 return None
def forced_original_restore(tag):
 broker.zadd('unacked_index',{tag:time.time()-3605})
 with app.connection_for_read()as connection:connection.channel().qos.restore_visible(start=0,num=16,interval=1)
def start_original_worker():
 global worker
 log=open('/evidence/original-eta-worker-private.log','ab');worker=subprocess.Popen(['/dispatcharrpy/bin/celery','-A','worker_tasks:app','worker','--pool=prefork','--concurrency=1','--without-gossip','--without-mingle','--without-heartbeat','--loglevel=WARNING','--hostname=rust-native-eta-source@fixture'],env=dict(os.environ,C_FORCE_ROOT='true',PYTHONPATH='/job-tools:/app'),stdout=log,stderr=log,start_new_session=True);wait(lambda:broker.exists('_kombu.binding.celery.pidbox'))
def stop_original_worker():
 global worker
 if worker is not None and worker.poll()is None:
  os.killpg(worker.pid,signal.SIGTERM)
  try:worker.wait(timeout=10)
  except subprocess.TimeoutExpired:os.killpg(worker.pid,signal.SIGKILL);worker.wait(timeout=5)
 worker=None
try:
 # Future task remains unacked/PENDING until due; actual native work starts late.
 ident,envelope=issue(3);eta=dt.datetime.fromisoformat(envelope['headers']['eta']);marker=Path('/evidence/native-eta-execute');p=spawn('execute',marker=marker.name);wait(marker.exists)
 tag,stored=reservation(ident)
 check('native-reserves-source-exact-tuple',stored==[envelope,'','celery']and tag.decode()==envelope['properties']['delivery_tag'])
 check('native-reserved-key-geometry',broker.type('unacked')==b'hash'and broker.type('unacked_index')==b'zset'and broker.ttl('unacked')==-1 and broker.ttl('unacked_index')==-1 and abs(broker.zscore('unacked_index',tag)-time.time())<2)
 check('eta-pending-before-due',dt.datetime.now(dt.UTC)<eta and meta(ident)is None and broker.llen('celery')==0)
 finish(p);record=meta(ident);updated=EPGSourceIndex.objects.get(source_id=source.id).updated_at
 check('native-executes-source-eta-at-or-after-due',updated>=eta and record['status']=='SUCCESS'and record['result']is None and EPGSourceIndex.objects.get(source_id=source.id).data==reference)
 check('native-eta-ack-removes-only-reservation',not broker.exists('unacked')and not broker.exists('unacked_index')and not cache.exists(lock))
 # Source119 warm order: reserve A0..A7, preserve queued C,D, RPUSH restores
 # arrival order and thus consumption order A7..A0,C,D. No tasks execute.
 labels=[]
 for _ in range(8):labels.append(issue(180)[0])
 tails=[task.delay(source.id+10000).id,task.delay(source.id+10001).id]
 warm=finish(spawn('warm',count=8));queue=[json.loads(raw)for raw in reversed(broker.lrange('celery',0,-1))]
 check('native-warm8-matches-original-reverse-arrival',warm['restored']==8 and [e['headers']['id']for e in queue]==list(reversed(labels))+tails)
 check('native-warm-redelivery-fields-only',all(e['headers'].get('redelivered')is True and e['properties']['delivery_info'].get('redelivered')is True for e in queue[:8])and all(meta(i)is None for i in labels))
 check('native-warm-no-unacked-or-handler-work',not broker.exists('unacked')and not broker.exists('unacked_index'))
 assert set(e['headers']['id']for e in queue)==set(labels+tails);broker.delete('celery') # Exact owned fixture publications only.
 # Native process exits after successful reservation; Original reads its tuple
 # and restores it through a fixture-aged visibility score, not a one-hour wait.
 ident,envelope=issue(3);reserved=finish(spawn('reserve'));tag,stored=reservation(ident);check('native-producer-exit-reservation-persists',reserved['task_id']==ident and stored==[envelope,'','celery']and meta(ident)is None)
 forced_original_restore(tag);restored=json.loads(broker.lindex('celery',-1));check('original-kombu-restores-native-reservation',restored['headers']['id']==ident and restored['headers']['redelivered']is True and restored['properties']['delivery_info']['redelivered']is True)
 finish(spawn('execute'));check('native-consumes-original-restored-envelope',AsyncResult(ident,app=app).get(timeout=5)is None and meta(ident)['status']=='SUCCESS')
 # Hard loss while pending ETA leaves the same durable Source reservation.
 ident,envelope=issue(4);marker=Path('/evidence/native-eta-hardloss');p=spawn('hold',marker=marker.name);wait(marker.exists);tag,stored=reservation(ident);os.killpg(p.pid,signal.SIGKILL);p.wait(timeout=5)
 check('native-hardloss-before-due-keeps-unacked',reservation(ident)is not None and meta(ident)is None and not cache.exists(lock))
 forced_original_restore(tag);finish(spawn('execute'));check('native-restart-forced-visible-restores-and-executes',AsyncResult(ident,app=app).get(timeout=5)is None and not broker.exists('unacked'))
 # Original worker consumes a native warm-restored redelivery without mutation.
 ident,envelope=issue(5);finish(spawn('warm',count=1));start_original_worker();wait(lambda:meta(ident));stop_original_worker()
 check('original-worker-consumes-native-warm-restore',AsyncResult(ident,app=app).get(timeout=5)is None and meta(ident)['status']=='SUCCESS'and EPGSourceIndex.objects.get(source_id=source.id).data==reference)
 report.update(passed=all(v['passed']for v in checks),native_eta_fixture_verified=True,native_application_driver_verified=False,source_restore_helper_forced_age_seconds=3605)
finally:
 stop_original_worker()
 for p in processes:
  if p.poll()is None:
   os.killpg(p.pid,signal.SIGTERM)
   try:p.wait(timeout=5)
   except subprocess.TimeoutExpired:os.killpg(p.pid,signal.SIGKILL);p.wait(timeout=5)
 Path('/evidence/native-eta-worker-private.json').write_text(json.dumps(report,indent=2)+'\n')
