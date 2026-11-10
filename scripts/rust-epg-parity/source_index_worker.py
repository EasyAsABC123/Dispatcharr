"""Owned original-worker index contract. Original Dispatcharr authors retain credit."""
import base64,datetime as dt,hashlib,json,os,signal,subprocess,sys,time
from pathlib import Path
if os.environ.get('JOB_DISPOSABLE_FIXTURE')!='playback-fixture-only' or not os.environ.get('POSTGRES_DB','').startswith('rust_probe_backup_'):raise RuntimeError('Copied fixture required')
sys.path[:0]=['/app','/index-tools'];sys.argv.append('shell');os.environ.setdefault('DJANGO_SETTINGS_MODULE','dispatcharr.settings')
import django;django.setup()
import redis
from dispatcharr.celery import app
from apps.epg.models import EPGSource,EPGSourceIndex
from apps.epg.tasks import build_programme_index_task as task
broker=redis.Redis.from_url(os.environ['CELERY_BROKER_URL']);backend=redis.Redis.from_url(os.environ['CELERY_RESULT_BACKEND']);cache=redis.Redis(host='localhost',password='playback-fixture-only',db=0)
if broker.connection_pool.connection_kwargs['db']!=8 or backend.connection_pool.connection_kwargs['db']!=9:raise RuntimeError('Isolated broker/result required')
broker.flushdb();backend.flushdb()
for key in ['dvr:recover_dispatch_lock','core:version_check_dispatch_lock']:cache.set(key,'1',ex=3600)
root=Path('/evidence/index-files');root.mkdir(exist_ok=True);guide=b'<tv><programme channel="fixture&amp;one" start="20260101000000 +0000"><title>Owned</title></programme></tv>'
path=root/'guide.xml';path.write_bytes(guide)
source=EPGSource.objects.create(name='rust-index-worker-owned',source_type='xmltv',is_active=False,file_path=str(path))
lock='task_lock_epg_source_file_'+str(source.id);marker=b'rust-index-worker-owned';worker=None;writer=None
report={'passed':False,'scope':'Original-only real index producer/worker; no native consumer or production dispatcher','checks':[],'captures':{},'configuration':{key:app.conf.get(key)for key in ['task_protocol','task_default_queue','task_default_exchange','task_default_routing_key','worker_prefetch_multiplier','broker_transport_options','result_backend_transport_options','result_expires']},'source_id':source.id,'guide_sha256':hashlib.sha256(guide).hexdigest(),'cwd':os.getcwd(),'media_root':str(__import__('django.conf',fromlist=['settings']).settings.MEDIA_ROOT)}
def check(name,passed,**detail):
 report['checks'].append({'name':name,'passed':bool(passed),**detail})
 if not passed:raise AssertionError(name)
def wait(fn,timeout=30):
 deadline=time.monotonic()+timeout
 while time.monotonic()<deadline:
  value=fn()
  if value:return value
  time.sleep(.025)
 raise TimeoutError('Owned index observation deadline')
def rows(name):
 p=Path('/evidence')/name
 return [json.loads(line)for line in p.read_text().splitlines()]if p.exists()else[]
def result(ident):
 raw=backend.get(app.backend.get_key_for_task(ident))
 if raw and len(raw)>65536:raise RuntimeError('Result cap')
 return json.loads(raw)if raw else None
def finished(ident):return next((row for row in rows('index-postrun.jsonl')if row['id']==ident),None)
def reservations():
 values=broker.hvals('unacked')
 if len(values)>64:raise RuntimeError('Reservation count cap')
 result=[]
 for raw in values:
  if len(raw)>131072:raise RuntimeError('Reservation byte cap')
  result.append(json.loads(raw)[0])
 return result
def publish(source_id,retry=None):
 args=['/dispatcharrpy/bin/python','/index-tools/index_worker_publish.py','--source',str(source_id)]
 if retry is not None:args+=['--retry',str(retry)]
 process=subprocess.run(args,text=True,capture_output=True,timeout=30);process.check_returncode()
 ident=json.loads(process.stdout.splitlines()[-1])['id'];queued=broker.lrange('celery',0,63)
 envelope=next(json.loads(raw)for raw in queued if json.loads(raw)['headers']['id']==ident)
 if len(json.dumps(envelope))>65536:raise RuntimeError('Envelope cap')
 report['captures'][ident]={'envelope':envelope,'payload':json.loads(base64.b64decode(envelope['body'])),'producer_exit':process.returncode}
 return ident
def quarantine():
 values=broker.lrange('celery',0,64)
 if len(values)>64:raise RuntimeError('Queue cap')
 for raw in values:
  item=json.loads(raw)
  if item['headers']['task']!=task.name:
   report.setdefault('quarantined',[]).append({'name':item['headers']['task'],'sha256':hashlib.sha256(raw).hexdigest()});broker.lrem('celery',0,raw)
def launch():
 global worker
 quarantine();broker.delete('_kombu.binding.celery.pidbox')
 with open('/evidence/index-worker-private.log','ab')as log:
  worker=subprocess.Popen(['/dispatcharrpy/bin/celery','-A','index_worker_observer:app','worker','--pool=prefork','--concurrency=1','--without-gossip','--without-mingle','--without-heartbeat','--loglevel=WARNING','--hostname=rust-index-probe@fixture'],env=dict(os.environ,C_FORCE_ROOT='true',PYTHONPATH='/index-tools:/app'),stdout=log,stderr=log,start_new_session=True)
 wait(lambda:broker.exists('_kombu.binding.celery.pidbox'))
def stop(hard=False):
 global worker
 if worker is not None and worker.poll()is None:
  os.killpg(worker.pid,signal.SIGKILL if hard else signal.SIGTERM)
  try:worker.wait(timeout=15)
  except subprocess.TimeoutExpired:
   os.killpg(worker.pid,signal.SIGKILL);worker.wait(timeout=5);report.setdefault('forced_cleanup',[]).append(worker.pid)
 worker=None
try:
 inspection=subprocess.run(['/dispatcharrpy/bin/python','/index-tools/index_worker_publish.py','--inspect','--source',str(source.id)],text=True,capture_output=True,timeout=30);inspection.check_returncode();report['task']=json.loads(inspection.stdout.splitlines()[-1])
 cache.set(lock,marker,ex=300);parent=publish(source.id)
 # A fresh producer process inspects the task without consuming its queued work.
 restart=subprocess.run(['/dispatcharrpy/bin/python','/index-tools/index_worker_publish.py','--inspect'],text=True,capture_output=True,timeout=30);restart.check_returncode()
 check('producer-exit-restart-keeps-publication',any(json.loads(raw)['headers']['id']==parent for raw in broker.lrange('celery',0,63)))
 launch();wait(lambda:finished(parent));childrow=wait(lambda:next((r for r in rows('index-child-envelopes.jsonl')if r.get('observed')),None));child=childrow['id'];stop()
 child_envelope=childrow['observed']['envelope'];payload=json.loads(base64.b64decode(child_envelope['body']));headers=child_envelope['headers']
 check('busy-file-new-child-not-retry',child!=parent and payload[1]=={'_defer_retry':1} and headers['retries']==0 and headers['root_id']==parent and headers['parent_id']==parent,envelope=child_envelope,parent_result=result(parent))
 eta=dt.datetime.fromisoformat(headers['eta']);published=dt.datetime.fromisoformat(childrow['utc']);check('busy-child-countdown-five',4<=(eta-published).total_seconds()<=5.5,seconds=(eta-published).total_seconds())
 check('graceful-worker-restores-eta',any(json.loads(raw)['headers']['id']==child for raw in broker.lrange('celery',0,63)))
 launch();wait(lambda:finished(child),20);second=wait(lambda:next((r for r in rows('index-child-envelopes.jsonl')if r['id']!=child and r.get('observed')),None));stop();grandchild=second['id'];h=second['observed']['envelope']['headers']
 check('restarted-worker-retains-lineage',h['parent_id']==child and h['root_id']==parent and json.loads(base64.b64decode(second['observed']['envelope']['body']))[1]=={'_defer_retry':2},parent_result=result(parent),child_result=result(child))
 check('fixture-still-owns-file-lock',cache.get(lock)==marker);cache.delete(lock);launch();wait(lambda:finished(grandchild),20)
 index=EPGSourceIndex.objects.get(source_id=source.id);check('actual-index-inactive-source-built',index.data=={'channels':{'fixture&one':[4]},'interleaved_channels':[]} and not cache.exists(lock),result=result(grandchild),index=index.data)
 stop()
 # FIFO is owned observation apparatus: block unchanged Source file I/O after
 # task_prerun, then inspect this exact task's broker ACK before writing bytes.
 fifo=root/'running.xml';os.mkfifo(fifo);source.file_path=str(fifo);source.save(update_fields=['file_path']);active=publish(source.id);launch()
 def open_writer():
  try:return os.open(fifo,os.O_WRONLY|os.O_NONBLOCK)
  except OSError:return None
 writer=wait(open_writer);wait(lambda:any(row['id']==active for row in rows('index-prerun.jsonl')))
 check('running-specific-task-early-acked',not any(item['headers']['id']==active for item in reservations()) and result(active)is None,lock_ttl=cache.ttl(lock),task_result=result(active))
 os.write(writer,guide);os.close(writer);writer=None;wait(lambda:finished(active));check('blocked-source-resumes-and-releases',not cache.exists(lock),result=result(active));stop()
 # Hard process-group loss is intentionally distinct from graceful ETA restore.
 lost=publish(source.id);launch();writer=wait(open_writer);wait(lambda:any(row['id']==lost for row in rows('index-prerun.jsonl')))
 check('hard-loss-task-already-acked',not any(item['headers']['id']==lost for item in reservations()) and result(lost)is None)
 stop(hard=True);os.close(writer);writer=None
 check('hard-loss-leaves-original-file-lease',cache.get(lock)==b'locked',ttl=cache.ttl(lock))
 # No retry/lease-expiry is manufactured. A replacement worker receives a fresh
 # missing-file task while the lost original task remains absent from broker.
 missing=root/'absent.xml';source.file_path=str(missing);source.save(update_fields=['file_path'])
 other=EPGSource.objects.create(name='rust-index-worker-missing',source_type='xmltv',file_path=str(missing))
 replacement=publish(other.id);launch();wait(lambda:finished(replacement));time.sleep(1)
 check('hard-restart-no-immediate-replay',result(lost)is None and not finished(lost) and not any(item['headers']['id']==lost for item in reservations()) and not any(json.loads(raw)['headers']['id']==lost for raw in broker.lrange('celery',0,63)),observation_seconds=1,remaining_original_lock_ttl=cache.ttl(lock))
 check('replacement-worker-missing-file-normal-result',finished(replacement)['state']=='SUCCESS' and result(replacement)['result']is None and not EPGSourceIndex.objects.filter(source_id=other.id).exists())
 stop()
 # Exact known synthetic source key after its entire original worker group is
 # dead; deliberate fixture reset, not a claimed Source release/fencing fix.
 check('hard-loss-cleanup-key-still-original',cache.get(lock)==b'locked');cache.delete(lock)
 missing_source=publish(1000000+source.id);launch();wait(lambda:finished(missing_source));stop()
 check('missing-source-normal-result',finished(missing_source)['state']=='SUCCESS' and result(missing_source)['result']is None)
 # Exhaustion is an ordinary null return, with no new scheduled child.
 source.file_path=str(path);source.save(update_fields=['file_path']);cache.set(lock,marker,ex=300);before=len(rows('index-child-envelopes.jsonl'));exhausted=publish(source.id,480);launch();wait(lambda:finished(exhausted));stop();check('retry-limit-normal-return-no-child',len(rows('index-child-envelopes.jsonl'))==before and finished(exhausted)['state']=='SUCCESS',result=result(exhausted))
 check('no-unrelated-task-executed',not rows('index-unexpected-tasks.jsonl'))
 report.update(passed=True,worker_started=True,native_worker_verified=False,hard_worker_restart_verified=True,visibility_expiry_verified=False)
finally:
 if writer is not None:os.close(writer)
 stop()
 if cache.get(lock)==marker:cache.delete(lock)
 report['observer']={name:rows(name)for name in ['index-prerun.jsonl','index-postrun.jsonl','index-child-envelopes.jsonl','index-unexpected-tasks.jsonl']}
 Path('/evidence/source-index-worker-private.json').write_text(json.dumps(report,indent=2,default=str))
