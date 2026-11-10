"""Original refresh earlyACK + real blocked parser, owned copied fixture only.
Original Dispatcharr/Celery/Kombu authors retain all credit/licenses.
No task/function replacement; non-refresh children are routed but not consumed.
"""
import errno,json,os,signal,subprocess,sys,time
from pathlib import Path
if os.environ.get('JOB_DISPOSABLE_FIXTURE')!='playback-fixture-only':raise RuntimeError('Owned fixture required')
db=os.environ.get('POSTGRES_DB','')
if not db.startswith('rust_probe_backup_jobs_')or len(db)>63 or not all(c in 'abcdefghijklmnopqrstuvwxyz0123456789_'for c in db):raise RuntimeError('Copied database required')
sys.path[:0]=['/app','/job-tools'];sys.argv.append('shell');os.environ.setdefault('DJANGO_SETTINGS_MODULE','dispatcharr.settings')
import django;django.setup()
import redis
from dispatcharr.celery import app
from apps.epg import tasks
from apps.epg.models import EPGSource,EPGSourceIndex
broker=redis.Redis.from_url(os.environ['CELERY_BROKER_URL']);backend=redis.Redis.from_url(os.environ['CELERY_RESULT_BACKEND']);cache=redis.Redis(host='127.0.0.1',password='playback-fixture-only',db=0)
if broker.connection_pool.connection_kwargs['db']!=8 or backend.connection_pool.connection_kwargs['db']!=9:raise RuntimeError('Owned namespaces required')
root=Path('/evidence/refresh-ack-files');root.mkdir();worker=None;owned=[];keys=[];report={'passed':False,'scope':'Actual Original refresh early ACK with owned FIFO parser I/O; graceful finish and hard process-group loss/restart bounded non-replay, Source only','checks':[],'cases':[],'fixture_control':{'child_routes':'non-refresh→owned-refresh-children, unconsumed','input':'owned FIFO .txt real local-file parser','task_or_function_replacement':False,'warm_signal':'SIGTERM to worker main PID only','hard_signal':'SIGKILL to owned entire process group'}}
def rows(name):
 p=Path('/evidence')/name;return [json.loads(v)for v in p.read_text().splitlines()]if p.exists()else[]
def check(name,value):
 report['checks'].append({'name':name,'passed':bool(value)})
 if not value:raise AssertionError(name)
def wait(fn,seconds=20):
 deadline=time.monotonic()+seconds
 while time.monotonic()<deadline:
  value=fn()
  if value:return value
  time.sleep(.025)
 raise TimeoutError('Owned observation deadline')
def stop():
 global worker
 if worker is not None and worker.poll()is None:
  os.killpg(worker.pid,signal.SIGTERM)
  try:worker.wait(timeout=10)
  except subprocess.TimeoutExpired:os.killpg(worker.pid,signal.SIGKILL);worker.wait(timeout=5);report.setdefault('forced_cleanup',[]).append(True)
 worker=None
def launch():
 global worker
 for raw in broker.lrange('celery',0,63):
  if len(raw)>65536 or json.loads(raw)['headers']['task']!=tasks.refresh_epg_data.name:raise RuntimeError('Unrelated consumed task')
 broker.delete('_kombu.binding.celery.pidbox')
 with Path('/evidence/epg-refresh-worker-private.log').open('ab')as log:
  worker=subprocess.Popen(['/dispatcharrpy/bin/celery','-A','epg_refresh_ack_observer:app','worker','-Q','celery','--pool=prefork','--concurrency=1','--without-gossip','--without-mingle','--without-heartbeat','--loglevel=WARNING','--hostname=rust-refresh-ack@fixture'],env=dict(os.environ,C_FORCE_ROOT='true',PYTHONPATH='/job-tools:/app'),stdout=log,stderr=log,start_new_session=True)
 wait(lambda:broker.exists('_kombu.binding.celery.pidbox'))
def source(label,active,path):
 value=EPGSource(name='rust-refresh-ack-'+label,source_type='xmltv',is_active=active,file_path=str(path),custom_properties={});EPGSource.objects.bulk_create([value]);owned.append(value.pk);return value
try:
 broker.flushdb();backend.flushdb()
 for key in ['dvr:recover_dispatch_lock','core:version_check_dispatch_lock']:cache.set(key,'1',ex=3600)
 for label in ['graceful','hardloss']:
  broker.flushdb();backend.flushdb();path=root/(label+'.txt');os.mkfifo(path,0o600);src=source(label,True,path);sid=src.pk;EPGSourceIndex.objects.create(source_id=sid,data={'owned':'before'});refresh='task_lock_refresh_epg_data_'+str(sid);file='task_lock_epg_source_file_'+str(sid);keys.extend([refresh,file]);issued=tasks.refresh_epg_data.delay(sid);raw=next(v for v in broker.lrange('celery',0,63)if json.loads(v)['headers']['id']==issued.id);envelope=json.loads(raw)
  launch();ack=wait(lambda:next((v for v in rows('epg-refresh-ack-prerun.jsonl')if v['id']==issued.id),None));wait(lambda:cache.get(file)==b'locked'and cache.get(refresh)==b'locked');time.sleep(.1);result_key=app.backend.get_key_for_task(issued.id)
  check('real-task-attributes-'+label,ack['acks_late']is False and ack['reject_on_worker_lost']is None and ack['track_started']is False and ack['ignore_result']is False and ack['max_retries']==3 and ack['time_limit']==14400 and ack['soft_time_limit']is None)
  check('early-ack-before-blocked-body-'+label,ack['own_unacked_tags']==[]and broker.hlen('unacked')==0 and broker.llen('celery')==0)
  check('pending-no-started-record-'+label,backend.get(result_key)is None and app.AsyncResult(issued.id).state=='PENDING')
  check('index-invalidated-before-parser-block-'+label,EPGSourceIndex.objects.get(source_id=sid).data is None)
  case={'name':label,'source_id':sid,'envelope':envelope,'ack_observation':ack}
  if label=='graceful':
   worker.send_signal(signal.SIGTERM);time.sleep(.2);check('warmstop-waits-for-real-body',worker.poll()is None and backend.get(result_key)is None)
   deadline=time.monotonic()+5;fd=None
   while time.monotonic()<deadline:
    try:fd=os.open(path,os.O_WRONLY|os.O_NONBLOCK);break
    except OSError as error:
     if error.errno!=errno.ENXIO:raise
     time.sleep(.025)
   if fd is None:raise TimeoutError('FIFO parser reader not observed')
   try:os.write(fd,b'<tv></tv>')
   finally:os.close(fd)
   post=wait(lambda:next((v for v in rows('epg-refresh-postrun.jsonl')if v['id']==issued.id),None));worker.wait(timeout=15);worker=None;meta=json.loads(backend.get(result_key));check('warmstop-completes-success-null',post['state']=='SUCCESS'and meta['status']=='SUCCESS'and meta['result']is None);check('warmstop-releases-both-locks',cache.get(refresh)is None and cache.get(file)is None);check('warmstop-result-ttl',3590<=backend.ttl(result_key)<=3600);case.update(metadata=meta,postrun=post)
  else:
   os.killpg(worker.pid,signal.SIGKILL);worker.wait(timeout=5);worker=None;check('hardloss-no-terminal-record',backend.get(result_key)is None and not any(v['id']==issued.id for v in rows('epg-refresh-postrun.jsonl')));check('hardloss-retains-leases',cache.get(refresh)==b'locked'and cache.get(file)==b'locked'and 280<=cache.ttl(refresh)<=300 and 280<=cache.ttl(file)<=300)
   # A replacement worker executes a fresh inactive task; the earlyACKed old
   # task is not replayed during this explicit one-second bounded witness.
   probe=source('replacement-inactive',False,path);fresh=tasks.refresh_epg_data.delay(probe.pk);launch();wait(lambda:next((v for v in rows('epg-refresh-postrun.jsonl')if v['id']==fresh.id),None));time.sleep(1);stop();fresh_meta=json.loads(backend.get(app.backend.get_key_for_task(fresh.id)));check('replacement-worker-processes-fresh-task',fresh_meta['status']=='SUCCESS'and fresh_meta['result']is None);check('hardloss-bounded-nonreplay',backend.get(result_key)is None and len([v for v in rows('epg-refresh-prerun.jsonl')if v['id']==issued.id])==1 and broker.hlen('unacked')==0 and broker.llen('celery')==0);case.update(replacement_metadata=fresh_meta,remaining_lock_ttl={'refresh':cache.ttl(refresh),'file':cache.ttl(file)},nonreplay_observation_seconds=1,controlled_process_group_sigkill=True)
  report['cases'].append(case)
  for key in [refresh,file]:cache.delete(key)
  path.unlink()
 check('no-child-family-executed',not rows('epg-refresh-unexpected-tasks.jsonl'));report['passed']=True
finally:
 stop()
 for key in keys:cache.delete(key)
 EPGSource.objects.filter(pk__in=owned).delete();broker.flushdb();backend.flushdb();Path('/evidence/source-epg-refresh-ack-private.json').write_text(json.dumps(report,indent=2)+'\n')
