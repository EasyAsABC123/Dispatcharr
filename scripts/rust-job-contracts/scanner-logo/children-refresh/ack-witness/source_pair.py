"""Actual pinned Source Index handler held in owned FIFO file I/O; no native claim.
Parent/root apply_async options are explicit fixture controls, not a scanner-parent
publication claim. Original Dispatcharr/Celery/Billiard authors retain credit.
"""
import json,os,signal,subprocess,sys,time,uuid,errno
from pathlib import Path
assert os.environ.get('JOB_DISPOSABLE_FIXTURE')=='playback-fixture-only'
assert os.environ['POSTGRES_DB'].startswith('rust_probe_backup_jobs_')
sys.path[:0]=['/app','/pair-tools'];sys.argv.append('shell');os.environ.setdefault('DJANGO_SETTINGS_MODULE','dispatcharr.settings')
import django;django.setup()
import redis
from dispatcharr.celery import app
from apps.epg.models import EPGSource,EPGSourceIndex
from apps.epg.tasks import build_programme_index_task
broker=redis.Redis.from_url(os.environ['CELERY_BROKER_URL']);backend=redis.Redis.from_url(os.environ['CELERY_RESULT_BACKEND']);cache=redis.Redis(host='localhost',port=6379,password='playback-fixture-only',db=0)
assert broker.connection_pool.connection_kwargs['db']==8 and backend.connection_pool.connection_kwargs['db']==9 and broker.dbsize()==backend.dbsize()==0
for key in ['dvr:recover_dispatch_lock','core:version_check_dispatch_lock']:cache.set(key,'1',ex=3600)
app.loader.import_default_modules();worker=None;writer=None
report={'passed':False,'native_verified':False,'scope':__doc__,'checks':[],'cases':[],'predeclared':{'real_fifo_handler_io':True,'ack_deadline_seconds':5,'minimum_before_release_seconds':.25,'task_deadline_seconds':30,'instant_prerun_reservation_is_diagnostic_only':True}}
def check(name,value):
 report['checks'].append({'name':name,'passed':bool(value)})
 if not value:raise AssertionError(name)
def rows(kind):
 p=Path('/evidence/periodic-worker-'+kind+'.jsonl');return [json.loads(v)for v in p.read_text().splitlines()]if p.exists()else[]
def wait(fn,seconds):
 end=time.monotonic()+seconds
 while time.monotonic()<end:
  v=fn()
  if v:return v
  if worker and worker.poll()is not None:raise RuntimeError('Owned worker exited')
  time.sleep(.01)
 raise TimeoutError('Bounded owned Source observation')
def unacked():
 values=broker.hvals('unacked');assert len(values)<=16
 for v in values:assert len(v)<=131072
 return [json.loads(v)[0]['headers']['id']for v in values]
try:
 root=Path('/data/rust-owned-child-ack');root.mkdir();fifo=root/'blocked.xml';os.mkfifo(fifo)
 source=EPGSource.objects.create(name='rust-owned-child-ack',source_type='xmltv',is_active=False,file_path=str(fifo));parent=str(uuid.uuid4());result=build_programme_index_task.apply_async(args=[source.id],root_id=parent,parent_id=parent);tid=result.id;lock='task_lock_epg_source_file_'+str(source.id)
 raw=broker.lindex('celery',-1);envelope=json.loads(raw);report['cases'].append({'name':'original-parented-index-held-file-read','task_id':tid,'parent_id':parent,'envelope':envelope,'prerun':None,'samples':[]});case=report['cases'][-1]
 check('actual-parented-default-queue-envelope',envelope['headers']['parent_id']==envelope['headers']['root_id']==parent and envelope['properties']['delivery_info']=={'exchange':'','routing_key':'celery'})
 log=open('/evidence/scanner-worker-private.log','ab');worker=subprocess.Popen(['/dispatcharrpy/bin/celery','-A','periodic_worker_observer:app','worker','--pool=prefork','--concurrency=1','-Q','celery','--without-gossip','--without-mingle','--without-heartbeat','--loglevel=WARNING','--hostname=rust-child-ack@fixture'],env=dict(os.environ,C_FORCE_ROOT='true',PYTHONPATH='/pair-tools:/app'),stdout=log,stderr=log,start_new_session=True);log.close()
 def open_writer():
  try:return os.open(fifo,os.O_WRONLY|os.O_NONBLOCK)
  except OSError as error:
   if error.errno==errno.ENXIO:return None
   raise
 writer=wait(open_writer,30);case['prerun']=wait(lambda:next((v for v in rows('prerun')if v['id']==tid),None),5)
 check('actual-handler-read-holds-original-file-lease',cache.get(lock)==b'locked'and backend.get(app.backend.get_key_for_task(tid))is None)
 start=time.monotonic()
 while True:
  ids=unacked();elapsed=time.monotonic()-start;case['samples'].append({'elapsed_seconds':elapsed,'own_task_unacked':tid in ids,'backend_present':backend.get(app.backend.get_key_for_task(tid))is not None,'file_lock_present':cache.get(lock)==b'locked'})
  if tid not in ids and elapsed>=.25:break
  assert elapsed<5;time.sleep(.01)
 check('Source-ACK-completes-during-held-actual-handler-before-result',all(not v['backend_present']and v['file_lock_present']for v in case['samples'])and not case['samples'][-1]['own_task_unacked'])
 os.write(writer,b'<tv><programme channel="owned.channel"><title>Owned</title></programme></tv>');os.close(writer);writer=None
 post=wait(lambda:next((v for v in rows('postrun')if v['id']==tid),None),30);metadata=json.loads(backend.get(app.backend.get_key_for_task(tid)));case.update(postrun=post,metadata=metadata,ttl=backend.ttl(app.backend.get_key_for_task(tid)),index=EPGSourceIndex.objects.get(source_id=source.id).data)
 check('real-handler-completes-after-owned-file-release',post['state']==metadata['status']=='SUCCESS'and metadata['result']is None and result.get(timeout=2)is None and not cache.exists(lock))
 check('known-parent-and-result-TTL',metadata['parent_id']==parent and 3580<=case['ttl']<=3600)
 check('actual-index-bytes-and-final-queue-drain',case['index']=={'channels':{'owned.channel':[4]},'interleaved_channels':[]}and broker.llen('celery')==broker.hlen('unacked')==0)
 report['passed']=True
finally:
 if writer is not None:os.close(writer)
 if worker and worker.poll()is None:
  os.killpg(worker.pid,signal.SIGTERM)
  try:worker.wait(timeout=15)
  except subprocess.TimeoutExpired:os.killpg(worker.pid,signal.SIGKILL);worker.wait(timeout=5);report['forced_cleanup']=True
 Path('/evidence/source-logo-scanner-private.json').write_text(json.dumps(report,indent=2)+'\n')
print(json.dumps({'passed':report['passed'],'checks':len(report['checks'])}))
