"""Actual Original EPG refresh wrapper; copied database and broker8/result9 only.
Original Dispatcharr/Celery/Kombu authors retain all code credit and licenses.
No native worker, remote ingestion, backup or restore.
"""
import base64,datetime as dt,json,os,signal,subprocess,sys,time
from pathlib import Path
if os.environ.get('JOB_DISPOSABLE_FIXTURE')!='playback-fixture-only':raise RuntimeError('Owned fixture required')
database=os.environ.get('POSTGRES_DB','')
if not database.startswith('rust_probe_backup_jobs_')or len(database)>63 or not all(c in 'abcdefghijklmnopqrstuvwxyz0123456789_'for c in database):raise RuntimeError('Copied database required')
sys.path[:0]=['/app','/job-tools'];sys.argv.append('shell');os.environ.setdefault('DJANGO_SETTINGS_MODULE','dispatcharr.settings')
import django;django.setup()
import redis
from dispatcharr.celery import app
from apps.epg import tasks
from apps.epg.models import EPGSource,EPGSourceIndex
from django_celery_beat.models import PeriodicTask,IntervalSchedule
task=tasks.refresh_epg_data;broker=redis.Redis.from_url(os.environ['CELERY_BROKER_URL']);backend=redis.Redis.from_url(os.environ['CELERY_RESULT_BACKEND']);cache=redis.Redis(host='127.0.0.1',password='playback-fixture-only',db=0)
if broker.connection_pool.connection_kwargs['db']!=8 or backend.connection_pool.connection_kwargs['db']!=9:raise RuntimeError('Owned namespaces required')
broker.flushdb();backend.flushdb()
for key in ['dvr:recover_dispatch_lock','core:version_check_dispatch_lock']:cache.set(key,'1',ex=3600)
root=Path('/evidence/refresh-files');root.mkdir();path=root/'guide.xml';path.write_bytes(b'<tv></tv>')
report={'passed':False,'scope':'Actual Original EPG refresh producer/worker wrapper for inactive/dummy/missing/busy/deferral/exhaustion; no imported/local/remote/native acceptance','checks':[],'cases':[],'task':task.name,'hard_time_limit':task.time_limit,'defer_seconds':tasks._EPG_REFRESH_DEFER_SECONDS,'max_defers':tasks._EPG_PARSE_DEFER_MAX};worker=None;keys=[];owned=[];periodic=[];intervals=[]
def check(name,value):
 report['checks'].append({'name':name,'passed':bool(value)})
 if not value:raise AssertionError(name)
def wait(fn,seconds=35):
 deadline=time.monotonic()+seconds
 while time.monotonic()<deadline:
  value=fn()
  if value:return value
  time.sleep(.025)
 raise TimeoutError('Owned refresh observation deadline')
def rows(name):
 p=Path('/evidence')/name;return [json.loads(v)for v in p.read_text().splitlines()]if p.exists()else[]
def stop():
 global worker
 if worker is not None and worker.poll()is None:
  os.killpg(worker.pid,signal.SIGTERM)
  try:worker.wait(timeout=15)
  except subprocess.TimeoutExpired:os.killpg(worker.pid,signal.SIGKILL);worker.wait(timeout=5);report.setdefault('forced_cleanup',[]).append(True)
 worker=None
def launch():
 global worker
 queued=broker.lrange('celery',0,64)
 if len(queued)>64:raise RuntimeError('Queue bound')
 # Fixture startup work is never delegated to this isolated family experiment.
 for raw in queued:
  value=json.loads(raw)
  if value['headers']['task']!=task.name:report.setdefault('quarantined',[]).append(value['headers']['task']);broker.lrem('celery',0,raw)
 broker.delete('_kombu.binding.celery.pidbox')
 with Path('/evidence/epg-refresh-worker-private.log').open('ab')as log:
  worker=subprocess.Popen(['/dispatcharrpy/bin/celery','-A','refresh_eta_observer:app','worker','--pool=prefork','--concurrency=1','--without-gossip','--without-mingle','--without-heartbeat','--loglevel=WARNING','--hostname=rust-refresh@fixture'],env=dict(os.environ,C_FORCE_ROOT='true',PYTHONPATH='/job-tools:/app'),stdout=log,stderr=log,start_new_session=True)
 wait(lambda:broker.exists('_kombu.binding.celery.pidbox'))
report['scope']='Actual Original refresh15-second manual ETA reservation and execution; Index/Series descendants routed to owned nonconsumed queues as explicit fixture control'
report['fixture_control']={'task_functions_replaced':False,'descendant_routes':{'apps.epg.tasks.build_programme_index_task':'rust-refresh-index-control','apps.channels.tasks.evaluate_series_rules':'rust-refresh-series-control'},'workers_consume_only':'celery','actual_wait_for_eta':True}
try:
 for label in ['file-busy','parser-busy']:
  broker.flushdb();backend.flushdb()
  source=EPGSource(name='rust-refresh-eta-'+label,source_type='xmltv',is_active=True,file_path=str(path),custom_properties={});EPGSource.objects.bulk_create([source]);owned.append(source.pk);sid=source.pk
  EPGSourceIndex.objects.create(source_id=sid,data={'owned':'before'})
  refresh_key='task_lock_refresh_epg_data_'+str(sid);file_key='task_lock_epg_source_file_'+str(sid);counter=tasks._source_tvg_parse_count_key(sid);keys.extend([refresh_key,file_key,counter])
  if label=='file-busy':cache.set(file_key,'owned-busy',ex=300)
  else:cache.set(counter,1,ex=300)
  issued=task.apply_async(args=[sid],kwargs={'force':True});ident=issued.id
  parent_envelope=json.loads(next(raw for raw in broker.lrange('celery',0,63)if json.loads(raw)['headers']['id']==ident))
  launch()
  parent_post=wait(lambda:next((row for row in rows('epg-refresh-postrun.jsonl')if row['id']==ident),None))
  def reservation():
   values=broker.hgetall('unacked')
   if len(values)>64:raise RuntimeError('Reservation count bound')
   for tag,raw in values.items():
    if len(raw)>131072:raise RuntimeError('Stored tuple byte bound')
    value=json.loads(raw)
    if isinstance(value,list)and len(value)==3 and value[0]['headers']['task']==task.name and value[0]['headers']['parent_id']==ident:return tag,raw,value
   return None
  tag,raw_tuple,stored=wait(reservation)
  child=stored[0];child_id=child['headers']['id'];payload=json.loads(base64.b64decode(child['body'],validate=True));score=broker.zscore('unacked_index',tag)
  check(label+'-parent-normal-before-child',parent_post['state']=='SUCCESS'and json.loads(backend.get(app.backend.get_key_for_task(ident)))['result']is None)
  check(label+'-typed-child-call',payload[:2]==[[sid],{'force':True,'_file_defer_retry':1}]and child['headers']['retries']==0)
  check(label+'-child-lineage',child['headers']['root_id']==ident and child['headers']['parent_id']==ident)
  check(label+'-actual-reservation-geometry',stored[1:]==['','celery']and tag.decode()==child['properties']['delivery_tag']and score is not None and abs(time.time()-score)<10 and broker.ttl('unacked')==-1 and broker.ttl('unacked_index')==-1)
  check(label+'-child-not-yet-started',not any(row['id']==child_id for row in rows('epg-refresh-prerun.jsonl'))and backend.get(app.backend.get_key_for_task(child_id))is None)
  # Remove only the owned blocker. The unchanged original worker waits its real
  # fifteen-second ETA and then runs the child normally; no clock override.
  cache.delete(file_key if label=='file-busy'else counter)
  child_post=wait(lambda:next((row for row in rows('epg-refresh-postrun.jsonl')if row['id']==child_id),None),seconds=35)
  stop()
  prerun=next(row for row in rows('epg-refresh-prerun.jsonl')if row['id']==child_id)
  eta=dt.datetime.fromisoformat(child['headers']['eta']);started=dt.datetime.fromisoformat(prerun['utc'])
  check(label+'-real-eta-not-before-deadline',0<=(started-eta).total_seconds()<10)
  child_raw=backend.get(app.backend.get_key_for_task(child_id));check(label+'-bounded-child-result',child_raw is not None and len(child_raw)<=262144);metadata=json.loads(child_raw)
  check(label+'-child-actual-normal-result',child_post['state']=='SUCCESS'and app.AsyncResult(child_id).get(timeout=2)is None and metadata['result']is None and metadata['parent_id']==ident)
  check(label+'-actual-ACK-on-execution',broker.hlen('unacked')==0 and broker.zcard('unacked_index')==0 and broker.llen('celery')==0)
  check(label+'-physical-locks-cleared',cache.get(refresh_key)is None and cache.get(file_key)is None and cache.get(counter)is None)
  check(label+'-terminal-source-success',EPGSource.objects.get(pk=sid).status==EPGSource.STATUS_SUCCESS)
  descendants=[]
  for queue,name in [('rust-refresh-index-control','apps.epg.tasks.build_programme_index_task'),('rust-refresh-series-control','apps.channels.tasks.evaluate_series_rules')]:
   values=broker.lrange(queue,0,63);check(label+'-one-quarantined-'+queue,len(values)==1)
   value=json.loads(values[0]);check(label+'-descendant-lineage-'+queue,value['headers']['task']==name and value['headers']['root_id']==ident and value['headers']['parent_id']==child_id);descendants.append(value)
  check(label+'-result-children-lineage',metadata['children']==[[[value['headers']['id'],None],None]for value in descendants])
  report['cases'].append({'name':label,'parent_envelope':parent_envelope,'parent_postrun':parent_post,'child_envelope':child,'stored_tuple':stored,'stored_tuple_base64':base64.b64encode(raw_tuple).decode(),'reservation_score':score,'prerun':prerun,'postrun':child_post,'result':metadata,'quarantined_descendants':descendants})
 check('no-unexpected-executed-family',not rows('epg-refresh-unexpected-tasks.jsonl'))
 report['passed']=True
finally:
 stop()
 for key in keys:cache.delete(key)
 EPGSource.objects.filter(pk__in=owned).delete();broker.flushdb();backend.flushdb()
 report['queue_and_results_after_cleanup']=[broker.dbsize(),backend.dbsize()]
 Path('/evidence/source-refresh-eta-private.json').write_text(json.dumps(report,indent=2,default=str)+'\n')
