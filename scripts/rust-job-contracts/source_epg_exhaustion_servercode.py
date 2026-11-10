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
report={'fault_sqlstate':'23514','passed':False,'scope':'Actual Original EPG refresh producer/worker wrapper for inactive/dummy/missing/busy/deferral/exhaustion; no imported/local/remote/native acceptance','checks':[],'cases':[],'task':task.name,'hard_time_limit':task.time_limit,'defer_seconds':tasks._EPG_REFRESH_DEFER_SECONDS,'max_defers':tasks._EPG_PARSE_DEFER_MAX};worker=None;keys=[];owned=[];periodic=[];intervals=[]
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
  worker=subprocess.Popen(['/dispatcharrpy/bin/celery','-A','epg_refresh_observer:app','worker','--pool=prefork','--concurrency=1','--without-gossip','--without-mingle','--without-heartbeat','--loglevel=WARNING','--hostname=rust-refresh@fixture'],env=dict(os.environ,C_FORCE_ROOT='true',PYTHONPATH='/job-tools:/app'),stdout=log,stderr=log,start_new_session=True)
 wait(lambda:broker.exists('_kombu.binding.celery.pidbox'))
try:
 for label in ['exhausted_file_status_fault','exhausted_parser_status_fault']:
  broker.flushdb();backend.flushdb()
  source=EPGSource.objects.create(name='rust-refresh-'+label,source_type='dummy'if label=='dummy'else'xmltv',is_active=False,file_path=str(path));owned.append(source.pk);sid=source.pk
  EPGSourceIndex.objects.update_or_create(source_id=sid,defaults={'data':{'owned':'before'}})
  if label not in ['inactive','missing']:EPGSource.objects.filter(pk=sid).update(is_active=True)
  if label=='missing':
   source.delete();interval=IntervalSchedule.objects.create(every=7919,period='seconds');intervals.append(interval.pk);p=PeriodicTask.objects.create(name='epg_source-refresh-'+str(sid),task=task.name,args=json.dumps([sid]),interval=interval);periodic.append(p.pk)
  refresh_key='task_lock_refresh_epg_data_'+str(sid);file_key='task_lock_epg_source_file_'+str(sid);parse_key=tasks._source_tvg_parse_count_key(sid);keys.extend([refresh_key,file_key,parse_key])
  if label=='refresh_busy':cache.set(refresh_key,'owned-busy',ex=300)
  if label.startswith('exhausted_file'):cache.set(file_key,'owned-busy',ex=300)
  if label.startswith('exhausted_parser'):cache.set(parse_key,2,ex=300)
  kwargs={'force':True,'_file_defer_retry':tasks._EPG_PARSE_DEFER_MAX if label.startswith('exhausted_')else 0}
  fault_name=None
  if label.endswith('_status_fault'):
   from django.db import connection
   fault_name='rust_exhaust_status_'+str(sid)
   with connection.cursor()as cursor:
    cursor.execute(f"CREATE FUNCTION {fault_name}() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'owned exhausted status blocked' USING ERRCODE='23514'; END; $$")
    cursor.execute(f"CREATE TRIGGER {fault_name}_trigger BEFORE UPDATE ON epg_epgsource FOR EACH ROW WHEN (OLD.id={sid} AND NEW.status='error') EXECUTE FUNCTION {fault_name}()")
  ident=task.apply_async(args=[sid],kwargs=kwargs).id
  raw=next(raw for raw in broker.lrange('celery',0,64)if json.loads(raw)['headers']['id']==ident);envelope=json.loads(raw)
  launch();post=wait(lambda:next((row for row in rows('epg-refresh-postrun.jsonl')if row['id']==ident),None));stop()
  raw_result=backend.get(app.backend.get_key_for_task(ident));check('bounded-result-'+label,raw_result is not None and len(raw_result)<=262144);meta=json.loads(raw_result)
  check('normal-success-'+label,post['state']=='SUCCESS' and meta['status']=='SUCCESS')
  expected=f'EPG source {sid} does not exist, task cleaned up'if label=='missing'else None
  check('exact-return-'+label,meta['result']==expected)
  children=[v for v in rows('epg-refresh-child-envelopes.jsonl')if v.get('observed')and v['observed']['envelope']['headers'].get('parent_id')==ident]
  if label in ['file_busy','parser_busy']:
   check('one-new-child-'+label,len(children)==1);child=children[0]['observed']['envelope'];payload=json.loads(base64.b64decode(child['body']));check('child-args-'+label,payload[0]==[sid]and payload[1]=={'force':True,'_file_defer_retry':1})
   check('child-lineage-'+label,child['headers']['root_id']==ident and child['headers']['parent_id']==ident and child['headers']['retries']==0)
   delay=(dt.datetime.fromisoformat(child['headers']['eta'])-dt.datetime.fromisoformat(children[0]['utc'])).total_seconds();check('actual-countdown-'+label,13<=delay<=16)
  else:check('no-child-'+label,not children)
  index=EPGSourceIndex.objects.filter(source_id=sid).values_list('data',flat=True).first()
  if label.startswith('exhausted_'):check('invalidates-index-before-deferral-'+label,index is None)
  if label in ['inactive','dummy','refresh_busy']:check('retains-index-'+label,index=={'owned':'before'})
  if label.endswith('_status_fault'):check('caught-status-fault-preserves-idle',EPGSource.objects.get(pk=sid).status==EPGSource.STATUS_IDLE)
  else:check('exhausted-sets-source-error',EPGSource.objects.get(pk=sid).status==EPGSource.STATUS_ERROR)
  if label=='missing':check('orphan-task-and-private-interval-removed',not PeriodicTask.objects.filter(name='epg_source-refresh-'+str(sid)).exists()and not IntervalSchedule.objects.filter(pk=interval.pk).exists())
  check('refresh-lock-effect-'+label,cache.get(refresh_key)==(b'owned-busy'if label=='refresh_busy'else None))
  report['cases'].append({'name':label,'source_id':sid,'envelope':envelope,'metadata':meta,'postrun':post,'children':children,'index':index,'refresh_lock':cache.get(refresh_key).decode()if cache.get(refresh_key)else None,'file_lock':cache.get(file_key).decode()if cache.get(file_key)else None,'parser_counter':cache.get(parse_key).decode()if cache.get(parse_key)else None,'source_state':list(EPGSource.objects.filter(pk=sid).values('status','last_message','custom_properties'))})
  if fault_name:
   with connection.cursor()as cursor:cursor.execute(f'DROP FUNCTION {fault_name}() CASCADE')
  for key in [refresh_key,file_key,parse_key]:cache.delete(key)
 check('no-unexpected-executed-family',not rows('epg-refresh-unexpected-tasks.jsonl'))
 report['passed']=True
finally:
 stop()
 for key in keys:cache.delete(key)
 EPGSource.objects.filter(pk__in=owned).delete();PeriodicTask.objects.filter(pk__in=periodic).delete();IntervalSchedule.objects.filter(pk__in=intervals).delete()
 Path('/evidence/source-epg-exhaustion-servercode-private.json').write_text(json.dumps(report,indent=2)+'\n')
