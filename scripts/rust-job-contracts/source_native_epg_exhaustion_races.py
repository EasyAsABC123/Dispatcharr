"""Actual Original EPG refresh wrapper; copied database and broker8/result9 only.
Original Dispatcharr/Celery/Kombu authors retain all code credit and licenses.
No native worker, remote ingestion, backup or restore.
"""
import asyncio,base64,datetime as dt,hashlib,inspect,json,os,signal,subprocess,sys,time,uuid
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
from django.db import connection,transaction
from channels.layers import get_channel_layer
from core.utils import release_task_lock,TaskLockRenewer
from django_celery_beat.models import PeriodicTask,IntervalSchedule
task=tasks.refresh_epg_data;broker=redis.Redis.from_url(os.environ['CELERY_BROKER_URL']);backend=redis.Redis.from_url(os.environ['CELERY_RESULT_BACKEND']);cache=redis.Redis(host='127.0.0.1',password='playback-fixture-only',db=0)
if broker.connection_pool.connection_kwargs['db']!=8 or backend.connection_pool.connection_kwargs['db']!=9:raise RuntimeError('Owned namespaces required')
broker.flushdb();backend.flushdb()
for key in ['dvr:recover_dispatch_lock','core:version_check_dispatch_lock']:cache.set(key,'1',ex=3600)
root=Path('/evidence/refresh-race-files');root.mkdir();path=root/'guide.xml';path.write_bytes(b'<tv></tv>')
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
  worker=subprocess.Popen(['/dispatcharrpy/bin/celery','-A','epg_refresh_observer:app','worker','--pool=prefork','--concurrency=1','--without-gossip','--without-mingle','--without-heartbeat','--loglevel=WARNING','--hostname=rust-refresh@fixture'],env=dict(os.environ,C_FORCE_ROOT='true',PYTHONPATH='/job-tools:/app'),stdout=log,stderr=log,start_new_session=True)
 wait(lambda:broker.exists('_kombu.binding.celery.pidbox'))

probe_functions=[]
probe_tables=[]
async def subscribe():
 layer=get_channel_layer();channel=await layer.new_channel('rust.exhaustion.');await layer.group_add('updates',channel);await layer.close_pools();return channel
async def receive(channel):
 layer=get_channel_layer();messages=[]
 try:
  for _ in range(8):
   try:messages.append(await asyncio.wait_for(layer.receive(channel),.3))
   except asyncio.TimeoutError:break
 finally:
  await layer.group_discard('updates',channel);await layer.close_pools()
 return messages

def observe_update(sid):
 name='rust_exhaust_rows_'+uuid.uuid4().hex[:12]
 with connection.cursor()as cursor:
  cursor.execute(f'CREATE TABLE {name}(source_id bigint,affected bigint,query text)')
  cursor.execute(f"CREATE FUNCTION {name}() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN INSERT INTO {name} SELECT {sid},count(*),current_query() FROM owned_new; RETURN NULL; END; $$")
  cursor.execute(f'CREATE TRIGGER {name}_trigger AFTER UPDATE ON epg_epgsource REFERENCING NEW TABLE AS owned_new FOR EACH STATEMENT EXECUTE FUNCTION {name}()')
 probe_functions.append(name);probe_tables.append(name);return name

def blocked_update():
 with connection.cursor()as cursor:
  cursor.execute('SELECT pg_stat_clear_snapshot()')
  cursor.execute("SELECT pid,query FROM pg_stat_activity WHERE datname=current_database() AND pid<>pg_backend_pid() AND wait_event_type='Lock' AND query LIKE 'UPDATE%epg_epgsource%status%'")
  rows=cursor.fetchall()
  if len(rows)>1:raise RuntimeError('Unexpected concurrent status UPDATE')
  return rows[0]if rows else None

try:
 report['scope']='Actual Original knownbusy480 concurrent row deletion and replacement/deleted refresh token: status UPDATE rowcount/publication/result/lease effects; no Native parity'
 report['original_release_source_sha256']=hashlib.sha256(inspect.getsource(release_task_lock).encode()).hexdigest()
 report['original_renewer_source_sha256']=hashlib.sha256(inspect.getsource(TaskLockRenewer).encode()).hexdigest()
 Path('/evidence/source-release-private.json').write_text(json.dumps({'release_task_lock':inspect.getsource(release_task_lock),'TaskLockRenewer':inspect.getsource(TaskLockRenewer)},indent=2))
 for label in ['delete_file','delete_parser','replace_file','replace_parser','absent_file','absent_parser']:
  broker.flushdb();backend.flushdb()
  source=EPGSource(name='rust-native-refresh-race-'+label,source_type='xmltv',is_active=True,file_path=str(path));EPGSource.objects.bulk_create([source]);owned.append(source.pk);sid=source.pk
  EPGSourceIndex.objects.create(source_id=sid,data={'owned':'before'})
  refresh_key='task_lock_refresh_epg_data_'+str(sid);file_key='task_lock_epg_source_file_'+str(sid);parse_key=tasks._source_tvg_parse_count_key(sid);keys.extend([refresh_key,file_key,parse_key])
  if label.endswith('_file'):cache.set(file_key,'owned-busy',ex=300)
  else:cache.set(parse_key,2,ex=300)
  channel=asyncio.run(subscribe());probe=observe_update(sid);started=time.monotonic();intervention={}
  with transaction.atomic():
   EPGSource.objects.select_for_update(no_key=True).get(pk=sid)
   ident=task.apply_async(args=[sid],kwargs={'force':True,'_file_defer_retry':480}).id
   launch();blocked=wait(blocked_update)
   check('actual-worker-blocked-status-UPDATE-'+label,bool(blocked))
   check('index-invalidated-before-intervention-'+label,EPGSourceIndex.objects.get(source_id=sid).data is None)
   before_token=cache.get(refresh_key);check('real-refresh-lock-acquired-'+label,before_token==b'locked')
   if label.startswith('delete_'):
    deleted=EPGSource.objects.filter(pk=sid).delete();intervention={'kind':'actual_Django_collector_delete_on_own_lock_connection','deleted':deleted}
   elif label.startswith('replace_'):
    cache.set(refresh_key,'owned-foreign-token',ex=300);intervention={'kind':'replace_only_owned_refresh_key','value':'owned-foreign-token','ttl':cache.ttl(refresh_key)}
   else:
    cache.delete(refresh_key);intervention={'kind':'delete_only_owned_refresh_key','value':None,'ttl':cache.ttl(refresh_key)}
  post=wait(lambda:next((r for r in rows('epg-refresh-postrun.jsonl')if r['id']==ident),None));stop();elapsed=time.monotonic()-started
  raw=backend.get(app.backend.get_key_for_task(ident));meta=json.loads(raw);messages=asyncio.run(receive(channel));progress=[m for m in messages if m.get('data',{}).get('source')==sid]
  with connection.cursor()as cursor:
   cursor.execute(f'SELECT affected,query FROM {probe}');updates=cursor.fetchall()
  check('exact-guarded-UPDATE-rowcount-'+label,len(updates)==1 and updates[0][0]==(0 if label.startswith('delete_')else 1))
  check('actual-Original-normal-null-'+label,post['state']=='SUCCESS'and meta['status']=='SUCCESS'and meta['result']is None and app.AsyncResult(ident).get(timeout=2)is None and meta['children']==[]and meta['traceback']is None)
  check('actual-publication-even-zero-UPDATE-'+label,len(progress)==1 and progress[0]['data'].get('status')=='error')
  check('Original-release-removes-current-or-missing-key-'+label,cache.get(refresh_key)is None)
  check('bounded-before-renewal-'+label,elapsed<60)
  state=list(EPGSource.objects.filter(pk=sid).values('status','last_message','custom_properties'))
  if label.startswith('delete_'):check('actual-source-and-index-deleted-'+label,not state and not EPGSourceIndex.objects.filter(source_id=sid).exists())
  else:check('source-error-after-unblock-'+label,len(state)==1 and state[0]['status']=='error')
  report['cases'].append({'name':label,'source_id':sid,'metadata':meta,'metadata_sha256':hashlib.sha256(raw).hexdigest(),'result_ttl':backend.ttl(app.backend.get_key_for_task(ident)),'intervention':intervention,'blocked_query_sha256':hashlib.sha256(blocked[1].encode()).hexdigest(),'actual_UPDATE_affected':updates[0][0],'actual_UPDATE_query_sha256':hashlib.sha256(updates[0][1].encode()).hexdigest(),'progress_messages':progress,'source_state':state,'refresh_after':None,'elapsed_seconds':elapsed,'renewal_interval_seconds':120})
  from exhaustion_race_native_compare import compare
  compare(globals())
  with connection.cursor()as cursor:
   cursor.execute(f'DROP FUNCTION {probe}() CASCADE');cursor.execute(f'DROP TABLE {probe}')
  probe_functions.remove(probe);probe_tables.remove(probe)
  for key in [refresh_key,file_key,parse_key]:cache.delete(key)
 check('no-unexpected-executed-family',not rows('epg-refresh-unexpected-tasks.jsonl'))
 report['passed']=True

finally:
 stop()
 with connection.cursor()as cursor:
  for function in probe_functions:cursor.execute(f'DROP FUNCTION IF EXISTS {function}() CASCADE')
  for table in probe_tables:cursor.execute(f'DROP TABLE IF EXISTS {table}')
 for key in keys:cache.delete(key)
 EPGSource.objects.filter(pk__in=owned).delete();PeriodicTask.objects.filter(pk__in=periodic).delete();IntervalSchedule.objects.filter(pk__in=intervals).delete()
 Path('/evidence/source-native-epg-exhaustion-races-private.json').write_text(json.dumps(report,indent=2)+'\n')
