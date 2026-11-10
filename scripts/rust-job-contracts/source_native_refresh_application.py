"""Actual Original producer to native immediate realFileJob worker and Original AsyncResult.
Original Dispatcharr authors retain full application/algorithm credit and AGPL.
An explicit fixture router diverts children into an owned unconsumed queue.
"""
import base64,datetime as dt,hashlib,json,os,signal,subprocess,sys,time,uuid
from pathlib import Path
if os.environ.get('JOB_DISPOSABLE_FIXTURE')!='playback-fixture-only':raise RuntimeError('Owned fixture required')
database=os.environ.get('POSTGRES_DB','')
if not database.startswith('rust_probe_backup_jobs_')or len(database)>63 or not all(c in 'abcdefghijklmnopqrstuvwxyz0123456789_'for c in database):raise RuntimeError('Copied database required')
sys.path[:0]=['/app','/job-tools'];sys.argv.append('shell');os.environ.setdefault('DJANGO_SETTINGS_MODULE','dispatcharr.settings')
import django;django.setup()
import redis
from dispatcharr.celery import app
from apps.epg import tasks
from apps.epg.models import EPGSource,EPGSourceIndex,EPGData,ProgramData
from apps.channels.models import Channel
TASK=tasks.refresh_epg_data;CHILD_QUEUE='owned-refresh-children'
broker=redis.Redis.from_url(os.environ['CELERY_BROKER_URL']);backend=redis.Redis.from_url(os.environ['CELERY_RESULT_BACKEND']);cache=redis.Redis(host='127.0.0.1',password='playback-fixture-only',db=0)
if broker.connection_pool.connection_kwargs['db']!=8 or backend.connection_pool.connection_kwargs['db']!=9:raise RuntimeError('Owned broker/result namespaces required')
root=Path('/evidence/refresh-import-files');root.mkdir();worker=None;owned=[];keys=[]
report={'passed':False,'scope':'Actual Original refresh argument defaults/positional forms and imported local-file child publication; no child execution/native/remote acceptance','task':TASK.name,'hard_time_limit':TASK.time_limit,'checks':[],'cases':[],'fixture_control':{'child_routes':'all tasks except refresh_epg_data → owned-refresh-children','worker_queues':['celery'],'purpose':'capture actual generated child envelopes without executing unrelated families','task_or_helper_replacement':False}}
def check(name,value):
 report['checks'].append({'name':name,'passed':bool(value)})
 if not value:raise AssertionError(name)
def rows(name):
 p=Path('/evidence')/name;return[json.loads(v)for v in p.read_text().splitlines()]if p.exists()else[]
def wait(fn,seconds=40):
 deadline=time.monotonic()+seconds
 while time.monotonic()<deadline:
  value=fn()
  if value:return value
  time.sleep(.025)
 raise TimeoutError('Owned refresh observation deadline')
def stop():
 global worker
 if worker is not None and worker.poll()is None:
  os.killpg(worker.pid,signal.SIGTERM)
  try:worker.wait(timeout=15)
  except subprocess.TimeoutExpired:os.killpg(worker.pid,signal.SIGKILL);worker.wait(timeout=5);report.setdefault('forced_cleanup',[]).append(True)
 worker=None
def launch():
 global worker
 # Only parent refresh messages are allowed into the consumed queue.
 for raw in broker.lrange('celery',0,63):
  if len(raw)>65536 or json.loads(raw)['headers']['task']!=TASK.name:raise RuntimeError('Unrelated consumed queue head')
 broker.delete('_kombu.binding.celery.pidbox')
 with Path('/evidence/epg-refresh-worker-private.log').open('ab')as log:
  worker=subprocess.Popen(['/dispatcharrpy/bin/celery','-A','epg_refresh_import_observer:app','worker','-Q','celery','--pool=prefork','--concurrency=1','--without-gossip','--without-mingle','--without-heartbeat','--loglevel=WARNING','--hostname=rust-refresh-import@fixture'],env=dict(os.environ,C_FORCE_ROOT='true',PYTHONPATH='/job-tools:/app'),stdout=log,stderr=log,start_new_session=True)
 wait(lambda:broker.exists('_kombu.binding.celery.pidbox'))
def payload(envelope):
 raw=base64.b64decode(envelope['body'],validate=True)
 if len(raw)>16384:raise RuntimeError('Body bound')
 return json.loads(raw)
def send(kind,sid):
 if kind.endswith('default'):return TASK.delay(sid)
 if kind.endswith('force_kw'):return TASK.apply_async(args=[sid],kwargs={'force':True})
 if kind.endswith('force_pos'):return TASK.delay(sid,True)
 if kind.endswith('retry_pos'):return TASK.delay(sid,True,7)
 if kind.endswith('all_kw'):return TASK.apply_async(kwargs={'source_id':sid,'force':False,'_file_defer_retry':2})
 raise RuntimeError('Known case required')
from django_celery_beat.models import PeriodicTask,IntervalSchedule
periodic=[];intervals=[]
report['scope']='Production refresh authority and real FileJob normal effects, Original producer/AsyncResult and actual Redis polling/retention gates; no app HTTP/main, scheduler or general-family consumer'
report['fixture_control']={'source_producer':'Unmodified Original task delay/apply_async','native_intake':'One explicitly invoked Rust peer, no default consumer','nonrefresh_children':'Remain in owned default celery queue; no worker consumes them','function_replacement':False}
def native(ident,sid,action='run',operational=False):
 value={'action':action,'task_id':ident,'source_id':sid,'database_url':'postgres://dispatch:playback-fixture-only@127.0.0.1:5432/'+database,'cache_url':'redis://:playback-fixture-only@127.0.0.1:6379/0','broker_url':os.environ['CELERY_BROKER_URL'],'result_url':os.environ['CELERY_RESULT_BACKEND'],'expect_operational':operational}
 peer=subprocess.run(['/evidence/native-refresh-worker','--ignored','--exact','refresh_worker::peer::native_refresh_worker_peer','--nocapture'],input=json.dumps(value),text=True,capture_output=True,timeout=45)
 with Path('/evidence/native-refresh-worker-private.log').open('a')as file:file.write(peer.stdout+peer.stderr)
 peer.check_returncode();marker='NATIVE_REFRESH_WORKER_JSON=';return json.loads(next(line.split(marker,1)[1]for line in peer.stdout.splitlines()if marker in line))

try:
 broker.flushdb();backend.flushdb()
 for key in ['dvr:recover_dispatch_lock','core:version_check_dispatch_lock']:cache.set(key,'1',ex=3600)
 for ordinal,label in enumerate(['inactive','dummy','refresh_busy','file_busy','parser_busy','missing_no_orphan','imported','unmapped','repeat','coarse_failed_operational','missing_orphan']):
  broker.flushdb();backend.flushdb();path=root/(label+'.xml');start=dt.datetime.now(dt.timezone.utc).replace(microsecond=0)+dt.timedelta(hours=1);end=start+dt.timedelta(hours=1);xml=('<tv><channel id="mapped"><display-name>Owned native guide</display-name></channel><programme channel="mapped" start="'+start.strftime('%Y%m%d%H%M%S +0000')+'" stop="'+end.strftime('%Y%m%d%H%M%S +0000')+'"><title>Owned native programme</title></programme></tv>').encode();path.write_bytes(xml)
  source=EPGSource(name='rust-native-refresh-'+label,source_type='dummy'if label=='dummy'else'xmltv',is_active=label not in ['inactive','repeat'],file_path=str(path),custom_properties={});EPGSource.objects.bulk_create([source]);sid=source.pk;owned.append(sid);epg=EPGData.objects.create(epg_source=source,tvg_id='mapped',name='Before guide')
  if label!='unmapped':Channel.objects.bulk_create([Channel(name='rust-native-refresh-'+label,channel_number=1970000+ordinal,epg_data=epg)])
  EPGSourceIndex.objects.create(source_id=sid,data={'owned':'before'})
  if label.startswith('missing'):
   source.delete()
   if label=='missing_orphan':
    interval=IntervalSchedule.objects.create(every=7933,period='seconds');intervals.append(interval.pk);period=PeriodicTask.objects.create(name='epg_source-refresh-'+str(sid),task=TASK.name,args=json.dumps([sid]),interval=interval);periodic.append(period.pk)
  refresh='task_lock_refresh_epg_data_'+str(sid);file='task_lock_epg_source_file_'+str(sid);counter=tasks._source_tvg_parse_count_key(sid);keys.extend([refresh,file,counter])
  if label=='refresh_busy':cache.set(refresh,'owned-busy',ex=300)
  if label=='file_busy':cache.set(file,'owned-busy',ex=300)
  if label=='coarse_failed_operational':cache.set(counter,'not-an-integer',ex=300)
  if label=='parser_busy':cache.set(counter,2,ex=300)
  kwargs={'force':True,'_file_defer_retry':480}if label=='coarse_failed_operational'else{}
  issued=TASK.apply_async(args=[sid],kwargs=kwargs);ident=issued.id;envelope=json.loads(next(v for v in broker.lrange('celery',0,63)if json.loads(v)['headers']['id']==ident));operational=label.endswith('_operational');audit=native(ident,sid,operational=operational);check('real-executor-default-ownership-'+label,audit['calls']==1 and audit['default_dispatch_calls']==0 and audit['caller_released']and audit['worker_drained']and audit['service_drained']and audit['transport_drained']and audit['physical_jobs']==0)
  raw=backend.get(app.backend.get_key_for_task(ident));metadata=None;children=[json.loads(v)for v in reversed(broker.lrange('celery',0,63))]
  if operational:
   check('unproved-outcome-no-fabricated-success-'+label,raw is None and app.AsyncResult(ident).state=='PENDING'and broker.llen('celery')==0 and broker.hlen('unacked')==0)
   check('coarse-failed-cleanup-is-settled',cache.get(refresh)is None and cache.get(file)is None and EPGSourceIndex.objects.get(source_id=sid).data is None)
  else:
   check('bounded-native-terminal-'+label,raw is not None and len(raw)<=262144);metadata=json.loads(raw);expected=f'EPG source {sid} does not exist, task cleaned up'if label in ['missing_no_orphan','missing_orphan']else None;decoded=app.AsyncResult(ident);value=decoded.get(timeout=2,propagate=False);check('actual-source-asyncresult-'+label,decoded.ready()and decoded.successful()and value==expected and metadata['result']==expected and metadata['status']=='SUCCESS'and metadata['traceback']is None)
   check('normal-metadata-geometry-'+label,metadata['task_id']==ident and 'parent_id'not in metadata and metadata['children']==[[[v['headers']['id'],None],None]for v in children]and 3590<=backend.ttl(app.backend.get_key_for_task(ident))<=3600)
   if label=='missing_orphan':check('orphan-helper-now-proven',not PeriodicTask.objects.filter(pk=period.pk).exists()and not IntervalSchedule.objects.filter(pk=interval.pk).exists())
   if label in ['file_busy','parser_busy']:
    check('native-deferral-child-'+label,len(children)==1 and children[0]['headers']['task']==TASK.name and payload(children[0])[0:2]==[[sid],{'force':False,'_file_defer_retry':1}]and children[0]['headers']['eta']is not None)
    check('native-deferral-effects-'+label,EPGSourceIndex.objects.get(source_id=sid).data is None)
   elif label in ['imported','unmapped']:
    check('native-import-child-families-'+label,[v['headers']['task']for v in children]==['apps.epg.tasks.build_programme_index_task','apps.channels.tasks.evaluate_series_rules']);check('native-import-lineage-'+label,all(v['headers']['root_id']==ident and v['headers']['parent_id']==ident and v['headers']['retries']==0 for v in children));check('native-import-sql-'+label,EPGData.objects.filter(pk=epg.pk,name='Owned native guide').exists()and ProgramData.objects.filter(epg_id=epg.pk,title='Owned native programme').count()==(1 if label=='imported'else 0)and EPGSourceIndex.objects.get(source_id=sid).data is None)
   else:check('normal-no-child-'+label,not children)
   if label in ['inactive','dummy','refresh_busy','repeat']:check('native-skipped-index-preserved-'+label,EPGSourceIndex.objects.get(source_id=sid).data=={'owned':'before'})
   check('native-refresh-lock-effect-'+label,cache.get(refresh)==(b'owned-busy'if label=='refresh_busy'else None))
  repeat=None
  if label=='repeat':
   old=raw;ttl=backend.ttl(app.backend.get_key_for_task(ident));EPGSource.objects.filter(pk=sid).update(is_active=True);time.sleep(1.05);TASK.apply_async(args=[sid],task_id=ident);repeat_audit=native(ident,sid);check('repeat-handler-reexecutes-before-success-write-guard','ResultAlreadySucceeded'in repeat_audit['outcome']and ProgramData.objects.filter(epg_id=epg.pk,title='Owned native programme').count()==1);check('repeat-preserves-result-bytes-and-ttl',backend.get(app.backend.get_key_for_task(ident))==old and backend.ttl(app.backend.get_key_for_task(ident))<=ttl-1);repeat=repeat_audit
  report['cases'].append({'name':label,'scope':'operational-safety-gap'if operational else'supported-normal','source_id':sid,'envelope':envelope,'audit':audit,'metadata':metadata,'metadata_sha256':hashlib.sha256(raw).hexdigest()if raw else None,'children':children,'repeat':repeat})
  for key in [refresh,file,counter]:cache.delete(key)
 # Known but unsupported heads must remain byte-identical and unexecuted.
 sid=owned[0]
 for label in ['foreign_index','eta','parented']:
  broker.flushdb();backend.flushdb()
  if label=='foreign_index':issued=tasks.build_programme_index_task.delay(sid)
  elif label=='eta':issued=TASK.apply_async(args=[sid],countdown=60)
  else:issued=TASK.apply_async(args=[sid],root_id=str(uuid.uuid4()),parent_id=str(uuid.uuid4()))
  audit=native(issued.id,sid,action='reject_head');check('unsupported-head-preserved-'+label,audit['head_unchanged']and audit['calls']==0 and audit['caller_released']and broker.llen('celery')==1 and backend.get(app.backend.get_key_for_task(issued.id))is None)
 broker.flushdb();backend.flushdb();env=dict(os.environ,JOB_REDIS_TEST_URL='redis://:playback-fixture-only@127.0.0.1:6379/0');resource=subprocess.run(['/evidence/native-refresh-worker','--ignored','refresh_worker::physical::','--nocapture','--test-threads=1'],env=env,text=True,capture_output=True,timeout=45);Path('/evidence/native-refresh-worker-physical.log').write_text(resource.stdout+resource.stderr);resource.check_returncode();check('five-production-constructor-polling-and-physical-resource-tests','5 passed; 0 failed'in resource.stdout);report['passed']=True
finally:
 stop()
 for key in keys:cache.delete(key)
 Channel.objects.filter(epg_data__epg_source_id__in=owned).delete();EPGSource.objects.filter(pk__in=owned).delete();PeriodicTask.objects.filter(pk__in=periodic).delete();IntervalSchedule.objects.filter(pk__in=intervals).delete();broker.flushdb();backend.flushdb();Path('/evidence/source-native-refresh-application-private.json').write_text(json.dumps(report,indent=2)+'\n')
