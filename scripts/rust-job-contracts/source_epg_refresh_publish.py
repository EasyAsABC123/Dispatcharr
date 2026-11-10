"""Native refresh publication into actual Original worker and native bounded result decoder.
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
report={'passed':False,'scope':'Native fixture refresh publication to actual Original worker and native normal-result reader; no native refresh consumer/result writer or non-refresh child execution','task':TASK.name,'hard_time_limit':TASK.time_limit,'checks':[],'cases':[],'fixture_control':{'child_routes':'all tasks except refresh_epg_data → owned-refresh-children','worker_queues':['celery'],'purpose':'capture actual generated child envelopes without executing unrelated families','task_or_helper_replacement':False}}
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
def native(action,ident,sid,args=None,kwargs=None,list_args=False,lineage=None):
 value={'action':action,'task_id':ident,'source_id':sid,'broker_url':os.environ['CELERY_BROKER_URL'],'result_url':os.environ['CELERY_RESULT_BACKEND']}
 if action!='read':value.update(args=args,kwargs=kwargs,list_args=list_args)
 if lineage:value.update(lineage)
 result=subprocess.run(['/evidence/native-refresh-publish','--ignored','--exact','refresh_publish::native_refresh_publication_peer','--nocapture'],input=json.dumps(value),text=True,capture_output=True,timeout=25)
 with Path('/evidence/native-refresh-publish-private.log').open('a')as file:file.write(result.stdout+result.stderr)
 result.check_returncode();marker='NATIVE_REFRESH_PUBLISH_JSON=';return json.loads(next(line.split(marker,1)[1]for line in result.stdout.splitlines()if marker in line))

try:
 broker.flushdb();backend.flushdb()
 for key in ['dvr:recover_dispatch_lock','core:version_check_dispatch_lock']:cache.set(key,'1',ex=3600)
 zero=dt.datetime.fromtimestamp(1800000000,dt.timezone.utc);report['source_datetime_iso']=[zero.isoformat(),zero.replace(microsecond=123456).isoformat()];check('source-microsecond-zero-fraction-format',report['source_datetime_iso']==['2027-01-15T08:00:00+00:00','2027-01-15T08:00:00.123456+00:00'])
 # Fixture authority must not silently widen production index-only constructors.
 count=broker.llen('celery');authority=native('authority',str(uuid.uuid4()),1,[1],{});check('index-only-authority-does-not-widen',authority['audit']['index_authority_rejected']and broker.llen('celery')==count)
 for ordinal,label in enumerate(['inactive_default','inactive_force_kw','inactive_force_pos','inactive_retry_pos','inactive_all_kw','dummy_default','missing_default','busy_default','exhausted','imported_default','imported_force_pos']):
  broker.flushdb();backend.flushdb();path=root/(label+'.xml');start=dt.datetime.now(dt.timezone.utc).replace(microsecond=0)+dt.timedelta(hours=1);end=start+dt.timedelta(hours=1);xml=('<tv><channel id="mapped"><display-name>Owned published guide</display-name></channel><programme channel="mapped" start="'+start.strftime('%Y%m%d%H%M%S +0000')+'" stop="'+end.strftime('%Y%m%d%H%M%S +0000')+'"><title>Owned published programme</title></programme></tv>').encode();path.write_bytes(xml)
  source=EPGSource(name='rust-refresh-publish-'+label,source_type='dummy'if label.startswith('dummy')else'xmltv',is_active=not label.startswith('inactive'),file_path=str(path),custom_properties={});EPGSource.objects.bulk_create([source]);sid=source.pk;owned.append(sid);epg=EPGData.objects.create(epg_source=source,tvg_id='mapped',name='Before guide');Channel.objects.bulk_create([Channel(name='rust-refresh-publish-'+label,channel_number=1980000+ordinal,epg_data=epg)]);EPGSourceIndex.objects.create(source_id=sid,data={'owned':'before'})
  if label.startswith('missing'):
   source.delete();interval=IntervalSchedule.objects.create(every=7929,period='seconds');intervals.append(interval.pk);period=PeriodicTask.objects.create(name='epg_source-refresh-'+str(sid),task=TASK.name,args=json.dumps([sid]),interval=interval);periodic.append(period.pk)
  refresh='task_lock_refresh_epg_data_'+str(sid);file='task_lock_epg_source_file_'+str(sid);counter=tasks._source_tvg_parse_count_key(sid);keys.extend([refresh,file,counter])
  if label.startswith('busy')or label=='exhausted':cache.set(file,'owned-busy',ex=300)
  if label.endswith('default'):args,kwargs,list_args=[sid],{},False
  elif label.endswith('force_kw'):args,kwargs,list_args=[sid],{'force':True},True
  elif label.endswith('force_pos'):args,kwargs,list_args=[sid,True],{},False
  elif label.endswith('retry_pos'):args,kwargs,list_args=[sid,True,7],{},False
  elif label.endswith('all_kw'):args,kwargs,list_args=[],{'source_id':sid,'force':False,'_file_defer_retry':2},False
  elif label=='exhausted':args,kwargs,list_args=[sid,True,480],{},False
  else:raise RuntimeError('Known vector required')
  ident=str(uuid.uuid4());published=native('publish',ident,sid,args,kwargs,list_args);check('native-durable-publication-'+label,published['audit']['published']and published['audit']['task_id']==ident and published['caller_released']and published['slots']==1 and published['drained']);raw=next(raw for raw in broker.lrange('celery',0,63)if json.loads(raw)['headers']['id']==ident);envelope=json.loads(raw);check('exact-native-body-'+label,payload(envelope)==[args,kwargs,{'callbacks':None,'errbacks':None,'chain':None,'chord':None}]);pending=native('read',ident,sid);check('native-absent-result-is-pending-'+label,pending['audit']=={'present':False}and pending['drained'])
  launch();post=wait(lambda:next((v for v in rows('epg-refresh-postrun.jsonl')if v['id']==ident),None));stop();raw_meta=backend.get(app.backend.get_key_for_task(ident));check('bounded-original-result-'+label,raw_meta is not None and len(raw_meta)<=262144);meta=json.loads(raw_meta);expected=f'EPG source {sid} does not exist, task cleaned up'if label.startswith('missing')else None;check('actual-original-success-return-'+label,post['state']=='SUCCESS'and meta['status']=='SUCCESS'and meta['result']==expected)
  read=native('read',ident,sid);record=read['audit'];check('native-bounded-result-decoder-'+label,record['present']and record['task_id']==ident and record['missing_source']==label.startswith('missing')and record['parent']is None and record['date_done']==meta['date_done']and record['children']==[c[0][0]for c in meta['children']]and read['caller_released']and read['drained'])
  check('result-ttl-'+label,3590<=backend.ttl(app.backend.get_key_for_task(ident))<=3600);children=[v for v in rows('epg-refresh-child-envelopes.jsonl')if v.get('observed')and v['observed']['envelope']['headers'].get('parent_id')==ident]
  deferred_audit=None
  if label.startswith('busy'):
   check('actual-deferred-child-'+label,len(children)==1 and children[0]['sender']==TASK.name and payload(children[0]['observed']['envelope'])[0:2]==[[sid],{'force':False,'_file_defer_retry':1}]);check('deferred-effects-'+label,EPGSourceIndex.objects.get(source_id=sid).data is None and cache.get(file)==b'owned-busy')
   child=children[0]['observed']['envelope'];child_id=child['headers']['id'];queued=[raw for raw in broker.lrange('celery',0,63)if json.loads(raw)['headers']['id']==child_id];check('owned-original-child-queued',len(queued)==1);check('remove-only-owned-child-for-native-republication',broker.lrem('celery',1,queued[0])==1);check('clear-only-owned-file-blocker',cache.eval("if redis.call('GET',KEYS[1])==ARGV[1]then return redis.call('DEL',KEYS[1])end;return 0",1,file,'owned-busy')==1)
   child_body=payload(child);pub=native('publish_deferred',child_id,sid,child_body[0],child_body[1],True,{'root_id':child['headers']['root_id'],'parent_id':child['headers']['parent_id'],'eta':child['headers']['eta']});native_child=json.loads(next(raw for raw in broker.lrange('celery',0,63)if json.loads(raw)['headers']['id']==child_id));check('native-deferred-published-context',pub['audit']['published']and native_child['headers']['root_id']==ident and native_child['headers']['parent_id']==ident and native_child['headers']['eta']==child['headers']['eta']and native_child['headers']['retries']==0)
   launch();child_post=wait(lambda:next((v for v in rows('epg-refresh-postrun.jsonl')if v['id']==child_id),None),seconds=30);stop();child_raw=backend.get(app.backend.get_key_for_task(child_id));check('bounded-native-deferred-source-result',child_raw is not None and len(child_raw)<=262144);child_meta=json.loads(child_raw);decoded_child=native('read',child_id,sid);check('native-deferred-consumed-and-decoded',child_post['state']=='SUCCESS'and child_meta['result']is None and child_meta['parent_id']==ident and decoded_child['audit']['parent']==ident and decoded_child['audit']['children']==[c[0][0]for c in child_meta['children']]);check('native-deferred-real-import-sql',ProgramData.objects.filter(epg_id=epg.pk,title='Owned published programme').count()==1 and EPGData.objects.filter(pk=epg.pk,name='Owned published guide').exists());check('native-deferred-parent-record-preserved',backend.get(app.backend.get_key_for_task(ident))==raw_meta)
   grand=[v for v in rows('epg-refresh-child-envelopes.jsonl')if v.get('observed')and v['observed']['envelope']['headers'].get('parent_id')==child_id];check('native-deferred-grandchild-lineage',[v['sender']for v in grand]==['apps.epg.tasks.build_programme_index_task','apps.channels.tasks.evaluate_series_rules']and all(v['observed']['envelope']['headers']['root_id']==ident for v in grand));check('native-deferred-file-lock-released',cache.get(file)is None)
   deferred_audit={'fixture_control':'Remove only owned queued Source child and owned file blocker, then republish same ID/root/parent/actual15s ETA using native helper; Source worker consumes it.','publication':pub,'envelope':native_child,'metadata':child_meta,'native_result':decoded_child,'postrun':child_post,'grandchildren':grand}

  elif label.startswith('imported'):
   check('actual-imported-child-families-'+label,[v['sender']for v in children]==['apps.epg.tasks.build_programme_index_task','apps.channels.tasks.evaluate_series_rules']);check('actual-imported-sql-'+label,EPGData.objects.filter(pk=epg.pk,name='Owned published guide').exists()and ProgramData.objects.filter(epg_id=epg.pk,title='Owned published programme').count()==1 and EPGSourceIndex.objects.get(source_id=sid).data is None)
  else:check('no-child-'+label,not children)
  if label.startswith('missing'):check('orphan-cleanup-is-original-worker',not PeriodicTask.objects.filter(pk=period.pk).exists()and not IntervalSchedule.objects.filter(pk=interval.pk).exists())
  if label=='exhausted':check('actual-exhausted-status',EPGSource.objects.get(pk=sid).status==EPGSource.STATUS_ERROR)
  if label.startswith('inactive')or label.startswith('dummy'):check('skipped-index-retained-'+label,EPGSourceIndex.objects.get(source_id=sid).data=={'owned':'before'})
  check('original-refresh-lease-released-'+label,cache.get(refresh)is None)
  report['cases'].append({'name':label,'source_id':sid,'native_publication':published,'envelope':envelope,'metadata':meta,'raw_metadata_sha256':hashlib.sha256(raw_meta).hexdigest(),'native_result':read,'postrun':post,'children':children,'result_ttl':backend.ttl(app.backend.get_key_for_task(ident)),'native_deferred':deferred_audit})
  for key in [refresh,file,counter]:cache.delete(key)
 check('no-nonrefresh-family-executed',not rows('epg-refresh-unexpected-tasks.jsonl'))
 broker.flushdb();backend.flushdb()
 environment=dict(os.environ,JOB_REDIS_TEST_URL='redis://:playback-fixture-only@127.0.0.1:6379/0');physical=subprocess.run(['/evidence/native-refresh-publish','--ignored','refresh_publish::physical::','--nocapture','--test-threads=1'],env=environment,text=True,capture_output=True,timeout=40);Path('/evidence/native-refresh-physical.log').write_text(physical.stdout+physical.stderr);physical.check_returncode();check('three-actual-physical-reply-tests','3 passed; 0 failed'in physical.stdout);report['passed']=True
finally:
 stop()
 for key in keys:cache.delete(key)
 Channel.objects.filter(epg_data__epg_source_id__in=owned).delete();EPGSource.objects.filter(pk__in=owned).delete();PeriodicTask.objects.filter(pk__in=periodic).delete();IntervalSchedule.objects.filter(pk__in=intervals).delete();broker.flushdb();backend.flushdb();Path('/evidence/source-epg-refresh-publish-private.json').write_text(json.dumps(report,indent=2)+'\n')
