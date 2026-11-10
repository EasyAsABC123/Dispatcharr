"""Actual Original refresh defaults/imported effects and child wire; Source only.
Original Dispatcharr authors retain full application/algorithm credit and AGPL.
An explicit fixture router diverts children into an owned unconsumed queue.
"""
import base64,datetime as dt,hashlib,json,os,signal,subprocess,sys,time
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
try:
 broker.flushdb();backend.flushdb()
 for key in ['dvr:recover_dispatch_lock','core:version_check_dispatch_lock']:cache.set(key,'1',ex=3600)
 for ordinal,label in enumerate(['inactive_default','inactive_force_kw','inactive_force_pos','inactive_retry_pos','inactive_all_kw','busy_default','busy_retry_pos','imported_default','imported_force_pos']):
  broker.flushdb();backend.flushdb()
  path=root/(label+'.xml');start=dt.datetime.now(dt.timezone.utc).replace(microsecond=0)+dt.timedelta(hours=1);end=start+dt.timedelta(hours=1)
  xml=('<tv><channel id="mapped"><display-name>Owned refreshed guide</display-name></channel><programme channel="mapped" start="'+start.strftime('%Y%m%d%H%M%S +0000')+'" stop="'+end.strftime('%Y%m%d%H%M%S +0000')+'"><title>Owned refresh programme</title></programme></tv>').encode();path.write_bytes(xml)
  source=EPGSource(name='rust-refresh-import-'+label,source_type='xmltv',is_active=not label.startswith('inactive'),file_path=str(path),custom_properties={});EPGSource.objects.bulk_create([source]);sid=source.pk;owned.append(sid)
  epg=EPGData.objects.create(epg_source=source,tvg_id='mapped',name='Before guide');Channel.objects.bulk_create([Channel(name='rust-refresh-import-'+label,channel_number=1900000+ordinal,epg_data=epg)])
  EPGSourceIndex.objects.create(source_id=sid,data={'owned':'before'})
  refresh_key='task_lock_refresh_epg_data_'+str(sid);file_key='task_lock_epg_source_file_'+str(sid);counter_key=tasks._source_tvg_parse_count_key(sid);keys.extend([refresh_key,file_key,counter_key])
  if label.startswith('busy'):cache.set(file_key,'owned-busy',ex=300)
  issued=send(label,sid);raw=next(raw for raw in broker.lrange('celery',0,63)if json.loads(raw)['headers']['id']==issued.id);envelope=json.loads(raw);body=payload(envelope)
  if label.endswith('default'):expected_args,expected_kwargs=[sid],{}
  elif label.endswith('force_kw'):expected_args,expected_kwargs=[sid],{'force':True}
  elif label.endswith('force_pos'):expected_args,expected_kwargs=[sid,True],{}
  elif label.endswith('retry_pos'):expected_args,expected_kwargs=[sid,True,7],{}
  elif label.endswith('all_kw'):expected_args,expected_kwargs=[],{'source_id':sid,'force':False,'_file_defer_retry':2}
  else:raise RuntimeError('Known input vector required')
  check('exact-published-input-'+label,body==[expected_args,expected_kwargs,{'callbacks':None,'errbacks':None,'chain':None,'chord':None}])
  launch();post=wait(lambda:next((r for r in rows('epg-refresh-postrun.jsonl')if r['id']==issued.id),None));stop()
  raw_result=backend.get(app.backend.get_key_for_task(issued.id));check('bounded-result-'+label,raw_result is not None and len(raw_result)<=262144);meta=json.loads(raw_result)
  check('success-null-'+label,post['state']=='SUCCESS'and meta['status']=='SUCCESS'and meta['result']is None)
  check('parent-initial-lineage-'+label,envelope['headers']['root_id']==issued.id and envelope['headers']['parent_id']is None and envelope['headers']['retries']==0 and envelope['headers']['timelimit']==[14400,None])
  observed=[row for row in rows('epg-refresh-child-envelopes.jsonl')if row.get('observed')and row['observed']['envelope']['headers'].get('parent_id')==issued.id]
  children=[row['observed']['envelope']for row in observed];index=EPGSourceIndex.objects.filter(source_id=sid).values_list('data',flat=True).first();source.refresh_from_db()
  if label.startswith('inactive'):
   check('inactive-no-children-'+label,not children and meta['children']==[])
   check('inactive-retains-index-'+label,index=={'owned':'before'})
  elif label.startswith('busy'):
   check('deferred-one-child-'+label,len(children)==1 and children[0]['headers']['task']==TASK.name)
   child=children[0];expected_force=label.endswith('retry_pos');expected_retry=8 if expected_force else 1
   check('deferred-defaults-'+label,payload(child)==[[sid],{'force':expected_force,'_file_defer_retry':expected_retry},{'callbacks':None,'errbacks':None,'chain':None,'chord':None}])
   delay=(dt.datetime.fromisoformat(child['headers']['eta'])-dt.datetime.fromisoformat(observed[0]['utc'])).total_seconds();check('deferred-countdown-'+label,13<=delay<=16)
   check('deferred-invalidates-index-'+label,index is None and cache.get(file_key)==b'owned-busy')
  else:
   expected=['apps.epg.tasks.build_programme_index_task','apps.channels.tasks.evaluate_series_rules']
   check('imported-two-child-families-'+label,[c['headers']['task']for c in children]==expected)
   check('imported-child-args-'+label,payload(children[0])[0:2]==[[sid],{}]and payload(children[1])[0:2]==[[],{}])
   check('imported-routing-control-'+label,all(c['properties']['delivery_info']['routing_key']==CHILD_QUEUE for c in children)and broker.llen(CHILD_QUEUE)==2)
   check('imported-real-sql-'+label,EPGData.objects.filter(pk=epg.pk,name='Owned refreshed guide').exists()and ProgramData.objects.filter(epg_id=epg.pk,title='Owned refresh programme').count()==1 and source.status=='success')
   check('imported-index-not-built-'+label,index is None)
  if children:
   check('child-lineage-'+label,all(c['headers']['root_id']==issued.id and c['headers']['parent_id']==issued.id and c['headers']['retries']==0 for c in children))
   check('result-child-identities-'+label,[c[0][0]for c in meta['children']]==[c['headers']['id']for c in children])
  check('refresh-released-'+label,cache.get(refresh_key)is None)
  check('result-ttl-'+label,3590<=backend.ttl(app.backend.get_key_for_task(issued.id))<=3600)
  report['cases'].append({'name':label,'source_id':sid,'envelope':envelope,'body':body,'metadata':meta,'raw_result_sha256':hashlib.sha256(raw_result).hexdigest(),'result_ttl':backend.ttl(app.backend.get_key_for_task(issued.id)),'postrun':post,'children':observed,'index':index,'source_status':source.status,'source_message':source.last_message,'xml_base64':base64.b64encode(xml).decode(),'programmes':list(ProgramData.objects.filter(epg_id=epg.pk).values('title','start_time','end_time')),'locks':{'refresh':cache.get(refresh_key).decode()if cache.get(refresh_key)else None,'file':cache.get(file_key).decode()if cache.get(file_key)else None}})
  for key in [refresh_key,file_key,counter_key]:cache.delete(key)
 request={'database_url':'postgres://dispatch:playback-fixture-only@127.0.0.1:5432/'+database,'redis_url':'redis://:playback-fixture-only@127.0.0.1:6379/0','source_ids':[c['source_id']for c in report['cases']if c['name'].startswith('imported')]}
 peer=subprocess.run(['/evidence/native-refresh-dispatch','--ignored','--exact','per_execution_refresh_trace_and_cancelled_physical_reply_are_retained','--nocapture'],input=json.dumps(request),text=True,capture_output=True,timeout=40)
 Path('/evidence/native-refresh-dispatch-private.log').write_text(peer.stdout+peer.stderr);peer.check_returncode();marker='NATIVE_REFRESH_DISPATCH_JSON=';audit=json.loads(next(line.split(marker,1)[1]for line in peer.stdout.splitlines()if marker in line));report['native_dispatch_audit']=audit
 check('native-default-and-concurrent-parent-trace',audit['default_preserved']and audit['concurrent_overrides_isolated']and len(audit['events'])==4)
 check('native-cancelled-physical-reply-owner-drain',audit['caller_released_after_reply']and audit['runner_drained']and audit['physical_jobs']==0)
 check('native-refresh-locks-and-foreign-file-owners',audit['refresh_locks_released']and audit['foreign_file_locks_preserved'])
 check('native-imported-local-callbacks',audit['imported_callbacks_local'])
 check('no-child-family-executed',not rows('epg-refresh-unexpected-tasks.jsonl'))
 report['passed']=True
finally:
 stop()
 for key in keys:cache.delete(key)
 Channel.objects.filter(epg_data__epg_source_id__in=owned).delete();EPGSource.objects.filter(pk__in=owned).delete()
 broker.flushdb();backend.flushdb()
 Path('/evidence/source-epg-refresh-native-private.json').write_text(json.dumps(report,indent=2,default=str)+'\n')
