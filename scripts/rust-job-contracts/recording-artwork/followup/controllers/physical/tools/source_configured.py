"""UNEXECUTED Original producer/Native local-VODLogo artwork paired corpus.
Actual tasks/models/signals; fixed fixture queues isolate known roles without
removing/quarantining any default descendant. No Native/defaultFIFO claim.
"""
import base64,datetime,fcntl,hashlib,json,os,signal,sys,time,uuid
from datetime import timedelta
from bounded_process import Collected
from interactive_peer import Peer
from pathlib import Path
if os.environ.get('JOB_DISPOSABLE_FIXTURE')!='playback-fixture-only'or not os.environ.get('POSTGRES_DB','').startswith('rust_probe_backup_jobs_'):raise ValueError('OwnedCopiedFixtureRequired')
sys.path[:0]=['/app','/job-tools'];sys.argv.append('shell');os.environ.setdefault('DJANGO_SETTINGS_MODULE','dispatcharr.settings')
from observe import app
import django;django.setup()
import redis
from django.conf import settings as django_settings
from django.db import connection
from django.utils import timezone
from django.core.cache import cache as django_cache
from channels.layers import get_channel_layer
from asgiref.sync import async_to_sync
from core.models import CoreSettings
from apps.epg.models import EPGSource,EPGData,ProgramData,EPGSourceIndex
from apps.epg import tasks as epg_tasks
from apps.channels.models import Channel,Recording,RecurringRecordingRule
from apps.vod.models import Movie,VODLogo
from django_celery_beat.models import PeriodicTask,ClockedSchedule,PeriodicTasks
from ledger_io import append_receipt
from owned_worker import OwnedWorker
from seed_spec import *
with connection.cursor()as copied_guard:
 copied_guard.execute('SELECT current_database()');actual_database=copied_guard.fetchone()[0]
 if actual_database!=os.environ['POSTGRES_DB']:raise ValueError('ActualCopiedDatabaseRequiredBeforeSeed')
ROOT=Path('/evidence');worker=None;known=[];owned_dir=None;native_publications=[];native_job=None
broker=redis.Redis.from_url(os.environ['CELERY_BROKER_URL']);backend=redis.Redis.from_url(os.environ['CELERY_RESULT_BACKEND']);cache=redis.Redis(host='127.0.0.1',port=6379,password='playback-fixture-only',db=0)
if broker.connection_pool.connection_kwargs['db']!=8 or backend.connection_pool.connection_kwargs['db']!=9 or broker.dbsize()or backend.dbsize():raise ValueError('FreshOwnedRedis8And9')
report={'lifecycle_began_utc':json.loads((ROOT/'controller-context.json').read_text())['lifecycle_began_utc'],'passed':False,'Source_verified':False,'Source_reference_verified':True,'Native_verified':False,'performance_verified':False,'scope':'Actual Original Refresh and parented Series producer -> Native local-VODLogo artwork + Original AsyncResult; owned routes only, default descendants untouched; no defaultFIFO/Main/provider claim','cases':[],'checks':[],'worker_exit_receipts':[],'predeclared_cases':CASES,'application_task_model_signal_methods_replaced':False,'HTTP_dependency_guard_installed':True,'fixture_routes':ROUTES,'future_Beat_run_recording_consumed':False,'actual_database':actual_database}
def utc():return timezone.now().isoformat()
def write_report():
 data=(json.dumps(report,indent=2,default=str)+'\n').encode()
 if len(data)>33554432:raise ValueError('SourceReport32MiBCapacity')
 temporary=ROOT/'source-parented-recordings-private.json.pending'
 with temporary.open('wb')as file:file.write(data);file.flush();os.fsync(file.fileno())
 os.replace(temporary,ROOT/'source-parented-recordings-private.json')
def read(kind):
 path=ROOT/('parented-'+kind+'.jsonl')
 if not path.exists():return[]
 if path.stat().st_size>16777216:raise ValueError('OwnedLedger16MiBCapacity')
 with(ROOT/'parented-ledger.lock').open('a+b')as lock:
  fcntl.flock(lock.fileno(),fcntl.LOCK_SH);return[json.loads(line)for line in path.read_text().splitlines()]
def rows(model,query=None):
 result=list((query if query is not None else model.objects.order_by('pk')).values()[:257])
 if len(result)>256:raise ValueError('OwnedSQLDiagnosticRowCapacity')
 return result

def raw_result(ident):
 key=app.backend.get_key_for_task(ident);raw=backend.get(key)
 if raw is not None and len(raw)>1048576:raise ValueError('OwnedResultDiagnosticCapacity')
 return {'key_base64':base64.b64encode(key).decode(),'raw_base64':None if raw is None else base64.b64encode(raw).decode(),'metadata':None if raw is None else json.loads(raw),'ttl':backend.ttl(key)}
def raw_redis(client):
 result=[];total=0
 # Small owned-fixture observation; SCAN COUNT is not a strict wire/CPU bound.
 for key in client.scan_iter(count=32):
  if len(result)>=256:raise ValueError('OwnedRedisDiagnosticKeyCapacity')
  kind=client.type(key);dump=client.dump(key);total+=len(key)+(len(dump)if dump else 0)
  if total>2097152:raise ValueError('OwnedRedisDiagnostic2MiBCapacity')
  row={'key_base64':base64.b64encode(key).decode(),'type':kind.decode(),'DUMP_base64':None if dump is None else base64.b64encode(dump).decode(),'ttl':client.ttl(key)}
  if kind==b'string':row['value_base64']=base64.b64encode(client.get(key)).decode()
  elif kind==b'list':
   value=client.lrange(key,0,64)
   if len(value)>64:raise ValueError('OwnedListDiagnostic64Capacity')
   row['list_base64']=[base64.b64encode(v).decode()for v in value]
  elif kind==b'zset':
   value=client.zrange(key,0,64,withscores=True)
   if len(value)>64:raise ValueError('OwnedZsetDiagnostic64Capacity')
   row['zset']=[{'member_base64':base64.b64encode(v).decode(),'score':score}for v,score in value]
  result.append(row)
 return result

def files():
 result=[]
 if owned_dir:
  for path in sorted(owned_dir.iterdir()):
   if path.is_file():
    stat=path.stat();data=path.read_bytes()
    if len(data)>1048576:raise ValueError('OwnedFileDiagnosticCapacity')
    result.append({'path':str(path),'bytes':len(data),'sha256':hashlib.sha256(data).hexdigest(),'mtime_ns':stat.st_mtime_ns,'mode':stat.st_mode,'uid':stat.st_uid,'gid':stat.st_gid,'readable':os.access(path,os.R_OK)})
 return result

def snapshot():
 queues={}
 for key in ['celery',*QUEUES.values()]:
  value=broker.lrange(key,0,64)
  if len(value)>64 or sum(map(len,value))>1048576:raise ValueError('OwnedQueueDiagnosticCapacity')
  queues[key]=[base64.b64encode(v).decode()for v in value]
 value={'utc':utc(),'channels':rows(Channel),'sources':rows(EPGSource),'epg_data':rows(EPGData),'programmes':rows(ProgramData),'source_indices':rows(EPGSourceIndex),'recordings':rows(Recording),'recurring_rules':rows(RecurringRecordingRule),'periodic':rows(PeriodicTask),'clocked':rows(ClockedSchedule),'sentinel':rows(PeriodicTasks),'core_settings':rows(CoreSettings),'VOD_movies':rows(Movie),'VOD_logos':rows(VODLogo),'queues':queues,'broker':raw_redis(broker),'backend':{ident:raw_result(ident)for ident in known},'result_backend_raw':raw_redis(backend),'cache':raw_redis(cache),'Channels_raw_base64':[base64.b64encode(v).decode()for v in cache.zrange(channel_key,0,64)],'Channels_decoded':[layer.deserialize(v)for v in cache.zrange(channel_key,0,64)],'files':files()}
 value=json.loads(json.dumps(value,default=str))
 with(ROOT/'parented-ledger.lock').open('a+b')as lock:
  fcntl.flock(lock.fileno(),fcntl.LOCK_EX);receipt=append_receipt(ROOT/'parented-snapshots.jsonl',value)
 if not receipt['full_record_retained']:raise ValueError('SnapshotLedgerCapacity')
 return value

def check(name,value):
 report['checks'].append({'name':name,'passed':bool(value)});write_report()
 if not value:raise AssertionError(name)
def wait(predicate):
 deadline=time.monotonic()+35
 while time.monotonic()<deadline:
  result=predicate()
  if result:return result
  if worker is not None and worker.process.poll()is not None:raise RuntimeError('SourceWorkerExitedBeforeExpectedPostrun')
  time.sleep(.01)
 raise TimeoutError('SourceFixedTask35sDeadline')
def stop_worker():
 global worker
 if worker is None:return
 value=worker.close();report['worker_exit_receipts'].append(value);worker=None;write_report()
 if not(value['actual_exit']==0 and value['actual_wait_completed']and value['pipes_settled']and value['all_observed_owned_PIDs_absent']and value['invalid']is None and not value['forced']):raise RuntimeError('ActualSourceWorkerNotCleanlySettled')
def phase(name,role,ids):
 global worker
 case={'name':name,'role':role,'ids':ids,'began_utc':utc(),'before':snapshot()};report['cases'].append(case);write_report()
 validate_role_frames(case['before']['queues'][QUEUES[role]],role,ids)
 worker=OwnedWorker(role,ROOT,dict(os.environ),name);case['worker_start']={'pid':worker.pid,'starttime':worker.starttime,'actual_initial_pgid':worker.actual_initial_pgid,'argv':worker.argv};write_report()
 # Every queued identity is observed before assertions; no instant-ACK claim.
 for ident in ids:wait(lambda:any(v['id']==ident for v in read('postrun')))
 stop_worker();case['finished_utc']=utc();case['after']=snapshot();case['tasks']=[]
 for ident in ids:
  known.append(ident)if ident not in known else None
  result=app.AsyncResult(ident);entry={'id':ident,'result':raw_result(ident),'Original_get_value':result.get(timeout=2,propagate=False),'Original_get_type':None,'prerun':[v for v in read('prerun')if v['id']==ident],'postrun':[v for v in read('postrun')if v['id']==ident],'sql':[v for v in read('sql')if v['id']==ident],'publications':[v for v in read('publications')if v['headers'].get('id')==ident or v['headers'].get('parent_id')==ident]};entry['Original_get_type']=type(entry['Original_get_value']).__module__+'.'+type(entry['Original_get_value']).__name__;case['tasks'].append(entry)
 case['observed_all_publications']=read('publications');case['HTTP_attempts']=read('HTTP-attempts');write_report()
 check(name+'-all-results-SUCCESS',all(t['result']['metadata']is not None and t['result']['metadata']['status']=='SUCCESS'and t['result']['metadata']['traceback']is None for t in case['tasks']))
 check(name+'-all-default-descendants-preserved',all(v in case['after']['queues']['celery']for v in case['before']['queues']['celery']))
 check(name+'-role-queue-empty',case['after']['queues'][QUEUES[role]]==[])
 check(name+'-zero-HTTP-dependency-attempts',case['HTTP_attempts']==[])
 return case

def task_ids(task,parent=None):
 source=[v['headers']['id']for v in read('publications')if v['task']==task and(parent is None or v['headers'].get('parent_id')==parent)]
 native=[v['frame']['headers']['id']for v in native_publications if v['frame']['headers']['task']==task and(parent is None or v['frame']['headers'].get('parent_id')==parent)]
 return source+native

def physical_endpoint_config(mode,base):
 value=dict(base)
 if mode in ('recording-read-Ready','update-Ready','update-error-Ready','fresh-status-merge'):
  value['database_url']='postgres://dispatch:playback-fixture-only@127.0.0.1:61651/'+os.environ['POSTGRES_DB']
 if mode=='Channels-reply':value['events_url']='redis://:playback-fixture-only@127.0.0.1:61603/0'
 if mode=='result-reply':
  value['broker_url']='redis://:playback-fixture-only@127.0.0.1:61603/8'
  value['result_url']='redis://:playback-fixture-only@127.0.0.1:61603/9'
 return value

# Unexecuted real first-child ownership cases. Actual fresh Original parents are
# produced by the unchanged seed/phase helpers before this function is called.
def physical_artwork(mode,ident):
 global native_job
 from interactive_peer import Peer
 from pg_reply_proxy import Proxy as PGProxy
 from redis_reply_proxy import Proxy as RedisProxy
 source_before=snapshot();raws=[r for r in source_before['queues'][QUEUES['artwork']]if json.loads(base64.b64decode(r,validate=True))['headers']['id']==ident]
 if len(raws)!=1:raise ValueError('OneActualOwnedChildFrame')
 raw=raws[0];frame=json.loads(base64.b64decode(raw,validate=True));parent=frame['headers']['parent_id'];root=frame['headers']['root_id'];previous=report['cases'][:]
 def exactly_one(values,label):
  if len(values)!=1:raise ValueError(label)
  return values[0]
 parent_task=exactly_one([t for c in previous for t in c['tasks']if t['id']==parent],'ActualCompletedSeriesParent')
 root_task=exactly_one([t for c in previous for t in c['tasks']if t['id']==root],'ActualCompletedRefreshRoot')
 series_frame=exactly_one([r for c in previous for r in c['before']['queues'][QUEUES['series']]if json.loads(base64.b64decode(r,validate=True))['headers']['id']==parent],'ActualSeriesFrame')
 refresh_frame=exactly_one([r for c in previous for r in c['before']['queues'][QUEUES['refresh']]if json.loads(base64.b64decode(r,validate=True))['headers']['id']==root],'ActualRefreshFrame')
 case={'name':mode,'role':'Native-first-artwork-physical','id':ident,'began_utc':utc(),'Source_before':source_before,'tasks':[],'observations':[],'checks':[],'native_process':{'spawn_state':'not_started','settled':False},'no_Source_query_or_prerun_fabricated':True};report['cases'].append(case);write_report()
 pg=None;reply=None;closed=None;proxy_receipts={};responses=[];started=time.monotonic();deadline=started+90;case['case_deadline_seconds']=90
 config={'broker_url':os.environ['CELERY_BROKER_URL'],'result_url':os.environ['CELERY_RESULT_BACKEND'],'events_url':'redis://:playback-fixture-only@127.0.0.1:6379/0','database_url':'postgres://dispatch:playback-fixture-only@127.0.0.1:5432/'+os.environ['POSTGRES_DB'],'refresh_frame_base64':refresh_frame,'series_frame_base64':series_frame,'artwork_frame_base64':raw,'refresh_backend_base64':root_task['result']['raw_base64'],'series_backend_base64':parent_task['result']['raw_base64']}
 def retain(label):
  capture={'label':label,'utc':utc(),'monotonic':time.monotonic(),'snapshot':snapshot(),'backend':raw_result(ident)}
  if native_job is not None:
   with native_job.lock:capture['stdout_base64']=base64.b64encode(bytes(native_job.output['stdout'])).decode();capture['stderr_base64']=base64.b64encode(bytes(native_job.output['stderr'])).decode();capture['collector_invalid']=native_job.invalid
  if pg is not None:
   with pg.lock:capture['PG_timeline']=list(pg.timeline);capture['PG_frames']=list(pg.frames);capture['PG_errors']=list(pg.errors)
  if reply is not None:
   with reply.lock:capture['RESP_receipts']=list(reply.receipts);capture['RESP_errors']=list(reply.errors)
  case['observations'].append(capture);write_report();return capture
 def assert_case(label,value):
  retain('before-assert:'+label);case['checks'].append({'name':label,'passed':value is True});write_report()
  if value is not True:raise AssertionError(label)
 def receive(label,seconds=15):
  value=native_job.receive(min(seconds,max(.01,deadline-time.monotonic())));responses.append({'label':label,'utc':utc(),'monotonic':time.monotonic(),'value':value});case['responses']=responses;write_report();return value
 def command(action,seconds=15):native_job.send({'action':action});return receive(action,seconds)
 def wait_gate(event,label):
  reached=event.wait(min(15,max(.01,deadline-time.monotonic())));observed=time.monotonic();capture=retain(label);assert_case(label,reached)
  if label=='actual-target-reply-held':
   observed=(pg if pg is not None else reply).held_at;assert_case('actual-proxy-held-clock-retained',type(observed)is float and observed<=time.monotonic())
  return observed
 def admin(statement,params=None):
  with connection.cursor()as c:
   c.execute('SELECT current_database()')
   if c.fetchone()[0]!=os.environ['POSTGRES_DB']:raise ValueError('ActualCopiedDBOnly')
   c.execute(statement,params)
 def stop_native():
  if native_job is None:return
  if native_job.poll()is None:native_job.process.terminate()
  try:native_job.process.wait(timeout=12)
  except __import__('subprocess').TimeoutExpired:native_job.process.kill();native_job.process.wait(timeout=5)
 try:
  if mode in ('recording-read-Ready','update-Ready','update-error-Ready','fresh-status-merge'):
   pg=PGProxy('movie-execution-Ready'if mode=='fresh-status-merge'else mode)
  if mode in ('Channels-reply','result-reply'):
   policy=json.loads(Path('/job-tools/redis-policy.json').read_text());reply=RedisProxy('events'if mode=='Channels-reply'else'result',ident,policy,policy['known_channel_key'].encode())
  if mode=='oldSUCCESS-preserved':
   old={'status':'SUCCESS','result':'owned-old-artwork-result','traceback':None,'children':[],'date_done':utc(),'task_id':ident,'parent_id':parent};payload=json.dumps(old,separators=(',',':')).encode();backend.setex(app.backend.get_key_for_task(ident),600,payload);case['old_metadata_before']=raw_result(ident)
  config=physical_endpoint_config(mode,config)
  case['before_native']=snapshot();write_report()
  delay=max(0,(datetime.datetime.fromisoformat(frame['headers']['eta'])-datetime.datetime.now(datetime.timezone.utc)).total_seconds());case['actual_ETA_wait']={'actual_eta':frame['headers']['eta'],'seconds':delay}
  if delay>2:raise ValueError('ActualCapturedETAWait2s')
  time.sleep(delay)
  argv=['/native-parented-peer','recording_artwork_peer::actual_local_artwork_peer','--exact','--ignored','--nocapture','--test-threads=1'];native_job=Peer(argv,dict(os.environ),stop_native)
  case['native_process'].update(spawn_state='actual_child_started',argv=argv,pid=native_job.process.pid,starttime=Path('/proc',str(native_job.process.pid),'stat').read_text().rsplit(')',1)[1].split()[19]);native_job.send(config);prepared=receive('configure');assert_case('actual-child-capability-prepared',prepared.get('prepared')is True and prepared.get('task_id')==ident)
  if mode=='oldSUCCESS-preserved':
   value=command('run',35);assert_case('actual-oldSUCCESS-no-overwrite-result',value['typed_result']=={'state':'AlreadySucceeded','task_id':ident,'changed':True});after=raw_result(ident);assert_case('old-metadata-bytes-TTL-preserved',after['raw_base64']==case['old_metadata_before']['raw_base64']and 1<=after['ttl']<=case['old_metadata_before']['ttl'])
  else:
   value=command('start');assert_case('actual-caller-started',value.get('started')is True)
   if mode=='update-error-Ready':
    wait_gate(pg.before_insert,'actual-UPDATE-Parse-before-forward')
    admin("CREATE FUNCTION rust_owned_artwork_fault() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN RAISE EXCEPTION 'owned artwork post-admission fault' USING ERRCODE='P0001'; END$$")
    admin('CREATE TRIGGER rust_owned_artwork_fault BEFORE UPDATE ON dispatcharr_channels_recording FOR EACH ROW EXECUTE FUNCTION rust_owned_artwork_fault()');pg.insert_release.set();retain('real-owned-trigger-installed-after-admission-before-Parse-forward')
   gate=pg.held if pg is not None else reply.held;reached=wait_gate(gate,'actual-target-reply-held')
   if mode=='fresh-status-merge':
    rec=Recording.objects.get(pk=frame['headers'].get('recording_id',json.loads(base64.b64decode(frame['body']))[0][0]));props=dict(rec.custom_properties);props['status']='recording';props['owned_current_row_marker']='preserved';admin('UPDATE dispatcharr_channels_recording SET custom_properties=%s::jsonb WHERE id=%s',[json.dumps(props),rec.id]);retain('separate-owned-current-status-written')
   # Sleep only to the fixed gate-relative point. No polling-until-green retries.
   time.sleep(max(0,reached+2.2-time.monotonic()));state=command('poll');assert_case('physical-owner-and-slot-held',state['caller_released']is False and state['available_slots']==0)
   if mode=='result-reply':
    assert_case('logical-result-error-may-complete-before-physical',state['caller_pending']is False);logical=command('wait');assert_case('actual-ambiguous-result-not-normal', 'AmbiguousResult'in logical['wait'])
   else:
    assert_case('SQL-or-service-caller-still-pending',state['caller_pending']is True)
    if mode not in ('update-error-Ready','Channels-reply'):
     aborted=command('abort');assert_case('observer-abort-request-recorded',aborted.get('abort_requested')is True)
   if mode=='update-error-Ready':
    # Two observations from the SAME actual peer after its real ErrorResponse
    # gate. Do not infer physical retention from one instantaneous scheduling
    # point, and do not delay the predeclared gate-relative6.2s release target.
    with pg.lock:subjects=[dict(v)for v in pg.timeline if v['stage']=='actual-matching-Ready-held']
    if len(subjects)!=1:raise ValueError('OneActualErrorGateSubject')
    subject=subjects[0];samples=[];case['post_error_held_samples']=samples
    for sample in range(2):
     if sample:
      time.sleep(max(0,samples[0]['sampled_monotonic']+.25-time.monotonic()));sampled=command('poll')
     else:sampled=state
     response=case['responses'][-1]
     observation=retain('post-error-held-ownership-'+str(sample+1))
     entry={'sampled_monotonic':response['monotonic'],'sampled_utc':response['utc'],'response':sampled,'peer_pid':case['native_process']['pid'],'peer_starttime':case['native_process']['starttime'],'gate_backend_pid':subject['backend_pid'],'gate_connection':subject['connection'],'gate_monotonic':subject['monotonic'],'proxy_release_not_requested':not pg.release.is_set(),'backend_raw_base64':observation['backend']['raw_base64']};samples.append(entry);write_report()
     assert_case('actual-same-peer-error-owner-sample-'+str(sample+1),sampled['caller_pending']is True and sampled['caller_released']is False and type(sampled['available_slots'])is int and sampled['available_slots']==0 and entry['proxy_release_not_requested']is True and entry['backend_raw_base64']is None)
   native_job.send({'action':'drain'});assert_case('drain-no-early-reply',native_job.poll_response(.05)is None)
   hold=retain('held-before-fixed-release')
   if mode=='recording-read-Ready':assert_case('preACK-raw-frame-retained',hold['snapshot']['queues'][QUEUES['artwork']]==case['before_native']['queues'][QUEUES['artwork']])
   if mode in ('recording-read-Ready','update-Ready','update-error-Ready','Channels-reply','fresh-status-merge'):assert_case('no-terminal-metadata-before-physical-settlement',hold['backend']['raw_base64']is None)
   time.sleep(max(0,reached+6.2-time.monotonic()));case['actual_gate_release_request']={'utc':utc(),'monotonic':time.monotonic(),'gate_reached_monotonic':reached,'fixed_hold_seconds':6.2};write_report()
   (pg.release if pg is not None else reply.release).set();drained=receive('drain-completed',15);assert_case('drain-only-after-physical-release',drained['drained']is True and drained['caller_released']is True)
   if mode!='result-reply':
    waited=command('wait')
    if mode=='update-error-Ready':assert_case('actual-typed-Update-P0001-operational-outcome','stage: Update'in waited['wait']and'P0001'in waited['wait'])
    elif mode=='Channels-reply':assert_case('actual-typed-Event-operational-outcome','Domain(Event)'in waited['wait'])
    else:assert_case('observer-cancellation-remains-cancellation','Cancelled'in waited['wait'])
  closed=command('close');assert_case('actual-worker-closed',closed=={'closed':True});native_job.finish(min(35,max(.01,deadline-time.monotonic())))
  case['after_native']=snapshot();case['result_after']=raw_result(ident)
  try:case['Original_get_value']=app.AsyncResult(ident).get(timeout=2,propagate=False);case['Original_get_type']=type(case['Original_get_value']).__module__+'.'+type(case['Original_get_value']).__name__
  except BaseException as error:case['Original_get_error']={'type':type(error).__module__+'.'+type(error).__name__,'text':str(error)[:1024]}
  write_report()
  if mode in ('update-error-Ready','Channels-reply'):assert_case('operational-outcome-no-invented-terminal',case['result_after']['metadata']is None)
  elif mode!='oldSUCCESS-preserved':assert_case('actual-visible-Source-compatible-ok',case['Original_get_value']=='ok')
  if mode=='fresh-status-merge':assert_case('fresh-status-and-unowned-data-preserved',case['after_native']['recordings'][0]['custom_properties']['status']=='recording'and case['after_native']['recordings'][0]['custom_properties']['owned_current_row_marker']=='preserved')
  case['passed']=True
 finally:
  errors=[]
  try:retain('finally-before-proxy-and-child-stop')
  except BaseException as error:errors.append({'resource':'FinallySnapshot','type':type(error).__name__,'text':str(error)[:1024]})
  for label,proxy in [('PG',pg),('RESP',reply)]:
   if proxy is not None:
    try:proxy_receipts[label]=proxy.close()
    except BaseException as error:errors.append({'resource':label,'type':type(error).__name__,'text':str(error)[:1024]})
  if native_job is not None:
   if native_job.writer is not None and native_job.writer.is_alive():
    try:native_job.commands.put_nowait(None)
    except BaseException as error:errors.append({'resource':'WriterWake','type':type(error).__name__})
   if native_job.poll()is None:
    try:native_job.abort('PhysicalFinally')
    except BaseException as error:errors.append({'resource':'NativeStop','type':type(error).__name__})
   join_deadline=time.monotonic()+17
   for thread in native_job.threads+([native_job.writer]if native_job.writer else[]):
    try:thread.join(max(0,join_deadline-time.monotonic()))
    except BaseException as error:errors.append({'resource':'NativePipeJoin','type':type(error).__name__})
   # Original collector validity is literal; joined pipes do not turn a timed-out
   # or killed attempt into a successful peer process.
   actual_pipes=not any(thread.is_alive()for thread in native_job.threads)and not(native_job.writer and native_job.writer.is_alive())
   case['native_process']['finally_actual_pipes_joined']=actual_pipes
   if not actual_pipes:errors.append({'resource':'NativePipes','type':'Unsettled'})
   case['native_process'].update(exit=native_job.poll(),settled=native_job.physically_settled,pipes_settled=not any(t.is_alive()for t in native_job.threads),writer_settled=native_job.writer is None or not native_job.writer.is_alive(),collector_invalid=native_job.invalid)
  case['proxy_close_receipts']=proxy_receipts;case['finally_errors']=errors;case['case_elapsed_seconds']=time.monotonic()-started;case['finished_utc']=utc();write_report()
 if errors or case['case_elapsed_seconds']>90 or not all(v['physical_close_verified']is True for v in proxy_receipts.values()):raise ValueError('ActualPhysicalCaseSettlement')
 return case

PHYSICAL_MODES=['recording-read-Ready','update-Ready','update-error-Ready','Channels-reply','result-reply','oldSUCCESS-preserved','fresh-status-merge']
INPUTS=json.loads((ROOT/'execution-inputs.json').read_text())
if INPUTS.get('physical_mode')not in PHYSICAL_MODES or INPUTS.get('authorized')is not True:raise ValueError('ExactReviewedPhysicalModeBeforeSeed')
report['scope']='Fresh Original parents to one Native artwork ownership case; no Source failure-result/concurrency/timing parity or full workflow'
report['predeclared_cases']=[CASES[0],CASES[1],INPUTS['physical_mode']]

try:
 if app.conf.task_always_eager is not False:raise ValueError('ActualBrokerWorkerModeRequiredBeforeModelSeed')
 report['task_always_eager']=app.conf.task_always_eager
 for key in ['dvr:recover_dispatch_lock','core:version_check_dispatch_lock']:cache.set(key,'1',ex=3600)
 app.loader.import_default_modules();CoreSettings.set_system_time_zone('UTC');RecurringRecordingRule.objects.all().update(enabled=False)
 if app.conf.task_routes!=ROUTES:raise ValueError('ExactFixtureRoutes')
 layer=get_channel_layer();receiver='specific.rust_owned_parented!receiver';channel_key=layer.prefix+receiver.split('!')[0]+'!';async_to_sync(layer.group_add)('updates',receiver)
 anchor=timezone.now().replace(microsecond=0);start=anchor+timedelta(hours=4);end=start+timedelta(hours=1);media=Path(django_settings.MEDIA_ROOT).resolve()
 if media not in(Path('/app/media'),Path('/data')):raise ValueError('OwnedMediaRootOnly')
 owned_dir=media/'rust-owned-parented-artwork';owned_dir.mkdir(parents=True,exist_ok=False);xml_path=owned_dir/'guide.xml';xml_path.write_bytes(xmltv(start,end));logo_path=owned_dir/'logo.png';logo_path.write_bytes(base64.b64decode(PNG_BASE64,validate=True));os.chmod(xml_path,0o644);os.chmod(logo_path,0o644)
 logo=VODLogo.objects.create(name='Owned Local VOD Logo',url=str(logo_path));movie=Movie.objects.create(name=TITLE,logo=logo,year=2026)
 source=EPGSource.objects.create(name=SOURCE,source_type='xmltv',is_active=True,url=None,file_path=str(xml_path));epg=EPGData.objects.create(epg_source=source,tvg_id=TVG,name=CHANNEL);channel=Channel.objects.create(name=CHANNEL,channel_number=999902,epg_data=epg,tvg_id=TVG)
 CoreSettings.objects.update_or_create(key='dvr_settings',defaults={'name':'Owned parented DVR fixture','value':{'series_rules':[{'title':TITLE,'tvg_id':TVG,'title_mode':'exact','mode':'all','channel_id':channel.id,'epg_source_id':source.id}],'pre_offset_minutes':0,'post_offset_minutes':0}});django_cache.delete(CoreSettings.group_cache_key('dvr_settings'));django_cache.delete(CoreSettings.group_cache_ver_key('dvr_settings'))
 report['seed']={'anchor_utc':anchor.isoformat(),'programme_start':start.isoformat(),'programme_end':end.isoformat(),'source_id':source.id,'channel_id':channel.id,'epg_id':epg.id,'VOD_logo_id':logo.id,'Movie_id':movie.id,'settings_MEDIA_ROOT':str(media),'uid':os.getuid(),'cwd':os.getcwd(),'files':files(),'actual_VOD_namespace':VODLogo._meta.db_table,'fixture_routing':ROUTES};report['setup']=snapshot();report['setup_publications']=read('publications');write_report()
 roots=task_ids(REFRESH);check('setup-exact-model-produced-refresh',len(roots)==1);root_a=roots[0];known.append(root_a)
 a=phase(CASES[0],'refresh',[root_a]);series_a=task_ids(SERIES,root_a);check('refresh-created-one-parented-series',len(series_a)==1);known.extend(series_a)
 b=phase(CASES[1],'series',series_a);art=task_ids(ARTWORK,series_a[0]);check('series-created-two-real-artwork-children',len(art)==2);known.extend(art)
 mode=json.loads((ROOT/'execution-inputs.json').read_text())['physical_mode'];c=physical_artwork(mode,art[0])
 report['native_publications']=native_publications;report['all_prerun']=read('prerun');report['all_postrun']=read('postrun');report['all_publications']=read('publications');report['all_HTTP_attempts']=read('HTTP-attempts');report['all_sql']=read('sql');report['final']=snapshot();write_report()
 report['Native_verified']=True;report['passed']=True;write_report()
except BaseException as error:
 report.update(passed=False,Source_verified=False,error_type=type(error).__name__,error=str(error)[:4096]);write_report();raise
finally:
 errors=[]
 if native_job is not None:
  try:
   if native_job.poll()is None:native_job.abort('GuestFinally')
  except BaseException as error:errors.append({'resource':'native','type':type(error).__name__,'text':str(error)[:1024]})
 try:stop_worker()
 except BaseException as error:errors.append({'resource':'worker','type':type(error).__name__,'text':str(error)[:1024]})
 for label,handle in [('broker',broker),('backend',backend),('cache',cache)]:
  try:handle.close()
  except BaseException as error:errors.append({'resource':label,'type':type(error).__name__})
 try:connection.close()
 except BaseException as error:errors.append({'resource':'DjangoSQL','type':type(error).__name__})
 report['finally_errors']=errors
 if errors:report['passed']=False
 write_report()
if not report['passed']:raise SystemExit(1)
try:
 from physical_contract import validate
 report['native_acceptance']=validate(0,report,json.loads(Path('/job-tools/reference.json').read_text()));report['Native_verified']=True;report['Source_verified']=False;write_report()
except BaseException as error:
 report.update(passed=False,Source_verified=False,contract_error_type=type(error).__name__,contract_error=str(error)[:4096]);write_report();raise
