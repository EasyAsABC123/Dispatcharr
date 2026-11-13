"""UNEXECUTED configured normal/known INSERT parent interoperability guest. Actual pinned
Source producer/models/signals+Native worker+Original AsyncResult; no Source
worker or descendant consumption, no Main/default-FIFO/workflow claim.
"""
import base64,fcntl,hashlib,json,os,signal,subprocess,sys,time,uuid,threading
from datetime import timedelta,datetime,timezone as utc_timezone
from pathlib import Path
if os.environ.get('JOB_DISPOSABLE_FIXTURE')!='playback-fixture-only'or not os.environ.get('POSTGRES_DB','').startswith('rust_probe_backup_jobs_'):raise ValueError('OwnedCopiedFixtureRequired')
sys.path[:0]=['/app','/job-tools'];sys.argv.append('shell');os.environ.setdefault('DJANGO_SETTINGS_MODULE','dispatcharr.settings')
import django;django.setup()
import redis
from django.db import connection
from django.utils import timezone
from django.core.cache import cache as django_cache
from asgiref.sync import async_to_sync
from channels.layers import get_channel_layer
from core.models import CoreSettings,SYSTEM_SETTINGS_KEY
from apps.channels.models import Channel,Recording,RecurringRecordingRule
from apps.channels import tasks
from apps.epg.models import EPGSource,EPGData,ProgramData
from django_celery_beat.models import PeriodicTask,PeriodicTasks,ClockedSchedule
from observe import app
from ledger_io import append_receipt
ROOT=Path('/evidence');QUEUE='rust-owned-configured-recordings';GROUP='task_lock_evaluate_series_rules_all'
from source_contract import CASES,CREATED,CHILDREN
REFERENCE=json.loads(Path('/job-tools/reference.json').read_text())
broker=redis.Redis.from_url(os.environ['CELERY_BROKER_URL']);backend=redis.Redis.from_url(os.environ['CELERY_RESULT_BACKEND']);cache=redis.Redis(host='127.0.0.1',port=6379,password='playback-fixture-only',db=0)
if broker.connection_pool.connection_kwargs['db']!=8 or backend.connection_pool.connection_kwargs['db']!=9 or broker.dbsize()or backend.dbsize():raise ValueError('FreshOwnedRedis8And9')
CONTEXT=json.loads((ROOT/'controller-context.json').read_text())
report={'lifecycle_began_utc':CONTEXT['lifecycle_began_utc'],'model_tables':{'Recording':Recording._meta.db_table,'Channel':Channel._meta.db_table,'EPGSource':EPGSource._meta.db_table,'EPGData':EPGData._meta.db_table,'ProgramData':ProgramData._meta.db_table,'PeriodicTask':PeriodicTask._meta.db_table,'ClockedSchedule':ClockedSchedule._meta.db_table},'passed':False,'scope':'Actual Original producer -> Native configured normal/known INSERT parents -> Original AsyncResult; fixed test-only owned parent route, all default children untouched; no application/fullworkflow claim','cases':[],'checks':[],'worker_exit_receipts':[],'predeclared_cases':[CASES[i]for i in (1,2,3,5,6,7,8,9,10)],'no_functions_replaced':True,'Native_verified':False,'source_reference_host_exit':1,'captured_SQLSTATE_verified':False,'performance_verified':False};worker=None;channel=None;known=[]
def utc():return timezone.now().isoformat()
def save(): (ROOT/'source-configured-recordings-private.json').write_text(json.dumps(report,indent=2,default=str)+'\n')
def append(kind,value):
 with (ROOT/'configured-ledger.lock').open('a+b')as lock:
  fcntl.flock(lock.fileno(),fcntl.LOCK_EX);receipt=append_receipt(ROOT/('configured-'+kind+'.jsonl'),value)
 if not receipt['full_record_retained']:raise ValueError('RetainedCapacityRefusal')
def read(kind):
 path=ROOT/('configured-'+kind+'.jsonl')
 if not path.exists():return[]
 with (ROOT/'configured-ledger.lock').open('a+b')as lock:
  fcntl.flock(lock.fileno(),fcntl.LOCK_SH);return[json.loads(v)for v in path.read_text().splitlines()]
def wait(test,seconds=35):
 end=time.monotonic()+seconds
 while time.monotonic()<end:
  value=test()
  if value:return value
  time.sleep(.01)
 raise TimeoutError('PredeclaredSourceDeadline35s')
def raw_result(ident):
 key=app.backend.get_key_for_task(ident);raw=backend.get(key)
 return{'key_base64':base64.b64encode(key).decode(),'raw_base64':None if raw is None else base64.b64encode(raw).decode(),'metadata':None if raw is None else json.loads(raw),'ttl':backend.ttl(key)}
def snapshot():
 frames=broker.lrange('celery',0,-1);reserved=broker.hgetall('unacked');groupkey=':1:'+CoreSettings.group_cache_key('dvr_settings');cached=cache.get(groupkey)
 value={'utc':utc(),'channels':list(Channel.objects.filter(name='Owned Configured').order_by('id').values()),'sources':list(EPGSource.objects.filter(name='rust-owned-configured-source').order_by('id').values()),'epg_data':list(EPGData.objects.filter(epg_source__name='rust-owned-configured-source').order_by('id').values()),'programmes':list(ProgramData.objects.filter(epg__epg_source__name='rust-owned-configured-source').order_by('id').values()),'system_settings':list(CoreSettings.objects.filter(key=SYSTEM_SETTINGS_KEY).values()),'recordings':list(Recording.objects.order_by('id').values()),'recurring_rules':list(RecurringRecordingRule.objects.order_by('id').values()),'periodic':list(PeriodicTask.objects.order_by('id').values()),'clocked':list(ClockedSchedule.objects.order_by('id').values()),'sentinel':list(PeriodicTasks.objects.values()),'dvr_settings':list(CoreSettings.objects.filter(key='dvr_settings').values()),'Django_cache':{'key':groupkey,'raw_base64':None if cached is None else base64.b64encode(cached).decode(),'ttl':cache.ttl(groupkey)},'lock':{'raw_base64':None if cache.get(GROUP)is None else base64.b64encode(cache.get(GROUP)).decode(),'ttl':cache.ttl(GROUP)},'frames':[{'raw_base64':base64.b64encode(v).decode(),'decoded':json.loads(v)}for v in frames],'unacked':{base64.b64encode(k).decode():base64.b64encode(v).decode()for k,v in reserved.items()},'backend':{v:raw_result(v)for v in known},'Channels_raw_base64':[base64.b64encode(v).decode()for v in cache.zrange(channel_key,0,-1)],'Channels_decoded':[layer.deserialize(v)for v in cache.zrange(channel_key,0,-1)],'output_cache':{'generation_raw_base64':None if cache.get('epg_content_generation')is None else base64.b64encode(cache.get('epg_content_generation')).decode(),'generation_ttl':cache.ttl('epg_content_generation'),'retired_chunk_raw_base64':[base64.b64encode(v).decode()for v in cache.lrange('rust-owned-configured-epg:g=701:chunks',0,-1)],'retired_chunk_ttl':cache.ttl('rust-owned-configured-epg:g=701:chunks'),'m3u_sentinel':django_cache.get('m3u_content:rust-owned-configured-1')}}
 append('snapshots',value)
 if len(value['recordings'])>256 or len(frames)>64 or sum(map(len,frames))>1048576 or len(reserved)>64:raise ValueError('OwnedDiagnosticCapacity')
 return json.loads(json.dumps(value,default=str))
def check(label,value):
 snapshot();report['checks'].append({'name':label,'passed':bool(value)});save()
 if not value:raise AssertionError(label)
def stop():
 global worker
 if worker is not None:
  if worker.poll()is None:os.killpg(worker.pid,signal.SIGTERM)
  try:code=worker.wait(timeout=12)
  except subprocess.TimeoutExpired:
   os.killpg(worker.pid,signal.SIGKILL);code=worker.wait(timeout=5);report['worker_exit_receipts'].append({'pid':worker.pid,'exit':code,'forced':True,'settled':True});worker=None;raise RuntimeError('SourceWorkerForcedStop')
  report['worker_exit_receipts'].append({'pid':worker.pid,'exit':code,'forced':False,'settled':True});worker=None
  if code!=0:raise ValueError('SourceWorkerExitNonzero')
def settings(rules):
 CoreSettings.objects.update_or_create(key='dvr_settings',defaults={'name':'Owned configured DVR fixture','value':{'series_rules':rules,'pre_offset_minutes':0,'post_offset_minutes':0}})
 django_cache.delete(CoreSettings.group_cache_key('dvr_settings'));django_cache.delete(CoreSettings.group_cache_ver_key('dvr_settings'))
def fault(enable):
 with connection.cursor()as c:
  c.execute('SELECT current_database()')
  if c.fetchone()[0]!=os.environ['POSTGRES_DB']:raise ValueError('ActualOwnedDB')
  if enable:
   c.execute("CREATE FUNCTION rust_owned_recording_fail() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN RAISE EXCEPTION 'owned configured recording failure' USING ERRCODE='P0001'; END$$")
   c.execute('CREATE TRIGGER rust_owned_recording_fail BEFORE INSERT ON dispatcharr_channels_recording FOR EACH ROW EXECUTE FUNCTION rust_owned_recording_fail()')
  else:c.execute('DROP TRIGGER IF EXISTS rust_owned_recording_fail ON dispatcharr_channels_recording');c.execute('DROP FUNCTION IF EXISTS rust_owned_recording_fail()')
def run_case(index,task,kwargs=None):
 global worker
 if index not in (1,2,3,5,6,7,8,9,10):raise ValueError('DeclaredNormalIndicesOnly')
 from bounded_process import Collected
 from pg_observer import Observer
 name=CASES[index];before=snapshot();ident=REFERENCE['cases'][index]['id'];known.append(ident)
 case={'name':name,'before':before,'id':ident,'began_utc':utc(),'source_fallback':False,'source_postrun':None};report['cases'].append(case);save()
 case['result_before']=raw_result(ident)
 result=task.apply_async(args=[],kwargs=kwargs or{},task_id=ident,queue=QUEUE)
 parent=broker.lrange(QUEUE,0,-1)
 case['published_parent_raw_base64']=[base64.b64encode(v).decode()for v in parent];save()
 proxy=Observer() if index not in(6,10)else __import__('pg_reply_proxy').Proxy('Recording-INSERT-P0001-Ready')
 job=None;stop_receipt={};injector=None;injection_errors=[];injection_state={}
 if index in(6,10):
  # Normal interoperability: no held-reply timing claim. The same real PG
  # exchange observer records target Error/Ready; release is already permitted.
  proxy.release.set();case['fault_injection']={'after_actual_target_Parse':False,'actual_Source_reference_capture':False,'before_admission_trigger_installed':False,'reply_hold_control':False};save()
  def install():
   from django.db import connection as admin_connection
   try:
    if not proxy.before_insert.wait(20):raise TimeoutError('ActualRecordingINSERTParseBeforeForward')
    injection_state.update(after_actual_target_Parse=True,began_utc=utc());append('fault-injection',{'id':ident,'stage':'actual-target-Parse-before-owned-DDL','utc':utc()})
    fault(True);injection_state.update(actual_admin_DDL_completed=True,finished_utc=utc());append('fault-injection',{'id':ident,'stage':'actual-owned-DDL-completed','utc':utc()})
   except BaseException as error:injection_errors.append({'type':type(error).__name__,'text':str(error)[:1024]})
   finally:admin_connection.close();proxy.insert_release.set()
  injector=threading.Thread(target=install);injector.start()
 def stop_child():
  if job is not None:
   stop_receipt.update(pid=job.process.pid,requested=True,already_exited=job.poll()is not None)
   if job.poll()is None:job.process.terminate()
 config={'family':'series'if index<7 else'recurring','broker_url':os.environ['CELERY_BROKER_URL'],'result_url':os.environ['CELERY_RESULT_BACKEND'],'cache_url':'redis://:playback-fixture-only@127.0.0.1:6379/0','database_url':'postgres://dispatch:playback-fixture-only@127.0.0.1:61651/'+os.environ['POSTGRES_DB']}
 commands=[config,{'action':'run'},{'action':'close'}]
 argv=['/native-configured-peer','configured_recording_peer::actual_configured_recording_parent_peer','--exact','--ignored','--nocapture','--test-threads=1']
 receipt={'argv':argv,'spawn_state':'constructor_not_called','began_utc':utc(),'exit':None,'settled':False,'pipes_settled':False};case['native_process']=receipt;save()
 try:
  job=Collected(argv,dict(os.environ),1048576,stop_child,stdout_path=ROOT/('native-'+name+'.stdout'),stderr_path=ROOT/('native-'+name+'.stderr'),body=(''.join(json.dumps(v)+'\n'for v in commands)).encode())
  receipt.update(spawn_state='actual_child_started',pid=job.process.pid,starttime=Path('/proc',str(job.process.pid),'stat').read_text().rsplit(')',1)[1].split()[19]);save()
  job.wait(35)
 finally:
  if job is not None:
   receipt.update(exit=job.poll(),settled=job.physically_settled,pipes_settled=not any(t.is_alive()for t in job.threads),writer_settled=job.writer is None or not job.writer.is_alive(),invalid=job.invalid,stop_receipt=stop_receipt,finished_utc=utc())
  else:receipt.update(spawn_state='constructor_did_not_return',invalid='UnknownConstructorSettlement')
  case['case_cleanup_errors']=[]
  if injector:
   injector.join(25);case['fault_injection'].update(dict(injection_state));case['fault_injection'].update(thread_settled=not injector.is_alive(),errors=list(injection_errors))
   # Never silently claim a settled injector or remove an unproved owner's DDL.
   if not injector.is_alive():
    try:fault(False)
    except BaseException as error:case['case_cleanup_errors'].append('owned-DDL:'+type(error).__name__)
  # Retain actual already-captured protocol bytes even if close itself fails.
  with proxy.lock:case['native_pg_partial_frames']=list(proxy.frames)
  try:case['native_pg']=proxy.close()
  except BaseException as error:case['native_pg']={'physical_close_verified':False,'errors':[type(error).__name__+':'+str(error)[:256]],'frames':case['native_pg_partial_frames']}
  case['finished_utc']=utc();case['after']=snapshot();case['result']=raw_result(ident)
  try:case['actual_get_value']=result.get(timeout=2,propagate=False);case['actual_get_type']=type(case['actual_get_value']).__module__+'.'+type(case['actual_get_value']).__name__
  except BaseException as error:case['actual_get_error']={'type':type(error).__module__+'.'+type(error).__name__,'text':str(error)[:1024]}
  output=ROOT/('native-'+name+'.stdout')
  case['native_responses']=[json.loads(v.split('NATIVE_CONFIGURED_RECORDINGS_JSON=',1)[1])for v in output.read_text().splitlines()if'NATIVE_CONFIGURED_RECORDINGS_JSON='in v]if output.exists()else[]
  case['publications']=[v for v in read('publications')if v['headers']['id']==ident];save()
 # All actual data/process/transcript/backend retained BEFORE semantic assertions.
 from normal_contract import validate_case
 case['comparison']=validate_case(REFERENCE['cases'][index],case,report)
 report['checks'].append({'name':name+'-full-normal-contract','passed':True});save()

try:
 for key in ['dvr:recover_dispatch_lock','core:version_check_dispatch_lock']:cache.set(key,'1',ex=3600)
 app.loader.import_default_modules();CoreSettings.set_system_time_zone('UTC');RecurringRecordingRule.objects.all().update(enabled=False)
 django_cache.set('m3u_content:rust-owned-configured-1','owned-output-sentinel',timeout=600);cache.set('epg_content_generation','701',ex=600);cache.rpush('rust-owned-configured-epg:g=701:chunks','owned-retired-XMLTV');cache.expire('rust-owned-configured-epg:g=701:chunks',600)
 layer=get_channel_layer();receiver='specific.rust_owned_configured!receiver';channel_key=layer.prefix+receiver.split('!')[0]+'!';async_to_sync(layer.group_add)('updates',receiver)
 anchor=datetime.combine(timezone.now().date(),datetime.fromisoformat(REFERENCE['anchor_utc']).timetz());report['anchor_utc']=anchor.isoformat();report['fixture_date_delta_days']=(anchor.date()-datetime.fromisoformat(REFERENCE['anchor_utc']).date()).days;source=EPGSource.objects.create(name='rust-owned-configured-source',source_type='xmltv',is_active=False);epg=EPGData.objects.create(epg_source=source,tvg_id='rust.owned.configured',name='Owned Configured');channel=Channel.objects.create(uuid=REFERENCE['setup']['channels'][0]['uuid'],name='Owned Configured',channel_number=999901,epg_data=epg,tvg_id=epg.tvg_id)
 report['setup']=snapshot();report['setup_expected_family']='apps.epg.tasks.parse_programs_for_tvg_id'
 check('setup-Channel-signal-frame-preserved',len(report['setup']['frames'])==1 and report['setup']['frames'][0]['decoded']['headers']['task']=='apps.epg.tasks.parse_programs_for_tvg_id')
 def programme(index):return ProgramData.objects.create(epg=epg,tvg_id=epg.tvg_id,title='Owned Exact Series',sub_title='Owned Episode '+str(index),start_time=anchor+timedelta(hours=22+index*2),end_time=anchor+timedelta(hours=23+index*2),custom_properties={'season':1,'episode':index})
 rule={'title':'Owned Exact Series','tvg_id':epg.tvg_id,'title_mode':'exact','mode':'all','channel_id':channel.id,'epg_source_id':source.id}
 settings([rule])
 programme(1);settings([rule]);run_case(1,tasks.evaluate_series_rules);run_case(2,tasks.evaluate_series_rules)
 programme(2);run_case(3,tasks.evaluate_series_rules)
 cache.set(GROUP,'owned-foreign-series-token',ex=300);run_case(5,tasks.evaluate_series_rules);check('foreign-series-lock-unchanged',cache.get(GROUP)==b'owned-foreign-series-token');cache.delete(GROUP)
 programme(3);run_case(6,tasks.evaluate_series_rules) # Actual known INSERT error after admission; Source11 reference remains unchanged.
 target=(anchor+timedelta(days=1)).date();recurring=RecurringRecordingRule.objects.create(channel=channel,name='Owned Recurring',enabled=True,days_of_week=[target.weekday()],start_date=target,end_date=target,start_time=anchor.time().replace(tzinfo=None),end_time=(anchor+timedelta(hours=1)).time().replace(tzinfo=None))
 run_case(7,tasks.maintain_recurring_recordings);run_case(8,tasks.maintain_recurring_recordings);recurring.enabled=False;recurring.save(update_fields=['enabled']);run_case(9,tasks.maintain_recurring_recordings)
 observed_rule=REFERENCE['cases'][10]['before']['recurring_rules'][-1];delta=timedelta(days=report['fixture_date_delta_days']);RecurringRecordingRule.objects.create(channel=channel,name=observed_rule['name'],enabled=observed_rule['enabled'],days_of_week=[(d+report['fixture_date_delta_days'])%7 for d in observed_rule['days_of_week']],start_date=datetime.fromisoformat(observed_rule['start_date']).date()+delta,end_date=datetime.fromisoformat(observed_rule['end_date']).date()+delta,start_time=datetime.strptime(observed_rule['start_time'],'%H:%M:%S').time(),end_time=datetime.strptime(observed_rule['end_time'],'%H:%M:%S').time());run_case(10,tasks.maintain_recurring_recordings)
 report['predeclared_cases']=[CASES[i]for i in (1,2,3,5,6,7,8,9,10)];report['Native_verified']=True
 report['passed']=True
finally:
 try:stop()
 except BaseException as error:report['passed']=False;report['cleanup_error']=type(error).__name__
 save()
if not report['passed']:raise SystemExit(1)
