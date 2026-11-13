"""UNEXECUTED owned Source configured-Series/positive-recurring corpus draft.
Real task/manager/model/signal code runs unchanged; descendants stay unconsumed.
Host role/default-audit/cleanup wrapper and Root slot approval are prerequisites.
"""
import base64,fcntl,hashlib,json,os,signal,subprocess,sys,time,uuid
from datetime import timedelta
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
from contract import CASES,CREATED,CHILDREN
broker=redis.Redis.from_url(os.environ['CELERY_BROKER_URL']);backend=redis.Redis.from_url(os.environ['CELERY_RESULT_BACKEND']);cache=redis.Redis(host='127.0.0.1',port=6379,password='playback-fixture-only',db=0)
if broker.connection_pool.connection_kwargs['db']!=8 or backend.connection_pool.connection_kwargs['db']!=9 or broker.dbsize()or backend.dbsize():raise ValueError('FreshOwnedRedis8And9')
report={'model_tables':{'Recording':Recording._meta.db_table,'Channel':Channel._meta.db_table,'EPGSource':EPGSource._meta.db_table,'EPGData':EPGData._meta.db_table,'ProgramData':ProgramData._meta.db_table,'PeriodicTask':PeriodicTask._meta.db_table,'ClockedSchedule':ClockedSchedule._meta.db_table},'passed':False,'scope':'Actual Source parent workers plus SQL/model-signal/artwork-publication effects; descendants deliberately queued intact, no artwork/media/Native/fullworkflow claim','cases':[],'checks':[],'worker_exit_receipts':[],'predeclared_cases':CASES,'no_functions_replaced':True,'Native_verified':False,'performance_verified':False};worker=None;channel=None;known=[]
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
 name=CASES[index];before=snapshot();prior={v['decoded']['headers']['id']for v in before['frames']};ident=str(uuid.uuid4());known.append(ident)
 case={'name':name,'before':before,'id':ident,'began_utc':utc()};report['cases'].append(case);save()
 with (ROOT/'source-configured-worker.log').open('ab')as log:
  worker=subprocess.Popen(['/dispatcharrpy/bin/celery','-A','observe:app','worker','--pool=prefork','--concurrency=1','-Q',QUEUE,'--without-gossip','--without-mingle','--without-heartbeat','--loglevel=WARNING','--hostname=rust-owned-configured@fixture'],env=dict(os.environ,C_FORCE_ROOT='true',PYTHONPATH='/job-tools:/app'),stdout=log,stderr=log,start_new_session=True)
 result=task.apply_async(args=[],kwargs=kwargs or{},task_id=ident,queue=QUEUE)
 case['postrun']=wait(lambda:next((v for v in read('postrun')if v['id']==ident),None));stop();case['finished_utc']=utc();case['after']=snapshot();case['result']=raw_result(ident);case['actual_get_value']=result.get(timeout=2,propagate=False);case['actual_get_type']=type(case['actual_get_value']).__module__+'.'+type(case['actual_get_value']).__name__;case['publications']=[v for v in read('publications')if v['headers']['id']==ident or v['headers'].get('root_id')==ident];case['sql']=[v for v in read('sql')if v['id']==ident];save()
 after=case['after'];new=[v for v in after['recordings']if v['id']not in {row['id']for row in before['recordings']}];children=[v for v in after['frames']if v['decoded']['headers']['id']not in prior]
 check(name+'-normal-metadata',case['postrun']['state']=='SUCCESS'and case['result']['metadata']['status']=='SUCCESS'and case['result']['metadata']['traceback']is None)
 check(name+'-actual-created',len(new)==CREATED[index]);check(name+'-actual-child-count',len(children)==CHILDREN[index]);check(name+'-earlier-descendants-retained',all(v in after['frames']for v in before['frames']))
 check(name+'-actual-artwork-only',all(v['decoded']['headers']['task']=='apps.channels.tasks.prefetch_recording_artwork'for v in children))
 check(name+'-clocked-not-immediate-recording',all(v['task_id']=='dvr-recording-'+str(v['id'])for v in new)and all(any(p['name']==v['task_id']and p['clocked_id']is not None and p['one_off']and p['enabled']for p in after['periodic'])for v in new))
 check(name+'-queue-unreserved',broker.llen(QUEUE)==0 and broker.hlen('unacked')==0)
 check(name+'-result-ttl',3580<=case['result']['ttl']<=3600)
 check(name+'-typed-scheduled',type(case['actual_get_value'])is dict and case['actual_get_value']['scheduled']==CREATED[index]if index<7 else type(case['actual_get_value'])is int and case['actual_get_value']==CREATED[index])
try:
 for key in ['dvr:recover_dispatch_lock','core:version_check_dispatch_lock']:cache.set(key,'1',ex=3600)
 app.loader.import_default_modules();CoreSettings.set_system_time_zone('UTC');RecurringRecordingRule.objects.all().update(enabled=False)
 django_cache.set('m3u_content:rust-owned-configured-1','owned-output-sentinel',timeout=600);cache.set('epg_content_generation','701',ex=600);cache.rpush('rust-owned-configured-epg:g=701:chunks','owned-retired-XMLTV');cache.expire('rust-owned-configured-epg:g=701:chunks',600)
 layer=get_channel_layer();receiver='specific.rust_owned_configured!receiver';channel_key=layer.prefix+receiver.split('!')[0]+'!';async_to_sync(layer.group_add)('updates',receiver)
 anchor=timezone.now().replace(microsecond=0);report['anchor_utc']=anchor.isoformat();source=EPGSource.objects.create(name='rust-owned-configured-source',source_type='xmltv',is_active=False);epg=EPGData.objects.create(epg_source=source,tvg_id='rust.owned.configured',name='Owned Configured');channel=Channel.objects.create(name='Owned Configured',channel_number=999901,epg_data=epg,tvg_id=epg.tvg_id)
 report['setup']=snapshot();report['setup_expected_family']='apps.epg.tasks.parse_programs_for_tvg_id'
 check('setup-Channel-signal-frame-preserved',len(report['setup']['frames'])==1 and report['setup']['frames'][0]['decoded']['headers']['task']=='apps.epg.tasks.parse_programs_for_tvg_id')
 def programme(index):return ProgramData.objects.create(epg=epg,tvg_id=epg.tvg_id,title='Owned Exact Series',sub_title='Owned Episode '+str(index),start_time=anchor+timedelta(hours=22+index*2),end_time=anchor+timedelta(hours=23+index*2),custom_properties={'season':1,'episode':index})
 rule={'title':'Owned Exact Series','tvg_id':epg.tvg_id,'title_mode':'exact','mode':'all','channel_id':channel.id,'epg_source_id':source.id}
 settings([{}]);run_case(0,tasks.evaluate_series_rules)
 programme(1);settings([rule]);run_case(1,tasks.evaluate_series_rules);run_case(2,tasks.evaluate_series_rules)
 programme(2);run_case(3,tasks.evaluate_series_rules);run_case(4,tasks.evaluate_series_rules,{'tvg_id':'rust.owned.not-matching'})
 cache.set(GROUP,'owned-foreign-series-token',ex=300);run_case(5,tasks.evaluate_series_rules);check('foreign-series-lock-unchanged',cache.get(GROUP)==b'owned-foreign-series-token');cache.delete(GROUP)
 programme(3);fault(True)
 try:run_case(6,tasks.evaluate_series_rules)
 finally:fault(False)
 target=(anchor+timedelta(days=1)).date();recurring=RecurringRecordingRule.objects.create(channel=channel,name='Owned Recurring',enabled=True,days_of_week=[target.weekday()],start_date=target,end_date=target,start_time=anchor.time().replace(tzinfo=None),end_time=(anchor+timedelta(hours=1)).time().replace(tzinfo=None))
 run_case(7,tasks.maintain_recurring_recordings);run_case(8,tasks.maintain_recurring_recordings);recurring.enabled=False;recurring.save(update_fields=['enabled']);run_case(9,tasks.maintain_recurring_recordings)
 RecurringRecordingRule.objects.create(channel=channel,name='Owned Recurring Fault',enabled=True,days_of_week=[target.weekday()],start_date=target,end_date=target,start_time=(anchor+timedelta(hours=2)).time().replace(tzinfo=None),end_time=(anchor+timedelta(hours=3)).time().replace(tzinfo=None));fault(True)
 try:run_case(10,tasks.maintain_recurring_recordings)
 finally:fault(False)
 check('only-known-owned-tasks-executed',all(v['task']in ('apps.channels.tasks.evaluate_series_rules','apps.channels.tasks.maintain_recurring_recordings')for v in read('postrun')))
 check('exact-eleven-workers-settled',len(report['worker_exit_receipts'])==11 and all(v['exit']==0 and not v['forced']for v in report['worker_exit_receipts']))
 report['passed']=True
finally:
 try:stop()
 except BaseException as error:report['passed']=False;report['cleanup_error']=type(error).__name__
 save()
if not report['passed']:raise SystemExit(1)
