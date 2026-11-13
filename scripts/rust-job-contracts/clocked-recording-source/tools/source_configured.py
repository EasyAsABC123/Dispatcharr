"""Source-only future Recording → actual DatabaseScheduler → natural dvr worker.
No task, signal, model, queue routing or media implementation is substituted.
Original Dispatcharr/dependency authors retain application and algorithm credit.
"""
import base64,fcntl,hashlib,json,os,signal,sys,time,urllib.request
from datetime import timedelta
from pathlib import Path
if os.environ.get('JOB_DISPOSABLE_FIXTURE')!='playback-fixture-only'or not os.environ.get('POSTGRES_DB','').startswith('rust_probe_backup_jobs_'):raise ValueError('OwnedCopiedFixtureRequired')
sys.path[:0]=['/app','/job-tools'];sys.argv.append('shell');os.environ.setdefault('DJANGO_SETTINGS_MODULE','dispatcharr.settings')
from fresh_context import empty_plugin_directory,empty_effect_context
plugin_directory_before_app=empty_plugin_directory(os.environ.get('DISPATCHARR_PLUGINS_DIR','/data/plugins'))
from observe import app,process_observer
from process_observer import starttime as process_starttime
import django;django.setup()
import redis
from django.conf import settings as django_settings
from django.db import connection,connections
from django.utils import timezone
from django.core.cache import cache as django_cache
from channels.layers import get_channel_layer
from asgiref.sync import async_to_sync
from core.models import CoreSettings,SystemEvent,SystemNotification,NotificationDismissal
from apps.connect.models import Integration,EventSubscription,DeliveryLog
from apps.plugins.models import PluginConfig,PluginRepo
from apps.channels.models import Channel,Recording,RecurringRecordingRule
from apps.vod.models import Movie,VODLogo
from django_celery_beat.models import PeriodicTask,ClockedSchedule,PeriodicTasks,IntervalSchedule,CrontabSchedule
from sibling_frames import validate as validate_siblings,validate_intervals
from ledger_io import append_receipt
from owned_worker import OwnedJob
from bounded_process import Collected
from seed_spec import *
import media_files
with connection.cursor()as guard:
 guard.execute('SELECT current_database()');actual_database=guard.fetchone()[0]
 if actual_database!=os.environ['POSTGRES_DB']:raise ValueError('ActualCopiedDatabaseBeforeSeed')
ROOT=Path('/evidence');jobs={};known=[]
broker=redis.Redis.from_url(os.environ['CELERY_BROKER_URL']);backend=redis.Redis.from_url(os.environ['CELERY_RESULT_BACKEND']);cache=redis.Redis(host='127.0.0.1',port=6379,password='playback-fixture-only',db=0)
if broker.connection_pool.connection_kwargs['db']!=8 or backend.connection_pool.connection_kwargs['db']!=9 or broker.dbsize()or backend.dbsize():raise ValueError('FreshRedis8And9')
report={'passed':False,'Source_verified':False,'Native_verified':False,'performance_verified':False,'scope':'One owned future clocked Recording; actual Beat→natural dvr FIFO→Source run_recording with owned synthetic TS origin/local logo; no Main/full default graph/recovery/duplicate/error parity','checks':[],'cases':[],'worker_exit_receipts':[],'actual_database':actual_database,'application_task_model_signal_methods_replaced':False,'actual_routing_never_overridden':True,'fixture_sequential_Beat_then_DVR':True}
def utc():return timezone.now().isoformat()
def write_report():
 data=(json.dumps(report,indent=2,default=str,allow_nan=False)+'\n').encode()
 if len(data)>33554432:raise ValueError('Report32MiB')
 temporary=ROOT/'source-clocked-recording-private.json.pending'
 with temporary.open('wb')as file:file.write(data);file.flush();os.fsync(file.fileno())
 os.replace(temporary,ROOT/'source-clocked-recording-private.json')
def read(kind):
 path=ROOT/('clocked-'+kind+'.jsonl')
 if not path.exists():return[]
 if path.stat().st_size>16777216:raise ValueError('Ledger16MiB')
 with(ROOT/'clocked-ledger.lock').open('a+b')as lock:
  fcntl.flock(lock.fileno(),fcntl.LOCK_SH);return[json.loads(line)for line in path.read_text().splitlines()]
def rows(model):
 value=list(model.objects.order_by('pk').values()[:257])
 if len(value)>256:raise ValueError('OwnedRowObservation256')
 return value
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


def check(name,value):
 report['checks'].append({'name':name,'passed':value is True});write_report()
 if value is not True:raise AssertionError(name)
def origin_state():
 request=urllib.request.Request('http://origin:8080/__owned_state__',headers={'X-Owned-Nonce':os.environ['CLOCKED_NONCE']})
 with urllib.request.urlopen(request,timeout=3)as response:
  data=response.read(1048577)
  if response.status!=200 or len(data)>1048576:raise ValueError('OriginState1MiB200')
  return json.loads(data)
def queues():
 value={}
 for key in (QUEUE_ARTWORK,QUEUE_RECORD):
  frames=broker.lrange(key,0,64)
  if len(frames)>64 or sum(map(len,frames))>1048576:raise ValueError('OwnedQueue64And1MiB')
  value[key]=[base64.b64encode(frame).decode()for frame in frames]
 return value
def snapshot(label):
 value={'label':label,'utc':utc(),'monotonic':time.monotonic(),'channels':rows(Channel),'recordings':rows(Recording),'periodic':rows(PeriodicTask),'clocked':rows(ClockedSchedule),'sentinel':rows(PeriodicTasks),'intervals':rows(IntervalSchedule),'crontabs':rows(CrontabSchedule),'core_settings':rows(CoreSettings),'system_events':rows(SystemEvent),'notifications':rows(SystemNotification),'notification_dismissals':rows(NotificationDismissal),'integrations':rows(Integration),'event_subscriptions':rows(EventSubscription),'delivery_logs':rows(DeliveryLog),'plugin_configs':rows(PluginConfig),'plugin_repos':rows(PluginRepo),'configured_plugin_directory':empty_plugin_directory(os.environ.get('DISPATCHARR_PLUGINS_DIR','/data/plugins')),'movies':rows(Movie),'logos':rows(VODLogo),'queues':queues(),'broker':raw_redis(broker),'backend':{ident:raw_result(ident)for ident in known},'result_backend_raw':raw_redis(backend),'cache':raw_redis(cache),'Channels_raw_base64':[base64.b64encode(v).decode()for v in cache.zrange(channel_key,0,64)],'Channels_decoded':[layer.deserialize(v)for v in cache.zrange(channel_key,0,64)],'files':media_files.snapshot(),'origin':origin_state()}
 value=json.loads(json.dumps(value,default=str,allow_nan=False))
 with(ROOT/'clocked-ledger.lock').open('a+b')as lock:
  fcntl.flock(lock.fileno(),fcntl.LOCK_EX);receipt=append_receipt(ROOT/'clocked-snapshots.jsonl',value)
 if not receipt['full_record_retained']:raise ValueError('SnapshotCapacity')
 return value
def decoded_frames(queue):
 result=[]
 for raw in queues()[queue]:
  frame=json.loads(base64.b64decode(raw,validate=True));body=json.loads(base64.b64decode(frame['body'],validate=True));result.append({'raw_base64':raw,'frame':frame,'decoded_body':body})
 return result
def only_frame(queue,family):
 values=decoded_frames(queue)
 # Raw complete queues/publications are persisted before any admission assertion.
 report['admission_'+queue]={'utc':utc(),'frames':values,'all_publications':read('publications')};write_report()
 check('exact-one-known-'+queue,len(values)==1 and values[0]['frame']['headers']['task']==family)
 return values[0]
def start(role):
 job=OwnedJob(role,ROOT,dict(os.environ));jobs[role]=job
 report.setdefault('worker_starts',[]).append({'role':role,'argv':job.argv,'pid':job.pid,'starttime':job.starttime,'actual_initial_pgid':job.actual_initial_pgid,'utc':utc()});write_report()
 if job.invalid is not None:raise ValueError('ActualOwnedWorkerIdentity')
def close(role):
 job=jobs.get(role)
 if job is None:return
 receipt=job.close();report['worker_exit_receipts'].append(receipt);write_report();jobs.pop(role)
 if not(receipt['actual_exit']==0 and receipt['actual_wait_completed']and receipt['pipes_settled']and receipt['all_observed_owned_PIDs_absent']and receipt['invalid']is None and not receipt['forced']):raise RuntimeError('ActualOwnedRoleNotCleanlySettled:'+role)
def wait(predicate,seconds,label,role=None,monitor=False):
 deadline=time.monotonic()+seconds;next_snapshot=time.monotonic()
 while time.monotonic()<deadline:
  value=predicate()
  if value:return value
  if role in jobs and jobs[role].process.poll()is not None:raise RuntimeError('ActualRoleExited:'+role)
  if monitor and time.monotonic()>=next_snapshot:snapshot(label+'-progress');next_snapshot=time.monotonic()+1
  time.sleep(.05)
 raise TimeoutError(label+':'+str(seconds))
def observed_task(ident):
 raw=raw_result(ident);entry={'id':ident,'raw_result':raw,'prerun':[v for v in read('prerun')if v['id']==ident],'postrun':[v for v in read('postrun')if v['id']==ident],'sql_with_actual_thread_task_id':[v for v in read('sql')if v['actual_thread_task_id']==ident],'publications':[v for v in read('publications')if v['headers'].get('id')==ident or v['headers'].get('parent_id')==ident]}
 if raw['metadata']is None:entry['Original_AsyncResult_get']={'executed':False,'reason':'actual configured backend absent; no null SUCCESS fabricated'}
 else:
  began=utc();result=app.AsyncResult(ident);value=result.get(timeout=2,propagate=False);entry['Original_AsyncResult_get']={'executed':True,'began_utc':began,'finished_utc':utc(),'value':value,'type':type(value).__module__+'.'+type(value).__name__,'actual_state':result.state,'propagate':False,'timeout_seconds':2}
 return entry

def media_command(argv,label):
 job=Collected(argv,dict(os.environ),1048576,lambda:[v.request_stop()for v in list(jobs.values())],stdout_path=ROOT/(label+'.stdout'),stderr_path=ROOT/(label+'.stderr'));began=utc();original_starttime=process_starttime(job.process.pid)
 try:job.wait(30)
 finally:
  receipt={'argv':argv,'pid':job.process.pid,'starttime':original_starttime,'began_utc':began,'finished_utc':utc(),'actual_exit':job.poll(),'settled':job.physically_settled,'invalid':job.invalid,'pipes_settled':not any(t.is_alive()for t in job.threads)};report.setdefault('media_commands',[]).append(receipt);write_report()
 if not(receipt['actual_exit']==0 and receipt['settled']and receipt['pipes_settled']and receipt['invalid']is None):raise ValueError('ActualMediaCommand:'+label)
 return (ROOT/(label+'.stdout')).read_bytes()
def trap(signum,frame):
 report.update(passed=False,signal_received=signum);write_report();raise SystemExit(128+signum)
for sig in(signal.SIGTERM,signal.SIGINT):signal.signal(sig,trap)
try:
 report['fresh_effect_context']={'integrations':rows(Integration),'subscriptions':rows(EventSubscription),'delivery_logs':rows(DeliveryLog),'plugin_configs':rows(PluginConfig)};report['configured_plugin_directory_before_app']=plugin_directory_before_app;report['fresh_effect_context_verified']=empty_effect_context(report['fresh_effect_context']);write_report()
 if app.conf.task_always_eager is not False:raise ValueError('RealWorkerBeforeSeed')
 if app.conf.task_routes!=EXPECTED_ROUTES:raise ValueError('ActualDvrRouteBeforeSeed')
 app.loader.import_default_modules()
 for key in ('dvr:recover_dispatch_lock','core:version_check_dispatch_lock'):cache.set(key,'1',ex=3600)
 report['initial_periodic_rows']=rows(PeriodicTask);report['initial_recurring_rows']=rows(RecurringRecordingRule)
 # Copied-DB isolation only. No frames are removed or tasks quarantined.
 PeriodicTask.objects.all().update(enabled=False);RecurringRecordingRule.objects.all().update(enabled=False);PeriodicTasks.update_changed()
 CoreSettings.set_system_time_zone('UTC')
 settings={'series_rules':[],'pre_offset_minutes':0,'post_offset_minutes':0,'comskip_enabled':False,'comskip_custom_path':'','comskip_mode':'cut','comskip_hw_accel':'none','output_profile_id':None}
 CoreSettings.objects.update_or_create(key='dvr_settings',defaults={'name':'Owned future recording','value':settings});django_cache.delete(CoreSettings.group_cache_key('dvr_settings'));django_cache.delete(CoreSettings.group_cache_ver_key('dvr_settings'))
 layer=get_channel_layer();receiver='specific.rust_owned_clocked!receiver';channel_key=layer.prefix+receiver.split('!')[0]+'!';async_to_sync(layer.group_add)('updates',receiver)
 check('owned-output-initially-empty',media_files.snapshot()['files']==[])
 wait(lambda:(ROOT/'origin-ready.json').exists(),45,'OriginGeneratedReady')
 report['origin_ready']=json.loads((ROOT/'origin-ready.json').read_text());check('actual-origin-generated',report['origin_ready'].get('passed')is True)
 media=Path(django_settings.MEDIA_ROOT).resolve()
 if media not in(Path('/app/media'),Path('/data')):raise ValueError('OwnedMediaRoot')
 owned=media/'rust-owned-clocked';owned.mkdir(exist_ok=False);logo_path=owned/'logo.png';logo_path.write_bytes(base64.b64decode(PNG_BASE64,validate=True));os.chmod(logo_path,0o644)
 logo=VODLogo.objects.create(name='Owned Clocked Logo',url=str(logo_path));movie=Movie.objects.create(name=TITLE,logo=logo,year=2026)
 channel=Channel.objects.create(name=CHANNEL,channel_number=999903)
 (ROOT/'origin-target.json').write_text(json.dumps({'channel_uuid':str(channel.uuid)}))
 anchor=timezone.now().replace(microsecond=0);start_time=anchor+timedelta(seconds=LEAD_SECONDS);end_time=start_time+timedelta(seconds=DURATION_SECONDS)
 rec=Recording.objects.create(channel=channel,start_time=start_time,end_time=end_time,custom_properties={'status':'scheduled','program':{'title':TITLE},'season':1,'episode':1,'owned_fixture_marker':'preserve-normal-clocked'})
 report['seed']={'anchor_utc':anchor.isoformat(),'start_time':str(start_time),'end_time':str(end_time),'Recording_id':rec.id,'channel_id':channel.id,'channel_uuid':str(channel.uuid),'Movie_id':movie.id,'VOD_logo_id':logo.id,'logo_sha256':hashlib.sha256(logo_path.read_bytes()).hexdigest(),'uid':os.getuid(),'dvr_settings':settings,'routing':app.conf.task_routes,'actual_static_beat_schedule':app.conf.beat_schedule,'internal_TS_base':os.environ['DISPATCHARR_INTERNAL_TS_BASE_URL']}
 report['setup']=snapshot('model-signal-before-consumers');report['setup_publications']=read('publications');write_report()
 art=only_frame(QUEUE_ARTWORK,ARTWORK);art_id=art['frame']['headers']['id'];known.append(art_id)
 check('natural-dvr-empty-before-due',queues()[QUEUE_RECORD]==[])
 check('model-created-one-future-clocked-row',PeriodicTask.objects.filter(name='dvr-recording-'+str(rec.id),task=RECORD,one_off=True,enabled=True,clocked__clocked_time=start_time).count()==1)
 start('artwork');wait(lambda:any(v['id']==art_id for v in read('postrun')),35,'Artwork35','artwork');close('artwork')
 report['artwork']=observed_task(art_id);report['after_artwork']=snapshot('artwork-settled');write_report()
 check('artwork-source-success',len(report['artwork']['postrun'])==1 and report['artwork']['postrun'][0]['state']=='SUCCESS')
 check('artwork-natural-queue-drained',queues()[QUEUE_ARTWORK]==[])
 check('future-window-not-consumed-by-setup',timezone.now()<start_time)
 start('beat');wait(lambda:bool(queues()[QUEUE_RECORD]),LEAD_SECONDS+15,'ActualBeatPublication','beat',True);close('beat')
 dispatch=only_frame(QUEUE_RECORD,RECORD);record_id=dispatch['frame']['headers']['id'];known.append(record_id)
 report['recording_dispatch']=dispatch;report['after_beat']=snapshot('beat-settled-before-dvr');write_report()
 check('actual-four-model-signal-arguments',dispatch['decoded_body'][0]==[rec.id,channel.id,str(start_time),str(end_time)]and dispatch['decoded_body'][1]=={})
 check('actual-dvr-routing-key',dispatch['frame']['properties']['delivery_info']['routing_key']==QUEUE_RECORD)
 beat_start=next(row for row in report['worker_starts']if row['role']=='beat');beat_end=next(row for row in report['worker_exit_receipts']if row['role']=='beat')['finished_utc'];report['unconsumed_Beat_siblings']=validate_siblings(report['after_beat']['queues'][QUEUE_ARTWORK],read('publications'),report['after_beat']['periodic'],app.conf.beat_schedule,beat_start,beat_end);report['sibling_interval_rows']=validate_intervals(report['after_beat']['periodic'],report['after_beat']['intervals'],report['unconsumed_Beat_siblings'],app.conf.beat_schedule);known.extend(report['unconsumed_Beat_siblings']['ids']);write_report()
 check('known-static-Beat-siblings-retained-unconsumed',report['unconsumed_Beat_siblings']['passed']is True)
 start('dvr');wait(lambda:any(v['id']==record_id for v in read('postrun')),TASK_DEADLINE_SECONDS,'Recording180','dvr',True);close('dvr')
 report['recording_task']=observed_task(record_id);report['final']=snapshot('dvr-settled-before-file-copy');report['retained_media']=media_files.snapshot(ROOT/'retained-recordings');report['all_publications']=read('publications');report['all_prerun']=read('prerun');report['all_postrun']=read('postrun');report['all_HTTP_attempts']=read('HTTP-attempts');report['all_SQL']=read('sql');report['all_subprocess']=read('subprocess');report['subprocess_postrun']=read('subprocess-postrun');report['guest_process_observer']=process_observer.snapshot();write_report()
 check('actual-recording-postrun-success',len(report['recording_task']['postrun'])==1 and report['recording_task']['postrun'][0]['state']=='SUCCESS')
 check('no-unexpected-Python-provider-attempts',report['all_HTTP_attempts']==[])
 check('dvr-drained-default-sibling-FIFO-byte-order-preserved',report['final']['queues'][QUEUE_RECORD]==[]and report['final']['queues'][QUEUE_ARTWORK]==report['unconsumed_Beat_siblings']['raw_FIFO_base64'])
 candidates=[row for row in report['retained_media']['files']if row.get('relative','').endswith('.mkv')and row.get('bytes',0)>0 and row.get('stable_across_this_read')is True]
 check('one-actual-stable-MKV',len(candidates)==1)
 path=candidates[0]['retained_copy'];probe=media_command(['ffprobe','-v','error','-show_format','-show_streams','-of','json',path],'actual-ffprobe');report['ffprobe']=json.loads(probe);write_report()
 check('actual-video-stream',any(row.get('codec_type')=='video'for row in report['ffprobe'].get('streams',[])))
 media_command(['ffmpeg','-nostdin','-hide_banner','-v','error','-i',path,'-map','0:v:0','-f','null','-'],'actual-full-decode')
 report['cases']=[{'name':'future-clocked-normal','Recording_id':rec.id,'channel_id':channel.id,'artwork_task_id':art_id,'recording_task_id':record_id,'anchor_utc':anchor.isoformat(),'finished_utc':utc(),'roles':['artwork','beat','dvr']}];report['passed']=True;report['Source_verified']=True;write_report()
except BaseException as error:
 report.update(passed=False,Source_verified=False,error_type=type(error).__name__,error=str(error)[:4096]);write_report();raise
finally:
 errors=[]
 for role in list(jobs):
  try:close(role)
  except BaseException as error:errors.append({'resource':role,'error_type':type(error).__name__,'text':str(error)[:1024]})
 for name,client in [('broker',broker),('backend',backend),('cache',cache)]:
  try:client.close()
  except BaseException as error:errors.append({'resource':name,'error_type':type(error).__name__})
 try:connections.close_all()
 except BaseException as error:errors.append({'resource':'guest-Django-connections','error_type':type(error).__name__})
 report['finally_errors']=errors
 if errors:report.update(passed=False,Source_verified=False)
 write_report()
if not report['passed']:raise SystemExit(1)
