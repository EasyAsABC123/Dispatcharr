"""UNEXECUTED Source-only parented rules/local-VOD-logo artwork corpus.
Actual tasks/models/signals; fixed fixture queues isolate known roles without
removing/quarantining any default descendant. No Native/defaultFIFO claim.
"""
import base64,fcntl,hashlib,json,os,signal,sys,time,uuid
from datetime import timedelta
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
ROOT=Path('/evidence');worker=None;known=[];owned_dir=None
broker=redis.Redis.from_url(os.environ['CELERY_BROKER_URL']);backend=redis.Redis.from_url(os.environ['CELERY_RESULT_BACKEND']);cache=redis.Redis(host='127.0.0.1',port=6379,password='playback-fixture-only',db=0)
if broker.connection_pool.connection_kwargs['db']!=8 or backend.connection_pool.connection_kwargs['db']!=9 or broker.dbsize()or backend.dbsize():raise ValueError('FreshOwnedRedis8And9')
report={'passed':False,'Source_verified':False,'Native_verified':False,'performance_verified':False,'scope':'Actual Source Refresh→parented Series→two local-VOD-logo artwork children; owned routing control, default descendants untouched; no Native/Main/defaultFIFO/media claim','cases':[],'checks':[],'worker_exit_receipts':[],'predeclared_cases':CASES,'application_task_model_signal_methods_replaced':False,'HTTP_dependency_guard_installed':True,'fixture_routes':ROUTES,'future_Beat_run_recording_consumed':False,'actual_database':actual_database}
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
 return [v['headers']['id']for v in read('publications')if v['task']==task and(parent is None or v['headers'].get('parent_id')==parent)]
def trap(signum,frame):
 report['signal_received']=signum;report['passed']=False;write_report();raise SystemExit(128+signum)
for sig in(signal.SIGTERM,signal.SIGINT):signal.signal(sig,trap)
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
 c=phase(CASES[2],'artwork',art)
 # Actual repeat producer; the first model signal was not duplicated manually.
 root_b=str(uuid.uuid4());known.append(root_b);epg_tasks.refresh_epg_data.apply_async(args=[source.id],task_id=root_b);report['repeat_refresh_actual_publications']=read('publications');write_report();d=phase(CASES[3],'refresh',[root_b]);series_b=task_ids(SERIES,root_b);check('repeat-refresh-created-one-parented-series',len(series_b)==1);known.extend(series_b);e=phase(CASES[4],'series',series_b)
 report['all_prerun']=read('prerun');report['all_postrun']=read('postrun');report['all_publications']=read('publications');report['all_HTTP_attempts']=read('HTTP-attempts');report['all_sql']=read('sql');report['final']=snapshot();write_report()
 report['passed']=True;write_report()
except BaseException as error:
 report.update(passed=False,Source_verified=False,error_type=type(error).__name__,error=str(error)[:4096]);write_report();raise
finally:
 errors=[]
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
 from contract import validate_source_capture
 report['source_acceptance']=validate_source_capture(0,report);report['Source_verified']=True;write_report()
except BaseException as error:
 report.update(passed=False,Source_verified=False,contract_error_type=type(error).__name__,contract_error=str(error)[:4096]);write_report();raise
