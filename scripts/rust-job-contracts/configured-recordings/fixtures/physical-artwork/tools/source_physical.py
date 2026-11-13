"""UNEXECUTED finite Native physical/refusal controls on actual configured worker.
Actual Original producer/AsyncResult; descendants never consumed.
No Source physical-timing parity, default FIFO, Main or fullworkflow claim.
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
report={'lifecycle_began_utc':CONTEXT['lifecycle_began_utc'],'model_tables':{'Recording':Recording._meta.db_table,'Channel':Channel._meta.db_table,'EPGSource':EPGSource._meta.db_table,'EPGData':EPGData._meta.db_table,'ProgramData':ProgramData._meta.db_table,'PeriodicTask':PeriodicTask._meta.db_table,'ClockedSchedule':ClockedSchedule._meta.db_table},'passed':False,'scope':'Actual Source producer -> normal Native configured parents -> Original AsyncResult; fixed test-only owned parent route, all default children untouched; no application/fullworkflow claim','cases':[],'checks':[],'worker_exit_receipts':[],'predeclared_cases':[CASES[i]for i in (1,2,3,5,7,8,9)],'no_functions_replaced':True,'Native_verified':False,'source_reference_host_exit':1,'captured_SQLSTATE_verified':False,'performance_verified':False};worker=None;channel=None;known=[]
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
# Separate native-only physical/refusal control corpus, never a Source recapture.
from interactive_peer import Peer
from fixture_endpoints import route_reply_gate,inspect_config
from pg_reply_proxy import Proxy as PGProxy,error_fields
from redis_reply_proxy import Proxy as RedisProxy
from acl_probes import permission_profile,denied_write_observation
POLICY=json.loads(Path('/job-tools/physical-policy.json').read_text())
MODE=CONTEXT['mode']
PREFIX_MODES=['settings-cache-reply','programme-DataRow-CommandComplete-Ready','Clocked-marker-COMMIT-Ready','Periodic-COMMIT-Ready','artwork-LPUSH-reply','Channels-reply','result-SETEX-reply','result-reply-lost','result-SETEX-denied','oldSUCCESS-preserved','Recording-INSERT-P0001-Ready','snapshot-server-error-Ready']
MODES=PREFIX_MODES[4:] if MODE=='physical-artwork-onward'else[MODE]
if MODE!='physical-artwork-onward':raise ValueError('RemainingEightOnly')
report={'lifecycle_began_utc':CONTEXT['lifecycle_began_utc'],'no_application_methods_replaced':True,'descendants_consumed':False,'passed':False,'Native_verified':False,'performance_verified':False,'scope':'Native physical ownership/refusal controls with actual Original producer/decoder; no Source timing parity/default FIFO/fullworkflow','mode':MODE,'predeclared_modes':MODES,'cases':[],'checks':[],'Source_reference_host_exit':1,'captured_SQLSTATE_verified':False}
peer=None;pg=None;reply=None;injector=None;acl=None;view=False

def capture():
 value={'utc':utc(),'backend':raw_result(case['id']),'parent_queue':[base64.b64encode(v).decode()for v in broker.lrange(QUEUE,0,-1)],'default_queue':[base64.b64encode(v).decode()for v in broker.lrange('celery',0,-1)],'global_lock_raw_base64':None if cache.get(GROUP)is None else base64.b64encode(cache.get(GROUP)).decode(),'global_lock_ttl':cache.ttl(GROUP)}
 try:value['effects']=snapshot()
 except BaseException as error:value['snapshot_error']={'type':type(error).__name__,'text':str(error)[:1024]}
 return value

def check(label,value):
 if peer is not None:retain_peer(case['name'],peer)
 if 'case'in globals():case.setdefault('before_assertions',[]).append({'name':label,'raw':capture()})
 report['checks'].append({'name':label,'passed':bool(value)});save()
 if not value:raise AssertionError(label)

def owned_sql():
 with connection.cursor()as c:
  c.execute('SELECT current_database()')
  if c.fetchone()[0]!=os.environ['POSTGRES_DB']:raise ValueError('ActualCopiedDBOnly')

def settings_fault(enable):
 global view
 owned_sql()
 with connection.cursor()as c:
  if enable:
   c.execute("CREATE FUNCTION rust_owned_configured_read_fail(value jsonb) RETURNS jsonb LANGUAGE plpgsql AS $$BEGIN RAISE EXCEPTION 'owned configured settings read failure' USING ERRCODE='P0001'; END$$")
   c.execute('ALTER TABLE core_coresettings RENAME TO rust_owned_configured_core_settings')
   c.execute("CREATE VIEW core_coresettings AS SELECT id,key,name,CASE WHEN key='dvr_settings' THEN rust_owned_configured_read_fail(value) ELSE value END AS value FROM rust_owned_configured_core_settings")
   view=True
  else:
   c.execute('DROP VIEW core_coresettings');c.execute('ALTER TABLE rust_owned_configured_core_settings RENAME TO core_coresettings');c.execute('DROP FUNCTION rust_owned_configured_read_fail(jsonb)');view=False

def start_peer(cfg,label):
 global peer
 state={};argv=['/native-configured-peer','configured_recording_peer::actual_configured_recording_parent_peer','--exact','--ignored','--nocapture','--test-threads=1']
 def stop_child():
  child=state.get('peer')
  if child and child.poll()is None:child.process.terminate()
 child=Peer(argv,dict(os.environ),stop_child);state['peer']=child;peer=child
 case['native_process']={'argv':argv,'pid':child.process.pid,'starttime':Path('/proc',str(child.process.pid),'stat').read_text().rsplit(')',1)[1].split()[19],'began_utc':utc(),'exit':None,'settled':False};save()
 child.send(cfg);prepared=child.receive(35);case['prepared_reply']=prepared;save();check(label+'-actual-prepared-parent-route',prepared.get('prepared')is True and prepared.get('parent_route')==QUEUE and prepared.get('children_consumed')is False)
 return child

def send(value):
 case.setdefault('native_commands',[]).append({'sent_utc':utc(),'request':value});save();peer.send(value)
 result=peer.receive(35);case['native_commands'][-1].update(reply=result,received_utc=utc());save();return result

def retain_peer(label,child):
 case['native_process'].update(exit=child.poll(),settled=child.physically_settled,pipes_settled=not any(t.is_alive()for t in child.threads),writer_settled=not child.writer.is_alive(),invalid=child.invalid,observed_utc=utc(),finished_utc=utc()if child.poll()is not None else None)
 (ROOT/('physical-'+label+'.stdout')).write_bytes(bytes(child.output['stdout']));(ROOT/('physical-'+label+'.stderr')).write_bytes(bytes(child.output['stderr']));save()

def close_peer(label):
 global peer
 closed=send({'action':'close'});check(label+'-normal-close-reply',closed=={'closed':True});peer.finish(35)
 retain_peer(label,peer)
 check(label+'-actual-process-and-pipes0',peer.poll()==0 and peer.physically_settled and peer.invalid is None and not any(t.is_alive()for t in peer.threads)and not peer.writer.is_alive());peer=None

def finish_proxies():
 global pg,reply
 errors=[]
 for name,proxy in [('PG',pg),('Redis',reply)]:
  if proxy is None:continue
  try:case[name]=proxy.close()
  except BaseException as error:
   errors.append(name+':'+type(error).__name__+':'+str(error)[:256]);case[name]={'physical_close_verified':False,'errors':[errors[-1]]}
  finally:
   if name=='PG':pg=None
   else:reply=None
 case.setdefault('proxy_close_errors',[]).extend(errors);save()
 if errors:raise RuntimeError('OwnedProxyCloseUnknown:'+','.join(errors))

def phase(index,label):
 global case,peer,pg,reply,injector,acl
 ident=str(uuid.uuid4());known.append(ident)
 programme=ProgramData.objects.create(epg=epg,tvg_id=epg.tvg_id,title='Owned Exact Series',sub_title='Owned Physical '+str(index),start_time=anchor+timedelta(hours=24+index*2),end_time=anchor+timedelta(hours=25+index*2),custom_properties={'season':1,'episode':index+1})
 if label=='oversize-programme-refused':programme.description='x'*8193;programme.save(update_fields=['description'])
 if label in ('settings-cache-reply','snapshot-server-error-Ready'):django_cache.delete(CoreSettings.group_cache_key('dvr_settings'));django_cache.delete(CoreSettings.group_cache_ver_key('dvr_settings'))
 case={'name':label,'id':ident,'began_utc':utc(),'before':snapshot(),'native_commands':[]};report['cases'].append(case);save()
 task=app.tasks['celery.backend_cleanup']if label=='foreign-family-refused'else tasks.evaluate_series_rules
 result=task.apply_async(args=[],kwargs={},task_id=ident,queue=QUEUE);case['published_parent']=[base64.b64encode(v).decode()for v in broker.lrange(QUEUE,0,-1)];save()
 cfg={'family':'series','broker_url':os.environ['CELERY_BROKER_URL'],'result_url':os.environ['CELERY_RESULT_BACKEND'],'cache_url':'redis://:playback-fixture-only@127.0.0.1:6379/0','database_url':os.environ['DATABASE_URL']}
 pgmode={'programme-DataRow-CommandComplete-Ready':'programme-success-Ready','Clocked-marker-COMMIT-Ready':'Clocked-COMMIT-Ready','Periodic-COMMIT-Ready':'Periodic-COMMIT-Ready','Recording-INSERT-P0001-Ready':'Recording-INSERT-P0001-Ready','snapshot-server-error-Ready':'settings-error-Ready'}.get(label)
 redismode={'settings-cache-reply':'settings','artwork-LPUSH-reply':'artwork','Channels-reply':'events','result-SETEX-reply':'result','result-reply-lost':'result-lost','result-SETEX-denied':'result-denied'}.get(label)
 if pgmode:pg=PGProxy(pgmode,expected_rows=2 if pgmode=='programme-success-Ready'else 1);cfg['database_url']='postgres://dispatch:playback-fixture-only@127.0.0.1:61651/'+os.environ['POSTGRES_DB']
 if redismode:
  reply=RedisProxy(redismode,ident,POLICY,channel_key.encode())
  target='cache_url'if redismode=='settings'else'broker_url'if redismode=='artwork'else'events_url'if redismode=='events'else'result_url'
  cfg=route_reply_gate(cfg,target)
 if label=='snapshot-server-error-Ready':settings_fault(True)
 if label=='Recording-INSERT-P0001-Ready':
  errors=[]
  def install():
   from django.db import connection as thread_connection
   try:
    if not pg.before_insert.wait(20):raise TimeoutError('ActualTaggedINSERTBeforeForward')
    with thread_connection.cursor()as c:
     c.execute('SELECT current_database()')
     if c.fetchone()[0]!=os.environ['POSTGRES_DB']:raise ValueError('OwnedInjectorDB')
     c.execute("CREATE FUNCTION rust_owned_recording_fail() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN RAISE EXCEPTION 'owned configured recording failure' USING ERRCODE='P0001'; END$$")
     c.execute('CREATE TRIGGER rust_owned_recording_fail BEFORE INSERT ON dispatcharr_channels_recording FOR EACH ROW EXECUTE FUNCTION rust_owned_recording_fail()')
   except BaseException as error:errors.append(type(error).__name__+':'+str(error)[:1024])
   finally:thread_connection.close();pg.insert_release.set()
  injector=threading.Thread(target=install);injector.start()
 if label=='result-SETEX-denied':
  info=cache.info('server');case['actual_Redis_INFO_server']=info;save();check(label+'-actual-pinned-server-version',info.get('redis_version')==POLICY['expected_redis_version'])
  acl='owned_configured_failure_1';cache.execute_command('ACL','SETUSER',acl,'on','>playback-fixture-only','~*','&*','+@all','-setex')
  case['ACL_preflight']=permission_profile(acl,app.backend.get_key_for_task(ident));save();check(label+'-exact-ACL-nonmutating-preflight',all(case['ACL_preflight'][key]is True for key in ('owned_profile_exact','setex_denied','publish_allowed','nonmutating')))
  cfg['result_url']='redis://'+acl+':playback-fixture-only@127.0.0.1:61603/9'
 if label=='oldSUCCESS-preserved':
  old=json.dumps({'status':'SUCCESS','result':'owned-old-configured-success','traceback':None,'children':[],'date_done':utc(),'task_id':ident}).encode();backend.setex(app.backend.get_key_for_task(ident),2700,old);case['old_backend']=raw_result(ident);save()
 case['transport_preflight']=inspect_config(cfg);case['before_native_child']={'utc':utc(),'native_process_created':False,'action_commands_sent':len(case['native_commands']),'proxy_commands':reply.count if reply else None};save()
 check(label+'-fixture-endpoints-before-Native-child',case['transport_preflight']['broker_result_same_address']is True and case['transport_preflight']['separate_broker_result_databases']==[8,9]and peer is None and 'native_process'not in case and case['before_native_child']['action_commands_sent']==0)
 if reply:check(label+'-proxy-unused-before-Native-child',case['before_native_child']['proxy_commands']==0)
 peer=start_peer(cfg,label)
 if label in ('result-SETEX-denied','oldSUCCESS-preserved','foreign-family-refused','oversize-programme-refused'):
  answer=send({'action':'run'});case['after_run']=capture();save()
  if label=='result-SETEX-denied':check(label+'-operational-no-fabricated-metadata',answer['ok']is False and case['after_run']['backend']['metadata']is None)
  elif label=='oldSUCCESS-preserved':check(label+'-oldSUCCESS-raw-and-TTL-preserved',answer['ok']is True and'AlreadySucceeded'in answer['result']and raw_result(ident)['raw_base64']==case['old_backend']['raw_base64']and 2600<=raw_result(ident)['ttl']<=case['old_backend']['ttl'])
  else:
   after_refusal=case['after_run']['effects'];unchanged=all(after_refusal[key]==case['before'][key]for key in ('recordings','periodic','clocked','programmes','system_settings','dvr_settings','sentinel'))
   if label=='foreign-family-refused':unchanged=unchanged and after_refusal['Django_cache']['raw_base64']==case['before']['Django_cache']['raw_base64']
   check(label+'-preACK-unsupported-frame-and-backend-preserved',broker.lrange(QUEUE,0,-1)==[base64.b64decode(v)for v in case['published_parent']]and raw_result(ident)['metadata']is None and unchanged)
 else:
  send({'action':'start'});gate=pg if pg else reply
  check(label+'-actual-target-reply-held',gate.held.wait(35));case['held_observation']=capture();case['held_monotonic']=time.monotonic();save()
  poll=send({'action':'poll'});check(label+'-caller-admission-held-before-abort',poll['caller_pending']is True and poll['caller_released']is False and poll['available_slots']==0)
  if label!='result-SETEX-reply'and label!='result-reply-lost':check(label+'-no-early-terminal',raw_result(ident)['metadata']is None)
  if label in ('settings-cache-reply','programme-DataRow-CommandComplete-Ready','snapshot-server-error-Ready'):check(label+'-parent-still-unconsumed',broker.lrange(QUEUE,0,-1)==[base64.b64decode(v)for v in case['published_parent']])
  send({'action':'abort'});poll=send({'action':'poll'});check(label+'-aborted-caller-retains-physical-owner',poll['caller_released']is False and poll['available_slots']==0)
  case['drain_command']={'action':'drain','sent_utc':utc()};save();peer.send({'action':'drain'});check(label+'-drain-cannot-acknowledge-held-reply',peer.poll_response(.25)is None)
  remaining=6.2-(time.monotonic()-case['held_monotonic'])
  if remaining>0:time.sleep(remaining)
  case['actual_release_request_utc']=utc();case['actual_release_monotonic']=time.monotonic();save();gate.release.set();drained=peer.receive(35);case['drain_reply']=drained;save();check(label+'-physical-drain-after-release',drained['drained']is True and drained['caller_released']is True)
  send({'action':'wait'})
 close_peer(label)
 if injector:injector.join(10);check(label+'-injector-settled',not injector.is_alive()and not errors);injector=None
 if view:settings_fault(False)
 if label=='Recording-INSERT-P0001-Ready':fault(False)
 finish_proxies();case['after']=snapshot();case['result']=raw_result(ident);save()
 for name in ('PG','Redis'):
  if name in case:check(label+'-'+name+'-physical-close-no-errors',case[name]['physical_close_verified']is True and not case[name]['errors'])
 try:case['Original_get_value']=result.get(timeout=2,propagate=False);case['Original_get_type']=type(case['Original_get_value']).__module__+'.'+type(case['Original_get_value']).__name__
 except BaseException as error:case['Original_get_error']={'type':type(error).__module__+'.'+type(error).__name__,'text':str(error)[:1024]}
 save()
 if label=='result-reply-lost':check(label+'-one-actual-write-no-replay',case['Redis']['target_replies']==1)
 if label=='result-SETEX-denied':check(label+'-actual-one-targeted-SETEX-denial',denied_write_observation(case['Redis'],POLICY['result_write_lua_sha1'],acl,POLICY['result_write_setex_line'],case['actual_Redis_INFO_server']['redis_version']))
 if label not in ('result-SETEX-denied','oldSUCCESS-preserved','snapshot-server-error-Ready','Recording-INSERT-P0001-Ready','foreign-family-refused','oversize-programme-refused'):
  metadata=case['result']['metadata'];check(label+'-actual-normal-result-and-two-children',metadata is not None and metadata.get('status')=='SUCCESS'and metadata.get('task_id')==ident and metadata.get('result')=={'scheduled':1,'details':[{'tvg_id':'rust.owned.configured','title':'Owned Exact Series','status':'ok','created':1}]}and metadata.get('traceback')is None and len(metadata.get('children',[]))==2 and case.get('Original_get_value')==metadata['result'])
 if label in ('snapshot-server-error-Ready','Recording-INSERT-P0001-Ready'):
  case['actual_PG_error_fields']=[error_fields(base64.b64decode(v['payload_base64'],validate=True))for v in case['PG']['frames']if v['direction']=='backend'and v['kind_hex']=='45'];save()
  message='owned configured settings read failure'if label=='snapshot-server-error-Ready'else'owned configured recording failure'
  check(label+'-actual-server-P0001-owned-error',len(case['actual_PG_error_fields'])==1 and case['actual_PG_error_fields'][0].get('C')=='P0001'and case['actual_PG_error_fields'][0].get('M')==message)
  check(label+'-operational-error-not-Source-normal-parity',case['result']['metadata']is None)
 if label not in ('foreign-family-refused','oversize-programme-refused','snapshot-server-error-Ready'):check(label+'-parent-ACK-observed',broker.llen(QUEUE)==0)
 check(label+'-all-default-children-untouched',not case['before']['frames']or case['after']['frames'][-len(case['before']['frames']):]==case['before']['frames'])
 if label not in ('foreign-family-refused','oversize-programme-refused'):check(label+'-no-residual-owned-global-lease',cache.get(GROUP)is None)
 if acl:cache.execute_command('ACL','DELUSER',acl);acl=None
 case['finished_utc']=utc();save()

try:
 for key in ['dvr:recover_dispatch_lock','core:version_check_dispatch_lock']:cache.set(key,'1',ex=3600)
 app.loader.import_default_modules();CoreSettings.set_system_time_zone('UTC');RecurringRecordingRule.objects.all().update(enabled=False)
 layer=get_channel_layer();receiver='specific.rust_owned_configured!receiver';channel_key=layer.prefix+receiver.split('!')[0]+'!';async_to_sync(layer.group_add)('updates',receiver)
 anchor=timezone.now().replace(microsecond=0);source=EPGSource.objects.create(name='rust-owned-configured-source',source_type='xmltv',is_active=False);epg=EPGData.objects.create(epg_source=source,tvg_id='rust.owned.configured',name='Owned Configured');channel=Channel.objects.create(name='Owned Configured',channel_number=999901,epg_data=epg,tvg_id=epg.tvg_id)
 rule={'title':'Owned Exact Series','tvg_id':epg.tvg_id,'title_mode':'exact','mode':'all','channel_id':channel.id,'epg_source_id':source.id};settings([rule])
 # Explicit setup context recreates the already-booked first four programmes,
 # not any already-passed Native physical control. Original model/signal/publication
 # calls are real; all seed rows/frames/instants are retained before Native.
 report['setup_context']={'kind':'four prebooked programmes via Original models/signals plus actual duplicate artwork producers','Native_control_replay':False,'Source_behavior_reference_capture':False,'before':snapshot(),'began_utc':utc()};save()
 seeded=[]
 for seed_index in range(4):
  booked=ProgramData.objects.create(epg=epg,tvg_id=epg.tvg_id,title='Owned Exact Series',sub_title='Owned Physical '+str(seed_index),start_time=anchor+timedelta(hours=24+seed_index*2),end_time=anchor+timedelta(hours=25+seed_index*2),custom_properties={'season':1,'episode':seed_index+1})
  props={'program':{'id':booked.id,'tvg_id':booked.tvg_id,'title':booked.title,'sub_title':booked.sub_title,'description':booked.description,'original_air_date':'','start_time':booked.start_time.isoformat(),'end_time':booked.end_time.isoformat(),'season':1,'episode':seed_index+1,'onscreen_episode':None,'epg_source_id':source.id}}
  recorded=Recording.objects.create(channel=channel,start_time=booked.start_time,end_time=booked.end_time,custom_properties=props)
  duplicate=tasks.prefetch_recording_artwork.apply_async(args=[recorded.id],countdown=1)
  seeded.append({'programme_id':booked.id,'recording_id':recorded.id,'duplicate_artwork_id':duplicate.id,'observed_utc':utc()})
 report['setup_context'].update(after=snapshot(),booked_rows=seeded,finished_utc=utc());save()
 for index,label in enumerate(MODES,start=4):phase(index,label)
 report.update(passed=True,Native_verified=True)
except BaseException as error:
 report.update(error_type=type(error).__name__,error=str(error)[:4096]);raise
finally:
 cleanup_errors=[]
 if pg:pg.release.set();pg.insert_release.set()
 if reply:reply.release.set()
 if peer:
  try:
   if peer.poll()is None:peer.abort('PhysicalFinally')
   peer.finish(10)
  except BaseException as error:cleanup_errors.append('peer:'+type(error).__name__)
  finally:
   try:retain_peer(case['name'],peer)
   except BaseException as error:cleanup_errors.append('peer-raw-retention:'+type(error).__name__)
 if injector:injector.join(10);cleanup_errors+=['injector-unsettled']if injector.is_alive()else[]
 if view:
  try:settings_fault(False)
  except BaseException as error:cleanup_errors.append('view:'+type(error).__name__)
 if pg or reply:
  try:finish_proxies()
  except BaseException as error:cleanup_errors.append('proxies:'+type(error).__name__)
 if case.get('name')=='Recording-INSERT-P0001-Ready':
  try:fault(False)
  except BaseException as error:cleanup_errors.append('trigger:'+type(error).__name__)
 if acl:
  try:cache.execute_command('ACL','DELUSER',acl)
  except BaseException as error:cleanup_errors.append('ACL:'+type(error).__name__)
 report['finally_errors']=cleanup_errors
 if cleanup_errors:report['passed']=False
 save()
if not report['passed']:raise SystemExit(1)
