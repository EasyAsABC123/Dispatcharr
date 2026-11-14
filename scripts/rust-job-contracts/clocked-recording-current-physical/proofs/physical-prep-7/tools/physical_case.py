"""Fixed real-owner physical controller; no process or socket starts on import.
Original Dispatcharr authors retain application/media/algorithm credit.
"""
import base64,datetime,json,os,re,signal,time
from pathlib import Path
from interactive_peer import Peer
from physical_pg_proxy import Proxy as PgProxy
from redis_reply_proxy import Proxy as RedisProxy
from run_source import settle_interrupted_collection
MODES=('cache-preack','sql-preack-ready','sql-update-error-ready','capture-observer-loss','channels-reply','result-reply','preexisting-active')

def identity(pid):
 try:return Path('/proc',str(pid),'stat').read_text().rsplit(')',1)[1].split()[19]
 except FileNotFoundError:return None

def execute(context):
 mode=os.environ['DVR_PHYSICAL_MODE']
 if mode not in MODES:raise ValueError('FixedDvrPhysicalMode')
 root=context['ROOT'];report=context['report'];write=context['write_report'];rec=context['rec'];dispatch=context['dispatch'];snapshot=context['snapshot'];cache=context['cache'];record_id=context['record_id'];connection=context['connection']
 if os.environ.get('JOB_DISPOSABLE_FIXTURE')!='playback-fixture-only'or not os.environ.get('POSTGRES_DB','').startswith('rust_probe_backup_jobs_'):raise ValueError('CopiedOwnedContext')
 policy=json.loads(Path('/job-tools/pg-policy.json').read_text());redis_policy=json.loads(Path('/job-tools/redis-policy.json').read_text());case={'mode':mode,'passed':False,'capture_only':True,'samples':[],'no_Source_fault_result_authority':True,'initial':snapshot('physical-before-native')};report['physical']=case;write();peer=None;sql=None;relay=None;ledger=None;holder={};number=0
 def save():write()
 def response(action,seconds=35):
  nonlocal number
  number+=1
  if number>16:raise ValueError('SixteenPeerCommands')
  if peer is None:raise ValueError('ActualPeerRequired')
  peer.send({'id':number,'action':action});value=peer.receive(seconds)
  if type(value.get('id'))is not int or value['id']!=number or value.get('action')!=action:raise ValueError('ExactActualPeerResponse')
  point={'purpose':action,'received_monotonic':time.monotonic(),'received_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'peer_pid':peer.process.pid,'peer_starttime':holder['starttime'],'observed_peer_starttime':identity(peer.process.pid),'reply':value,'snapshot':snapshot('physical-'+action+'-'+str(number))}
  if point['observed_peer_starttime']!=holder['starttime']:raise ValueError('SameActualPeerIdentity')
  if sql is not None and sql.stats()['held']is not None:
   held=sql.stats()['held'];pid=held['backend_pid']
   with connection.cursor()as cursor:
    cursor.execute('SELECT pid,datname,state,query FROM pg_stat_activity WHERE pid=%s AND datname=current_database() LIMIT 2',[pid]);rows=cursor.fetchall()
   point['actual_target_backend']={'pid':pid,'rows':rows,'connection':held['connection'],'sql':held['sql']}
  case['samples'].append(point);save();return point
 def stop_actual_peer():
  case['owned_stop_requested']=True;save()
  process=holder.get('process');start=holder.get('starttime')
  if process is not None and process.poll()is None and start is not None and identity(process.pid)==start:process.terminate()
 def wait_until(deadline):
  while time.monotonic()<deadline:
   if peer is not None and peer.process.poll()is not None:raise RuntimeError('ActualPhysicalPeerExitedBeforeGate')
   time.sleep(min(.02,max(0,deadline-time.monotonic())))
 try:
  native={'mode':mode,'database_url':os.environ['DATABASE_URL'],'broker_url':os.environ['CELERY_BROKER_URL'],'result_url':os.environ['CELERY_RESULT_BACKEND'],'cache_url':'redis://:playback-fixture-only@localhost:6379/0','events_url':'redis://:playback-fixture-only@localhost:6379/0','media_uid':os.getuid()}
  active='dvr:recording-active:'+str(rec.id)
  if mode=='preexisting-active':
   if not cache.set(active,b'1',nx=True,ex=45):raise ValueError('ActualForeignFixtureKeyAbsentBeforeSETNX')
   case['foreign_active_before']=snapshot('owned-foreign-literal-before-native');save()
  if mode in('sql-preack-ready','sql-update-error-ready'):
   error=mode=='sql-update-error-ready'
   if error:
    with connection.cursor()as cursor:
     cursor.execute("CREATE FUNCTION rust_owned_dvr_prime_failure() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'owned-dvr-physical-prime' USING ERRCODE='P0001'; END; $$")
     cursor.execute("CREATE TRIGGER rust_owned_dvr_prime_failure BEFORE UPDATE OF custom_properties ON dispatcharr_channels_recording FOR EACH ROW WHEN (NEW.id="+str(int(rec.id))+" AND NEW.custom_properties->>'status'='recording') EXECUTE FUNCTION rust_owned_dvr_prime_failure()")
    case['owned_trigger']={'function':'rust_owned_dvr_prime_failure','table':'dispatcharr_channels_recording','recording':rec.id,'declared_code':'P0001','Source_error_authority':False};save()
   want={'sql':policy['prime_update']if error else policy['snapshot'],'rows':0 if error else 1,'commands':[]if error else['SELECT 1'],'errors':['P0001']if error else[],'tx':'I','recording_id':int(rec.id),'parameter_kind':'prime-properties-cas'if error else'recording-id-only','expected_old_properties':case['initial']['recordings'][0]['custom_properties']}
   ledger=(root/'physical-pg-events.jsonl').open('x');sql=PgProxy(('127.0.0.1',5432),[1],{1:want},ledger);native['database_url']=native['database_url'].replace('@localhost:5432/','@localhost:62130/')
  if mode in('cache-preack','channels-reply','result-reply'):
   kind={'cache-preack':'cache','channels-reply':'events','result-reply':'result'}[mode];relay=RedisProxy(kind,record_id,redis_policy,context['channel_key'].encode());field={'cache':'cache_url','events':'events_url','result':'result_url'}[kind];native[field]=native[field].replace(':6379/',':'+str(relay.server.server_address[1])+'/')
  case['actual_config']=native;case['actual_command_began_utc']=context['utc']();save()
  env=dict(os.environ,DVR_NATIVE_MODE='physical-once');argv=['/native-peer/native-job-tests','recording_dvr_physical_peer::actual_retained_dvr_gate_peer','--exact','--ignored','--nocapture','--test-threads=1'];peer=Peer(argv,env,stop_actual_peer);holder.update(process=peer.process,starttime=identity(peer.process.pid))
  if not holder['starttime']or not holder['starttime'].isdigit():raise ValueError('ObservedNativePIDStartRequired')
  case['actual_peer']={'argv':argv,'pid':peer.process.pid,'starttime':holder['starttime'],'actual_pgid':os.getpgid(peer.process.pid)};peer.send(native);started=peer.receive(20);case['actual_started']=started;command_monotonic=time.monotonic();save()
  if started.get('stage')!='started'or started.get('mode')!=mode:raise ValueError('ActualFixedModeStarted')
  if mode=='preexisting-active':response('finish',35)
  else:
   if mode=='result-reply':
    # Result cannot precede the real configured end and capture/remux settlement.
    # This is a stage wait, not an increased target-probe or task budget.
    from result_stage import stage_wait, settled_media
    stage=stage_wait(context['end_time'],context['timezone'].now(),time.monotonic(),command_monotonic)
    case['result_stage']=stage;save();wait_until(stage['end_monotonic']);stage['actual_end_wait_complete_monotonic']=time.monotonic();save()
   deadline=time.monotonic()+15;anchor=None
   if mode=='result-reply':case['result_stage']['target_probe_deadline_monotonic']=deadline;save()
   while time.monotonic()<deadline:
    if sql is not None:
     stats=sql.stats();held=stats['held']
     if held is not None:
      events=[e for e in stats['events']if e['kind']=='ready-held'and e['backend_pid']==held['backend_pid']and e['connection']==held['connection']];anchor=events[-1]['monotonic']if len(events)==1 else None
      if anchor is not None:case['actual_gate']={'kind':'PGReady','held':held,'held_monotonic':anchor,'events':stats['events']};break
    elif relay is not None:
     if relay.held.is_set():
      anchor=relay.held_at
      if mode=='result-reply':
       point=response('sample');point['purpose']='actual-capture-remux-settled-before-result-hold'
       settled_media(point['reply']['diagnostics']['processes']['rows'])
       if anchor<case['result_stage']['end_monotonic']:raise ValueError('NoResultWriteBeforeActualConfiguredEnd')
       case['result_stage']['actual_settlement_observed_monotonic']=point['received_monotonic'];save()
      case['actual_gate']={'kind':'RedisReply','held_monotonic':anchor,'receipts':list(relay.receipts)};break
    else:
     point=response('sample');rows=point['reply']['diagnostics']['processes']['rows'];running=[r for r in rows if r['settled_utc']is None and type(r.get('identity'))is dict]
     if running:anchor=point['received_monotonic'];case['actual_gate']={'kind':'OwnedCaptureChild','held_monotonic':anchor,'children':running};break
    if peer.process.poll()is not None:raise RuntimeError('ActualPeerExitedBeforePhysicalGate')
    time.sleep(.05)
   if anchor is None:raise TimeoutError('ActualPhysicalTarget15s')
   if time.monotonic()-command_monotonic>=180:raise TimeoutError('UnchangedActualTask180')
   save();wait_until(anchor+2.2);first=response('sample');first['purpose']='held-sample1'
   if mode in('cache-preack','sql-preack-ready','capture-observer-loss'):response('abort-observer')
   wait_until(max(anchor+2.55,time.monotonic()+.27));second=response('sample');second['purpose']='held-sample2';save()
   if mode=='channels-reply':
    wait_until(anchor+3.3);response('sample')['purpose']='after-3s-deadline-sample1';wait_until(max(anchor+3.65,time.monotonic()+.27));response('sample')['purpose']='after-3s-deadline-sample2';save()
   response('drain-probe');wait_until(anchor+6.2);case['release_requested_monotonic']=time.monotonic();save()
   if sql is not None:sql.unlock(0)
   elif relay is not None:relay.release.set()
   if mode=='capture-observer-loss':
    seconds=max(0,(context['end_time']-context['timezone'].now()).total_seconds())+8;wait_until(time.monotonic()+seconds);response('sample')['purpose']='after-natural-body-settlement';save()
   response('finish',45)
  peer.commands.put_nowait(None)
  try:peer.wait(20)
  finally:
   (root/'physical-peer.stdout').write_bytes(bytes(peer.output['stdout']));(root/'physical-peer.stderr').write_bytes(bytes(peer.output['stderr']))
  case['peer_collection']={'actual_exit':peer.process.returncode,'physically_settled':peer.physically_settled,'invalid':peer.invalid,'writer_error':peer.writer_error,'pipes_settled':not any(t.is_alive()for t in peer.threads),'writer_settled':not peer.writer.is_alive(),'original_process_absent':identity(peer.process.pid)!=holder['starttime']};save()
  case['actual_native']=json.loads((root/'native-dvr-physical-private.json').read_text());case['final']=snapshot('physical-final');case['retained_media']=context['media_files'].snapshot(root/'retained-physical-recordings');case['actual_command_finished_utc']=context['utc']();case['capture_complete']=True;save()
 finally:
  cleanup_errors=[]
  if peer is not None and not peer.physically_settled:
   try:
    try:peer.commands.put_nowait(None)
    except Exception as error:case['writer_close_error']=type(error).__name__
    case['failed_peer_settlement']=settle_interrupted_collection(peer,'PhysicalCaseInterrupted');(root/'physical-peer.stdout').write_bytes(bytes(peer.output['stdout']));(root/'physical-peer.stderr').write_bytes(bytes(peer.output['stderr']))
   except BaseException as error:cleanup_errors.append({'resource':'physical-peer','type':type(error).__name__,'error':str(error)[:512]})
  for name,resource in [('PG',sql),('Redis',relay)]:
   if resource is not None:
    try:case[name+'_close']=resource.close()
    except BaseException as error:cleanup_errors.append({'resource':name,'type':type(error).__name__,'error':str(error)[:512]})
  if ledger is not None:
   try:ledger.flush();os.fsync(ledger.fileno());ledger.close()
   except BaseException as error:cleanup_errors.append({'resource':'PG-ledger','type':type(error).__name__,'error':str(error)[:512]})
  case['cleanup_errors']=cleanup_errors;save()
 return case
