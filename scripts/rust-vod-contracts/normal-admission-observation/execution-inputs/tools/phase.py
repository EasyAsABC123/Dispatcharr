"""Blocked fixed normal-capacity observation phase, real SQL/HTTP only after cue."""
import concurrent.futures,hashlib,json,os,sys,time,threading,urllib.request
from pathlib import Path
if os.environ.get('NORMAL_REVIEWED_EXECUTION')!='one-normal-max2-pair':raise RuntimeError('ReviewedNormalPairRequired')
from normal_guard import child_guard
child_guard()
sys.path.insert(0,'/app');sys.argv.append('shell');os.environ.setdefault('DJANGO_SETTINGS_MODULE','dispatcharr.settings')
import django;django.setup()
import redis,socket
from absolute_http import Observation
from acceptance import phase as accept_phase
from apps.m3u.models import M3UAccount,M3UAccountProfile
from django.db import connections
from schedule import Controller,SESSIONS
from reader import Reader,ownership,close_response
from decoder import decode
ROOT=Path('/evidence');seed=json.loads((ROOT/'pending-main-private.json').read_text());mode=os.environ['NORMAL_MODE'];base=os.environ['NORMAL_BASE'];nonce=os.environ['NORMAL_FIXTURE_NONCE'];assert mode in['source','native'];report={'mode':mode,'HTTP_capture_valid':False,'scenario_ready':False,'performance_acceptance':False,'full_M4_acceptance':False,'probe_outcomes':[],'checkpoints':[]};began=time.monotonic();schedule=Controller();responses={};owners={};readers={};auth={'Authorization':'Bearer '+seed['access_token']};store=redis.Redis(host='127.0.0.1',port=6390 if mode=='source'else 6391,password='playback-fixture-only',decode_responses=True,socket_timeout=3,socket_connect_timeout=3)
from urllib.parse import urlsplit
resolved_HTTP_address=socket.gethostbyname(urlsplit(base).hostname)
report['HTTP_pre_resolved_address']=resolved_HTTP_address
movies={'shared_initial':seed['movie_uuid'],'shared_follower':seed['movie_uuid'],'other':seed['other_movie_uuid'],'probe1':seed['probe1_movie_uuid'],'probe2':seed['probe2_movie_uuid']}
PREFIX='02921f832147133cdb84ff2dfae8ac4868e95abefdd1cc98a73d54a5c6202a1f'
def deadline():
 if time.monotonic()-began>=120:raise TimeoutError('FullNormalPhase120Seconds')
def caps():
 account=M3UAccount.objects.get(pk=seed['provider_account_id']);profile=M3UAccountProfile.objects.get(pk=seed['provider_profile_id']);value={'account':account.max_streams,'profile':profile.max_streams}
 if value!={'account':2,'profile':2}:raise ValueError('NormalMax2NeverMutated')
 return value
def origin():
 request=urllib.request.Request('http://origin:8080/ledger',headers={'X-Fixture-Nonce':nonce})
 with urllib.request.urlopen(request,timeout=3)as response:raw=response.read(65537)
 if len(raw)>65536:raise ValueError('OriginSnapshotCapacity')
 value=json.loads(raw)
 if value.get('invalid')or value.get('nonce_sha256')!=hashlib.sha256(nonce.encode()).hexdigest():raise ValueError('OwnedOriginSnapshot')
 return value
def source_signals():
 if mode!='source':return{'available':False,'claim':'Native server finalization signal not instrumented'}
 with urllib.request.urlopen(urllib.request.Request('http://127.0.0.1:6385/state',headers={'X-Fixture-Nonce':nonce}),timeout=3)as response:raw=response.read(4097)
 if len(raw)>4096:raise ValueError('SignalStatusBound')
 status=json.loads(raw);rows=[json.loads(line)for line in(ROOT/'normal-signals.jsonl').read_text().splitlines()]if(ROOT/'normal-signals.jsonl').exists()else[]
 if status['invalid']or status['rows']!=len(rows):raise ValueError('NormalSignalCoverageInvalid')
 from normal_signals import validate
 validate(status,rows)
 return{'available':True,'status':status,'rows':rows,'shared_finalized':all(any(row['kind']=='request_finalized'and row['role']==role for row in rows)for role in['shared_initial','shared_follower'])}
def private_other():
 if mode!='native':return None
 owner='rust:coordination:vod:session:'+SESSIONS['other']+':owner';token=store.get(owner)
 if not token or len(token)>256:raise ValueError('OtherPrivateOwnerMissing')
 ledger=store.hgetall('rust:coordination:lease:'+token);meta=store.hgetall('vod_persistent_connection:'+SESSIONS['other']);return{'owner':token,'owner_confirm':store.get(owner),'ledger':{key:ledger.get(key)for key in['token','owner','profile','reserved','fence']},'metadata':{key:meta.get(key)for key in['content_uuid','m3u_profile_id','active_streams','created_at','rust_owner_token']}}
def checkpoint(label):
 deadline();counter=store.get('profile_connections:'+str(seed['provider_profile_id']));schedule.evidence_counter(label,counter);value={'label':label,'at':time.monotonic(),'literal_profile_counter':counter,'shared_metadata':{key:store.hget('vod_persistent_connection:'+SESSIONS['shared_initial'],key)for key in['active_streams','request_count','created_at','m3u_profile_id']},'origin':origin(),'readers':{role:reader.snapshot()for role,reader in list(readers.items())},'native_other_private':private_other()if'other'in responses else None};report['checkpoints'].append(value);return value
def safe_checkpoint(label):
 try:return checkpoint(label)
 except Exception as error:
  value={'label':label,'at':time.monotonic(),'observation_error':type(error).__name__};report['checkpoints'].append(value);return value
def safe_signals():
 try:return source_signals()
 except Exception as error:return{'available':mode=='source','observation_error':type(error).__name__,'shared_finalized':False}
def open_response(role):
 owner=Observation(base,address=resolved_HTTP_address);response=owner.request('/proxy/vod/movie/'+movies[role]+'/'+SESSIONS[role],{**auth,'User-Agent':'owned-normal-'+role});responses[role]=response;owners[role]=ownership(response)
 return response
def snapshot_live(role):
 if role not in readers:return {'missing':True}
 value=readers[role].snapshot();value['socket_fd']=readers[role].sock.fileno();return value
def close_role(role):
 if role in readers:row=readers.pop(role).close()
 else:row=close_response(responses[role],role,owners[role])
 report.setdefault('client_closes',[]).append(row);responses.pop(role,None);return row
def probe(role):
 result={'role':role,'began':time.monotonic()}
 try:
  response=open_response(role);result.update(status=response.status_code,headers=dict(response.headers),headers_at=time.monotonic())
  if response.status_code==200:
   reader=Reader(response,ROOT/(mode+'-'+role+'.mp4'),role);readers[role]=reader;value=reader.await_initial(max(.001,response.owner.deadline-time.monotonic()));result['prefix_sha256']=hashlib.sha256(value).hexdigest();result['prefix_matches_fixture']=result['prefix_sha256']==PREFIX
  else:
   body=response.raw.read(65537)
   if len(body)>65536:raise ValueError('ProbeErrorBodyCapacity')
   result.update(body_hex=body.hex(),body_bytes=len(body),body_sha256=hashlib.sha256(body).hexdigest())
  result['absolute_observation']=response.owner.complete()
  if response.status_code!=200:response.close();responses.pop(role,None)
 except Exception as error:
  result.update(transport_error=type(error).__name__,observation_error=True)
  if hasattr(error,'physical_close_receipt'):result['failure_physical_close']=error.physical_close_receipt
  if role in responses:
   try:result['failure_physical_close']=close_role(role)
   except Exception as close_error:result['physical_close_error']=type(close_error).__name__
 result['initial_observation_complete']=time.monotonic();report['probe_outcomes'].append(result);return result
try:
 report['caps_before']=caps();me_owner=Observation(base,seconds=8,address=resolved_HTTP_address)
 try:
  me=me_owner.request('/api/accounts/users/me/',auth);me_body=me.raw.read(65537);me_owner.complete()
 finally:me_owner.cancel()
 if len(me_body)>65536:raise ValueError('UserBodyCapacity')
 report['actual_SQL_Bearer_verified']=me.status_code==200 and json.loads(me_body)['id']==seed['real_user_id']
 if not report['actual_SQL_Bearer_verified']:raise ValueError('ActualSQLBearerUser')
 for role in['shared_initial','shared_follower','other']:
  response=open_response(role)
  if response.status_code!=200:report.setdefault('prelude_statuses',{})[role]=response.status_code;raise ValueError('FixedPreludeNotEstablished')
  owners[role]=ownership(response);value=response.raw.read(8192);response.owner.complete();digest=hashlib.sha256(value).hexdigest();schedule.prelude_open(role,response.status_code,digest)
  if role=='other':readers[role]=Reader(response,ROOT/(mode+'-other.mp4'),role,initial=value)
 report['before_stop']=safe_checkpoint('before_stop');initial_grant=report['before_stop'].get('native_other_private');stop_owner=Observation(base,seconds=8,address=resolved_HTTP_address)
 try:
  stop=stop_owner.request('/proxy/vod/stop_client/',{**auth,'User-Agent':'owned-normal-admin-stop','Content-Type':'application/json'},method='POST',body=json.dumps({'client_id':SESSIONS['shared_initial']}).encode());stop_body=stop.raw.read(65537);stop_owner.complete();report['admin_stop']={'status':stop.status_code,'body_hex':stop_body[:65536].hex(),'body_bytes':len(stop_body)}
 finally:stop_owner.cancel()
 if len(stop_body)>65536:report['admin_body_oversized']=True
 admin_at=time.monotonic();schedule.admin_completed(admin_at,stop.status_code);report['early_stop_literal']=store.get('vod_proxy:client:'+SESSIONS['shared_initial']+':stop');first=close_role('shared_initial');follower=close_role('shared_follower');schedule.shared_closed(first,follower)
 observations=concurrent.futures.ThreadPoolExecutor(max_workers=2);target=schedule.probe1_at;delay=target-time.monotonic()
 if delay>0:time.sleep(delay)
 before_one=observations.submit(safe_checkpoint,'fixed_probe1_boundary');signals_one=observations.submit(safe_signals);report['other_at_probe1_dispatch']=snapshot_live('other');schedule.dispatch_probe1(time.monotonic());one=probe('probe1');schedule.probe1_observed(one['initial_observation_complete'],one.get('status'),one.get('transport_error'))
 # Observation reads cannot delay/skip probe2; collect in a separately owned task.
 after_one=observations.submit(safe_checkpoint,'after_probe1')
 delay=schedule.probe2_at-time.monotonic()
 if delay>0:time.sleep(delay)
 report['probe1_at_probe2_dispatch']=snapshot_live('probe1');report['other_at_probe2_dispatch']=snapshot_live('other');schedule.dispatch_probe2(time.monotonic());two=probe('probe2');before_one.result(timeout=15);after_one.result(timeout=15);report['shared_finalization_observation']=signals_one.result(timeout=15);report['probe1_dispatch_monotonic']=schedule.receipts[0]['dispatched'];report['shared_response_finalized_by_probe1']=all(any(row['role']==role and row['kind']=='request_finalized'and row['monotonic']<=schedule.receipts[0]['dispatched']for row in report['shared_finalization_observation'].get('rows',[]))for role in['shared_initial','shared_follower'])if mode=='source'else None;observations.shutdown(wait=True);safe_checkpoint('after_probe2')
 # All admitted probe bodies stay open and draining through second admission.
 if len(readers)>3:raise ValueError('MaximumThreeRetainedStreams')
 with concurrent.futures.ThreadPoolExecutor(max_workers=3)as pool:
  jobs={role:pool.submit(reader.await_prefix)for role,reader in readers.items()}
  for role,future in jobs.items():future.result(timeout=max(.01,min(40,began+120-time.monotonic())))
  jobs={role:pool.submit(decode,reader.path,ROOT/(mode+'-'+role+'-decode.log'))for role,reader in readers.items()};report['decoders']={role:future.result(timeout=max(.01,min(45,began+120-time.monotonic())))for role,future in jobs.items()}
 if any(value['retained_sha256']!='b4a9992aa94b69eaaebf85025ec5b55f3ebf6360b143196b0d056fcb04c6b097'for value in report['decoders'].values()):raise ValueError('ExactActualFixturePrefixes')
 report['after_decode']=safe_checkpoint('after_decode');report['caps_after']=caps();report['native_other_grant_before']=initial_grant;report['native_other_grant_after']=report['after_decode'].get('native_other_private');report['native_other_grant_preserved']=None
 if mode=='native':
  try:
   from grants import preserved
   report['native_other_grant_preserved']=preserved(initial_grant,report['native_other_grant_after'],'profile_connections:'+str(seed['provider_profile_id']),seed['other_movie_uuid'])
  except Exception as error:report['native_other_grant_preserved']={'preserved':False,'error':type(error).__name__}
 report['no_counter_or_status_dispatch_input']=schedule.snapshot()
except Exception as error:report['error_type']=type(error).__name__;report['error_message']=str(error)[:128]
finally:
 if 'observations'in locals():observations.shutdown(wait=True)
 report['readers_before_teardown']={role:snapshot_live(role)for role in list(readers)};report['other_before_teardown']=snapshot_live('other')
 for role in['probe2','probe1','other','shared_follower','shared_initial']:
  if role in responses:
   try:close_role(role)
   except Exception as error:report.setdefault('close_errors',[]).append({'role':role,'error':type(error).__name__});report['HTTP_capture_valid']=False
 try:
  if mode=='source':
   end=time.monotonic()+10
   while True:
    try:report['terminal_signals']=source_signals()
    except ValueError as transient:
     if str(transient)not in['NormalSignalCoverageInvalid','NormalSignalStatusCount']or time.monotonic()>=end:raise
     time.sleep(.05);continue
    from normal_signals import validate
    try:report['terminal_signal_validation']=validate(report['terminal_signals']['status'],report['terminal_signals']['rows'],complete=True);report['shared_response_finalized_by_probe1']=all(any(row['role']==role and row['kind']=='request_finalized'and row['monotonic']<=report.get('probe1_dispatch_monotonic',-1)for row in report['terminal_signals']['rows'])for role in['shared_initial','shared_follower']);break
    except ValueError as error:
     if str(error)!='NormalFinalizationIncomplete'or time.monotonic()>=end:raise
     time.sleep(.05)
  else:report['terminal_signals']=source_signals()
  report['terminal_origin']=origin();report['caps_terminal']=caps()
 except Exception as error:report['terminal_observation_error']=type(error).__name__;report['HTTP_capture_valid']=False
 connections.close_all();report['elapsed_seconds']=time.monotonic()-began
 if report['elapsed_seconds']>120:report['HTTP_capture_valid']=False;report['phase_deadline_exceeded']=True
 report.update(accept_phase(report))
 (ROOT/('normal-'+mode+'-private.json')).write_text(json.dumps(report,indent=2)+'\n')
if not report['HTTP_capture_valid']:raise SystemExit(1)
