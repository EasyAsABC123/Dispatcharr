"""UNEXECUTED genuine Source/Main SQL/auth early-follower phase, owned input only."""
import hashlib,http.client,json,os,socket,sys,time,threading
from urllib.parse import urlsplit
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
assert os.environ.get('PENDING_MAIN_FIXTURE')=='playback-fixture-only'
assert os.environ.get('POSTGRES_DB','').startswith('rust_pending_')
sys.path.insert(0,'/app');sys.argv.append('shell');os.environ.setdefault('DJANGO_SETTINGS_MODULE','dispatcharr.settings')
import django;django.setup()
import redis,requests
from fixture_guards import other_reservation,early_stop
import observation
from target_client import Target
from django.db import connections
from apps.m3u.models import M3UAccount,M3UAccountProfile
ROOT=Path('/evidence');seed=json.loads((ROOT/'pending-main-private.json').read_text());mode=os.environ['PENDING_MODE'];case=os.environ['PENDING_CASE'];assert mode in['source','native']and case in['success','busy','clientloss']
base=os.environ['PENDING_BASE'];client=redis.Redis(host='127.0.0.1',port=int(os.environ['REDIS_PORT']),password='playback-fixture-only',decode_responses=True,socket_timeout=3,socket_connect_timeout=3);key='profile_connections:'+str(seed['provider_profile_id']);sid='fixture-pending-main-'+case;metadata='vod_persistent_connection:'+sid
responses=[];report={'observation_valid':False,'parity_passed':None,'scope':'Separate fixed4s HTTP/SQL/auth observation; validity and parity distinct; no full metadata/latebody/performance claim','backend':mode,'case':case,'observations':[],'source_control':'actual Original Redis INCR reply held'if mode=='source'else'actual Redis executed provider reply held'}
auth={'Authorization':'Bearer '+seed['access_token']}
def cap(value):
 account=M3UAccount.objects.get(pk=seed['provider_account_id']);profile=M3UAccountProfile.objects.get(pk=seed['provider_profile_id']);assert account.server_url=='http://origin:8080/player_api.php';account.max_streams=value;account.save(update_fields=['max_streams']);profile.max_streams=value;profile.save(update_fields=['max_streams'])
def wait(check,seconds=10):
 end=time.monotonic()+seconds
 while time.monotonic()<end:
  if check():return
  time.sleep(.01)
 raise TimeoutError('OwnedPhaseDeadline')
def gate(action):
 with socket.create_connection(('127.0.0.1',6384 if mode=='source'else 6382),timeout=3)as connection:
  value={'action':action}
  if action=='arm':value.update(profile_key=key,expected_reply=(2 if mode=='source'else -2)if case=='busy'else 1)
  connection.sendall((json.dumps(value)+'\n').encode());raw=connection.makefile('rb').readline(1048577)
  if len(raw)>1048576:raise ValueError('GateControlCapacity')
  return json.loads(raw)
def reached():
 state=gate('state');return bool(state.get('reached')or state.get('seen',0)>=1)
def snapshot():
 began=time.monotonic();row=client.hgetall(metadata);provider=client.get(key);stop=client.get('vod_proxy:client:'+sid+':stop')
 return{'began_monotonic':began,'completed_monotonic':time.monotonic(),'atomic':False,'metadata':{k:row.get(k)for k in ['user_id','active_streams','request_count','created_at','content_uuid','m3u_profile_id']},'provider':provider,'stop':stop}
def other_private():
 owner_key='rust:coordination:vod:session:fixture-pending-main-other:owner';token=client.get(owner_key)
 if not token:raise ValueError('OtherMovieOwnerMissing')
 if len(token)>256:raise ValueError('OtherMovieTokenCapacity')
 ledger_key='rust:coordination:lease:'+token
 pipe=client.pipeline(transaction=True);pipe.hgetall(ledger_key);pipe.hgetall('vod_persistent_connection:fixture-pending-main-other');pipe.get(owner_key)
 ledger,meta,confirm=pipe.execute()
 return{'owner_key':owner_key,'owner_token':token,'owner_confirm':confirm,'ledger_key':ledger_key,'ledger':{k:ledger.get(k)for k in['token','owner','profile','reserved','fence']},'metadata':{k:meta.get(k)for k in['content_uuid','user_id','m3u_profile_id','active_streams','created_at','rust_owner_token']}}
def physical_boundary():
 value=gate('state')
 rows=[r for r in value['rows']if r['kind']=='backend-target-reply-read-held']
 if len(rows)!=1:raise ValueError('UniqueActualReplyBoundary')
 return rows[0]['monotonic']
def get(movie,session,headers):
 response=requests.get(base+'/proxy/vod/movie/'+movie+'/'+session,headers=headers,stream=True,timeout=8,allow_redirects=False);responses.append(response)
 response._fixture_socket=extract_socket(response)
 return response
def prefix(response):
 value=response.raw.read(8192)
 if len(value)!=8192:raise ValueError('PrefixSize')
 return hashlib.sha256(value).hexdigest()
def extract_socket(response):
 original=getattr(response.raw,'_original_response',None);fp=getattr(original,'fp',None);raw=getattr(fp,'raw',None)
 return getattr(raw,'_sock',None)or getattr(getattr(response.raw,'_connection',None),'sock',None)
def close_response(response,role):
 sock=getattr(response,'_fixture_socket',None);row={'role':role,'close_begin':time.monotonic(),'socket_observed':sock is not None,'server_cancellation_inferred':False}
 try:
  if sock is None:raise ValueError('ConsumerSocketOwnershipUnknown')
  row['fd_before']=sock.fileno()
  if row['fd_before']>=0:
   try:sock.shutdown(socket.SHUT_RDWR);row['local_shutdown']=True
   except OSError as error:row['shutdown_error_type']=type(error).__name__
  else:row['already_local_descriptor_closed']=True
 finally:
  try:response.close()
  finally:
   if sock is not None:sock.close();row['fd_after']=sock.fileno()
   row['close_complete']=time.monotonic();report.setdefault('client_closes',[]).append(row)
 if row.get('fd_after')!=-1:raise ValueError('ConsumerSocketNotClosed')
 return row
outcome_lock=threading.Lock();target_record={};abort_target=Target(base)if case=='clientloss'else None
def target_request():
 began=time.monotonic()
 try:
  if abort_target is None:response=get(seed['movie_uuid'],sid,{'User-Agent':'owned-pending-target'})
  else:
   response=abort_target.request('/proxy/vod/movie/'+seed['movie_uuid']+'/'+sid,{'User-Agent':'owned-pending-target'});responses.append(response)
  row={'outcome_kind':'HTTP_headers','request_started':began,'headers_received':time.monotonic(),'status':response.status_code,'headers':dict(response.headers)}
  with outcome_lock:target_record.update(row)
  row['state_at_outcome']=snapshot()
  observation.at_checkpoint(row['state_at_outcome']['completed_monotonic'],row['headers_received'],0)
  with outcome_lock:target_record.update(row)
  if response.status_code!=200:
   body=response.raw.read(65537)
   if len(body)>65536:raise ValueError('TargetErrorBodyCapacity')
   row.update(error_body_hex=body.hex(),error_body_bytes=len(body),error_body_sha256=hashlib.sha256(body).hexdigest(),error_body_completed=time.monotonic())
  with outcome_lock:target_record.update(row)
  return response
 except Exception as error:
  ended=time.monotonic();state=snapshot();observation.at_checkpoint(state['completed_monotonic'],ended,0)
  with outcome_lock:target_record.update(outcome_kind='client_transport_error',request_started=began,transport_error=type(error).__name__,completed=ended,state_at_outcome=state)
  if case=='clientloss'and abort_target.receipt is not None:return None
  raise
def checkpoint(label):
 row={'label':label,'state':snapshot()}
 with outcome_lock:row['target_headers_already_observed']=bool(target_record)
 report.setdefault('checkpoints',[]).append(row);return row
def until_at(boundary,offset):
 remaining=boundary+offset-time.monotonic()
 if remaining>0:time.sleep(remaining)
try:
 me=requests.get(base+'/api/accounts/users/me/',headers=auth,timeout=5);assert me.status_code==200 and me.json()['id']==seed['real_user_id']and me.json()['username']==seed['real_username'];report['actual_SQL_Bearer_user_verified']=True
 cap(2 if case=='busy'else 1)
 first=get(seed['movie_uuid'],sid,{**auth,'User-Agent':'owned-pending-initial'});assert first.status_code==200;assert prefix(first)=='02921f832147133cdb84ff2dfae8ac4868e95abefdd1cc98a73d54a5c6202a1f';created=snapshot()['metadata']['created_at'];other=None
 if case=='busy':
  other=get(seed['other_movie_uuid'],'fixture-pending-main-other',{**auth,'User-Agent':'owned-pending-other'});assert other.status_code==200;assert prefix(other)=='02921f832147133cdb84ff2dfae8ac4868e95abefdd1cc98a73d54a5c6202a1f'
 other_before=other_private()if case=='busy'and mode=='native'else None
 if other_before:report['other_private_before']=other_before
 close_response(first,'initial');expected=1 if case=='busy'else 0;wait(lambda:snapshot()['metadata']['active_streams']=='0'and int(client.get(key)or 0)==expected);cap(1);report['idle']=snapshot();gate('arm')
 pool=ThreadPoolExecutor(max_workers=1)
 try:
  future=pool.submit(target_request);wait(reached,5);held_at=physical_boundary();report['held_boundary_monotonic']=held_at;checkpoint('actual_reply_held')
  stop=requests.post(base+'/proxy/vod/stop_client/',headers={**auth,'User-Agent':'owned-causal-admin-stop'},json={'client_id':sid},timeout=5);report['admin_stop']={'status':stop.status_code,'body':stop.text};assert stop.status_code==200;assert early_stop(snapshot())
  follower=get(seed['movie_uuid'],sid,{'User-Agent':'owned-pending-follower'});assert follower.status_code==200;assert prefix(follower)=='02921f832147133cdb84ff2dfae8ac4868e95abefdd1cc98a73d54a5c6202a1f';state=snapshot();assert state['metadata']['created_at']==created;assert state['metadata']['active_streams']=='2'and state['metadata']['request_count']=='2'and state['metadata']['user_id']==str(seed['real_user_id']);assert early_stop(state);report['early_follower_valid']=True;report['follower']={'status':follower.status_code,'prefix_sha256':'02921f832147133cdb84ff2dfae8ac4868e95abefdd1cc98a73d54a5c6202a1f','prefix_bytes':8192,'state':state}
  if case=='clientloss':
   abort_target.abort();future.result(timeout=max(.001,held_at+3.1-time.monotonic()));report['target_client_loss']=abort_target.joined();report.setdefault('client_closes',[]).append(report['target_client_loss'])
  control_done=time.monotonic();until_at(held_at,3.1);point=checkpoint('absolute_3_1s');report['checkpoint_3_1_timing']=observation.at_checkpoint(point['state']['began_monotonic'],held_at,3.1);observation.at_checkpoint(point['state']['completed_monotonic'],held_at,3.1)
  until_at(held_at,4);checkpoint('immediately_before_release');gate('release')
  def release_time():
   rows=[r for r in gate('state')['rows']if r['kind']=='backend-target-reply-released']
   return rows[0]['monotonic']if len(rows)==1 else None
  wait(lambda:release_time()is not None,2);released=release_time();report['hold_observation']=observation.hold(held_at,control_done,released);point=checkpoint('immediately_after_release');observation.at_checkpoint(point['state']['began_monotonic'],released,0);observation.at_checkpoint(point['state']['completed_monotonic'],released,0)
  target=future.result(timeout=8)
  with outcome_lock:report['target_outcome']=dict(target_record)
  if case=='clientloss':
   report['client_loss_contract']=observation.client_loss(report)
   report['Source_expected_reference_outcome_matched']=report['client_loss_contract']['matched']if mode=='source'else None
  else:
   assert report['target_outcome']['state_at_outcome']['began_monotonic']>=report['target_outcome']['headers_received']
   report['Source_expected_reference_outcome_matched']=target.status_code==(429 if case=='busy'else 200)if mode=='source'else None
   # Outcome snapshot was taken in the request thread before consumer close.
   close_response(target,'target')
  close_response(follower,'follower');close_boundary=time.monotonic();report['consumer_close_boundary']=close_boundary
  for offset in[1.5,3,6,15]:
   until_at(close_boundary,offset);point=checkpoint('post_close_'+str(offset));observation.at_checkpoint(point['state']['began_monotonic'],close_boundary,offset);observation.at_checkpoint(point['state']['completed_monotonic'],close_boundary,offset)
  if case=='busy':
   report['other_global_after_close']=int(client.get(key)or 0)
   if mode=='native':report['other_private_after']=other_private();report['other_ownership_observation']=other_reservation(other_before,report['other_private_after'],key,seed['other_movie_uuid'],seed['real_user_id'],report['other_global_after_close'])
   report['other_next_prefix']=prefix(other);assert report['other_next_prefix']=='1e5ff7cc43f1c07e665ad794635d3e2fe08f7d9ea648d6fecef559bb8776c213';close_response(other,'other')
  report['observation_valid']=True
 finally:
  gate('release');pool.shutdown(wait=True,cancel_futures=False)
except Exception as error:report['observation_error']={'type':type(error).__name__,'message':str(error)}
finally:
 try:gate('release')
 except Exception:report['observation_valid']=False;report['gate_release_error']=True
 for response in responses:
  try:response.close()
  except Exception as error:report['observation_valid']=False;report.setdefault('finalization_errors',[]).append(type(error).__name__)
 try:connections.close_all()
 except Exception as error:report['observation_valid']=False;report.setdefault('finalization_errors',[]).append(type(error).__name__)
 (ROOT/('pending-main-'+mode+'-'+case+'-private.json')).write_text(json.dumps(report,indent=2)+'\n')
if not report['observation_valid']:raise RuntimeError('FourSecondObservationInvalid')
