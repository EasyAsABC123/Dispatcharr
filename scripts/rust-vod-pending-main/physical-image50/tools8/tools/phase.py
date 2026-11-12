"""UNEXECUTED genuine Source/Main SQL/auth early-follower phase, owned input only."""
import hashlib,http.client,json,os,socket,sys,time
from urllib.parse import urlsplit
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
assert os.environ.get('PENDING_MAIN_FIXTURE')=='playback-fixture-only'
assert os.environ.get('POSTGRES_DB','').startswith('rust_pending_')
sys.path.insert(0,'/app');sys.argv.append('shell');os.environ.setdefault('DJANGO_SETTINGS_MODULE','dispatcharr.settings')
import django;django.setup()
import redis,requests
from fixture_guards import hold_observation,other_reservation,early_stop
from django.db import connections
from apps.m3u.models import M3UAccount,M3UAccountProfile
ROOT=Path('/evidence');seed=json.loads((ROOT/'pending-main-private.json').read_text());mode=os.environ['PENDING_MODE'];case=os.environ['PENDING_CASE'];assert mode in['source','native']and case in['success','busy','abort']
base=os.environ['PENDING_BASE'];client=redis.Redis(host='127.0.0.1',port=int(os.environ['REDIS_PORT']),password='playback-fixture-only',decode_responses=True);key='profile_connections:'+str(seed['provider_profile_id']);sid='fixture-pending-main-'+case;metadata='vod_persistent_connection:'+sid
responses=[];report={'passed':False,'scope':'Actual selected early HTTP/SQL/auth pending follower only; no full metadata/latebody/performance parity','backend':mode,'case':case,'observations':[],'source_control':'actual Original Redis INCR reply held'if mode=='source'else'actual Redis executed provider reply held'}
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
 row=client.hgetall(metadata)
 return{'metadata':{k:row.get(k)for k in ['user_id','active_streams','request_count','created_at','content_uuid','m3u_profile_id']},'provider':client.get(key),'stop':client.get('vod_proxy:client:'+sid+':stop')}
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
 response=requests.get(base+'/proxy/vod/movie/'+movie+'/'+session,headers=headers,stream=True,timeout=8);responses.append(response);return response
def prefix(response):
 value=response.raw.read(8192)
 if len(value)!=8192:raise ValueError('PrefixSize')
 return hashlib.sha256(value).hexdigest()
class TargetSocket:
 def __init__(self):
  host=urlsplit(base);self.connection=http.client.HTTPConnection(host.hostname,host.port,timeout=8);self.connection.connect();self.socket=self.connection.sock
 def request(self):
  self.connection.request('GET','/proxy/vod/movie/'+seed['movie_uuid']+'/'+sid,headers={'User-Agent':'owned-pending-target'})
  response=self.connection.getresponse();return response.status
 def abort(self):
  self.socket.shutdown(socket.SHUT_RDWR)
try:
 me=requests.get(base+'/api/accounts/users/me/',headers=auth,timeout=5);assert me.status_code==200 and me.json()['id']==seed['real_user_id']and me.json()['username']==seed['real_username'];report['actual_SQL_Bearer_user_verified']=True
 cap(2 if case=='busy'else 1)
 first=get(seed['movie_uuid'],sid,auth);assert first.status_code==200;assert prefix(first)=='02921f832147133cdb84ff2dfae8ac4868e95abefdd1cc98a73d54a5c6202a1f';created=snapshot()['metadata']['created_at'];other=None
 if case=='busy':other=get(seed['other_movie_uuid'],'fixture-pending-main-other',auth);assert other.status_code==200;assert prefix(other)=='02921f832147133cdb84ff2dfae8ac4868e95abefdd1cc98a73d54a5c6202a1f'
 other_before=other_private()if case=='busy'and mode=='native'else None
 if other_before:report['other_private_before']=other_before
 first.close();expected=1 if case=='busy'else 0;wait(lambda:snapshot()['metadata']['active_streams']=='0'and int(client.get(key)or 0)==expected);cap(1);report['idle']=snapshot();gate('arm')
 with ThreadPoolExecutor(max_workers=1)as pool:
  raw_target=TargetSocket()if case=='abort'else None
  future=pool.submit(raw_target.request)if raw_target else pool.submit(get,seed['movie_uuid'],sid,{'User-Agent':'owned-pending-target'});wait(reached,5);held_at=physical_boundary();report['held']=snapshot();report['held_boundary_monotonic']=held_at
  stop=requests.post(base+'/proxy/vod/stop_client/',headers=auth,json={'client_id':sid},timeout=5);report['admin_stop']={'status':stop.status_code,'raw_body':stop.text,'headers':dict(stop.headers)};assert stop.status_code==200
  report['early_stop_after_admin']=snapshot();assert early_stop(report['early_stop_after_admin'])
  follower=get(seed['movie_uuid'],sid,{'User-Agent':'owned-pending-follower'});assert follower.status_code==200;bodyhash=prefix(follower);assert bodyhash=='02921f832147133cdb84ff2dfae8ac4868e95abefdd1cc98a73d54a5c6202a1f';report['follower']={'status':200,'prefix_bytes':8192,'prefix_sha256':bodyhash,'headers':dict(follower.headers),'state':snapshot()};assert report['follower']['state']['metadata']['active_streams']=='2';assert report['follower']['state']['metadata']['request_count']=='2';assert report['follower']['state']['metadata']['user_id']==str(seed['real_user_id']);assert report['follower']['state']['metadata']['created_at']==created;assert early_stop(report['follower']['state']);report['early_stop_preserved_after_8192']=True
  # Both gates now hold actual Redis replies for predeclared2.4s.
  # Historical Source2704s remains separate; Main's3s observer is unchanged.
  # Main older-timer proof remains open; component1.2s/1s proof is separate.
  hold=2.4;control_complete=time.monotonic()
  remaining=hold-(control_complete-held_at)
  if remaining>0:time.sleep(remaining)
  actual_hold=time.monotonic();report['hold_observation_raw']={'boundary':held_at,'control_complete':control_complete,'release_request':actual_hold,'seconds':actual_hold-held_at,'overshoot_seconds':max(0,actual_hold-held_at-hold)}
  report['hold_observation']=hold_observation(held_at,control_complete,actual_hold)
  report['held_end']=snapshot()
  if raw_target:
   raw_target.abort();report['actual_target_client_socket_closed']=True
  gate('release')
  def released_boundary():
   value=gate('state')
   rows=[r for r in value['rows']if r['kind']=='backend-target-reply-released']
   return rows[0]['monotonic']if len(rows)==1 else None
  wait(lambda:released_boundary()is not None,2)
  actual_release=released_boundary();report['hold_observation_raw']['actual_gate_released_monotonic']=actual_release
  report['hold_observation']=hold_observation(held_at,control_complete,actual_release)
  if raw_target:
   try:
    status=future.result(timeout=8);raise AssertionError('Aborted target unexpectedly received status '+str(status))
   except (http.client.HTTPException,OSError)as error:report['target_after_client_loss']={'client_error':type(error).__name__}
   raw_target.connection.close();report['server_handler_cancellation_inferred']=False
  else:
   target=future.result(timeout=8);report['target']={'status':target.status_code,'headers':dict(target.headers)};assert target.status_code==(429 if case=='busy'else 200)
   if case=='busy':report['target']['raw_body']=target.text;assert target.content==b'Connection limit exceeded for profile'
   target.close()
  follower.close();time.sleep(1.5);report['post_close']=snapshot()
  if case=='busy':
   report['other_global_after_follower_close']=int(client.get(key)or 0)
   if mode=='native':
    after=other_private();report['other_private_after_follower_close']=after;report['other_reservation_witness']=other_reservation(other_before,after,key,seed['other_movie_uuid'],seed['real_user_id'],report['other_global_after_follower_close'])
   else:report['Source_public_counter_after_follower_close_observation']=report['other_global_after_follower_close'];report['Source_counter_parity_claimed']=False
  if other is not None:report['other_next_prefix']=prefix(other);assert report['other_next_prefix']=='1e5ff7cc43f1c07e665ad794635d3e2fe08f7d9ea648d6fecef559bb8776c213';other.close()
 report['passed']=True
finally:
 try:gate('release')
 except Exception:report['gate_release_error']=True
 for response in responses:response.close()
 connections.close_all();(ROOT/('pending-main-'+mode+'-'+case+'-private.json')).write_text(json.dumps(report,indent=2)+'\n')
