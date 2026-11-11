"""Actual pinned Original HTTP VOD body across owned Redis-only state loss.
Original Dispatcharr authors retain full application/algorithm credit.
No replacement metadata is seeded; only the owned local MP4 origin is used.
"""
import hashlib,json,os,socket,subprocess,sys,tempfile,time,urllib.request,urllib.error
from pathlib import Path
assert os.environ.get('POSTGRES_PASSWORD')=='playback-fixture-only'
assert os.environ.get('REDIS_PASSWORD')=='playback-fixture-only'
sys.path.insert(0,'/app');sys.argv.append('shell');os.environ.setdefault('DJANGO_SETTINGS_MODULE','dispatcharr.settings')
import django
django.setup()
from django.test import RequestFactory
from apps.vod.models import Movie
from apps.m3u.models import M3UAccountProfile
from core.utils import RedisClient
from core.models import SystemEvent
seed=json.loads(Path('/evidence/vod-seed.json').read_text());client=RedisClient.get_client();assert client is not None
if list(client.scan_iter(match='vod_persistent_connection:*')):raise RuntimeError('Owned VOD fixture must start idle')
movie=Movie.objects.get(uuid=seed['movie_uuid']);profile=M3UAccountProfile.objects.get(pk=seed['provider_profile_id']);cached=id(client);sid='fixture-vod-active-restart';key='vod_persistent_connection:'+sid;responses=[];work=Path(tempfile.mkdtemp(prefix='owned-vod-redis-'));redis_down=False
report={'capture_complete':False,'scope':'Actual pinned Original HTTP media/body state-loss characterization; separate Django process is independent DB/Redis observer','pid':os.getpid(),'source_hashes':{name:hashlib.sha256(Path('/app',name).read_bytes()).hexdigest()for name in ['apps/proxy/vod_proxy/multi_worker_connection_manager.py','core/utils.py','core/redis_connection.py']},'redis_run_ids':[],'observations':[],'performance_verified':False,'native_implementation_verified':False,'original_http_capture':True,'body_sample_limit':34*8192,'long_prefix_limit':1048576}
other_movie=None;idle_other_future=None;idle_other_executor=None
if os.environ.get('ACTIVE_IDLE_PROVIDER_BUSY')=='1':
 from apps.vod.models import M3UMovieRelation
 account=profile.m3u_account
 assert account.server_url=='http://origin:8080/player_api.php' and account.username=='fixture' and account.password=='fixture'
 account.max_streams=2;account.save(update_fields=['max_streams'])
 profile.max_streams=2;profile.save(update_fields=['max_streams'])
 other_movie=Movie.objects.create(name='Owned idle provider-capacity fixture')
 M3UMovieRelation.objects.create(m3u_account=account,movie=other_movie,stream_id='fixture-idle-other',container_extension='mp4')
 report['idle_busy_fixture']={'initial_profile_max_streams':profile.max_streams,'initial_account_max_streams':account.max_streams,'other_movie_uuid':str(other_movie.uuid)}
event_start=SystemEvent.objects.order_by('-id').values_list('id',flat=True).first()or 0
cli_env=dict(os.environ,REDISCLI_AUTH='playback-fixture-only')
def start_redis():
 global redis_down
 subprocess.run(['redis-server','--daemonize','yes','--port','6379','--bind','0.0.0.0','--protected-mode','yes','--requirepass','playback-fixture-only','--save','','--appendonly','no','--dir',str(work),'--pidfile',str(work/'redis.pid')],check=True,capture_output=True,timeout=10)
 deadline=time.monotonic()+10
 while time.monotonic()<deadline:
  try:
   if client.ping():redis_down=False;return
  except Exception:pass
  time.sleep(.1)
 raise TimeoutError('Owned restarted Redis readiness')
def error_info(error):
 args=[]
 for value in error.args[:8]:
  if isinstance(value,bytes):args.append({'byte_length':len(value),'sha256':hashlib.sha256(value).hexdigest()})
  elif isinstance(value,(int,float,bool))or value is None:args.append(value)
  else:args.append(str(value)[:1024])
 return {'class':type(error).__name__,'module':type(error).__module__,'args':args}
def state(name):
 value={'name':name,'same_client':id(RedisClient._client)==cached,'observer_only':True}
 try:
  row=client.hgetall(key);value.update(metadata=dict(row),provider_count=client.get('profile_connections:'+str(profile.pk)),ttl=client.ttl(key));value['stop_signal']=client.get('vod_proxy:client:'+sid+':stop');value['stop_ttl']=client.ttl('vod_proxy:client:'+sid+':stop')
 except Exception as error:value['error']=error_info(error)
 try:
  with urllib.request.urlopen('http://origin:8080/stats',timeout=2)as response:
   value['origin_vod']=json.load(response)['vod']
 except Exception as error:value['origin_error']=error_info(error)
 value['event_order_so_far']=list(SystemEvent.objects.filter(id__gt=event_start).order_by('id').values_list('event_type',flat=True)[:64])
 report['observations'].append(value);return value
base='http://127.0.0.1:5656'
authorization=None
if os.environ.get('ACTIVE_CAPPED_VIEWER')=='1':
 from django.contrib.auth import get_user_model
 from rest_framework_simplejwt.tokens import RefreshToken
 viewer=get_user_model().objects.get(username='rust-vod-fixture-capped')
 assert viewer.is_active and viewer.stream_limit==1
 authorization='Bearer '+str(RefreshToken.for_user(viewer).access_token)
 report['viewer_auth']={'mode':'fixture Bearer JWT','user_id':viewer.pk,'stream_limit':viewer.stream_limit}
def request_headers():
 headers={'User-Agent':'owned-active-restart','X-Forwarded-For':'192.168.123.23'}
 if authorization:headers['Authorization']=authorization
 return headers
class WireResponse:
 def __init__(self,response):self.response=response;self.status_code=response.status;self.streaming=response.status<400
 @property
 def streaming_content(self):
  while True:
   chunk=self.response.read(8192)
   if not chunk:return
   yield chunk
 def close(self):self.response.close()
def open_response():
 request=urllib.request.Request(base+'/proxy/vod/movie/'+str(movie.uuid)+'/'+sid,headers=request_headers())
 try:response=urllib.request.urlopen(request,timeout=15)
 except urllib.error.HTTPError as error:response=error
 wrapped=WireResponse(response);responses.append(wrapped);return wrapped
try:
 deadline=time.monotonic()+90
 while True:
  try:
   with urllib.request.urlopen('http://origin:8080/stats',timeout=2)as response:origin=json.load(response)
   break
  except OSError:
   if time.monotonic()>deadline:raise TimeoutError('Owned paced origin readiness')
   time.sleep(.2)
 report['origin_fixture']=origin['vod'];assert report['origin_fixture']['bytes_per_second']==32768
 report['redis_run_ids'].append(client.info('server')['run_id'])
 response=open_response();report['initial_status']=response.status_code;assert response.streaming
 iterator=iter(response.streaming_content);chunks=[];chunk=next(iterator);assert len(chunk)==8192;chunks.append(chunk);initial=state('active-before-loss')
 if report.get('viewer_auth'):
  assert initial.get('metadata',{}).get('user_id')==str(report['viewer_auth']['user_id']),'Fixture Bearer viewer was not authenticated in actual metadata'
  report['authenticated_viewer_observed']=True
 subprocess.run(['redis-cli','--no-auth-warning','SHUTDOWN','NOSAVE'],env=cli_env,check=True,capture_output=True,timeout=10);redis_down=True
 state('server-down-body-held')
 # Do not replace manager, iterator, cached client or local provider handle.
 start_redis();report['redis_run_ids'].append(client.info('server')['run_id']);assert report['redis_run_ids'][0]!=report['redis_run_ids'][1];lost=state('server-restarted-no-reseed');assert not lost.get('metadata')and lost.get('provider_count')is None
 if os.environ.get('ACTIVE_STRIP_AUTH_AFTER_LOSS')=='1':
  authorization=None;report['authorization_omitted_after_loss']=True
 deadline=time.monotonic()+30;sample_error=None;ended=False
 for _ in range(0 if os.environ.get('ACTIVE_FAST_REOPEN')=='1' else 33):
  if time.monotonic()>deadline:raise TimeoutError('Bounded post-loss body sampling')
  try:chunk=next(iterator)
  except StopIteration:ended=True;break
  except Exception as error:sample_error=error_info(error);break
  chunks.append(chunk)
 payload=b''.join(chunks);full_old=bytearray(payload)if os.environ.get('ACTIVE_CAPTURE_LONG_PREFIX')=='1'else None;full_young=None;assert 0<len(payload)<=report['body_sample_limit'];Path('/evidence/vod-active-http-source.bin').write_bytes(payload);report['old_body']={'bytes':len(payload),'sha256':hashlib.sha256(payload).hexdigest(),'ended':ended,'error':sample_error};state('old-body-after-loss-sampling')
 # Literal same-ID reopen while old response is still alive. Record outcomes,
 # including state replacement/admission, rather than assuming recovery.
 sibling=open_response();report['same_id_reopen']={'status':sibling.status_code,'streaming':sibling.streaming}
 if sibling.streaming:
  try:
   first=next(iter(sibling.streaming_content));full_young=bytearray(first)if os.environ.get('ACTIVE_CAPTURE_LONG_PREFIX')=='1'else None;report['same_id_reopen']['first_bytes']=len(first);report['same_id_reopen']['first_sha256']=hashlib.sha256(first).hexdigest()
  except StopIteration:report['same_id_reopen']['ended_without_bytes']=True
  except Exception as error:report['same_id_reopen']['error']=error_info(error)
 state('same-id-reopen-before-close')
 if os.environ.get('ACTIVE_EXTRA_ENTRY')=='1':
  third=open_response();extra={'status':third.status_code,'streaming':third.streaming}
  if third.streaming:
   block=next(iter(third.streaming_content));extra.update(first_bytes=len(block),first_sha256=hashlib.sha256(block).hexdigest())
  if not third.streaming:extra['error_body']=third.response.read(8193)[:8192].decode('utf-8','replace')
  report['additional_current_entry']=extra;state('third-current-entry-open-old-and-younger-held')
  if third.streaming:third.response.fp.raw._sock.shutdown(socket.SHUT_RDWR)
  third.close();third.close();deadline=time.monotonic()+10
  current=state('third-current-entry-closed-old-and-younger-held')
  while current.get('metadata',{}).get('active_streams')!='1'and time.monotonic()<deadline:
   time.sleep(.1);current=state('third-close-public-count-poll')
  report['third_close_returned_public_count_to_one']=current.get('metadata',{}).get('active_streams')=='1'
 if os.environ.get('ACTIVE_ADMIN_STOP')=='1':
  from django.contrib.auth import get_user_model
  from rest_framework_simplejwt.tokens import RefreshToken
  administrator,_=get_user_model().objects.update_or_create(username='rust-vod-fixture-admin-stop',defaults={'is_active':True,'user_level':10,'stream_limit':0,'password':'!'})
  admin_token=str(RefreshToken.for_user(administrator).access_token)
  request=urllib.request.Request(base+'/proxy/vod/stop_client/',data=json.dumps({'client_id':sid}).encode(),headers={'Authorization':'Bearer '+admin_token,'Content-Type':'application/json'},method='POST')
  try:stopped=urllib.request.urlopen(request,timeout=10)
  except urllib.error.HTTPError as error:stopped=error
  with stopped:report['admin_stop']={'status':stopped.status,'body':stopped.read(8193).decode('utf-8','replace'),'user_id':administrator.pk}
  assert report['admin_stop']['status']==200, report['admin_stop']
  state('admin-stop-after-third-close-old-and-younger-held')
  if os.environ.get('ACTIVE_ENTRY_AFTER_ADMIN_STOP')=='1':
   pending=open_response();entry={'status':pending.status_code,'streaming':pending.streaming}
   if pending.streaming:
    block=next(iter(pending.streaming_content));entry.update(first_bytes=len(block),first_sha256=hashlib.sha256(block).hexdigest())
   else:entry['error_body']=pending.response.read(8193)[:8192].decode('utf-8','replace')
   report['entry_while_admin_stop_pending']=entry
   state('additional-entry-open-while-admin-stop-pending')
   if pending.streaming:pending.response.fp.raw._sock.shutdown(socket.SHUT_RDWR)
   pending.close();pending.close()
   state('additional-entry-closed-while-admin-stop-pending')
 # Distinguish both still-open provider bodies; a recreated public session hash
 # must not be mistaken for adoption of the pre-restart body.
 try:
  following=next(iterator)
  if full_old is not None:full_old.extend(following)
  report['old_body_after_reopen']={'offset':len(payload),'bytes':len(following),'sha256':hashlib.sha256(following).hexdigest()}
 except StopIteration:report['old_body_after_reopen']={'ended':True}
 except Exception as error:report['old_body_after_reopen']={'error':error_info(error)}
 state('both-bodies-held-after-extra-old-read')
 if os.environ.get('ACTIVE_OBSERVE_OLD_PROGRESS')=='1':
  # Read beyond the old producer's100th8192-byte yield. The younger sibling
  # remains held at its first client block; record actual metadata, not a claim
  # that request counts are generation-fenced in Original.
  old_chunks=len(chunks)+(1 if report.get('old_body_after_reopen',{}).get('bytes')else 0)
  old_total=len(payload)+report.get('old_body_after_reopen',{}).get('bytes',0)
  rolling=hashlib.sha256();deadline=time.monotonic()+40;old_ended=False;old_error=None
  while old_chunks<101:
   if time.monotonic()>deadline:raise TimeoutError('Bounded old100th-yield sampling')
   try:following=next(iterator)
   except StopIteration:old_ended=True;break
   except Exception as failure:old_error=error_info(failure);break
   assert len(following)<=8192
   rolling.update(following);old_total+=len(following);old_chunks+=1
   if full_old is not None:
    full_old.extend(following);assert len(full_old)<=report['long_prefix_limit']
  report['old_progress_after_reopen']={'chunks_read':old_chunks,'bytes_read':old_total,'tail_sha256':rolling.hexdigest(),'ended':old_ended,'error':old_error}
  state('old-body-beyond100th-yield-with-younger-sibling-held')
  if full_old is not None:
   Path('/evidence/vod-active-long-old-source.bin').write_bytes(full_old);report['old_long_prefix']={'bytes':len(full_old),'sha256':hashlib.sha256(full_old).hexdigest()}
 if os.environ.get('ACTIVE_CLOSE_OLD_FIRST')=='1':
  # A real owned client socket shutdown avoids confusing buffered Python file
  # closure with an observed server-side generator teardown. No worker is killed.
  if os.environ.get('ACTIVE_IDLE_REDIRECT')=='1':
   from core.models import CoreSettings,StreamProfile
   settings=CoreSettings.get_stream_settings().copy();settings['default_stream_profile']=StreamProfile.get_locked('Redirect').pk
   CoreSettings.objects.update_or_create(key='stream_settings',defaults={'name':'Fixture redirectidle settings','value':settings})
   report['redirect_policy_observer']=CoreSettings.is_default_stream_profile_redirect()
  if other_movie is not None:
   from concurrent.futures import ThreadPoolExecutor
   idle_other_executor=ThreadPoolExecutor(max_workers=1)
   other_id='fixture-owned-idle-provider-other'
   def start_owned_contender():
    request=urllib.request.Request(base+'/proxy/vod/movie/'+str(other_movie.uuid)+'/'+other_id,headers={'User-Agent':'owned-idle-provider-other'})
    try:wire=urllib.request.urlopen(request,timeout=15)
    except urllib.error.HTTPError as error:wire=error
    result=WireResponse(wire);responses.append(result);return result
   idle_other_future=idle_other_executor.submit(start_owned_contender)
   deadline=time.monotonic()+5;other_admitted=False
   while time.monotonic()<deadline:
    row=client.hgetall('vod_persistent_connection:'+other_id);count=client.get('profile_connections:'+str(profile.pk))
    if row.get('active_streams')=='1'and row.get('content_uuid')==str(other_movie.uuid)and count=='2':other_admitted=True;break
    time.sleep(.005)
   assert other_admitted,'Actual owned contender admission not observed before closing target'
   report['owned_idle_contender_admission']={'provider_count':count,'active_streams':row['active_streams'],'content_uuid':row['content_uuid'],'request_in_flight':True,'admitted_under_limit':2}
   # Legitimate live policy change: both groups were admitted under2; only the
   # other group will remain when target closes. No counter/metadata is seeded.
   account.max_streams=1;account.save(update_fields=['max_streams'])
   profile.max_streams=1;profile.save(update_fields=['max_streams'])
   report['idle_busy_fixture'].update(final_profile_max_streams=1,final_account_max_streams=1,policy_lowered_before_target_close=True)
  if response.response.fp is not None:
   sock=response.response.fp.raw._sock;sock.shutdown(socket.SHUT_RDWR)
   report['old_close_mode']='owned HTTP client socket shutdown then response.close twice'
  else:report['old_close_mode']='bodyalreadyEOF beforeexplicitclose'
  response.close();response.close()
  if os.environ.get('ACTIVE_IDLE_SAME_ENTRY')=='1':
   deadline=time.monotonic()+5;idle=None
   while time.monotonic()<deadline:
    row=client.hgetall(key);available=client.get('profile_connections:'+str(profile.pk))
    capacity=(available=='1')if other_movie is not None else available in(None,'0')
    if row and row.get('active_streams')=='0' and capacity:idle=row;observed_at=time.monotonic();break
    if not row:break
    time.sleep(.005)
   report['controlled_idle_entry']={'qualified':idle is not None,'attempted':False,'provider_before':available}
   if idle is not None:
    before={field:idle.get(field)for field in ['active_streams','created_at','request_count','user_id','content_uuid','content_obj_type']}
    request_at=time.monotonic();adopted=open_response()
    follow={'status':adopted.status_code,'streaming':adopted.streaming,'response_headers':{name.lower():value for name,value in list(adopted.response.headers.items())[:64]}}
    follow['response_header_pairs']=[[name,value]for name,value in list(adopted.response.headers.items())[:64]]
    if adopted.streaming:
     block=next(iter(adopted.streaming_content));follow.update(first_bytes=len(block),first_sha256=hashlib.sha256(block).hexdigest())
    else:follow['error_body']=adopted.response.read(8193)[:8192].decode('utf-8','replace')
    after=client.hgetall(key)
    report['controlled_idle_entry'].update(attempted=True,read_to_request_ms=(request_at-observed_at)*1000,before=before,wire=follow,after={field:after.get(field)for field in before},provider_after=client.get('profile_connections:'+str(profile.pk)))
    if adopted.streaming:report['public_idle_follow']=follow
    state('controlled-public-idle-same-id-entry')
    time.sleep(1.5);state('controlled-idle-entry-after-prior-cleanup-timer')
   if idle_other_future is not None:
    other=idle_other_future.result(timeout=15);evidence={'status':other.status_code,'streaming':other.streaming}
    assert other.status_code==200
    first=next(iter(other.streaming_content));evidence.update(first_bytes=len(first),first_sha256=hashlib.sha256(first).hexdigest(),provider_count=client.get('profile_connections:'+str(profile.pk)))
    report['owned_idle_contender']=evidence
    if other.streaming:other.response.fp.raw._sock.shutdown(socket.SHUT_RDWR)
    other.close();other.close();state('owned-idle-provider-contender-closed')
  if os.environ.get('ACTIVE_IDLE_MINT')=='1':
   deadline=time.monotonic()+10;idle=None
   while time.monotonic()<deadline:
    row=client.hgetall(key)
    if row and row.get('active_streams')=='0':idle=row;break
    if not row:break
    time.sleep(.02)
   report['public_idle_observed_before_delete']=idle is not None
   if idle is not None:
    state('public-idle-before-no-sid-mint')
    class NoRedirect(urllib.request.HTTPRedirectHandler):
     def redirect_request(self,*args):return None
    mint_request=urllib.request.Request(base+'/proxy/vod/movie/'+str(movie.uuid),headers=request_headers())
    try:mint=urllib.request.build_opener(NoRedirect).open(mint_request,timeout=10)
    except urllib.error.HTTPError as error:mint=error
    with mint:report['public_idle_mint']={'status':mint.status,'location':mint.headers.get('Location'),'body_bytes':len(mint.read(8193))}
    state('public-idle-after-no-sid-mint')
    if os.environ.get('ACTIVE_IDLE_FOLLOW')=='1' and report['public_idle_mint']['location'].rstrip('/').endswith('/'+sid):
     adopted=open_response();follow={'status':adopted.status_code,'streaming':adopted.streaming}
     if adopted.streaming:
      block=next(iter(adopted.streaming_content));follow.update(first_bytes=len(block),first_sha256=hashlib.sha256(block).hexdigest())
     report['public_idle_follow']=follow;state('public-idle-followed-same-session')
     time.sleep(1.5);state('public-idle-follow-survives-old-cleanup-timer')
  deadline=time.monotonic()+10;closed_state=state('old-client-closed-younger-body-held')
  while time.monotonic()<deadline and not report.get('public_idle_follow'):
   if not closed_state.get('metadata')or closed_state.get('provider_count')in(None,'0'):break
   time.sleep(.5);closed_state=state('old-close-poll-younger-body-held')
  report['old_close_public_effect_observed']=not closed_state.get('metadata')or closed_state.get('provider_count')in(None,'0')
  younger=iter(sibling.streaming_content);young_chunks=1;young_bytes=report['same_id_reopen'].get('first_bytes',0)
  ended=False;error=None;deadline=time.monotonic()+45
  while young_chunks<103:
   if time.monotonic()>deadline:raise TimeoutError('Bounded younger body after old close')
   try:following=next(younger)
   except StopIteration:ended=True;break
   except Exception as failure:error=error_info(failure);break
   young_chunks+=1;young_bytes+=len(following)
   if full_young is not None:
    full_young.extend(following);assert len(full_young)<=report['long_prefix_limit']
  report['younger_after_old_close']={'chunks_read':young_chunks,'bytes_read':young_bytes,'ended':ended,'error':error}
  state('younger-beyond100th-yield-after-old-client-closed')
  if os.environ.get('ACTIVE_RETIRED_REOPEN')=='1':
   replacement=open_response();reopened={'status':replacement.status_code,'streaming':replacement.streaming}
   if replacement.streaming:
    block=next(iter(replacement.streaming_content));reopened.update(first_bytes=len(block),first_sha256=hashlib.sha256(block).hexdigest())
   report['retired_public_reopen']=reopened;state('metadata-retired-reopen-while-younger-body-held')
   try:
    following=next(younger);
    if full_young is not None:
     full_young.extend(following);assert len(full_young)<=report['long_prefix_limit']
    report['younger_after_retired_reopen']={'bytes':len(following),'sha256':hashlib.sha256(following).hexdigest(),'offset':young_bytes}
   except StopIteration:report['younger_after_retired_reopen']={'ended':True}
   state('younger-extra-read-after-retired-public-reopen')
   if replacement.streaming:replacement.response.fp.raw._sock.shutdown(socket.SHUT_RDWR)
   replacement.close();replacement.close()
  if full_young is not None:
   Path('/evidence/vod-active-long-young-source.bin').write_bytes(full_young);report['young_long_prefix']={'bytes':len(full_young),'sha256':hashlib.sha256(full_young).hexdigest()}
  if report.get('public_idle_follow',{}).get('streaming'):
   adopted.response.fp.raw._sock.shutdown(socket.SHUT_RDWR);adopted.close();adopted.close()
  sibling.close();sibling.close();time.sleep(1.5);state('all-bodies-closed-after-grace')
 else:
  sibling.close();sibling.close();state('sibling-closed-old-body-held');response.close();response.close();time.sleep(1.5);state('all-bodies-closed-after-grace')
 # Reference only the sampled prefix; this is not a full-file/EOF gate.
 request=urllib.request.Request('http://origin:8080/vod.mp4',headers={'Range':'bytes=0-'+str(len(payload)-1)})
 with urllib.request.urlopen(request,timeout=10)as reference:
  expected=reference.read(report['body_sample_limit']+1);report['reference_status']=reference.status
 report['body_prefix_matches_origin']=expected==payload
 report['reopened_body_starts_at_prefix']=report['same_id_reopen'].get('first_sha256')==hashlib.sha256(payload[:8192]).hexdigest()
 extra=report.get('old_body_after_reopen',{})
 if extra.get('bytes'):
  request=urllib.request.Request('http://origin:8080/vod.mp4',headers={'Range':'bytes='+str(extra['offset'])+'-'+str(extra['offset']+extra['bytes']-1)})
  with urllib.request.urlopen(request,timeout=10)as reference:
   sampled=reference.read(8193);report['old_body_after_reopen_matches_offset']=len(sampled)==extra['bytes']and hashlib.sha256(sampled).hexdigest()==extra['sha256']

 report['events']=list(SystemEvent.objects.filter(id__gt=event_start).order_by('id').values('event_type','details'))
 if len(report['events'])>64:raise RuntimeError('Owned audit bound')
 report['same_process']=os.getpid()==report['pid'];report['capture_complete']=True
finally:
 if idle_other_future is not None:
  try:idle_other_future.result(timeout=15)
  except Exception as error:report['owned_contender_cleanup_error']=type(error).__name__
 if idle_other_executor is not None:idle_other_executor.shutdown(wait=True,cancel_futures=True)
 if redis_down:start_redis()
 for response in reversed(responses):
  try:response.close()
  except Exception:pass
 Path('/evidence/vod-active-restart-source.json').write_text(json.dumps(report,indent=2)+'\n')
print(json.dumps({'capture_complete':report['capture_complete'],'body_prefix_matches_origin':report.get('body_prefix_matches_origin'),'bytes':report.get('old_body',{}).get('bytes')}))
