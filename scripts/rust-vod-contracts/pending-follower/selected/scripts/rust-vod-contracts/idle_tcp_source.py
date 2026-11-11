"""Pinned full Original view idle reservation scheduling, owned fixtures only.
Original Dispatcharr authors retain application and algorithm credit. The hook
pauses AFTER an actual profile INCR reply inside this copied process; it is a
method-return scheduling witness, not withheld transport/physical-owner proof.
"""
import hashlib,json,os,sys,time,threading,subprocess,requests
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
import urllib.request
assert os.environ.get('POSTGRES_PASSWORD')=='playback-fixture-only'
assert os.environ.get('REDIS_PASSWORD')=='playback-fixture-only'
sys.path.insert(0,'/app');sys.argv.append('shell');os.environ.setdefault('DJANGO_SETTINGS_MODULE','dispatcharr.settings')
import django
django.setup()
from django.test import RequestFactory
from django.urls import resolve
from django.db import close_old_connections
from django.contrib.auth import get_user_model
from rest_framework_simplejwt.tokens import RefreshToken
from apps.vod.models import Movie,M3UMovieRelation
from apps.m3u.models import M3UAccountProfile
from core.utils import RedisClient
seed=json.loads(Path('/evidence/vod-seed.json').read_text());client=RedisClient.get_client();assert client is not None
assert not list(client.scan_iter(match='vod_persistent_connection:*'))
profile=M3UAccountProfile.objects.get(pk=seed['provider_profile_id']);account=profile.m3u_account
assert account.server_url=='http://origin:8080/player_api.php' and account.username=='fixture' and account.password=='fixture'
profile_key='profile_connections:'+str(profile.pk)
factory=RequestFactory();responses=[]
admin,_=get_user_model().objects.update_or_create(username='rust-vod-fixture-held-admin',defaults={'is_active':True,'user_level':10,'stream_limit':0,'password':'!'})
admin_token=str(RefreshToken.for_user(admin).access_token)
report={'capture_complete':False,'scope':'Actual TCP→gevent WSGI→pinned Original Django views +realSQL/Redis/local provider; controlled post-INCR method-return hook, not withheld physical Redis reply/native acceptance.', 'hook':'ActualTCP target greenlet pauses after actual INCR(profile) reply; no physical-reply or late-body/stop-boundary acceptance.', 'source_sha256':{p:hashlib.sha256(Path('/app',p).read_bytes()).hexdigest()for p in ['apps/proxy/vod_proxy/views.py','apps/proxy/vod_proxy/multi_worker_connection_manager.py','apps/m3u/connection_pool.py']},'cases':[]}

class Wire:
 def __init__(self,response):self.response=response;self.status_code=response.status_code;self.streaming=response.status_code in[200,206];self.streaming_content=response.iter_content(chunk_size=8192)
 def close(self):self.response.close()
 @property
 def content(self):return self.response.content
 def items(self):return self.response.headers.items()
def control(name,data=None):return requests.post('http://127.0.0.1:5657/__owned_fixture/'+name,json=data,timeout=3).json()
class RemoteGate:
 def wait(self,seconds):
  end=time.monotonic()+seconds
  while time.monotonic()<end:
   if control('state')['reached']:return True
   time.sleep(.01)
  return False
 def set(self):control('release')
def call(movie,sid,agent):
 response=Wire(requests.get('http://127.0.0.1:5657/proxy/vod/movie/'+str(movie.uuid)+'/'+sid,headers={'User-Agent':agent},stream=True,timeout=20));responses.append(response);return response
def stop(sid):
 response=requests.post('http://127.0.0.1:5657/proxy/vod/stop_client/',json={'client_id':sid},headers={'Authorization':'Bearer '+admin_token},timeout=5)
 return {'status':response.status_code,'body':response.text}

def snapshot(sid):
 row=client.hgetall('vod_persistent_connection:'+sid)
 return {'metadata':{k:row.get(k)for k in ['active_streams','request_count','created_at','user_id','content_uuid','m3u_profile_id']},'metadata_present':bool(row),'provider':client.get(profile_key),'stop':client.get('vod_proxy:client:'+sid+':stop')}

def wait(condition,seconds=5):
 deadline=time.monotonic()+seconds
 while time.monotonic()<deadline:
  if condition():return
  time.sleep(.005)
 raise TimeoutError('Owned Source scheduling condition')

def _ready():
 try:return control('state')['armed']is False
 except requests.RequestException:return False
serverlog=Path('/evidence/idle-tcp-server-private.log').open('wb');server=subprocess.Popen(['/dispatcharrpy/bin/python','/vod-verification/idle_tcp_server.py'],stdout=serverlog,stderr=serverlog)
wait(lambda:server.poll()is None and _ready(),seconds=20)
try:
 movie=Movie.objects.get(uuid=seed['movie_uuid']);base_relation=M3UMovieRelation.objects.get(pk=seed['relation_id'])
 for busy in [False,True]:
  case={'name':'busy-with-follower'if busy else'success-with-follower','busy':busy};report['cases'].append(case)
  if busy:
   movie=Movie.objects.create(name='Owned held-idle target')
   base_relation.movie=movie;base_relation.save(update_fields=['movie'])
  account.max_streams=2 if busy else 1;account.save(update_fields=['max_streams'])
  profile.max_streams=account.max_streams;profile.save(update_fields=['max_streams'])
  sid='fixture-held-idle-'+('busy'if busy else'success');agent='owned-'+sid
  first=call(movie,sid,agent);assert first.status_code==200 and first.streaming
  iterator=iter(first.streaming_content);initial=next(iterator);assert len(initial)==8192
  other=None;other_iterator=None
  if busy:
   other_movie=Movie.objects.create(name='Owned held-idle other')
   M3UMovieRelation.objects.create(m3u_account=account,movie=other_movie,stream_id='fixture-idle-other',container_extension='mp4')
   other=call(other_movie,'fixture-held-idle-other','owned-held-idle-other');assert other.status_code==200 and other.streaming
   other_iterator=iter(other.streaming_content);assert len(next(other_iterator))==8192
   assert client.get(profile_key)=='2'
   account.max_streams=1;account.save(update_fields=['max_streams']);profile.max_streams=1;profile.save(update_fields=['max_streams'])
  first.close();wait(lambda:snapshot(sid)['metadata']['active_streams']=='0');case['idle_before']=snapshot(sid)
  assert case['idle_before']['metadata']['active_streams']=='0'
  assert case['idle_before']['provider']==('1'if busy else'0')
  reached=RemoteGate();release=reached;target_ident=[None];original=client.incr
  def held(name,*args,**kwargs):
   result=original(name,*args,**kwargs)
   if name==profile_key and threading.get_ident()==target_ident[0] and not reached.is_set():
    reached.set()
    if not release.wait(8):raise TimeoutError('Owned held INCR return gate')
   return result
  control('arm',{'profile_key':profile_key,'target_agent':agent})
  pool=ThreadPoolExecutor(max_workers=1,thread_name_prefix='owned-idle-attempt')
  def attempt():
   close_old_connections();target_ident[0]=threading.get_ident()
   try:return call(movie,sid,agent)
   finally:close_old_connections()
  future=pool.submit(attempt)
  try:
   assert reached.wait(5),'Original profile INCR boundary not reached'
   held_at=time.monotonic();case['held']=snapshot(sid)
   assert case['held']['metadata']['active_streams']=='1'
   assert case['held']['provider']==('2'if busy else'1')
   case['admin_stop']=stop(sid);assert case['admin_stop']['status']==200
   follower=call(movie,sid,agent);case['follower_status_while_held']=follower.status_code
   assert follower.status_code==200 and follower.streaming
   follower_iterator=iter(follower.streaming_content);block=next(follower_iterator);assert len(block)==8192
   case['follower_first_sha256']=hashlib.sha256(block).hexdigest();case['with_follower']=snapshot(sid)
   remaining=4-(time.monotonic()-held_at)
   if remaining>0:time.sleep(remaining)
   case['after_prior_timer_while_held']=snapshot(sid);case['held_seconds']=time.monotonic()-held_at
   release.set();target=future.result(timeout=15)
   case['target_after_release']={'status':target.status_code,'streaming':target.streaming}
   if not target.streaming:
    if hasattr(target,'render'):target.render()
    case['target_after_release'].update(body=target.content.decode(),headers={k.lower():v for k,v in target.items()})
   case['after_target_settled']=snapshot(sid)
   case['late_stop_body_boundary']='Not asserted in early-admission TCP scope; prior fullTCP1 timed out, retained separately'
   target.close();follower.close()
   if other is not None:other.close()
   time.sleep(1.5);case['after_close']=snapshot(sid)
  finally:
   release.set()
   try:future.result(timeout=15)
   except Exception:pass
   pool.shutdown(wait=True,cancel_futures=True)
 report['capture_complete']=True
finally:
 for response in reversed(responses):
  try:response.close()
  except Exception:pass
 time.sleep(1.5)
 server.terminate();server.wait(timeout=5);serverlog.close()
 report['remaining_metadata']=list(client.scan_iter(match='vod_persistent_connection:*'))
 report['provider_after_cleanup']=client.get(profile_key)
 Path('/evidence/vod-idle-tcp-source.json').write_text(json.dumps(report,indent=2)+'\n')
