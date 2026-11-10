"""Actual Original active VOD body across owned Redis-only state loss.
Original Dispatcharr authors retain full application/algorithm credit.
No replacement metadata is seeded, no upstream/provider jobs are executed.
"""
import hashlib,json,os,subprocess,sys,tempfile,time,urllib.request
from pathlib import Path
assert os.environ.get('POSTGRES_PASSWORD')=='playback-fixture-only'
assert os.environ.get('REDIS_PASSWORD')=='playback-fixture-only'
sys.path.insert(0,'/app');sys.argv.append('shell');os.environ.setdefault('DJANGO_SETTINGS_MODULE','dispatcharr.settings')
import django
django.setup()
from django.test import RequestFactory
from apps.vod.models import Movie
from apps.m3u.models import M3UAccountProfile
from apps.proxy.vod_proxy.multi_worker_connection_manager import MultiWorkerVODConnectionManager
from core.utils import RedisClient
from core.models import SystemEvent
seed=json.loads(Path('/evidence/vod-seed.json').read_text());client=RedisClient.get_client();assert client is not None
if list(client.scan_iter(match='vod_persistent_connection:*')):raise RuntimeError('Owned VOD fixture must start idle')
movie=Movie.objects.get(uuid=seed['movie_uuid']);profile=M3UAccountProfile.objects.get(pk=seed['provider_profile_id']);manager=MultiWorkerVODConnectionManager.get_instance();cached=id(client);sid='fixture-vod-active-restart';key='vod_persistent_connection:'+sid;responses=[];work=Path(tempfile.mkdtemp(prefix='owned-vod-redis-'));redis_down=False
report={'capture_complete':False,'scope':'Actual Original full manager/body state-loss characterization; not frontend/auth or native recovery acceptance','pid':os.getpid(),'source_hashes':{name:hashlib.sha256(Path('/app',name).read_bytes()).hexdigest()for name in ['apps/proxy/vod_proxy/multi_worker_connection_manager.py','core/utils.py','core/redis_connection.py']},'redis_run_ids':[],'observations':[],'performance_verified':False,'native_implementation_verified':False,'body_sample_limit':34*8192}
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
def state(name):
 value={'name':name,'same_client':id(RedisClient._client)==cached,'manager_same_client':manager.redis_client is client}
 try:
  row=client.hgetall(key);value.update(metadata=dict(row),provider_count=client.get('profile_connections:'+str(profile.pk)),ttl=client.ttl(key))
 except Exception as error:value['error']={'class':type(error).__name__,'module':type(error).__module__,'args':list(error.args)}
 report['observations'].append(value);return value
def open_response():
 response=manager.stream_content_with_session(sid,movie,'http://origin:8080/vod.mp4',profile,'192.0.2.23','owned-active-restart',RequestFactory().get('/proxy/vod/fixture'))
 responses.append(response);return response
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
 iterator=iter(response.streaming_content);chunks=[];chunk=next(iterator);assert len(chunk)==8192;chunks.append(chunk);state('active-before-loss')
 subprocess.run(['redis-cli','--no-auth-warning','SHUTDOWN','NOSAVE'],env=cli_env,check=True,capture_output=True,timeout=10);redis_down=True
 state('server-down-body-held')
 # Do not replace manager, iterator, cached client or local provider handle.
 start_redis();report['redis_run_ids'].append(client.info('server')['run_id']);assert report['redis_run_ids'][0]!=report['redis_run_ids'][1];lost=state('server-restarted-no-reseed');assert not lost.get('metadata')and lost.get('provider_count')is None
 deadline=time.monotonic()+30;sample_error=None;ended=False
 for _ in range(33):
  if time.monotonic()>deadline:raise TimeoutError('Bounded post-loss body sampling')
  try:chunk=next(iterator)
  except StopIteration:ended=True;break
  except Exception as error:sample_error={'class':type(error).__name__,'module':type(error).__module__,'args':list(error.args)};break
  chunks.append(chunk)
 payload=b''.join(chunks);assert 0<len(payload)<=report['body_sample_limit'];Path('/evidence/vod-active-source.bin').write_bytes(payload);report['old_body']={'bytes':len(payload),'sha256':hashlib.sha256(payload).hexdigest(),'ended':ended,'error':sample_error};state('old-body-after-loss-sampling')
 # Literal same-ID reopen while old response is still alive. Record outcomes,
 # including state replacement/admission, rather than assuming recovery.
 sibling=open_response();report['same_id_reopen']={'status':sibling.status_code,'streaming':sibling.streaming}
 if sibling.streaming:
  try:
   first=next(iter(sibling.streaming_content));report['same_id_reopen']['first_bytes']=len(first);report['same_id_reopen']['first_sha256']=hashlib.sha256(first).hexdigest()
  except StopIteration:report['same_id_reopen']['ended_without_bytes']=True
  except Exception as error:report['same_id_reopen']['error']={'class':type(error).__name__,'module':type(error).__module__,'args':list(error.args)}
 state('same-id-reopen-before-close');sibling.close();sibling.close();state('sibling-closed-old-body-held');response.close();response.close();time.sleep(1.5);state('all-bodies-closed-after-grace')
 # Reference only the sampled prefix; this is not a full-file/EOF gate.
 request=urllib.request.Request('http://origin:8080/vod.mp4',headers={'Range':'bytes=0-'+str(len(payload)-1)})
 with urllib.request.urlopen(request,timeout=10)as reference:
  expected=reference.read(report['body_sample_limit']+1);report['reference_status']=reference.status
 report['body_prefix_matches_origin']=expected==payload
 report['events']=list(SystemEvent.objects.filter(id__gt=event_start).order_by('id').values('event_type','details'))
 if len(report['events'])>64:raise RuntimeError('Owned audit bound')
 report['same_process']=os.getpid()==report['pid'];report['capture_complete']=True
finally:
 if redis_down:start_redis()
 for response in reversed(responses):
  try:response.close()
  except Exception:pass
 Path('/evidence/vod-active-restart-source.json').write_text(json.dumps(report,indent=2)+'\n')
print(json.dumps({'capture_complete':report['capture_complete'],'body_prefix_matches_origin':report.get('body_prefix_matches_origin'),'bytes':report.get('old_body',{}).get('bytes')}))
