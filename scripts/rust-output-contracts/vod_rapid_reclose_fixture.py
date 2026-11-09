"""Original rapid-reconnect/close race characterization, not native acceptance.
All application credit belongs to original Dispatcharr authors/contributors.
"""
import os,sys,json,time,asyncio
from pathlib import Path
if os.environ.get('POSTGRES_PASSWORD')!='playback-fixture-only':raise RuntimeError('Disposable fixture only')
sys.path.insert(0,'/app');os.environ.setdefault('DJANGO_SETTINGS_MODULE','dispatcharr.settings')
import django
django.setup()
from django.test import RequestFactory
from channels.layers import get_channel_layer
from apps.vod.models import Movie
from apps.m3u.models import M3UAccountProfile
from core.models import SystemEvent
from core.utils import RedisClient
from apps.proxy.vod_proxy.multi_worker_connection_manager import MultiWorkerVODConnectionManager
seed=json.loads(Path('/evidence/vod-seed.json').read_text());redis=RedisClient.get_client()
if list(redis.scan_iter(match='vod_persistent_connection:*')):raise RuntimeError('VOD fixture mustbeidle')
movie=Movie.objects.get(uuid=seed['movie_uuid']);profile=M3UAccountProfile.objects.get(pk=seed['provider_profile_id']);manager=MultiWorkerVODConnectionManager.get_instance()
runner=asyncio.Runner();layer=get_channel_layer();observer=runner.run(layer.new_channel('rustprobevodrapid.'));runner.run(layer.group_add('updates',observer));responses=[]
async def collect_start():
    events=[];deadline=time.monotonic()+5
    while time.monotonic()<deadline:
        event=await asyncio.wait_for(layer.receive(observer),deadline-time.monotonic());events.append(event)
        if {'vod_started','vod_stats'}<={value.get('data',{}).get('type') for value in events}:return events
    raise RuntimeError('Original start timeout')
async def collect(seconds):
    events=[];deadline=time.monotonic()+seconds
    while time.monotonic()<deadline:
        try:events.append(await asyncio.wait_for(layer.receive(observer),deadline-time.monotonic()))
        except asyncio.TimeoutError:break
    return events
sid='fixture-vod-rapid-reclose';baseline=SystemEvent.objects.order_by('-id').values_list('id',flat=True).first() or 0
try:
    def open_response():
        response=manager.stream_content_with_session(sid,movie,'http://origin:8080/vod.mp4',profile,'192.0.2.25','fixture-rapid-reclose',RequestFactory().get('/proxy/vod/fixture'));responses.append(response)
        if response.status_code!=200:raise RuntimeError('Source rapid fixture response failed')
        next(iter(response.streaming_content));return response
    first=open_response();events=runner.run(collect_start());began=time.monotonic();first.close();second=open_response();second.close();reclose_seconds=time.monotonic()-began
    events+=runner.run(collect(1.6))
    types=[value.get('data',{}).get('type') for value in events]
    audit=list(SystemEvent.objects.filter(id__gt=baseline,event_type__in=['vod_start','vod_stop']).order_by('id').values('event_type','details'))
    print(json.dumps({'format_version':1,'scope':'Actual original manager/provider/Redis/Channels rapidreclose timer race only','reclose_elapsed_seconds':reclose_seconds,'within_original_grace':reclose_seconds<1,'event_types':types,'events':events,'audit':audit,'remaining_metadata':bool(redis.exists('vod_persistent_connection:'+sid)),'provider_connections':int(redis.get('profile_connections:'+str(profile.pk)) or 0),'native_implementation_verified':False,'application_parity_verified':False,'performance_verified':False}))
finally:
    for response in responses:response.close()
    runner.run(layer.group_discard('updates',observer));runner.close()
