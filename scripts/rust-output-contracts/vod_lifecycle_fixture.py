"""Original VOD response-body lifecycle with real upstream HTTP/Redis/Channels.
Original Dispatcharr authors/contributors retain all application credit.
This tests the actual manager response iterator, not frontend authentication.
"""
import os,sys,json,time,asyncio,hashlib
from pathlib import Path
if os.environ.get('POSTGRES_PASSWORD')!='playback-fixture-only':raise RuntimeError('Disposable fixture only')
sys.path.insert(0,'/app');os.environ.setdefault('DJANGO_SETTINGS_MODULE','dispatcharr.settings')
import django
django.setup()
from channels.layers import get_channel_layer
from django.test import RequestFactory
from apps.vod.models import Movie
from apps.m3u.models import M3UAccountProfile
from apps.proxy.vod_proxy.multi_worker_connection_manager import MultiWorkerVODConnectionManager
from core.utils import RedisClient
from core.models import SystemEvent
seed=json.loads(Path('/evidence/vod-seed.json').read_text());redis=RedisClient.get_client()
if list(redis.scan_iter(match='vod_persistent_connection:*')):raise RuntimeError('VOD fixture must be idle')
movie=Movie.objects.get(uuid=seed['movie_uuid']);profile=M3UAccountProfile.objects.get(pk=seed['provider_profile_id'])
manager=MultiWorkerVODConnectionManager.get_instance();runner=asyncio.Runner();layer=get_channel_layer()
observer=runner.run(layer.new_channel('rustprobevodbody.'));runner.run(layer.group_add('updates',observer))
async def collect(seconds):
    result=[];deadline=time.monotonic()+seconds
    while time.monotonic()<deadline:
        try:message=await asyncio.wait_for(layer.receive(observer),deadline-time.monotonic())
        except asyncio.TimeoutError:break
        if message.get('data',{}).get('type') in ('vod_started','vod_stopped','vod_stats'):result.append(message)
    return result
async def collect_start():
    result=[];deadline=time.monotonic()+5
    while time.monotonic()<deadline:
        try:message=await asyncio.wait_for(layer.receive(observer),deadline-time.monotonic())
        except asyncio.TimeoutError:break
        result.append(message)
        if {'vod_started','vod_stats'}<={value['data']['type'] for value in result}:return result
    raise RuntimeError('Original start/stat messages timed out; observed types='+repr([message.get('data',{}).get('type') for message in result]))
event_baseline_id=SystemEvent.objects.order_by('-id').values_list('id',flat=True).first() or 0
cases=[];responses=[]
def open_response(sid,url='http://origin:8080/vod.mp4'):
    value=manager.stream_content_with_session(sid,movie,url,profile,'192.0.2.23','fixture-body-'+sid,RequestFactory().get('/proxy/vod/fixture'))
    responses.append(value);return value
def state(sid):
    row=redis.hgetall('vod_persistent_connection:'+sid)
    return {'exists':bool(row),'active_streams':int(row.get('active_streams',0)),'provider_connections':int(redis.get('profile_connections:'+str(profile.pk)) or 0),'metadata_user_id':row.get('user_id'),'bytes_sent':int(row.get('bytes_sent',0))}
def finish(name,sid,events,extra=None):
    kinds=[value['data']['type'] for value in events]
    cases.append({'name':name,'events':events,'event_types':kinds,'final':state(sid),**(extra or {})})
    if state(sid)['provider_connections']!=0 or state(sid)['exists']:raise RuntimeError('Source lifecycle cleanup incomplete: '+name)
def check(name,events,starts,stops):
    types=[value['data']['type'] for value in events]
    if types.count('vod_started')!=starts or types.count('vod_stopped')!=stops:raise RuntimeError('Unexpected original body publications: '+name+repr(types))
try:
    sid='fixture-vod-unpolled';response=open_response(sid);initial=state(sid)
    before=runner.run(collect(.2));response.close();response.close();events=before+runner.run(collect(1.3));check(sid,events,0,0);finish('new-unpolled-duplicate-close',sid,events,{'initial':initial})
    sid='fixture-vod-disconnect';response=open_response(sid);chunk=next(iter(response.streaming_content));initial=state(sid)
    events=runner.run(collect_start());response.close();events+=runner.run(collect(1.3));check(sid,events,1,1);finish('new-started-disconnect',sid,events,{'first_bytes':len(chunk),'initial':initial})
    sid='fixture-vod-reused-unpolled';first=open_response(sid);next(iter(first.streaming_content));events=runner.run(collect_start());second=open_response(sid)
    if state(sid)['active_streams']!=2:raise RuntimeError('Original sibling admission failed')
    first.close();during=runner.run(collect(1.2));check(sid,during,0,0);second.close();events+=during+runner.run(collect(1.3));check(sid,events,1,1);finish('reused-unpolled-last-close',sid,events)
    sid='fixture-vod-reconnect';first=open_response(sid);next(iter(first.streaming_content));events=runner.run(collect_start());first.close();second=open_response(sid);next(iter(second.streaming_content))
    during=runner.run(collect(1.2));check(sid,during,0,0);second.close();events+=during+runner.run(collect(1.3));check(sid,events,1,1);finish('reconnect-suppresses-grace-stop',sid,events)
    sid='fixture-vod-completed';response=open_response(sid);digest=hashlib.sha256();count=0
    iterator=iter(response.streaming_content);chunk=next(iterator);digest.update(chunk);count+=len(chunk)
    # Same first-chunk/start-snapshot barrier as native; otherwise Source's
    # background stats thread races the entire synchronous file consumption.
    events=runner.run(collect_start())
    for chunk in iterator:digest.update(chunk);count+=len(chunk)
    sample=state(sid)
    response.close();events+=runner.run(collect(1.3));check(sid,events,1,1);finish('normal-completion',sid,events,{'bytes':count,'sha256':digest.hexdigest(),'sample_after_eof':sample})
    sid='fixture-vod-provider-failed';response=open_response(sid,'http://origin:8080/not-a-vod-fixture.mp4');status=response.status_code;response.close();events=runner.run(collect(1.3));check(sid,events,0,0);finish('provider-failure-before-body',sid,events,{'status':status})
    audit=list(SystemEvent.objects.filter(id__gt=event_baseline_id,event_type__in=['vod_start','vod_stop']).order_by('id').values('event_type','details'))
    print(json.dumps({'format_version':1,'scope':'Actual original VOD manager response-body timing, real upstream HTTP/Redis/Channels/audit; no frontend auth/native acceptance','cases':cases,'audit':audit,'application_parity_verified':False,'performance_verified':False}))
finally:
    Path('/evidence/vod-lifecycle-partial.json').write_text(json.dumps({'cases':cases,'audit_types':list(SystemEvent.objects.filter(event_type__in=['vod_start','vod_stop']).values_list('event_type',flat=True)),'owned_state':{sid:state(sid) for sid in ['fixture-vod-unpolled','fixture-vod-disconnect']}}))
    for response in responses:response.close()
    runner.run(layer.group_discard('updates',observer));runner.close()
