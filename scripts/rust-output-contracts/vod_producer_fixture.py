"""Original VOD statistics/event helper oracle on disposable actual DB/Redis.
All application credit belongs to original Dispatcharr authors/contributors.
This is a component oracle; HTTP/body timing needs its separate lifecycle gate.
"""
import os,sys,json,asyncio,time
from pathlib import Path
from types import SimpleNamespace
if os.environ.get('POSTGRES_PASSWORD')!='playback-fixture-only':raise RuntimeError('Disposable fixture only')
sys.path.insert(0,'/app');os.environ.setdefault('DJANGO_SETTINGS_MODULE','dispatcharr.settings')
import django
django.setup()
from django.core.serializers.json import DjangoJSONEncoder
from asgiref.sync import async_to_sync
from channels.layers import get_channel_layer
from core.utils import RedisClient
from core.models import SystemEvent
from apps.vod.models import Movie,Episode,Series,VODLogo
from apps.proxy.vod_proxy import views
from apps.proxy.vod_proxy.multi_worker_connection_manager import MultiWorkerVODConnectionManager
seed=json.loads(Path('/evidence/vod-seed.json').read_text())
redis=RedisClient.get_client()
if list(redis.scan_iter(match='vod_persistent_connection:*')):raise RuntimeError('VOD fixture is not idle')
clock=1700001000.75
base={'content_obj_type':'movie','content_uuid':seed['movie_uuid'],'client_ip':'192.0.2.21','user_id':'42','client_user_agent':'fixture-vod-producer','created_at':str(clock-30.25),'last_activity':str(clock-2),'m3u_profile_id':str(seed['provider_profile_id']),'position_seconds':'10','bytes_sent':'1000','total_content_size':'1000000'}
movie=Movie.objects.get(uuid=seed['movie_uuid']);episode=Episode.objects.get(uuid=seed['episode_uuid'])
original_movie_duration=movie.duration_secs;original_episode_duration=episode.duration_secs
series=episode.series
movie_fields=['year','rating','genre','description','tmdb_id','imdb_id','logo_id']
episode_fields=['air_date','rating','description','tmdb_id','imdb_id','season_number','episode_number']
series_fields=['year','genre','logo_id']
restore_records=[(Movie,movie.pk,Movie.objects.filter(pk=movie.pk).values(*movie_fields).get()),(Episode,episode.pk,Episode.objects.filter(pk=episode.pk).values(*episode_fields).get()),(Series,series.pk,Series.objects.filter(pk=series.pk).values(*series_fields).get())]
logo=VODLogo.objects.create(name='Producer metadata Ω',url='https://logo.fixture.invalid/producer-vod.png')
movie_values={'year':2021,'rating':'7.25','genre':'Fixture & Drama','description':'Movie metadata Ω','tmdb_id':'fixture-movie-tmdb','imdb_id':'fixture-movie-imdb','logo_id':logo.pk}
episode_values={'air_date':'2020-02-03','rating':'8.5','description':'Episode metadata Ω','tmdb_id':'fixture-episode-tmdb','imdb_id':'fixture-episode-imdb','season_number':3,'episode_number':7}
series_values={'year':2019,'genre':'Fixture series genre','logo_id':logo.pk}
Movie.objects.filter(pk=movie.pk).update(**movie_values)
Episode.objects.filter(pk=episode.pk).update(**episode_values)
Series.objects.filter(pk=series.pk).update(**series_values)
database_records={'movie':{'id':movie.pk,'values':movie_values},'episode':{'id':episode.pk,'values':episode_values},'series':{'id':series.pk,'values':series_values},'logo':{'id':logo.pk,'name':logo.name,'url':logo.url}}
original_time=views.time
views.time=SimpleNamespace(time=lambda:clock)
cases=[]
def capture(name,change=None,*,session='fixture-vod-stats',duration=None):
    key='vod_persistent_connection:'+session
    row={**base,**(change or {})}
    Movie.objects.filter(pk=movie.pk).update(duration_secs=duration)
    redis.hset(key,mapping=row);redis.expire(key,120)
    try:
        value=views.build_vod_stats_data(redis)
        cases.append({'name':name,'session_id':session,'row':row,'movie_duration_secs':duration,'expected':value})
    finally:redis.delete(key)
try:
    capture('movie-no-duration')
    capture('movie-duration',duration=7200)
    capture('movie-seek-duration-estimate',{'last_seek_percentage':'25','last_seek_byte':'250000'})
    capture('movie-seek-elapsed',{'last_seek_percentage':'50','last_seek_timestamp':str(clock-10.9)},duration=100)
    capture('movie-seek-duration-cap',{'last_seek_percentage':'99','last_seek_timestamp':str(clock-10.9)},duration=100)
    capture('movie-position-elapsed',{'last_position_update':str(clock-12.8)},duration=100)
    capture('movie-position-invalid-time',{'last_position_update':'not-a-time'},duration=100)
    capture('movie-future-position-anchor',{'last_position_update':str(clock+3.8)},duration=100)
    capture('episode-default-duration',{'content_obj_type':'episode','content_uuid':seed['episode_uuid']})
    capture('missing-content',{'content_uuid':'00000000-0000-0000-0000-000000000000'})
    capture('unknown-content-type',{'content_obj_type':'fixture-unknown'})
    capture('profile-db-miss-fallback',{'m3u_profile_id':'999999999','m3u_profile_name':'Stored profile'})
    capture('profile-invalid-fallback',{'m3u_profile_id':'not-an-id','m3u_profile_name':'Stored profile'})
    capture('invalid-position-skips-row',{'position_seconds':'not-a-number'})
    capture('invalid-bytes-skips-row',{'bytes_sent':'not-a-number'})
    capture('invalid-seek-skips-row',{'last_seek_percentage':'not-a-number'})
    capture('bad-created-at-client-id-fallback',{'created_at':'bad'},session='vod_1700000990000_fixture')
    capture('bad-created-at-final-zero',{'created_at':'bad'})
    capture('future-created-at-negative-duration',{'created_at':str(clock+3.9)})
    views.time=original_time
    # Actual original Channels publication + DB audit, no fake sink/mocked helper.
    layer=get_channel_layer();observer=async_to_sync(layer.new_channel)('rustprobemedia.')
    async_to_sync(layer.group_add)('updates',observer)
    manager=MultiWorkerVODConnectionManager.get_instance()
    published=[];before=SystemEvent.objects.filter(event_type__in=['vod_start','vod_stop']).count()
    async def receive_pair(kind):
        found=[]
        for _ in range(8):
            message=await asyncio.wait_for(layer.receive(observer),10)
            if message.get('data',{}).get('type') in (kind,'vod_stats'):
                found.append(message)
                if {value['data']['type'] for value in found}=={kind,'vod_stats'}:return found
        raise RuntimeError('Missing original VOD producer message')
    try:
        for kind in ['vod_started','vod_stopped']:
            manager._send_vod_event(kind,'fixture-session','Fixture content',seed['movie_uuid'],'192.0.2.21','42',username='fixture-media-user')
            published.extend(async_to_sync(receive_pair)(kind))
    finally:async_to_sync(layer.group_discard)('updates',observer)
    audit=list(SystemEvent.objects.filter(event_type__in=['vod_start','vod_stop']).order_by('id').values('event_type','details'))[before:]
    result={'format_version':1,'scope':'Original actual VOD statistics projection and helper-produced Channels/DB audit; no HTTP lifecycle timing acceptance','clock':clock,'database_records':database_records,'seed':{key:seed[key] for key in ['movie_uuid','episode_uuid','provider_profile_id','provider_account_id']},'stats_cases':cases,'published':published,'audit':audit,'application_parity_verified':False,'performance_verified':False}
    print(json.dumps(result,cls=DjangoJSONEncoder))
finally:
    views.time=original_time
    for model,pk,values in restore_records:model.objects.filter(pk=pk).update(**values)
    logo.delete()
    Movie.objects.filter(pk=movie.pk).update(duration_secs=original_movie_duration)
    Episode.objects.filter(pk=episode.pk).update(duration_secs=original_episode_duration)
