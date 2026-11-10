"""Disposable original-schema XMLTV fixture; original Dispatcharr author credit."""
import hashlib,json,os,sys
from datetime import timedelta
if os.environ.get('POSTGRES_PASSWORD')!='playback-fixture-only':raise RuntimeError('owned fixture required')
sys.path.insert(0,'/xc-probe')
import control as xc
from django.utils import timezone
from django_redis import get_redis_connection
from apps.channels.models import Channel,ChannelOverride
from apps.epg.models import EPGSource,EPGData,ProgramData
from core.models import SystemEvent
state={}
def command(request):
    op=request['op']
    if op=='lock_xc_epg_metadata':
        from django.db import connection
        assert not state.get('epg_locked')
        with connection.cursor()as cursor:
            cursor.execute('BEGIN');cursor.execute('LOCK TABLE dispatcharr_channels_channel IN ACCESS EXCLUSIVE MODE')
        state['epg_locked']=True
        return {'locked':'only owned fixture channel metadata table'}
    if op=='xc_epg_waiting':
        from django.db import connection
        with connection.cursor()as cursor:
            # This connection intentionally holds the fixture table lock in an
            # explicit transaction; refresh its cached statistics each poll.
            cursor.execute('SELECT pg_stat_clear_snapshot()')
            cursor.execute("SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND pid<>pg_backend_pid() AND wait_event_type='Lock' AND query LIKE 'SELECT c.id::bigint AS id,COALESCE(o.channel_number%'")
            return {'waiting':cursor.fetchone()[0]}
    if op=='unlock_xc_epg_metadata':
        from django.db import connection
        assert state.get('epg_locked')
        with connection.cursor()as cursor:cursor.execute('ROLLBACK')
        state['epg_locked']=False
        return {'unlocked':True}
    if op=='seed':
        seeded=xc.command(request)
        if state:return {**seeded,**state['public']}
        Channel.objects.exclude(pk__in=seeded['channels']).update(hidden_from_output=True)
        real=EPGSource.objects.create(name='XMLTV wire real',source_type='xmltv',url='https://never-requested.invalid/guide.xml')
        dummy=EPGSource.objects.create(name='XMLTV empty dummy',source_type='dummy',custom_properties={})
        guide=EPGData.objects.create(name='Guide & café',tvg_id='fixture.guide',epg_source=real)
        empty=EPGData.objects.create(name='Guide empty dummy',tvg_id='fixture.dummy',epg_source=dummy)
        channels=seeded['channels'];Channel.objects.filter(pk__in=channels[:3]).update(epg_data=guide)
        Channel.objects.filter(pk=channels[3]).update(epg_data=empty)
        ChannelOverride.objects.filter(channel_id=channels[1]).update(tvg_id='override&id',tvc_guide_stationid='station"id')
        Channel.objects.filter(pk=channels[0]).update(tvg_id='base<&id',tvc_guide_stationid='station-base')
        at=timezone.now().replace(microsecond=0)
        rich={'categories':['News & events','日本語'], 'keywords':['one<two'], 'onscreen_episode':'S2E3','episode':'3','season':'2','dd_progid':'EP123','thetvdb.com_id':'TV&1','language':'en','original_language':'fr','length':{'value':30,'units':'minutes'},'video':{'present':'yes','colour':'yes','aspect':'16:9','quality':'HD'},'audio':{'present':'yes','stereo':'stereo'},'subtitles':[{'type':'teletext','language':'en'}],'rating':'TV-PG','rating_system':'TV Parental Guidelines','star_ratings':[{'system':'imdb','value':'8/10'}],'reviews':[{'content':'Great & odd','source':'Fixture','reviewer':'A < B'}],'images':[{'url':'https://image.invalid/a?x=1&y=2','type':'poster','size':'large'}],'credits':{'director':['Director & Co'],'actor':[{'name':'Actor <A>','role':'Guest "role"','guest':True}]},'date':'20261101','country':'US','sd_icon':'https://sd.invalid/poster.jpg','previously_shown':True,'previously_shown_details':{'start':'20260101000000 +0000','channel':'old&channel'},'premiere':True,'premiere_text':'First!','last_chance':True,'last_chance_text':'Last!','new':True,'live':True}
        specs=[(-4,4,'Long overlapping',rich),(-2,-1,'Recent history',{}),(-1,1,'Current <&> café',{'icon':'https://icon.invalid/a?x=1&y=2'}),(2,3,'Future narrow',{}),(28,30,'After day one',{}),(72,73,'Day three',{}),(-72,-71,'Old history',{})]
        programs=[]
        for start,end,title,props in specs:
            row=ProgramData.objects.create(epg=guide,start_time=at+timedelta(hours=start),end_time=at+timedelta(hours=end),title=title,sub_title='Sub "title"' if props else None,description='Desc < & > \' " 東京' if props else '',custom_properties=props);programs.append(row.pk)
        redis=get_redis_connection('default');redis.incr('epg_content_generation')
        public={'guide':guide.pk,'programs':programs,'clock':at.isoformat(),'seed_hour':at.hour}
        state.update(public=public,guide=guide.pk,programs=programs)
        return {**seeded,**public}
    if op=='window_vectors':
        from window_source import capture
        return capture()
    if op=='content_vectors':
        from content_source import capture
        return capture(xc.state['users'])
    if op=='grid_datetime_fuzz':
        from grid_datetime_fuzz import capture
        return capture(request.get('start',0),request.get('count',64))
    if op=='xc_epg_vectors':
        from epg_source import capture
        return capture(xc.state['users'],xc.state['channels'],state['public']['clock'],request.get('start',0),request.get('count',32))
    if op=='xc_epg_hash':
        from epg_source import hash_characterization
        return hash_characterization()
    if op=='cache_vectors':
        from cache_source import capture
        return capture()
    if op=='render_vectors':
        from render_source import capture
        return capture(ProgramData.objects.get(pk=state['programs'][0]).custom_properties)
    if op=='cache':
        redis=get_redis_connection('default');rows=[]
        for key in sorted(redis.scan_iter(match='epg_content:*',count=100)):
            if len(rows)>=500:raise RuntimeError('fixture cache bound')
            name=key.decode();kind=redis.type(key).decode();entry={'key':name,'kind':kind,'ttl':redis.ttl(key)}
            if kind=='string':
                raw=redis.get(key)
                if raw is None:continue  # TTL can expire between TYPE and GET.
                entry['value']=raw.decode()
            elif kind=='list':
                length=redis.llen(key);assert length<=65536;chunks=redis.lrange(key,0,-1);assert sum(map(len,chunks))<=8*1024*1024;entry.update(chunks=length,body_bytes=sum(map(len,chunks)),body_sha256=hashlib.sha256(b''.join(chunks)).hexdigest())
            rows.append(entry)
        return {'generation':(redis.get('epg_content_generation') or b'0').decode(),'rows':rows,'epg_download_events':SystemEvent.objects.filter(event_type='epg_download').count(),'epg_blocked_events':SystemEvent.objects.filter(event_type='epg_blocked').count()}
    if op=='invalidate':
        from apps.output.streaming_chunk_cache import invalidate_epg_chunk_cache
        invalidate_epg_chunk_cache();return {'invalidated':True}
    if op=='clear_anon_download_dedup':
        # Original download auditing has a two-second wall-clock cooldown.
        # Make the subsequent cold rebuild checkpoint independent of how fast
        # either backend completed the preceding captured HTTP cases.
        redis=get_redis_connection('default')
        keys=list(redis.scan_iter(match=':1:epg_download:anonymous:all:*',count=100))
        if len(keys)>100:raise RuntimeError('Owned audit fixture unexpectedly large')
        ttls=[redis.pttl(key) for key in keys]
        if keys:redis.delete(*keys)
        return {'owned_keys_deleted':len(keys),'prior_ttl_ms':ttls}
    if op=='mutate_program':
        ProgramData.objects.filter(pk=state['programs'][0]).update(title=request['title']);return {'updated':True}
    if op=='reset_phase':
        ProgramData.objects.filter(pk=state['programs'][0]).update(title='Long overlapping')
        redis=get_redis_connection('default');redis.incr('epg_content_generation')
        keys=list(redis.scan_iter(match=':1:epg_download:*',count=100));assert len(keys)<500
        if keys:redis.delete(*keys)
        SystemEvent.objects.filter(event_type__in=['epg_download','epg_blocked']).delete()
        return {'reset':'only owned fixture XMLTV title/generation/event dedup/history'}
    if op=='epg_settings':xc.settings('epg_settings',request['values']);return {'updated':True}
    return xc.command(request)
if __name__=='__main__':
    print(json.dumps({'ready':True}),flush=True)
    for line in sys.stdin:
        if len(line)>65536:raise ValueError('bounded control')
        request=json.loads(line);print(json.dumps(command(request)),flush=True)
        if request['op']=='close':break
