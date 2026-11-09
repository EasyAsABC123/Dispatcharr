"""Actual Source Timeshift registration publication oracle; original authors' credit.
Component only: uses real ORM/Redis/Channels, not an HTTP timing acceptance.
"""
import os,sys,json,asyncio,time
from pathlib import Path
if os.environ.get('POSTGRES_PASSWORD')!='playback-fixture-only':raise RuntimeError('Disposable fixture only')
sys.path.insert(0,'/app');os.environ.setdefault('DJANGO_SETTINGS_MODULE','dispatcharr.settings')
import django
django.setup()
from channels.layers import get_channel_layer
from core.utils import RedisClient
from apps.timeshift import views
from apps.timeshift.redis_keys import TimeshiftRedisKeys
seed=json.loads(Path('/evidence/seed.json').read_text());redis=RedisClient.get_client()
runner=asyncio.Runner()
layer=get_channel_layer();observer=runner.run(layer.new_channel('rustprobetsmedia.'))
runner.run(layer.group_add('updates',observer))
async def receive(timeout):
    try:return await asyncio.wait_for(layer.receive(observer),timeout)
    except asyncio.TimeoutError:return None
cases=[];owned=[]
def run(name,*,existing=None,emit=True,repeat=False):
    sid='fixture-ts-producer-'+str(len(cases));stats=f"{seed['channel_pk']}_{sid}"
    key=TimeshiftRedisKeys.client_metadata(stats,sid)
    if redis.exists(key):raise RuntimeError('Owned producer key already exists')
    owned.append((stats,sid))
    if existing:redis.hset(key,mapping=existing)
    options=dict(channel_display_name='Producer fixture',timestamp_utc='2024-01-01 00:00:00',primary_url='http://origin:8080/fixture.ts',programme_vid=f"{seed['channel_pk']}_1704067200",channel_id=seed['channel_pk'],channel_uuid=seed['channel_uuid'],emit_stats_update=emit)
    views._register_stats_client(redis,stats,sid,'192.0.2.22','fixture-ts-producer',None,**options)
    first=runner.run(receive(2 if emit and not existing else .5))
    value={'name':name,'preexisting':existing,'emit_stats_update':emit,'published':first}
    if repeat:
        views._register_stats_client(redis,stats,sid,'192.0.2.22','fixture-ts-producer',None,**options)
        value['repeat_published']=runner.run(receive(.5))
    cases.append(value)
try:
    run('new-viewer',repeat=True)
    run('new-viewer-suppressed-by-caller',emit=False)
    run('existing-same-programme',existing={'connected_at':str(time.time()-10),'programme_vid':f"{seed['channel_pk']}_1704067200"})
    run('existing-connected-at-no-programme',existing={'connected_at':str(time.time()-10)})
    run('existing-programme-hop',existing={'connected_at':str(time.time()-10),'programme_vid':f"{seed['channel_pk']}_1704063600"})
    expected={'new-viewer':True,'new-viewer-suppressed-by-caller':False,'existing-same-programme':False,'existing-connected-at-no-programme':False,'existing-programme-hop':True}
    for case in cases:
        if bool(case['published'])!=expected[case['name']]:raise RuntimeError('Unexpected original registration publication: '+case['name'])
        if case.get('repeat_published') is not None:raise RuntimeError('Repeated viewer unexpectedly republished')
        message=case['published']
        if message:
            if message['type']!='update' or message['data'].get('success') is not True or message['data'].get('type')!='timeshift_stats' or not isinstance(message['data'].get('stats'),str):raise RuntimeError('Unexpected original Timeshift envelope')
            json.loads(message['data']['stats'])
    # Original displaces other owned positions before selecting/opening the new
    # upstream, so the old-viewer removal must publish even if later setup fails.
    for stats,sid in owned:views._unregister_stats_client(redis,stats,sid)
    owned.clear()
    from apps.accounts.models import User
    user=User.objects.get(username='rust-vod-fixture-capped')
    sid='fixture-ts-displaced';stats=f"{seed['channel_pk']}_{sid}";owned.append((stats,sid))
    views._register_stats_client(redis,stats,sid,'192.0.2.22','fixture-ts-producer',user,channel_display_name='Producer fixture',timestamp_utc='2024-01-01 00:00:00',primary_url='http://origin:8080/fixture.ts',programme_vid=f"{seed['channel_pk']}_1704067200",channel_id=seed['channel_pk'],channel_uuid=seed['channel_uuid'],emit_stats_update=False)
    views._terminate_previous_timeshift_sessions(redis,user,seed['channel_pk'],f"{seed['channel_pk']}_1704070800",'fixture-ts-new-position')
    displacement=runner.run(receive(3))
    if not displacement or displacement.get('data',{}).get('type')!='timeshift_stats' or json.loads(displacement['data']['stats'])['total_connections']!=0:raise RuntimeError('Missing original immediate displacement snapshot')
    print(json.dumps({'format_version':1,'scope':'Original actual Redis/Channels Timeshift stats registration component; no HTTP lifecycle timing acceptance','cases':cases,'immediate_displacement':{'published':displacement,'before_new_upstream':True},'application_parity_verified':False,'performance_verified':False}))
finally:
    for stats,sid in owned:views._unregister_stats_client(redis,stats,sid)
    runner.run(layer.group_discard('updates',observer))
    runner.close()
