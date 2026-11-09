"""Disposable original Timeshift management controls; upstream credit retained."""
import json
import os
from pathlib import Path
import sys
from datetime import datetime,timezone
if os.environ.get('POSTGRES_PASSWORD')!='playback-fixture-only':
    raise RuntimeError('Restricted to disposable playback fixture')
sys.path.insert(0,'/app')
os.environ.setdefault('DJANGO_SETTINGS_MODULE','dispatcharr.settings')
import django
django.setup()
from apps.accounts.models import User
from apps.channels.models import Channel
from apps.epg.models import ProgramData
from core.models import CoreSettings
from core.utils import RedisClient
from apps.timeshift.redis_keys import TimeshiftRedisKeys as K


def main():
    action,label=sys.argv[1:3]
    if action not in ('begin','seed','inspect','deny-ui','allow-ui','inactive','active','observe-active','restore') or label not in ('source','candidate'):
        raise ValueError('invalid disposable management control')
    path=Path('/evidence')/('management-control-'+label+'.json')
    channel=Channel.objects.get(uuid='0282858c-3869-471b-a58b-e913c1b6bd33')
    user=User.objects.get(username='rust-catchup-owner')
    sid='management-fixture';other='management-unrelated';stats=f'{channel.pk}_{sid}'
    vid=f'{channel.pk}_2026-11-09T11-00-00_fixture'
    client=RedisClient.get_client()
    keys=[K.channel_metadata(stats),K.clients(stats),K.client_metadata(stats,sid),K.client_stop(vid,sid),K.stream_generation(vid,sid),K.stream_generation(vid+'-old',sid),K.client_stop(vid+'-old',sid),K.api_session(sid),K.pool_superseded(sid),K.stats_grace(stats,sid),'timeshift:management-fixture:unrelated']
    if action=='begin':
        if path.exists():raise RuntimeError('management control exists')
        network=CoreSettings.objects.filter(key='network_access').first()
        program=ProgramData.objects.create(epg=channel.epg_data,start_time=datetime(2026,11,9,11,45,30,tzinfo=timezone.utc),end_time=datetime(2026,11,9,12,30,tzinfo=timezone.utc),title='Offline next programme',sub_title='Next subtitle',description='Next description')
        path.write_text(json.dumps({'user_id':user.pk,'level':user.user_level,'active':user.is_active,'network_exists':network is not None,'network_value':network.value if network else None,'program_id':program.pk}))
        user.user_level=10;user.save(update_fields=['user_level'])
    else:
        saved=json.loads(path.read_text())
        if saved['user_id']!=user.pk:raise RuntimeError('identity mismatch')
        if action=='seed':
            if any(client.exists(key) for key in keys):raise RuntimeError('management keys not clean')
            client.hset(K.channel_metadata(stats),mapping={'init_time':'0','total_bytes':'1024','channel_id':str(channel.pk),'channel_uuid':str(channel.uuid),'channel_name':'Stale Redis channel name','resolution':'320x180','video_codec':'h264','audio_codec':'aac'})
            client.sadd(K.clients(stats),sid)
            client.hset(K.client_metadata(stats,sid),mapping={'programme_start':'2026-11-09T11:00:00Z','position_anchor_at':'100','playback_base_secs':'12.5','paused':'yes','connected_at':'','user_id':str(user.pk),'username':user.username,'ip_address':'192.0.2.123','user_agent':'management-fixture','programme_vid':vid})
            for key,value in [(K.stream_generation(vid,sid),'2'),(K.stream_generation(vid+'-old',sid),'1'),(K.api_session(sid),'fixture-record'),(K.pool_superseded(sid),'fixture-marker'),(K.stats_grace(stats,sid),'fixture-grace'),('timeshift:management-fixture:unrelated','preserve')]:client.set(key,value,ex=300)
        if action=='deny-ui':CoreSettings.objects.update_or_create(key='network_access',defaults={'name':'Fixture UI deny','value':{**(saved['network_value'] or {}),'UI':'192.0.2.0/24'}})
        if action in ('allow-ui','restore'):
            if saved['network_exists']:CoreSettings.objects.update_or_create(key='network_access',defaults={'value':saved['network_value']})
            else:CoreSettings.objects.filter(key='network_access').delete()
        if action in ('inactive','active'):
            user.is_active=action=='active';user.save(update_fields=['is_active'])
        if action=='restore':
            user.user_level=saved['level'];user.is_active=saved['active'];user.save(update_fields=['user_level','is_active'])
            ProgramData.objects.filter(pk=saved['program_id']).delete()
            client.delete(*keys)
    report={'action':action,'fixture_only':True}
    if action=='observe-active':
        sid=sys.argv[3]
        if not isinstance(sid,str) or len(sid)>100 or not sid.replace('-','').replace('_','').isalnum():raise ValueError('invalid fixture sid')
        from apps.m3u.models import M3UAccountProfile
        profile=M3UAccountProfile.objects.get(m3u_account__name='Rust VOD fixture XC',is_default=True)
        clients=list(client.scan_iter(match=f'timeshift:channel:*:clients:{sid}',count=100))
        grace_only=sum(not client.hexists(key,'programme_start') and client.hexists(key,'stats_grace_deadline') and client.hexists(key,'stats_grace_disconnected_at') for key in clients)
        report['state']={'grace_only_client_count':grace_only,'api_exists':bool(client.exists(K.api_session(sid))),'stats_client_count':len(list(client.scan_iter(match=f'timeshift:channel:*:clients:{sid}',count=100))),'profile_connections':int(client.get('profile_connections:'+str(profile.pk)) or 0),'pool_exists':bool(client.exists(K.pool(sid))),'pool_busy':client.hget(K.pool(sid),'busy')}
    if action=='inspect':
        report['cleanup']={'metadata':bool(client.exists(keys[0])),'client_set':bool(client.exists(keys[1])),'client':bool(client.exists(keys[2])),'generations':int(client.exists(keys[4],keys[5])),'api_session':bool(client.exists(keys[7])),'superseded':bool(client.exists(keys[8])),'grace':bool(client.exists(keys[9])),'unrelated':client.get(keys[10])=='preserve','stop_reasons':[client.get(keys[3]),client.get(keys[6])],'stop_ttl_bounded':all(0<client.ttl(key)<=60 for key in [keys[3],keys[6]])}
    print(json.dumps(report))


if __name__=='__main__':main()
