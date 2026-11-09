"""Disposable original M3U export controls; all Dispatcharr authors retain credit."""
import json
import os
from pathlib import Path
import sys
import uuid
if os.environ.get('POSTGRES_PASSWORD')!='playback-fixture-only':raise RuntimeError('Restricted to disposable playback fixture')
sys.path.insert(0,'/app');os.environ.setdefault('DJANGO_SETTINGS_MODULE','dispatcharr.settings')
import django
django.setup()
from apps.accounts.models import User
from apps.channels.models import Channel,ChannelOverride,ChannelProfile,ChannelProfileMembership,ChannelGroup,Logo,Stream,ChannelStream
from apps.m3u.models import M3UAccount
from core.models import CoreSettings,SystemEvent


def main():
    action,label=sys.argv[1:3]
    if action not in ('begin','deny-m3u','deny-xc','allow','catchup-off','catchup-on','admin-allow-none','admin-unrestricted','mutate-name','restore-name','events','cache-roundtrip','large-catalog','large-cache-read','license','restore') or label not in ('source','candidate'):raise ValueError('invalid export fixture control')
    path=Path('/evidence')/('export-control-'+label+'.json')
    profile_name='rust-export-profile'
    if action=='begin':
        if path.exists():raise RuntimeError('export control exists')
        if label=='source':
            profile=ChannelProfile.objects.create(name=profile_name)
            ChannelProfileMembership.objects.filter(channel_profile=profile).delete()
            groups=[ChannelGroup.objects.create(name=name) for name in ['Export group A','Export group B']]
            logos=[Logo.objects.create(name=name,url=url) for name,url in [('Export HTTP','https://logo.fixture.invalid/image.png'),('Export local','/media/export-fixture.png')]]
            channel_ids=[]
            specs=[('Alpha & café',301,False,False,0,False),('Radio "quoted"',302.5,False,False,0,True),('Raw override',399,False,False,0,True),('Hidden',303,True,False,0,False),('Adult',304,False,True,0,False),('Elevated',305,False,False,11,False),('Unnumbered',None,False,False,0,False),('Disabled member',306,False,False,0,False)]
            account=M3UAccount.objects.get(name='Rust VOD fixture XC')
            for index,(name,number,hidden,adult,level,radio) in enumerate(specs):
                channel=Channel.objects.create(uuid=uuid.uuid5(uuid.NAMESPACE_URL,'offline-export-'+str(index)),name=name,channel_number=number,channel_group=groups[0],logo=logos[index%2],tvg_id='fixture.id.'+str(index),tvc_guide_stationid='guide'+str(index),hidden_from_output=hidden,is_adult=adult,user_level=level,is_radio=radio)
                ChannelProfileMembership.objects.update_or_create(channel_profile=profile,channel=channel,defaults={'enabled':index not in (6,7)})
                stream=Stream.objects.create(name='Export stream '+str(index),url='/'.join([account.server_url.removesuffix('/player_api.php'),'live',account.username,account.password,str(index)+'.ts']),m3u_account=account,is_custom=True,is_catchup=index==0,catchup_days=7 if index==0 else 0)
                ChannelStream.objects.create(channel=channel,stream=stream,order=0)
                if index==2:ChannelOverride.objects.create(channel=channel,name='Override Ω',channel_number=300.5,channel_group=groups[1],logo=logos[0],tvg_id='override.id',tvc_guide_stationid='override-guide',is_radio=False)
                channel_ids.append(channel.pk)
            for alias_name in ['Kids & News','Plus+News','Café 東京','Percent%26News']:
                alias=ChannelProfile.objects.create(name=alias_name)
                ChannelProfileMembership.objects.filter(channel_profile=alias).delete()
                ChannelProfileMembership.objects.bulk_create([ChannelProfileMembership(channel_profile=alias,channel_id=membership.channel_id,enabled=membership.enabled) for membership in ChannelProfileMembership.objects.filter(channel_profile=profile)])
            for kind,level,active,props in [('standard',1,True,{'hide_adult_content':True}),('admin',10,True,{}),('inactive',1,False,{'hide_adult_content':True})]:
                user=User.objects.create(username='rust-export-'+kind,user_level=level,is_active=active,custom_properties={**props,'xc_password':'export-fixture-only'})
                user.set_unusable_password();user.save(update_fields=['password'])
                user.channel_profiles.add(profile)
        profile=ChannelProfile.objects.get(name=profile_name)
        channel=Channel.objects.get(uuid=uuid.uuid5(uuid.NAMESPACE_URL,'offline-export-0'))
        def setting(key):
            row=CoreSettings.objects.filter(key=key).first();return {'exists':row is not None,'value':row.value if row else None}
        path.write_text(json.dumps({'network':setting('network_access'),'system':setting('system_settings'),'channel_id':channel.pk,'channel_name':channel.name,'admin_properties':User.objects.get(username='rust-export-admin').custom_properties,'event_baseline':{kind:SystemEvent.objects.filter(event_type=kind).count() for kind in ['m3u_download','m3u_blocked']}}))
    else:
        saved=json.loads(path.read_text())
        if action in ('deny-m3u','deny-xc'):
            key='M3U_EPG' if action=='deny-m3u' else 'XC_API'
            CoreSettings.objects.update_or_create(key='network_access',defaults={'name':'Export fixture networks','value':{**(saved['network']['value'] or {}),key:'192.0.2.0/24'}})
        if action in ('allow','restore'):
            if saved['network']['exists']:CoreSettings.objects.update_or_create(key='network_access',defaults={'value':saved['network']['value']})
            else:CoreSettings.objects.filter(key='network_access').delete()
        if action in ('catchup-off','catchup-on','restore'):
            if action=='restore':
                if saved['system']['exists']:CoreSettings.objects.update_or_create(key='system_settings',defaults={'value':saved['system']['value']})
                else:CoreSettings.objects.filter(key='system_settings').delete()
            else:CoreSettings.objects.update_or_create(key='system_settings',defaults={'name':'Export fixture system','value':{**(saved['system']['value'] or {}),'catchup_enabled':action=='catchup-on'}})
        if action in ('admin-allow-none','admin-unrestricted','restore'):
            user=User.objects.get(username='rust-export-admin');props=dict(saved['admin_properties'])
            if action=='admin-allow-none':props['allowed_m3u_profile_ids']=[]
            user.custom_properties=props;user.save(update_fields=['custom_properties'])
        if action in ('mutate-name','restore-name','restore'):
            Channel.objects.filter(pk=saved['channel_id']).update(name='Changed export name' if action=='mutate-name' else saved['channel_name'])
    result={'action':action,'fixture_only':True,'profile_name':profile_name,'users':{kind:'rust-export-'+kind for kind in ['standard','admin','inactive']}}
    if action=='large-catalog':
        large,created=ChannelProfile.objects.get_or_create(name='rust-export-large')
        if created:
            ChannelProfileMembership.objects.filter(channel_profile=large).delete()
            channels=[Channel(uuid=uuid.uuid5(uuid.NAMESPACE_URL,'offline-large-export-'+str(i)),name=f'Export catalog {i:05d} '+('Long deterministic programme channel name Ω '*4),channel_number=100000+i,user_level=0) for i in range(6000)]
            Channel.objects.bulk_create(channels,batch_size=500)
            ChannelProfileMembership.objects.bulk_create([ChannelProfileMembership(channel_profile=large,channel=channel,enabled=True) for channel in channels],batch_size=500)
        result['large_catalog']={'profile_name':large.name,'channels':ChannelProfileMembership.objects.filter(channel_profile=large,enabled=True).count(),'fixture_id':'m3u-large-6000-unicode-long-names-v1'}
    if action=='large-cache-read':
        from django.core.cache import cache
        from core.utils import RedisClient
        import hashlib,pickletools
        key='m3u_content:rust-export-large:anonymous:_fixture_case=large-catalog:origin=http://export.fixture.invalid:8088'
        value=cache.get(key)
        raw=RedisClient.get_buffer().get(':1:'+key)
        if isinstance(value,str):cache.set(key,value,timeout=2) # Feed original Python's encoding back to the native warm reader.
        result['cache']={'rewritten_by_original_django':True,'python_type':type(value).__name__,'bytes':len(value.encode()) if isinstance(value,str) else None,'sha256':hashlib.sha256(value.encode()).hexdigest() if isinstance(value,str) else None,'pickle_bytes':len(raw) if raw else None,'pickle_opcodes':[op.name for op,arg,pos in pickletools.genops(raw)] if raw else []}
    if action=='cache-roundtrip':
        from django.core.cache import cache
        import hashlib
        value=cache.get('m3u_content:all:anonymous:_fixture_case=anonymous-not-xc-network:origin=http://export.fixture.invalid:8088')
        result['cache']={'python_type':type(value).__name__,'sha256':hashlib.sha256(value.encode()).hexdigest() if isinstance(value,str) else None}
    if action=='license':
        from importlib.metadata import distribution
        import hashlib
        package=distribution('Django')
        candidates=[item for item in package.files if item.name=='LICENSE' and any(part.endswith('.dist-info') for part in item.parts)]
        if len(candidates)!=1:raise RuntimeError('Django license selection mismatch')
        content=package.locate_file(candidates[0]).read_text()
        result.update(django_version=package.version,django_license=content,django_license_sha256=hashlib.sha256(content.encode()).hexdigest())
    if action=='events':result['counts']={kind:SystemEvent.objects.filter(event_type=kind).count()-saved['event_baseline'][kind] for kind in ['m3u_download','m3u_blocked']}
    print(json.dumps(result))


if __name__=='__main__':main()
