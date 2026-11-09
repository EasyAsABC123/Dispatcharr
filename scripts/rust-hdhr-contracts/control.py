"""Owned original-schema HDHR controls; original Dispatcharr credit retained.

Capabilities are returned to the parent in memory and must never be logged.
"""
import json
import os
from pathlib import Path
import sys
import uuid
if os.environ.get('POSTGRES_PASSWORD')!='playback-fixture-only':raise RuntimeError('disposable fixture required')
sys.path.insert(0,'/app');os.environ.setdefault('DJANGO_SETTINGS_MODULE','dispatcharr.settings')
import django
django.setup()
from django.db import connection
from apps.accounts.models import User
from apps.channels.models import Channel,ChannelOverride,ChannelProfile,ChannelProfileMembership,Stream
from apps.m3u.models import M3UAccount,M3UAccountProfile
from apps.hdhr.models import HDHRDevice
from core.models import CoreSettings,OutputProfile
from rest_framework_simplejwt.tokens import RefreshToken
state={}

def clear_devices():
    HDHRDevice.objects.all().delete()
    with connection.cursor() as cursor:cursor.execute("SELECT setval(pg_get_serial_sequence('hdhr_hdhrdevice','id'),1,false)")
def setting(key,value):CoreSettings.objects.update_or_create(key=key,defaults={'name':'HDHR fixture settings','value':value})
def main(request):
    action=request['op']
    if action=='seed':
        if state:return {'seeded':True,'users':state['users'],'active_output':state['active'],'inactive_output':state['inactive'],'profile':state['profile'],'aliases':state['aliases']}
        saved={key:(CoreSettings.objects.filter(key=key).values_list('value',flat=True).first() or {}) for key in ['network_access','stream_settings']}
        rows=[]
        for index,(number,hidden,name) in enumerate([(701,False,'HDHR A & café'),(702.5,False,'HDHR B'),(799,False,'Original C'),(703,True,'Hidden HDHR'),(None,False,'No number'),(704,False,'Disabled member')]):
            row=Channel.objects.create(uuid=uuid.uuid5(uuid.NAMESPACE_URL,'hdhr-fixture-'+str(index)),name=name,channel_number=number,hidden_from_output=hidden,user_level=11,is_adult=True)
            if index==2:ChannelOverride.objects.create(channel=row,name='Override 東京',channel_number=700.5)
            rows.append(row)
        profile=ChannelProfile.objects.create(name='HDHR fixture')
        ChannelProfileMembership.objects.filter(channel_profile=profile).delete()
        for index,row in enumerate(rows):ChannelProfileMembership.objects.create(channel_profile=profile,channel=row,enabled=index!=5)
        aliases=[]
        for name in ['Kids & News','Café 東京','Plus+News','devices']:
            alias=ChannelProfile.objects.create(name=name);ChannelProfileMembership.objects.filter(channel_profile=alias).delete()
            for index,row in enumerate(rows):ChannelProfileMembership.objects.create(channel_profile=alias,channel=row,enabled=index!=5)
            aliases.append(name)
        active=OutputProfile.objects.create(name='HDHR active fixture',command='ffmpeg',parameters='-i pipe:0 -c copy -f mpegts pipe:1')
        inactive=OutputProfile.objects.create(name='HDHR inactive fixture',command='ffmpeg',parameters='-i pipe:0 -c copy -f mpegts pipe:1',is_active=False)
        users={}
        for kind,level,enabled in [('viewer',0,True),('standard',1,True),('admin',10,True),('inactive',10,False)]:
            row=User.objects.create(username='hdhr-'+kind,user_level=level,is_active=enabled,api_key=uuid.uuid4().hex,custom_properties={'allowed_networks':{'M3U_EPG':'192.0.2.0/24'}})
            row.set_unusable_password();row.save();users[kind]={'access':str(RefreshToken.for_user(row).access_token),'api_key':row.api_key}
        state.update(saved=saved,users=users,active=active.pk,inactive=inactive.pk,profile=profile.name,aliases=aliases,
            profiles=list(M3UAccountProfile.objects.values('pk','max_streams','is_active')))
        clear_devices();return {'seeded':True,'users':users,'active_output':active.pk,'inactive_output':inactive.pk,'profile':profile.name,'aliases':aliases}
    if action=='reset':
        clear_devices()
        if state.get('aux_accounts'):
            M3UAccount.objects.filter(pk__in=state.pop('aux_accounts')).delete()
        User.objects.filter(username='hdhr-standard').update(custom_properties={'allowed_networks':{'M3U_EPG':'192.0.2.0/24'}})
        for key,value in state['saved'].items():setting(key,value)
        for row in state['profiles']:M3UAccountProfile.objects.filter(pk=row['pk']).update(max_streams=row['max_streams'],is_active=row['is_active'])
        return {'reset':True}
    if action=='network':
        policy=dict(state['saved']['network_access']);policy.update(request['values']);setting('network_access',policy);return {'network_changed':True}
    if action=='default_output':
        policy=dict(state['saved']['stream_settings']);policy['hdhr_output_profile_id']=request['value'];setting('stream_settings',policy);return {'default_output_changed':True}
    if action=='tuner_fixture':
        if not state.get('aux_accounts'):
            accounts=M3UAccount.objects.bulk_create([M3UAccount(name='HDHR tuner active',is_active=True),M3UAccount(name='HDHR tuner inactive',is_active=False)])
            state['aux_accounts']=[row.pk for row in accounts]
            M3UAccountProfile.objects.bulk_create([M3UAccountProfile(m3u_account=accounts[0],name='HDHR two',max_streams=2,search_pattern='(.*)',replace_pattern='$1'),M3UAccountProfile(m3u_account=accounts[0],name='HDHR four',max_streams=4,search_pattern='(.*)',replace_pattern='$1'),M3UAccountProfile(m3u_account=accounts[0],name='HDHR inactive',max_streams=0,is_active=False,search_pattern='(.*)',replace_pattern='$1'),M3UAccountProfile(m3u_account=accounts[1],name='HDHR disabled account unlimited',max_streams=0,search_pattern='(.*)',replace_pattern='$1')])
            Stream.objects.create(name='HDHR inactive custom',url='http://origin:8080/never-requested.ts',m3u_account=accounts[1],is_custom=True)
        M3UAccountProfile.objects.filter(m3u_account_id=state['aux_accounts'][0],name='HDHR two').update(max_streams=0 if request['unlimited'] else 2)
        return {'tuner_fixture_changed':True}
    if action=='stream_map':setting('stream_settings',request['value']);return {'stream_map_changed':True}
    if action=='tuner_limited':
        M3UAccountProfile.objects.exclude(pk=1).filter(is_active=True,m3u_account__is_active=True).update(max_streams=3)
        return {'limited':True}
    if action=='user_ui':
        user=User.objects.get(username='hdhr-standard');user.custom_properties={'allowed_networks':{'UI':request['value'],'M3U_EPG':'192.0.2.0/24'}};user.save(update_fields=['custom_properties']);return {'user_changed':True}
    if action=='close':return {'closed':True}
    raise ValueError('unknown control')
if __name__=='__main__':
    print(json.dumps({'ready':True}),flush=True)
    while True:
        line=sys.stdin.buffer.readline(65537)
        if not line:break
        if len(line)>65536:raise ValueError('control limit')
        request=json.loads(line);print(json.dumps(main(request)),flush=True)
        if request['op']=='close':break
