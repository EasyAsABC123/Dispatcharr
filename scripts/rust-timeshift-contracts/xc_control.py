"""Disposable original ORM XC route controls; upstream retains all credit."""
import json
import os
from pathlib import Path
import sys
import time
if os.environ.get('POSTGRES_PASSWORD')!='playback-fixture-only':raise RuntimeError('Disposable fixture required')
sys.path.insert(0,'/app');sys.argv.append('shell');os.environ.setdefault('DJANGO_SETTINGS_MODULE','dispatcharr.settings')
import django
django.setup()
from apps.accounts.models import User
from apps.channels.models import Channel
from core.models import CoreSettings,StreamProfile
from core.utils import RedisClient


def main():
    action,label=sys.argv[1:3]
    if action not in ('begin','proxy','redirect','redirect-empty','deny-streams','deny-xc','deny-user','allow','wait-idle','restore') or label not in ('source','candidate'):raise ValueError('invalid XC fixture control')
    path=Path('/evidence')/('xc-route-control-'+label+'.json')
    channel=Channel.objects.get(uuid='0282858c-3869-471b-a58b-e913c1b6bd33')
    roles=['owner','other','viewer','disabled']
    if action=='begin':
        if path.exists():raise RuntimeError('XC route fixture already exists')
        users=[User.objects.get(username='rust-catchup-'+role) for role in roles]
        network=CoreSettings.objects.filter(key='network_access').first()
        inactive=User.objects.create(username='rust-catchup-xc-inactive',is_active=False,user_level=1,custom_properties={'catchup_enabled':True,'xc_password':'xc-fixture-only'})
        inactive.set_unusable_password();inactive.save(update_fields=['password'])
        saved={'channel_id':channel.pk,'profile_id':channel.stream_profile_id,'level':channel.user_level,'users':{str(user.pk):user.custom_properties for user in users},'inactive_id':inactive.pk,'network_exists':network is not None,'network_value':network.value if network else None}
        path.write_text(json.dumps(saved,indent=2)+'\n')
        for user in users:user.custom_properties={**(user.custom_properties or {}),'xc_password':'xc-fixture-only'};user.save(update_fields=['custom_properties'])
        channel.user_level=1;channel.save(update_fields=['user_level'])
        print(json.dumps({'channel_id':channel.pk,'usernames':{**{role:user.username for role,user in zip(roles,users)},'inactive':inactive.username}}));return
    saved=json.loads(path.read_text())
    if channel.pk!=saved['channel_id']:raise RuntimeError('XC fixture identity mismatch')
    owner=User.objects.get(username='rust-catchup-owner')
    if action in ('proxy','redirect','redirect-empty','deny-user','allow'):
        if action in ('proxy','redirect','redirect-empty'):
            channel.stream_profile=StreamProfile.get_locked('Proxy' if action=='proxy' else 'Redirect');channel.save(update_fields=['stream_profile'])
        properties={**(saved['users'][str(owner.pk)] or {}),'xc_password':'xc-fixture-only'}
        properties.pop('allowed_m3u_profile_ids',None)
        if action=='redirect-empty':properties['allowed_m3u_profile_ids']=[]
        if action=='deny-user':properties['allowed_networks']={'XC_API':'192.0.2.0/24'}
        owner.custom_properties=properties;owner.save(update_fields=['custom_properties'])
    if action in ('deny-streams','deny-xc'):
        key='STREAMS' if action=='deny-streams' else 'XC_API'
        CoreSettings.objects.update_or_create(key='network_access',defaults={'name':'Fixture XC route networks','value':{**(saved['network_value'] or {}),key:'192.0.2.0/24'}})
    if action in ('allow','restore'):
        if saved['network_exists']:CoreSettings.objects.update_or_create(key='network_access',defaults={'value':saved['network_value']})
        else:CoreSettings.objects.filter(key='network_access').delete()
    if action=='wait-idle':
        profile=json.loads(Path('/evidence/vod-seed.json').read_text())['provider_profile_id'];redis=RedisClient.get_client();deadline=time.monotonic()+5
        while int(redis.get('profile_connections:'+str(profile)) or 0)!=0:
            if time.monotonic()>deadline:raise RuntimeError('XC provider quota did not return idle')
            time.sleep(.05)
    if action=='restore':
        channel.stream_profile_id=saved['profile_id'];channel.user_level=saved['level'];channel.save(update_fields=['stream_profile','user_level'])
        for key,properties in saved['users'].items():
            user=User.objects.get(pk=int(key));user.custom_properties=properties;user.save(update_fields=['custom_properties'])
        User.objects.filter(pk=saved['inactive_id'],username='rust-catchup-xc-inactive').delete()
    print(json.dumps({'action':action,'fixture_only':True}))


if __name__=='__main__':main()
