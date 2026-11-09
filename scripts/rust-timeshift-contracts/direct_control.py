"""Actual ORM direct-auth fixture controls; original Dispatcharr credit retained."""
import json
import os
from pathlib import Path
import sys
if os.environ.get('POSTGRES_PASSWORD')!='playback-fixture-only':
    raise RuntimeError('Restricted to disposable playback fixture')
sys.path.insert(0,'/app')
os.environ.setdefault('DJANGO_SETTINGS_MODULE','dispatcharr.settings')
import django
django.setup()
from apps.accounts.models import User
from apps.channels.models import Channel
from core.models import CoreSettings,StreamProfile


def main():
    action,label=sys.argv[1:3]
    if action not in ('begin','proxy','redirect','redirect-empty','deny-streams','allow-streams','restore') or label not in ('source','candidate'):
        raise ValueError('invalid disposable direct-auth control')
    path=Path('/evidence')/('direct-control-'+label+'.json')
    channel=Channel.objects.get(uuid='0282858c-3869-471b-a58b-e913c1b6bd33')
    user=User.objects.get(username='rust-catchup-owner')
    if action=='begin':
        if path.exists():raise RuntimeError('direct control already exists')
        network=CoreSettings.objects.filter(key='network_access').first()
        path.write_text(json.dumps({'channel_id':channel.pk,'profile_id':channel.stream_profile_id,
            'user_id':user.pk,'user_properties':user.custom_properties,'network_exists':network is not None,
            'network_value':network.value if network else None},indent=2)+'\n')
    else:
        saved=json.loads(path.read_text())
        if saved['channel_id']!=channel.pk or saved['user_id']!=user.pk:raise RuntimeError('fixture identity mismatch')
        if action in ('proxy','redirect','redirect-empty'):
            channel.stream_profile=StreamProfile.get_locked('Proxy' if action=='proxy' else 'Redirect')
            channel.save(update_fields=['stream_profile'])
            properties=dict(saved['user_properties'] or {})
            if action=='redirect-empty':properties['allowed_m3u_profile_ids']=[]
            else:properties.pop('allowed_m3u_profile_ids',None)
            user.custom_properties=properties;user.save(update_fields=['custom_properties'])
        if action=='deny-streams':
            CoreSettings.objects.update_or_create(key='network_access',defaults={'name':'Fixture direct network','value':{**(saved['network_value'] or {}),'STREAMS':'192.0.2.0/24'}})
        if action in ('allow-streams','restore'):
            if saved['network_exists']:CoreSettings.objects.update_or_create(key='network_access',defaults={'value':saved['network_value']})
            else:CoreSettings.objects.filter(key='network_access').delete()
        if action=='restore':
            channel.stream_profile_id=saved['profile_id'];channel.save(update_fields=['stream_profile'])
            user.custom_properties=saved['user_properties'];user.save(update_fields=['custom_properties'])
    print(json.dumps({'action':action,'fixture_only':True}))


if __name__=='__main__':main()
