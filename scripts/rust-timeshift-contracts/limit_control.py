"""Disposable actual-ORM positive-cap fixture; original Dispatcharr credit retained."""
import json
import os
from pathlib import Path
import sys
import time
import uuid
if os.environ.get('POSTGRES_PASSWORD')!='playback-fixture-only':raise RuntimeError('Restricted to disposable playback fixture')
sys.path.insert(0,'/app')
os.environ.setdefault('DJANGO_SETTINGS_MODULE','dispatcharr.settings')
import django
django.setup()
from apps.accounts.models import User
from core.models import CoreSettings,StreamProfile
from apps.channels.models import Channel
from core.utils import RedisClient

def main():
    action,label=sys.argv[1:3]
    redirect_context=len(sys.argv)>3 and sys.argv[3]=='redirect'
    if action not in ('begin','clear-live','restore') or label not in ('source','candidate'):raise ValueError('invalid fixture control')
    path=Path('/evidence')/('limit-control-'+label+'.json')
    user=User.objects.get(username='rust-catchup-owner');r=RedisClient.get_client()
    if action=='begin':
        if path.exists():raise RuntimeError('limit control already exists')
        setting=CoreSettings.objects.filter(key='user_limit_settings').first()
        live_key=f'live:channel:{uuid.uuid4()}:clients:{uuid.uuid4()}'
        saved={'user_id':user.pk,'stream_limit':user.stream_limit,'setting_existed':setting is not None,'setting_value':setting.value if setting else None,'live_key':live_key}
        if redirect_context:
            channel=Channel.objects.get(uuid='0282858c-3869-471b-a58b-e913c1b6bd33')
            saved.update(channel_id=channel.pk,channel_profile_id=channel.stream_profile_id,user_properties=user.custom_properties)
            channel.stream_profile=StreamProfile.get_locked('Redirect');channel.save(update_fields=['stream_profile'])
            user.custom_properties={**(user.custom_properties or {}),'allowed_m3u_profile_ids':[]}
        path.write_text(json.dumps(saved,indent=2)+'\n')
        user.stream_limit=1;user.save(update_fields=['stream_limit','custom_properties'] if redirect_context else ['stream_limit'])
        CoreSettings.objects.update_or_create(key='user_limit_settings',defaults={'name':setting.name if setting else 'Fixture native limit','value':{'terminate_on_limit_exceeded':False,'prioritize_single_client_channels':True,'ignore_same_channel_connections':False,'terminate_oldest':True}})
        r.hset(live_key,mapping={'user_id':str(user.pk),'connected_at':str(time.time()),'username':user.username})
        r.expire(live_key,180)
        print(json.dumps({'user_limit':1,'terminate_on_limit_exceeded':False,'existing_live_record':'synthetic owned source-accounting hash; no media body','stream_profile_context':'Redirect channel established capability; allowed profiles empty' if redirect_context else 'Proxy channel'}))
        return
    saved=json.loads(path.read_text())
    if saved['user_id']!=user.pk or not saved['live_key'].startswith('live:channel:'):raise RuntimeError('fixture control identity mismatch')
    if r.hget(saved['live_key'],'user_id') not in (None,str(user.pk)):raise RuntimeError('foreign fixture live owner')
    r.delete(saved['live_key'])
    if action=='restore':
        user.stream_limit=saved['stream_limit']
        if 'user_properties' in saved:
            user.custom_properties=saved['user_properties']
            channel=Channel.objects.get(pk=saved['channel_id']);channel.stream_profile_id=saved['channel_profile_id'];channel.save(update_fields=['stream_profile'])
        user.save(update_fields=['stream_limit','custom_properties'] if 'user_properties' in saved else ['stream_limit'])
        if saved['setting_existed']:CoreSettings.objects.update_or_create(key='user_limit_settings',defaults={'value':saved['setting_value']})
        else:CoreSettings.objects.filter(key='user_limit_settings').delete()
    print(json.dumps({'action':action,'owned_live_record_removed':True}))
if __name__=='__main__':main()
