"""Original user-limit/hop policy with real Redis and isolated Django fixtures.
All original application credit belongs to Dispatcharr authors/contributors.
This captures policy/accounting, not media, authentication or WebSocket parity.
"""
import hashlib
import json
import os
from pathlib import Path
import sys
import uuid
from unittest.mock import patch
if os.environ.get('POSTGRES_PASSWORD') != 'playback-fixture-only':
    raise RuntimeError('Restricted to disposable playback fixture')
sys.path.insert(0,'/app')
os.environ.setdefault('DJANGO_SETTINGS_MODULE','dispatcharr.settings')
import django
django.setup()
from apps.accounts.models import User
from apps.channels.models import Channel
from apps.proxy.utils import get_user_active_connections,check_user_stream_limits
from apps.timeshift.views import _terminate_previous_timeshift_sessions
from core.models import CoreSettings
from core.utils import RedisClient


def main():
    r=RedisClient.get_client()
    user=User.objects.get(username='rust-catchup-owner')
    channel=Channel.objects.get(uuid='0282858c-3869-471b-a58b-e913c1b6bd33')
    if get_user_active_connections(user.id):
        raise RuntimeError('Policy oracle requires fixture owner without active streams')
    # Instance-only limit input; no account/settings database writes.
    user.stream_limit=1
    events=[]
    policies={'deny':dict(terminate_on_limit_exceeded=False,prioritize_single_client_channels=True,ignore_same_channel_connections=False,terminate_oldest=True),
              'preempt':dict(terminate_on_limit_exceeded=True,prioritize_single_client_channels=True,ignore_same_channel_connections=False,terminate_oldest=True)}
    for name,kind,policy_name in [('initial','none','deny'),('hop-before-live-refusal','hop-live','deny'),('different-channel-refusal','timeshift-other','deny'),('vod-preemption','vod','preempt'),('timeshift-all-generations','timeshift-other','preempt'),('same-session-exemption','same-session','deny')]:
        owned=set();seed=[];old=uuid.uuid4().hex;incoming=old if kind=='same-session' else uuid.uuid4().hex;media=f'{channel.pk}_2026-11-09T11-00-00Z';stops={};observed={}
        def remember(key):owned.add(key);return key
        def hash_(key,fields,label):
            remember(key);r.hset(key,mapping=fields);seed.append({'key':key,'fields':{key:str(value) for key,value in fields.items()}});observed[label]=key
        def timeshift(other=False):
            pk=channel.pk+1000000 if other else channel.pk
            stats=f'{pk}_{old}';programme=f'{pk}_programme_provider';key=f'timeshift:channel:{stats}:clients:{old}'
            hash_(key,{'user_id':user.pk,'connected_at':'1','programme_vid':programme,'rust_owner_token':incoming},'old_client')
            members=remember(f'timeshift:channel:{stats}:clients');r.sadd(members,old);seed.append({'key':members,'members':[old]});observed['old_set']=members
            hash_(f'timeshift:channel:{stats}:metadata',{'channel_id':pk},'old_metadata')
            stop=remember(f'timeshift:channel:{programme}:client:{old}:stop');stops['old_timeshift']=stop
            if name=='timeshift-all-generations':
                for index in [1,2]:
                    generation_media=f'{pk}_generation_{index}';key=remember(f'timeshift:stream_gen:{generation_media}:{old}');r.setex(key,60,1);seed.append({'key':key,'string':'1'})
                    stops['generation_'+str(index)]=remember(f'timeshift:channel:{generation_media}:client:{old}:stop')
        try:
            if kind in ('hop-live','timeshift-other','same-session'):timeshift(other=kind=='timeshift-other')
            if kind=='hop-live':
                live=uuid.uuid4().hex;live_client=uuid.uuid4().hex
                hash_(f'live:channel:{live}:clients:{live_client}',{'user_id':user.pk,'connected_at':'1'},'live_client')
                stops['live']=remember(f'live:channel:{live}:client:{live_client}:stop')
            if kind=='vod':
                hash_(f'vod_persistent_connection:{old}',{'user_id':user.pk,'created_at':'1','content_uuid':'fixture-movie'},'vod_client')
                stops['vod']=remember(f'vod_proxy:client:{old}:stop')
            before=len(get_user_active_connections(user.id))
            with patch.object(CoreSettings,'get_user_limits_settings',return_value=policies[policy_name]):
                _terminate_previous_timeshift_sessions(r,user,channel.pk,media,incoming)
                allowed=check_user_stream_limits(user,incoming,media_id=media)
            state={'exists':{label:bool(r.exists(key)) for label,key in observed.items()},'stops':{label:r.get(key) for label,key in stops.items()},'stop_ttls':{label:r.ttl(key) for label,key in stops.items()},'active_before':before,'active_after':len(get_user_active_connections(user.id))}
            events.append({'case':name,'scope':'original helper policy/accounting with synthetic owned Redis records','input':{'user_id':user.pk,'stream_limit':1,'channel_id':channel.pk,'session_id':incoming,'programme_media_id':media,'owner_token':incoming,'policy':policies[policy_name],'seed':seed,'observe':observed,'stops':stops,'mode':'check' if kind=='same-session' else 'create'},'allowed':allowed,'state':state})
        finally:
            if owned:r.delete(*owned)
    report={'format_version':1,'scope':'original Timeshift hop-before-cap and shared source accounting helper oracle','capture_complete':len(events)==6,'application_parity_verified':False,'performance_verified':False,'media_authentication_websocket_verified':False,'policy_source':'explicit policy input patched only around original CoreSettings getter; actual original functions and Redis execute','source_sha256':{name:hashlib.sha256((Path('/app')/name).read_bytes()).hexdigest() for name in ['apps/proxy/utils.py','apps/timeshift/views.py']},'events':events}
    print(json.dumps(report))
if __name__=='__main__':main()
