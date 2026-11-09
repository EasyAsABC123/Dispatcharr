"""Seed actual catch-up channel/users only in the disposable playback fixture.
Original application credit belongs entirely to upstream Dispatcharr authors.
"""
import json
import os
import sys
import uuid
if os.environ.get('POSTGRES_PASSWORD') != 'playback-fixture-only':
    raise RuntimeError('Restricted to disposable playback fixture')
sys.path.insert(0, '/app')
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'dispatcharr.settings')
import django
django.setup()
from apps.accounts.models import User
from apps.channels.models import Channel, ChannelStream, Stream
from apps.m3u.models import M3UAccount
from core.models import StreamProfile
from rest_framework_simplejwt.tokens import RefreshToken
from apps.epg.models import EPGData, ProgramData
from datetime import datetime, timezone
from apps.timeshift.helpers import get_programme_duration

profile = StreamProfile.get_locked('Proxy')
account = M3UAccount.objects.get(name='Rust VOD fixture XC')
stream = Stream.objects.create(name='Offline catch-up stream', url='http://origin:8080/live/0.ts',
    m3u_account=account, stream_profile=profile, is_custom=True, is_catchup=True,
    catchup_days=7, custom_properties={'stream_id': 'fixture', 'tv_archive': 1, 'tv_archive_duration': 7})
channel = Channel.objects.create(uuid=uuid.UUID('0282858c-3869-471b-a58b-e913c1b6bd33'),
    name='Offline native catch-up', channel_number=9001, stream_profile=profile)
ChannelStream.objects.create(channel=channel, stream=stream, order=0)
channel.refresh_from_db()
assert channel.is_catchup
epg = EPGData.objects.create(name='Offline catch-up guide', tvg_id='offline-catchup')
ProgramData.objects.create(epg=epg, start_time=datetime(2026, 11, 9, 11, tzinfo=timezone.utc),
    end_time=datetime(2026, 11, 9, 11, 45, 30, tzinfo=timezone.utc), title='Offline catch-up programme')
channel.epg_data = epg
channel.save(update_fields=['epg_data'])
assert get_programme_duration(channel, '2026-11-09T11:00:00Z') == 50

unsupported = Channel.objects.create(uuid=uuid.UUID('fe15cb90-3bcf-4ba8-aef2-69216c97cb90'),
    name='Offline no catch-up', channel_number=9002, stream_profile=profile)
users = {}
for role in ['owner', 'other', 'viewer', 'disabled']:
    user = User.objects.create(username='rust-catchup-'+role, user_level=0 if role == 'viewer' else 1,
        is_active=True, stream_limit=0, custom_properties={'catchup_enabled': role != 'disabled'})
    user.set_unusable_password()
    user.save()
    users[role] = {'id': user.pk, 'token': str(RefreshToken.for_user(user).access_token)}
# Private output is read only into the runner pipe, never persisted as evidence.
print(json.dumps({'channel_uuid': str(channel.uuid), 'channel_id': channel.pk,
                  'unsupported_uuid': str(unsupported.uuid), 'users': users}))
