"""Seed only disposable benchmark DB; no production/provider settings loaded."""
import json
import os
import sys
import uuid
from pathlib import Path

sys.path.insert(0, '/app')
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'dispatcharr.settings')
import django
django.setup()
from apps.channels.models import Channel, ChannelStream, Stream
from apps.m3u.models import M3UAccount
from core.models import CoreSettings, NETWORK_ACCESS_KEY, StreamProfile

profile = StreamProfile.get_locked('Proxy')
account = M3UAccount.get_custom_account()
assert account.is_active
default = account.profiles.get(is_default=True)
assert default.is_active and default.max_streams == 0
CoreSettings.objects.update_or_create(key=NETWORK_ACCESS_KEY, defaults={
    'name': 'Isolated benchmark networks',
    'value': {'STREAMS': '127.0.0.0/8,10.0.0.0/8,172.16.0.0/12,192.168.0.0/16,::1/128'}})
channel_id = uuid.UUID('11ca0bac-7532-414a-9d5b-9790cb6784a1')
stream = Stream.objects.create(name='Offline benchmark stream', url='http://origin:8080/live.ts',
                               m3u_account=account, stream_profile=profile, is_custom=True)
channel = Channel.objects.create(uuid=channel_id, name='Offline benchmark channel',
                                  channel_number=1, stream_profile=profile)
ChannelStream.objects.create(channel=channel, stream=stream, order=0)
Path('/evidence/seed.json').write_text(json.dumps({
    'channel_uuid': str(channel.uuid), 'channel_pk': channel.pk, 'stream_pk': stream.pk,
    'profile': profile.name, 'profile_locked': profile.locked,
    'provider_account': account.name, 'default_provider_max_streams': default.max_streams,
    'path': f'/proxy/ts/stream/{channel.uuid}?output_format=ts',
    'uwsgi_workers': 1, 'gevent_slots': 400,
    'background_services': 'internal PostgreSQL17 and Redis; no Celery/Daphne/nginx',
}, indent=2) + '\n')
