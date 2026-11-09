"""Isolated Source ORM policy and credentials, shared unchanged by both backends.
Application credit belongs to original Dispatcharr authors and contributors.
Private token replies must stay in the parent runner's memory.
"""
import json
from datetime import timedelta
import os
import sys
if os.environ.get('POSTGRES_PASSWORD') != 'playback-fixture-only':
    raise RuntimeError('Disposable fixture only')
sys.path.insert(0, '/app')
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'dispatcharr.settings')
import django
django.setup()
from apps.accounts.models import User
from apps.m3u.models import M3UAccount
from apps.channels.models import Channel
from rest_framework_simplejwt.tokens import AccessToken
def fixture_token(user):
    # This helper is restricted to the disposable fixture above. Keep production
    # JWT settings intact; both backends receive these same synthetic tokens.
    token = AccessToken.for_user(user)
    token.set_exp(lifetime=timedelta(hours=2))
    return str(token)
account = M3UAccount.objects.get(name='Rust VOD fixture XC')
account.max_streams = 128
account.save(update_fields=['max_streams'])
provider = account.profiles.get(is_default=True)
provider.max_streams = 128
provider.save(update_fields=['max_streams'])
users = []
for index in range(10):
    user, _ = User.objects.update_or_create(username=f'rust-m4-sustained-{index}',
        defaults={'password': '!', 'is_active': True, 'user_level': 1,
                  'stream_limit': 0, 'custom_properties': {'catchup_enabled': True}})
    users.append({'id': user.pk, 'token': fixture_token(user)})
observer, _ = User.objects.update_or_create(username='rust-m4-sustained-observer',
    defaults={'password': '!', 'is_active': True, 'user_level': 10, 'stream_limit': 0})
channel = Channel.objects.get(uuid='0282858c-3869-471b-a58b-e913c1b6bd33')
print(json.dumps({'users': users, 'observer_token': fixture_token(observer),
                  'fixture_access_token_lifetime_seconds': 7200,
                  'channel_uuid': str(channel.uuid), 'provider_profile_id': provider.pk,
                  'movie_uuid': 'a4d78cd1-bf9f-4a63-a746-f40566825cf9'}))
