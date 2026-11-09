"""Create ephemeral compatibility users; print credentials only to the runner pipe."""
import json
import os
import secrets
import sys

if os.environ.get('POSTGRES_PASSWORD') != 'playback-fixture-only':
    raise RuntimeError('This seed is restricted to the disposable playback fixture')
sys.path.insert(0, '/app')
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'dispatcharr.settings')
import django
django.setup()
from apps.accounts.models import User
from apps.channels.models import ChannelGroup, ChannelGroupM3UAccount
from apps.m3u.models import M3UAccount
from rest_framework_simplejwt.tokens import RefreshToken

users = {}
for name, level, active in [('admin', 10, True), ('standard', 1, True),
                            ('viewer', 0, True), ('inactive', 10, False)]:
    password = secrets.token_urlsafe(24)
    user, _ = User.objects.update_or_create(username='rust-probe-' + name,
        defaults={'user_level': level, 'is_active': active, 'stream_limit': 0,
                  'custom_properties': None if name == 'viewer' else {},
                  'is_staff': level == 10, 'is_superuser': level == 10})
    user.set_password(password)
    user.api_key = secrets.token_urlsafe(32)
    user.save()
    refresh = RefreshToken.for_user(user)
    users[name] = {'id': user.pk, 'username': user.username, 'password': password,
                   'api_key': user.api_key, 'access': str(refresh.access_token),
                   'refresh': str(refresh)}

# Through-model fixtures cannot be produced by public group create, which always
# claims a group as manual. These rows belong only to this guarded disposable DB.
provider = M3UAccount.objects.filter(name='rust-probe-group-provider').first()
if provider is None:
    # Bulk creation intentionally bypasses refresh/scheduler signals. This is
    # a relationship-only provider fixture, never an external import target.
    M3UAccount.objects.bulk_create([M3UAccount(name='rust-probe-group-provider',
        account_type='STD', is_active=True, locked=False, server_url=None)])
    provider = M3UAccount.objects.get(name='rust-probe-group-provider')
group_fixtures = {}
for key, claimed in [('provider_group', False), ('claimed_provider_group', True)]:
    group, _ = ChannelGroup.objects.get_or_create(name='rust-probe-' + key.replace('_', '-'))
    ChannelGroupM3UAccount.objects.filter(channel_group=group).delete()
    ChannelGroupM3UAccount.objects.create(channel_group=group, m3u_account=provider)
    if claimed:
        ChannelGroup.objects.mark_custom(group)
    group_fixtures[key] = {'id': group.pk, 'name': group.name}
inactive_provider = M3UAccount.objects.filter(name='rust-probe-inactive-provider').first()
if inactive_provider is None:
    M3UAccount.objects.bulk_create([M3UAccount(name='rust-probe-inactive-provider',
        account_type='STD', is_active=False, locked=False, server_url=None)])
    inactive_provider = M3UAccount.objects.get(name='rust-probe-inactive-provider')
users['_fixtures'] = {**group_fixtures, 'provider_id': provider.pk,
                      'inactive_provider_id': inactive_provider.pk}
print(json.dumps(users))
