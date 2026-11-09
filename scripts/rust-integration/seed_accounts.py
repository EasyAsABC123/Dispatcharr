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
print(json.dumps(users))
