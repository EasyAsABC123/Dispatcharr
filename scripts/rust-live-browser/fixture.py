"""Synthetic user for an owned playback fixture; secrets only on the pipe."""
import os, sys, json, secrets
if os.environ.get('POSTGRES_PASSWORD') != 'playback-fixture-only':
    raise RuntimeError('Disposable playback fixture required')
sys.path.insert(0, '/app')
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'dispatcharr.settings')
import django
django.setup()
from apps.accounts.models import User
from core.models import CoreSettings
password = secrets.token_urlsafe(32)
user = User.objects.create(username='rust-live-browser-admin', user_level=10,
                           is_active=True, is_staff=True, is_superuser=True,
                           stream_limit=0, custom_properties={})
user.set_password(password)
user.save()
settings, _ = CoreSettings.objects.get_or_create(key='system_settings', defaults={'name':'Fixture system', 'value':{}})
settings.value = {**(settings.value or {}), 'enable_ip_lookup':False}
settings.save()
print(json.dumps({'username':user.username, 'password':password, 'user_id':user.pk}))
