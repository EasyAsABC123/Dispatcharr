"""Owned live benchmark identity only; Original authors retain credit."""
import json,os,sys
assert os.environ.get('LIVE_DISPOSABLE_FIXTURE')=='playback-fixture-only'
sys.path.insert(0,'/app');sys.argv.append('shell');os.environ.setdefault('DJANGO_SETTINGS_MODULE','dispatcharr.settings')
import django
django.setup()
from apps.accounts.models import User
from rest_framework_simplejwt.tokens import AccessToken
from datetime import timedelta
user,created=User.objects.get_or_create(username='rust-live-owned-admin',defaults={'is_superuser':True,'is_staff':True,'is_active':True,'user_level':10,'password':'!'})
assert created and user.is_superuser
access=AccessToken.for_user(user);access.set_exp(lifetime=timedelta(hours=2))
from apps.m3u.models import M3UAccount
profile=M3UAccount.get_custom_account().profiles.get(is_default=True);profile.max_streams=1;profile.save(update_fields=['max_streams'])
print(json.dumps({'profile_id':profile.pk,'max_streams':profile.max_streams,'user_id':user.pk,'token':str(access),'fixture_lifetime_seconds':7200}))
