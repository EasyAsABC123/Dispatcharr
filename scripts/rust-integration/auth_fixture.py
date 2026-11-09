"""Pinned original login hash-upgrade contracts; disposable fixture only.

All original application credit remains with Dispatcharr contributors. Secrets
stay in private fixture files; report output contains no password or hash bytes.
"""
import json
import os
import sys
if os.environ.get('POSTGRES_PASSWORD') != 'playback-fixture-only':
    raise RuntimeError('Disposable fixture required')
sys.path.insert(0, '/app')
sys.argv.append('shell')
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'dispatcharr.settings')
import django
django.setup()
from django.contrib.auth.hashers import PBKDF2PasswordHasher, PBKDF2SHA1PasswordHasher, check_password
from django.core.cache import cache
from rest_framework.test import APIRequestFactory
from apps.accounts.models import User
from apps.accounts.api_views import TokenObtainPairView
PASSWORD = 'hash-fixture-only'
factory = APIRequestFactory()
cases = []
for index, (name, algorithm, iterations, salt, active, correct) in enumerate([
    ('legacy-rounds', 'sha256', 10, 'abcdefghijklmnopqrstuv', True, True),
    ('legacy-sha1', 'sha1', 10, 'abcdefghijklmnopqrstuv', True, True),
    ('weak-salt', 'sha256', 1200000, 'short', True, True),
    ('preferred', 'sha256', 1200000, 'abcdefghijklmnopqrstuv', True, True),
    ('inactive-upgrades-before-rejection', 'sha256', 10, 'abcdefghijklmnopqrstuv', False, True),
    ('wrong-password-preserves-hash', 'sha256', 10, 'abcdefghijklmnopqrstuv', True, False),
]):
    hasher = PBKDF2SHA1PasswordHasher() if algorithm == 'sha1' else PBKDF2PasswordHasher()
    original_hash = hasher.encode(PASSWORD, salt, iterations)
    username = 'rust-probe-auth-hash-' + str(index)
    user, _ = User.objects.get_or_create(username=username)
    User.objects.filter(pk=user.pk).update(password=original_hash, is_active=active, user_level=1, custom_properties={})
    ip = '198.51.100.' + str(100 + index)
    cache.delete('throttle_login_' + ip)
    request = factory.post('/api/accounts/token/', {'username': username, 'password': PASSWORD if correct else 'wrong-fixture-only'}, format='json', REMOTE_ADDR=ip)
    result = TokenObtainPairView.as_view()(request)
    user.refresh_from_db()
    fields = user.password.split('$')
    expected = {'status': result.status_code, 'changed': user.password != original_hash,
                'password_valid': check_password(PASSWORD, user.password),
                'algorithm': fields[0], 'iterations': int(fields[1]), 'salt_characters': len(fields[2])}
    # Restore exactly the starting credential for the independent Rust attempt.
    User.objects.filter(pk=user.pk).update(password=original_hash, last_login=None)
    cases.append({'name': name, 'user_id': user.pk, 'username': username, 'ip': ip,
                  'correct': correct, 'original_hash': original_hash, 'expected': expected})
print(json.dumps({'hash_cases': cases, 'django_version': django.get_version()}))
