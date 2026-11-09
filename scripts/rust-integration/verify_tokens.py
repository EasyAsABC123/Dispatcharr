"""Validate Rust-issued tokens using the unchanged Python SimpleJWT backend."""
import json
import os
import sys
sys.path.insert(0, '/app')
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'dispatcharr.settings')
import django
django.setup()
from rest_framework_simplejwt.backends import TokenBackend
from django.conf import settings

tokens = json.load(sys.stdin)
backend = TokenBackend(algorithm='HS256', signing_key=settings.SECRET_KEY)
result = {}
for kind in ('access', 'refresh'):
    claims = backend.decode(tokens[kind], verify=True)
    assert claims['token_type'] == kind
    result[kind] = {key: claims[key] for key in ('token_type', 'user_id')}
print(json.dumps(result))
