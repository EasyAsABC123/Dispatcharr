"""Pinned SimpleJWT factory clock witness; no DB, HTTP, or logged signed tokens.

Original Dispatcharr and SimpleJWT authors retain application/framework credit.
Run inside the pinned Original image, network disabled, disposable settings only.
"""
import datetime
import importlib.metadata
import json
from types import SimpleNamespace
from unittest.mock import patch

from django.conf import settings

settings.configure(
    SECRET_KEY='playback-fixture-only', USE_TZ=True,
    INSTALLED_APPS=['django.contrib.auth', 'django.contrib.contenttypes'],
    SIMPLE_JWT={'SIGNING_KEY': 'playback-fixture-only', 'ALGORITHM': 'HS256',
                'ACCESS_TOKEN_LIFETIME': datetime.timedelta(minutes=30),
                'REFRESH_TOKEN_LIFETIME': datetime.timedelta(days=1)},
)
import django
django.setup()
import jwt
from rest_framework_simplejwt.serializers import TokenObtainPairSerializer
import startup_probes

now=datetime.datetime.now(datetime.timezone.utc)
base=now.replace(microsecond=0)-datetime.timedelta(seconds=2)
rows=[]
for label,offset in [('same-second', 0), ('cross-second', 6000)]:
    t1=base.replace(microsecond=994000)
    t2=t1+datetime.timedelta(microseconds=offset)
    with patch('rest_framework_simplejwt.tokens.aware_utcnow',side_effect=[t1,t2]):
        refresh=TokenObtainPairSerializer.get_token(SimpleNamespace(id='7',is_active=True))
        access=refresh.access_token
    pair={'access':str(access),'refresh':str(refresh)}
    claims={key:jwt.decode(value,'playback-fixture-only',algorithms=['HS256']) for key,value in pair.items()}
    a,r=claims['access'],claims['refresh']
    normalized=startup_probes.token_pair(pair,7)
    rows.append({'case':label,'signature_and_current_time_verified':True,
                 'claims':{key:{field:value[field] for field in ['token_type','user_id','iat','exp']} for key,value in claims.items()},
                 'distinct_nonempty_jti':bool(a['jti'] and r['jti'] and a['jti']!=r['jti']),
                 'access_exp_minus_refresh_iat':a['exp']-r['iat'],
                 'refresh_exp_minus_refresh_iat':r['exp']-r['iat'],
                 'access_exp_minus_access_iat':a['exp']-a['iat'],
                 'normalizer_passed':normalized=={'access':'<verified-fixture-access>','refresh':'<verified-fixture-refresh>'}})
assert rows[0]['access_exp_minus_access_iat']==1800
assert rows[1]['access_exp_minus_access_iat']==1799
assert all(row['normalizer_passed'] and row['distinct_nonempty_jti'] and row['access_exp_minus_refresh_iat']==1800 and row['refresh_exp_minus_refresh_iat']==86400 for row in rows)
print(json.dumps({'passed':True,'scope':'Actual pinned SimpleJWT serializer/token factory with controlled creation clock; signature/time verified, no database or TCP execution, not an observed cause attribution for historical TCP failure',
                  'simplejwt_version':importlib.metadata.version('djangorestframework-simplejwt'),'rows':rows},sort_keys=True))
