"""Pinned original login hash-upgrade contracts; disposable fixture only.

All original application credit remains with Dispatcharr contributors. Secrets
stay in private fixture files; report output contains no password or hash bytes.
"""
import json
import os
import sys
import time
if os.environ.get('POSTGRES_PASSWORD') != 'playback-fixture-only':
    raise RuntimeError('Disposable fixture required')
sys.path.insert(0, '/app')
sys.argv.append('shell')
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'dispatcharr.settings')
import django
django.setup()
from django.contrib.auth.hashers import PBKDF2PasswordHasher, PBKDF2SHA1PasswordHasher, ScryptPasswordHasher, check_password
from django.core.cache import cache
from rest_framework.test import APIRequestFactory
from apps.accounts.models import User
from apps.accounts.api_views import TokenObtainPairView, TokenRefreshView
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
    ('supported-default-scrypt-upgrades', 'scrypt', 16384, 'abcdefghijklmnopqrstuv', True, True),
    ('wrong-scrypt-preserves-hash', 'scrypt', 16384, 'abcdefghijklmnopqrstuv', True, False),
]):
    hasher = ScryptPasswordHasher() if algorithm == 'scrypt' else PBKDF2SHA1PasswordHasher() if algorithm == 'sha1' else PBKDF2PasswordHasher()
    original_hash = hasher.encode(PASSWORD, salt) if algorithm == 'scrypt' else hasher.encode(PASSWORD, salt, iterations)
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


from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework.permissions import AllowAny
from rest_framework_simplejwt.authentication import JWTAuthentication
from apps.accounts.authentication import ApiKeyAuthentication
from core.models import SystemEvent, SystemNotification, CoreSettings
class KeyProbe(APIView):
    authentication_classes = [JWTAuthentication, ApiKeyAuthentication]
    permission_classes = [AllowAny]
    def get(self, request):
        return Response({'user_id': request.user.pk if request.user.is_authenticated else None})
active, _ = User.objects.get_or_create(username='rust-probe-auth-key-active')
inactive, _ = User.objects.get_or_create(username='rust-probe-auth-key-inactive')
for target, enabled, key in [(active, True, 'auth-fixture-active'), (inactive, False, 'auth-fixture-inactive')]:
    User.objects.filter(pk=target.pk).update(is_active=enabled, api_key=key, custom_properties={})
CoreSettings.objects.filter(key='system_settings').update(value={'max_system_events': 10000})
cache.delete(CoreSettings.group_cache_key('system_settings'))
def reset(ip):
    for key in ['throttle_login_'+ip, 'throttle_api_key_auth_'+ip, 'login_rate_limit_lockout_emitted:'+ip, 'login_rate_limit_notice_cooldown', 'api_key_auth_lockout_notice_cooldown']:
        cache.delete(key)
    SystemNotification.objects.filter(notification_key__in=['login-rate-limit','api-key-auth-lockout']).delete()
    SystemEvent.objects.filter(details__client_ip=ip).delete()
def effects(ip):
    return {'events': list(SystemEvent.objects.filter(details__client_ip=ip).order_by('id').values('event_type','details')),
            'notices': list(SystemNotification.objects.filter(notification_key__in=['login-rate-limit','api-key-auth-lockout']).order_by('notification_key').values('notification_key','notification_type','priority','source','title','message','action_data','is_active','admin_only')),
            'login_history': len(cache.get('throttle_login_'+ip, [])),
            'api_history': len(cache.get('throttle_api_key_auth_'+ip, [])),
            'login_suppression':cache.get('login_rate_limit_lockout_emitted:'+ip) is not None,
            'login_cooldown':cache.get('login_rate_limit_notice_cooldown') is not None,
            'api_cooldown':cache.get('api_key_auth_lockout_notice_cooldown') is not None}
scenarios=[]
def scenario(name,index,kinds,prefill_scope=None,prefill=0,network=None):
    ip='198.51.100.'+str(160+index);reset(ip);steps=[]
    cache.delete(CoreSettings.group_cache_key('network_access'))
    if network is not None:cache.set(CoreSettings.group_cache_key('network_access'),{'UI': network},300)
    prefill_wire=None
    if prefill_scope:
        key='throttle_'+prefill_scope+'_'+ip
        cache.set(key,[time.time()]*prefill,60)
        prefill_wire=cache.client.encode(cache.get(key)).hex()
    for kind in kinds:
        headers={'REMOTE_ADDR':ip,'HTTP_USER_AGENT':'rust-probe-auth-UA'}
        if kind.startswith('login-'):
            payload={'login-missing':{},'login-null':{'username':None,'password':None},'login-blank':{'username':' ','password':' '},'login-nonstring':{'username':False,'password':{}},'login-nul':{'username':'invalid\x00name','password':'invalid\x00password'}}[kind]
            result=TokenObtainPairView.as_view()(factory.post('/api/accounts/token/',payload,format='json',**headers))
        else:
            if kind!='key-missing':headers['HTTP_X_API_KEY']={'key-valid':'auth-fixture-active','key-inactive':'auth-fixture-inactive'}.get(kind,'auth-fixture-invalid')
            result=KeyProbe.as_view()(factory.get('/auth-key-probe',**headers))
        result.render()
        steps.append({'kind':kind,'expected':{'status':result.status_code,'body':dict(result.data),'retry_after':result.get('Retry-After'),'effects':effects(ip)}})
    scenarios.append({'name':name,'ip':ip,'steps':steps,'prefill_scope':prefill_scope,'prefill_wire':prefill_wire,'network':network})
scenario('shared-login-validation-and-once-lockout',0,['login-missing']*5)
scenario('failure-only-key-lockout-valid-denied-missing-ignored',1,['key-valid']*3+['key-invalid']*10+['key-valid','key-missing'])
scenario('inactive-key-budget-uses-actual-username',2,['key-inactive']*10+['key-valid'])
scenario('django-serialized-login-entries-share-remaining-budget',3,['login-missing','login-missing'],prefill_scope='login',prefill=2)
scenario('django-serialized-api-key-lockout-blocks-valid',4,['key-valid'],prefill_scope='api_key_auth',prefill=10)
scenario('cached-ui-denial-before-validation-consumes-login-slot',5,['login-missing'],network='192.0.2.0/24')
scenario('overfull-django-login-history-omits-retry',10,['login-missing'],prefill_scope='login',prefill=4)
scenario('null-field-validation-audit',6,['login-null'])
scenario('blank-field-validation-audit',7,['login-blank'])
scenario('nonstring-field-validation-audit',8,['login-nonstring'])
scenario('nul-field-validation-audit',9,['login-nul'])
for item in scenarios:reset(item['ip'])
cache.delete(CoreSettings.group_cache_key('network_access'))


# Source proxy/session helpers are exercised through their real DRF routes.
# Trust is explicit and confined to this fixture process and loopback peer.
from apps.accounts.api_views import AuthViewSet
from rest_framework_simplejwt.tokens import RefreshToken
from django.contrib.sessions.backends.db import SessionStore
os.environ['DISPATCHARR_TRUSTED_PROXIES']='127.0.0.0/8'
CoreSettings.objects.update_or_create(key='reverse_proxy_auth', defaults={'name':'Proxy fixture', 'value':{'enabled':False,'header':''}})
proxy_access=str(RefreshToken.for_user(active).access_token)
proxy_scenarios=[]
def proxy_scenario(name,index,kinds,prefill=0,network=None):
    ip='198.51.100.'+str(180+index);reset(ip)
    cache.delete(CoreSettings.group_cache_key('network_access'))
    if network is not None:cache.set(CoreSettings.group_cache_key('network_access'),{'UI':network},300)
    cache.set(CoreSettings.group_cache_key('reverse_proxy_auth'),{'enabled':True,'header':'X-Probe-Identity'},300)
    prefill_wire=None
    if prefill:
        cache.set('throttle_login_'+ip,[time.time()]*prefill,60)
        prefill_wire=cache.client.encode(cache.get('throttle_login_'+ip)).hex()
    steps=[]
    for kind in kinds:
        headers={'REMOTE_ADDR':'127.0.0.1','HTTP_X_REAL_IP':ip,'HTTP_USER_AGENT':'rust-probe-auth-UA'}
        if kind!='proxy-missing':headers['HTTP_X_PROBE_IDENTITY']=inactive.username if kind=='proxy-inactive' else 'rust-probe-no-such-identity' if kind=='proxy-unknown' else active.username
        if kind in ['proxy-authenticated','logout-authenticated']:headers['HTTP_AUTHORIZATION']='Bearer '+proxy_access
        if kind=='proxy-invalid-bearer':headers['HTTP_AUTHORIZATION']='Bearer invalid'
        action='logout' if kind=='logout-authenticated' else 'proxy_login'
        request=factory.post('/api/accounts/auth/'+action+'/',{},format='json',**headers)
        request.session=SessionStore()
        result=TokenRefreshView.as_view()(request) if kind=='refresh-network-denied' else AuthViewSet.as_view({'post':action})(request)
        result.render()
        body={'token_pair':True,'user_id':active.pk} if result.status_code==200 and action=='proxy_login' else dict(result.data)
        steps.append({'kind':kind,'expected':{'status':result.status_code,'body':body,'retry_after':result.get('Retry-After'),'challenge':result.get('WWW-Authenticate'),'effects':effects(ip)}})
    proxy_scenarios.append({'name':name,'ip':ip,'steps':steps,'prefill_wire':prefill_wire,'network':network})
proxy_scenario('availability-probe-does-not-charge-budget',0,['proxy-missing']*5)
proxy_scenario('proxy-shared-budget-lockout-and-audits',1,['proxy-unknown','proxy-inactive','proxy-valid','proxy-valid','proxy-valid'])
proxy_scenario('authenticated-proxy-bypasses-anonymous-budget-logout-keeps-jwt',2,['proxy-authenticated','logout-authenticated','proxy-authenticated','proxy-invalid-bearer'],prefill=3)
proxy_scenario('cached-ui-policy-denies-proxy-refresh-and-logout',3,['proxy-valid','refresh-network-denied','logout-authenticated'],network='192.0.2.0/24')
for item in proxy_scenarios:reset(item['ip'])
cache.delete(CoreSettings.group_cache_key('network_access'))
cache.delete(CoreSettings.group_cache_key('reverse_proxy_auth'))
print(json.dumps({'hash_cases':cases,'throttle_scenarios':scenarios,'proxy_scenarios':proxy_scenarios,'proxy_access':proxy_access,'key_active_id':active.pk,'django_version':django.get_version()}))
