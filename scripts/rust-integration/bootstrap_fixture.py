"""First-admin source goldens on a fresh disposable migrated user table.

No unowned users may exist. Original Dispatcharr contributors retain credit.
Only fixture passwords are used; hash bytes and passwords are never emitted.
"""
import json,os,sys
if os.environ.get('POSTGRES_PASSWORD')!='playback-fixture-only':raise RuntimeError('Disposable fixture required')
sys.path.insert(0,'/app');sys.argv.append('shell');os.environ.setdefault('DJANGO_SETTINGS_MODULE','dispatcharr.settings')
import django
django.setup()
from django.test import RequestFactory
from django.contrib.auth.hashers import check_password
from django.core.cache import cache
from apps.accounts.models import User
from apps.accounts.api_views import initialize_superuser
from core.models import CoreSettings
if User.objects.exists():raise RuntimeError('Fresh empty user table required')
factory=RequestFactory();password='bootstrap-fixture-only';prefix='rust-probe-first-admin-'
def witness(check_password_value=password):
    return [{'username':u.username,'email':u.email,'user_level':u.user_level,'is_superuser':u.is_superuser,'is_staff':u.is_staff,'is_active':u.is_active,'custom_properties':u.custom_properties,'avatar_config':u.avatar_config,'stream_limit':u.stream_limit,'api_key':u.api_key,'last_login_set':u.last_login is not None,'date_joined_set':u.date_joined is not None,'password_valid':check_password(check_password_value,u.password)}for u in User.objects.order_by('username')]
cases=[]
def case(name,method='GET',body=None,raw=None,peer='127.0.0.1',headers=None,override=None,preseed=None,ui_deny=False):
    if User.objects.exists():raise RuntimeError('Previous fixture users not cleaned')
    if override is None:os.environ.pop('DISPATCHARR_SETUP_ALLOWED_IP',None)
    else:os.environ['DISPATCHARR_SETUP_ALLOWED_IP']=override
    cache.delete(CoreSettings.group_cache_key('network_access'))
    if ui_deny:cache.set(CoreSettings.group_cache_key('network_access'),{'UI':'192.0.2.0/24'},300)
    if preseed:User.objects.create(username=preseed['username'],password='!',user_level=preseed.get('level',0),is_active=preseed.get('active',True))
    wire=raw if raw is not None else json.dumps(body if body is not None else {}).encode()
    request=factory.generic(method,'/api/accounts/initialize-superuser/',data=wire,content_type='application/json',REMOTE_ADDR=peer,**(headers or {}))
    result=initialize_superuser(request)
    cases.append({'name':name,'method':method,'body':wire.decode(),'peer':peer,'headers':headers or {},'override':override,'preseed':preseed,'ui_deny':ui_deny,'expected':{'status':result.status_code,'body':json.loads(result.content),'users':witness(body.get('password') if isinstance(body,dict) and isinstance(body.get('password'),str) else password)}})
    # Table was asserted empty immediately before each owned fixture operation.
    # Every row here was created by this specific operation or its explicit seed.
    if User.objects.count()>2:raise RuntimeError('Unexpected fixture row count')
    User.objects.all().delete()
case('empty-private-status')
case('empty-public-status',peer='198.51.100.22')
case('nonpost-status',method='DELETE',raw=b'invalid-json')
case('public-denial-before-json',method='POST',peer='198.51.100.22',raw=b'invalid-json')
case('trusted-client-header-denies',method='POST',headers={'HTTP_X_REAL_IP':'198.51.100.22'},raw=b'invalid-json')
case('untrusted-peer-cannot-spoof-local',method='POST',peer='198.51.100.22',headers={'HTTP_X_REAL_IP':'127.0.0.1'},raw=b'invalid-json')
case('missing-credentials',method='POST',body={})
case('falsy-credentials',method='POST',body={'username':0,'password':False})
case('empty-json',method='POST',raw=b'')
case('truncated-json',method='POST',raw=b'{')
case('nonmapping-json',method='POST',body=[])
case('wrong-password-type',method='POST',body={'username':prefix+'wrongtype','password':True})
case('duplicate-nonadmin',method='POST',body={'username':prefix+'duplicate','password':password},preseed={'username':prefix+'duplicate','level':0})
case('ordinary-first-admin',method='POST',body={'username':prefix+'ordinary','password':password,'email':' User@EXAMPLE.INVALID ','user_level':0,'is_staff':False})
case('nfkc-and-unvalidated-name',method='POST',body={'username':prefix+'\uff21+\u0301','password':password})
case('source-unassigned-compatibility-char',method='POST',body={'username':prefix+chr(0x1ccd6)+'\u0301','password':password})
case('numeric-username-and-email-none',method='POST',body={'username':1,'password':password,'email':None})
case('no-at-email-retains-whitespace',method='POST',body={'username':prefix+'email','password':password,'email':' no-at-email '})
case('setup-independent-of-ui-denial',method='POST',body={'username':prefix+'ui-deny','password':password},ui_deny=True)
case('explicit-public-setup-override',method='POST',peer='198.51.100.22',override='198.51.100.22',body={'username':prefix+'remote','password':password})
case('short-password-no-strength-validator',method='POST',body={'username':prefix+'short','password':'x'})
case('whitespace-password-not-trimmed',method='POST',body={'username':prefix+'whitespace','password':'   '})
case('email-lowercase-source-unassigned-barrier',method='POST',body={'username':prefix+'email-unicode','password':password,'email':'User@ΟΣ.'+chr(0xa7dc)})
case('invalid-override-denies',method='POST',override='not-an-ip',raw=b'invalid-json')
case('inactive-level10-closes-before-body',method='POST',preseed={'username':prefix+'existing','level':10,'active':False},raw=b'invalid-json')
cache.delete(CoreSettings.group_cache_key('network_access'))
print(json.dumps({'bootstrap_cases':cases,'fresh_user_table':True,'password_literal':'fixture-only-defined-in-code'}))
