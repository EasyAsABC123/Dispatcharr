"""Original React startup/auth helper oracles on root's disposable fixture.

Do not call on a production DB. Credentials, signed tokens and bodies are never
returned to record(); only statuses, scalar assertions and canonical hashes.
"""
import base64
import copy
import hashlib
import hmac
import json
import time
import uuid


def digest(value):
    return hashlib.sha256(json.dumps(value,sort_keys=True,allow_nan=False,separators=(',',':')).encode()).hexdigest()


def token_pair(value,user_id):
    result=copy.deepcopy(value)
    identifiers=[]
    claims={}
    now=time.time()
    for key in ['access','refresh']:
        encoded=result[key].split('.')
        if len(encoded)!=3:raise ValueError('Invalid token encoding')
        decode=lambda part:base64.urlsafe_b64decode(part+'='*((-len(part))%4))
        header=json.loads(decode(encoded[0]));payload=json.loads(decode(encoded[1]))
        # The key is fixture-only, matching the runner's disposable Compose.
        signature=hmac.new(b'playback-fixture-only',('.'.join(encoded[:2])).encode(),hashlib.sha256).digest()
        if header.get('alg')!='HS256' or not hmac.compare_digest(signature,decode(encoded[2])):
            raise ValueError('Invalid token signature')
        if (payload.get('token_type')!=key or payload.get('user_id')!=str(user_id)
            or type(payload.get('iat')) is not int or type(payload.get('exp')) is not int
            or abs(payload['iat']-now)>30 or payload['exp']<=now
            or not isinstance(payload.get('jti'),str) or not payload['jti']):
            raise ValueError('Invalid token claims')
        claims[key]=payload
        identifiers.append(payload['jti'])
        result[key]='<verified-fixture-'+key+'>'
    if identifiers[0]==identifiers[1]:raise ValueError('Reused token identity')
    # SimpleJWT creates access.iat at fresh AccessToken construction, but
    # access.exp is anchored to the refresh token's original current_time.
    # Crossing a second can therefore give access.exp-access.iat == 1799.
    access,refresh=claims['access'],claims['refresh']
    if (access['exp']-refresh['iat']!=1800
        or refresh['exp']-refresh['iat']!=86400
        or access['iat']<refresh['iat']):
        raise ValueError('Invalid token pair timing')
    return result


def run(http,baseline,candidate,accounts,record):
    root='/api/accounts/';admin=accounts['admin']['access'];viewer=accounts['viewer']
    def compare(name,path,expected,body=None,method=None,token=None,headers=None,normalize=lambda value:value):
        try:
            a=http(baseline+path,body=body,method=method,token=token,headers=headers)
            b=http(candidate+path,body=body,method=method,token=token,headers=headers)
            left,right=normalize(a[1]),normalize(b[1])
            record('startup:'+name,a[0]==b[0]==expected and digest(left)==digest(right),
                   {'python_status':a[0],'rust_status':b[0],
                    'python_body_sha256':digest(left),'rust_body_sha256':digest(right)})
        except Exception as error:record('startup:'+name,False,{'error_type':type(error).__name__})

    setup=root+'initialize-superuser/'
    try:
        before=http(baseline+root+'users/',token=admin)
        if (before[0]!=200 or not isinstance(before[1],list)
            or not any(row.get('id')==accounts['admin']['id'] and row.get('username')==accounts['admin']['username']
                       and row.get('user_level',0)>=10 for row in before[1])):
            raise RuntimeError('Configured disposable administrator required')
        for method,body in [('GET',None),('POST',{}),('PUT',{}),('DELETE',None)]:
            compare('configured-status-'+method,setup,200,body=body,method=method)
        compare('configured-ignores-invalid-credentials',setup,200,headers={'Authorization':'Bearer invalid'})
        compare('configured-post-no-bootstrap',setup,200,method='POST',
                body={'username':'rust-probe-bootstrap-noop-'+uuid.uuid4().hex,
                      'password':'fixture-only-'+uuid.uuid4().hex})
        after=http(baseline+root+'users/',token=admin)
        record('startup:configured-post-no-user-mutation',before[0]==after[0]==200 and digest(before[1])==digest(after[1]),
               {'before_sha256':digest(before[1]),'after_sha256':digest(after[1])})
    except Exception as error:record('startup:configured-status-fixture',False,{'error_type':type(error).__name__})

    for role,expected in [(None,200),('admin',200),('standard',200),('viewer',200),('inactive',401)]:
        compare('version-'+str(role),'/api/core/version/',expected,token=accounts[role]['access'] if role else None)
    compare('version-invalid-token','/api/core/version/',401,headers={'Authorization':'Bearer invalid'})
    for role,expected in [(None,401),('admin',200),('standard',200),('viewer',200),('inactive',401)]:
        compare('environment-disabled-lookup-'+str(role),'/api/core/settings/env/',expected,
                token=accounts[role]['access'] if role else None)
    compare('environment-invalid-token','/api/core/settings/env/',401,headers={'Authorization':'Bearer invalid'})
    for role in [None,'admin','standard','viewer','inactive']:
        compare('proxy-availability-'+str(role),root+'auth/proxy-login/',401,method='POST',
                token=accounts[role]['access'] if role else None)
    compare('proxy-invalid-token',root+'auth/proxy-login/',401,method='POST',headers={'Authorization':'Bearer invalid'})
    compare('login-alias-authenticates-first',root+'auth/login/',401,method='POST',body={},headers={'Authorization':'Bearer invalid'})
    # Source and Rust use one shared 3/minute login bucket. Alias and validation
    # pairs are independent contracts and therefore receive separate windows.
    try:
        before_login=http(baseline+root+'users/me/',token=viewer['access'])
        compare('login-alias-token-pair',root+'auth/login/',200,method='POST',
                body={'username':' '+viewer['username']+' ','password':viewer['password']},
                normalize=lambda value:token_pair(value,viewer['id']))
        after_login=http(baseline+root+'users/me/',token=viewer['access'])
        record('startup:padded-login-preserves-source-last-login',before_login[0]==after_login[0]==200
               and digest(before_login[1].get('last_login'))==digest(after_login[1].get('last_login')),
               {'before_sha256':digest(before_login[1].get('last_login')),'after_sha256':digest(after_login[1].get('last_login'))})
    except Exception as error:record('startup:login-alias-lifecycle',False,{'error_type':type(error).__name__})
    for _ in range(2):time.sleep(31)
    compare('direct-token-ignores-bearer-before-field-validation',root+'token/',400,method='POST',
            body={'username':None,'password':True},headers={'Authorization':'Bearer invalid'})
    for suffix,body in [('missing',{}),('null',{'refresh':None}),('blank',{'refresh':' '}),
                        ('wrong-list',{'refresh':[]}),('wrong-bool',{'refresh':True}),
                        ('numeric',{'refresh':123}),('nonobject',[])]:
        compare('refresh-validation-'+suffix,root+'token/refresh/',401 if suffix=='numeric' else 400,
                body=body,method='POST')
    for role,expected in [(None,401),('admin',200),('viewer',200),('inactive',401)]:
        compare('logout-'+str(role),root+'auth/logout/',expected,method='POST',
                token=accounts[role]['access'] if role else None)
    compare('logout-does-not-revoke-jwt',root+'users/me/',200,token=viewer['access'])
