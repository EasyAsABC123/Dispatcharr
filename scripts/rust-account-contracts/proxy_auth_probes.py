"""Opt-in, disposable trusted-proxy authentication golden comparisons.

Requires --contract-suites proxy_auth --trusted-proxy-fixture. Never use against
production: this suite temporarily changes source fixture network/auth settings.
Original Dispatcharr authors retain full credit for the source contracts.
"""
import copy
import importlib.util
from pathlib import Path
import secrets
import uuid

spec=importlib.util.spec_from_file_location('dispatcharr_startup_token_oracle',Path(__file__).with_name('startup_probes.py'))
startup=importlib.util.module_from_spec(spec)
spec.loader.exec_module(startup)


def run(http,baseline,candidate,accounts,record):
    admin=accounts['admin']['access'];users='/api/accounts/users/';settings='/api/core/settings/'
    prefix='rust-probe-proxy-'+uuid.uuid4().hex[:12]
    control={'X-Real-IP':'198.18.0.1'};owned={};saved={};counter=0
    def source(path,body=None,method=None):
        return http(baseline+path,body=body,method=method,token=admin,headers=control)
    def compare(name,path='/api/accounts/auth/proxy-login/',identity=None,expected=401,user=None,
                body=None,method='POST',token=None):
        nonlocal counter
        counter+=1
        headers={'X-Real-IP':'198.18.1.'+str(counter)}
        if identity is not None:headers['X-Probe-Identity']=identity
        if token=='invalid':headers['Authorization']='Bearer invalid';token=None
        try:
            a=http(baseline+path,body=body,method=method,token=token,headers=headers)
            b=http(candidate+path,body=body,method=method,token=token,headers=headers)
            normalize=(lambda value:startup.token_pair(value,user)) if user is not None else (lambda value:value)
            left,right=normalize(a[1]),normalize(b[1])
            record('proxy-auth:'+name,a[0]==b[0]==expected and startup.digest(left)==startup.digest(right),
                   {'python_status':a[0],'rust_status':b[0],
                    'python_body_sha256':startup.digest(left),'rust_body_sha256':startup.digest(right)})
        except Exception as error:record('proxy-auth:'+name,False,{'error_type':type(error).__name__})
    def configure(key,value):
        if key not in saved:
            response=source(settings)
            if response[0]!=200:raise RuntimeError('Fixture settings lookup failed')
            row=next((row for row in response[1] if row['key']==key),None)
            saved[key]=copy.deepcopy(row)
        row=saved[key]
        current_id=row['id'] if row else saved.get(key+'_created_id')
        path=settings+str(current_id)+'/' if current_id is not None else settings
        method='PATCH' if current_id is not None else 'POST'
        response=source(path,{'value':value} if current_id is not None else {'key':key,'name':'Probe '+key,'value':value},method)
        if response[0] not in (200,201):raise RuntimeError('Fixture settings write failed')
        if row is None and current_id is None:
            # Keep identity separately from the original None marker so cleanup
            # removes only the exact row created by this fixture.
            saved[key+'_created_id']=response[1]['id']
    try:
        for suffix,email in [('Case','shared'),('case','shared'),('Unique','unique')]:
            name=prefix+suffix
            response=source(users,{'username':name,'password':secrets.token_urlsafe(24),'user_level':1,
                                  'email':prefix+'-'+email+'@example.invalid',
                                  'custom_properties':{'allowed_networks':{'UI':'127.0.0.0/8'}}},'POST')
            if response[0]!=201:raise RuntimeError('Fixture user creation failed')
            owned[suffix]=(response[1]['id'],name)
        configure('reverse_proxy_auth',{'enabled':True,'header':'X-Probe-Identity'})
        compare('missing-header')
        compare('blank-header',identity=' ')
        compare('unknown-identity',identity=prefix+'missing')
        compare('inactive-identity',identity=accounts['inactive']['username'])
        for suffix in ['Case','case','Unique']:
            user,name=owned[suffix]
            compare('exact-'+suffix,identity=name,expected=200,user=user)
        user,name=owned['Unique']
        compare('unique-case-insensitive',identity=name.upper(),expected=200,user=user)
        compare('unique-email',identity=(prefix+'-unique@example.invalid').upper(),expected=200,user=user)
        compare('identity-whitespace',identity=' '+name+' ',expected=200,user=user)
        compare('ambiguous-username',identity=(prefix+'Case').upper())
        compare('ambiguous-email',identity=prefix+'-shared@example.invalid')
        compare('invalid-bearer-before-identity',identity=name,token='invalid')
        compare('viewer-level-exchange',identity=accounts['viewer']['username'],expected=200,user=accounts['viewer']['id'])
        configure('reverse_proxy_auth',{'enabled':False,'header':'X-Probe-Identity'})
        compare('disabled-with-identity',identity=name)
        configure('reverse_proxy_auth',{'enabled':True,'header':'X-Probe-Identity'})
        # Management remains reachable only through control's synthetic client
        # IP so the original API can restore the policy in finally.
        original_network=next((row for row in source(settings)[1] if row['key']=='network_access'),None)
        network={**((original_network or {}).get('value') or {}),'UI':'198.18.0.1/32'}
        configure('network_access',network)
        compare('global-ui-blocks-exchange',identity=name,expected=403)
        compare('global-ui-blocks-refresh-before-validation',path='/api/accounts/token/refresh/',body={},expected=403)
        compare('global-ui-blocks-logout',path='/api/accounts/auth/logout/',token=admin,expected=403)
        compare('version-allow-any-ignores-ui-policy',path='/api/core/version/',method='GET',token=admin,expected=200)
        compare('configured-setup-ignores-ui-policy',path='/api/accounts/initialize-superuser/',method='GET',expected=200)
    except Exception as error:record('proxy-auth:fixture-lifecycle',False,{'error_type':type(error).__name__})
    finally:
        for key in ['network_access','reverse_proxy_auth']:
            if key not in saved:continue
            try:
                row=saved[key]
                if row is not None:
                    response=source(settings+str(row['id'])+'/',{'name':row['name'],'value':row['value']},'PATCH')
                    if response[0]!=200:raise RuntimeError('Fixture settings restore failed')
                elif key+'_created_id' in saved:
                    path=settings+str(saved[key+'_created_id'])+'/'
                    checked=source(path)
                    if checked[0]!=200 or checked[1].get('key')!=key:raise RuntimeError('Unsafe fixture setting deletion')
                    if source(path,method='DELETE')[0]!=204:raise RuntimeError('Fixture settings deletion failed')
            except Exception as error:record('proxy-auth:settings-cleanup-'+key,False,{'error_type':type(error).__name__})
        for user,name in owned.values():
            try:
                path=users+str(user)+'/'
                checked=source(path)
                if checked[0]!=200 or checked[1].get('username')!=name or not name.startswith(prefix):raise RuntimeError('Unsafe fixture user deletion')
                if source(path,method='DELETE')[0]!=204:raise RuntimeError('Fixture user deletion failed')
            except Exception as error:record('proxy-auth:user-cleanup',False,{'error_type':type(error).__name__})
