"""Actual original environment/IP worker goldens against private HTTP providers.

No real IP-provider requests, credentials or tokens are emitted. Management
preparation follows the original initialization branch; API/worker code stays
unchanged, with transport destinations injected into loopback-only fixtures.
"""
import json
import pickletools
import os
import socket
import sys
import threading
import time
if os.environ.get('POSTGRES_PASSWORD')!='playback-fixture-only':raise RuntimeError('Disposable fixture required')
sys.path.insert(0,'/app');sys.argv.append('shell');os.environ.setdefault('DJANGO_SETTINGS_MODULE','dispatcharr.settings')
import django
django.setup()
import requests
from django.core.cache import cache
from django.test import RequestFactory,override_settings
from rest_framework.test import force_authenticate
from apps.accounts.models import User
from core.models import CoreSettings
from core import api_views as views
from core import utils
ORIGIN='http://origin:8080';CACHE='dispatcharr:ip_lookup_result';LOCK='dispatcharr:ip_lookup_lock'
user,_=User.objects.get_or_create(username='rust-probe-ip-environment',defaults={'user_level':1,'is_active':True,'password':'!'})
if not CoreSettings.objects.filter(key='system_settings').exists():CoreSettings.objects.bulk_create([CoreSettings(key='system_settings',name='System Settings',value={})])
CoreSettings.objects.filter(key='system_settings').update(value={})
original_get=requests.get;original_socket=socket.socket;original_thread=threading.Thread;original_send=utils.send_websocket_update
threads=[];published=[]
def private_get(url,*args,**kwargs):
    if url=='https://api64.ipify.org?format=json':url=ORIGIN+'/ipify'
    elif url.startswith('https://ipapi.co/') and url.endswith('/json/'):url=ORIGIN+'/geo/'+url.removeprefix('https://ipapi.co/').removesuffix('/json/')
    elif url=='http://ip-api.com/json/':url=ORIGIN+'/fallback'
    else:raise RuntimeError('Unexpected external lookup destination')
    return original_get(url,*args,**kwargs)
class LocalProbe(original_socket):
    def connect(self,address):return super().connect(('127.0.0.1',80) if address==('203.0.113.1',80) else address)
class TrackedThread(original_thread):
    def __init__(self,*args,**kwargs):
        super().__init__(*args,**kwargs)
        if kwargs.get('target') is views._perform_ip_lookup:threads.append(self)
def observe_send(group,kind,data,*args,**kwargs):
    published.append({'type':kind,'data':data});return original_send(group,kind,data,*args,**kwargs)
requests.get=private_get;socket.socket=LocalProbe;threading.Thread=TrackedThread;utils.send_websocket_update=observe_send
factory=RequestFactory()
def response():
    request=factory.get('/api/core/settings/env/',REMOTE_ADDR='127.0.0.1');force_authenticate(request,user=user)
    result=views.environment(request)
    if result.status_code!=200:raise RuntimeError('Source environment response failed')
    return dict(result.data)
def control(path):return original_get(ORIGIN+path,timeout=5).json()
def cached():
    value=cache.get(CACHE)
    if isinstance(value,dict):return {key:value for key,value in value.items() if key!='verified_at'}
    return value
def run_case(name,mode='success',seed=None,lock_present=False,enabled=True,env_enabled=True):
    cache.delete(CACHE);cache.delete(LOCK);cache.delete(CoreSettings.group_cache_key('system_settings'));threads.clear();published.clear();control('/control?mode='+mode)
    if enabled is not True:cache.set(CoreSettings.group_cache_key('system_settings'),{'enable_ip_lookup':enabled},300)
    seed_kind=seed;seed_hex=None;verified_offset=None
    if seed:
        seed={'public_ip':'203.0.113.55','local_ip':'127.0.0.1','country_code':'OLD','country_name':'Old Country','city':'Old City','extra':'preserved','verified_at':time.time()-(120 if seed=='stale' else 0)};cache.set(CACHE,seed,3600)
        encoded=cache.client.encode(seed);seed_hex=encoded.hex();verified_offset=[position+1 for opcode,arg,position in pickletools.genops(encoded) if opcode.name=='BINFLOAT'][-1]
    if lock_present:cache.set(LOCK,True,30)
    with override_settings(ENABLE_IP_LOOKUP=env_enabled):
        initial=response()
        if mode.startswith('slow') and threads:
            deadline=time.monotonic()+3
            while not control('/stats')['calls']:
                if time.monotonic()>deadline:raise RuntimeError('Private lookup did not start')
                time.sleep(.01)
            repeat=response();control('/release')
        else:repeat=None
        for worker in list(threads):worker.join(8)
        if any(worker.is_alive() for worker in threads):raise RuntimeError('Source worker failed to finish')
        after=response()
        # A successful lookup or failed cached recheck must now be fresh; this
        # call catches implementations that fail to advance verified_at.
        for worker in list(threads):worker.join(8)
    return {'name':name,'mode':mode,'seed':seed_kind,'seed_hex':seed_hex,'verified_offset':verified_offset,'settings_hex':cache.client.encode({'enable_ip_lookup':enabled}).hex() if enabled is not True else None,'lock_present':lock_present,'enabled':enabled,'env_enabled':env_enabled,'initial':initial,'repeat':repeat,'after':after,'cached':cached(),'published':list(published),'calls':control('/stats')['calls'],'lock_remaining':cache.get(LOCK) is not None,'cache_ttl_positive':cache.ttl(CACHE)>0 if cache.get(CACHE) is not None else False,'system_settings':CoreSettings.get_system_settings()}
steps=[]
for name,mode in [('cold-success','success'),('fallback-on-status','fallback'),('geo-parse-no-fallback','geo-json-error'),('geo-array-no-fallback','geo-array'),('invalid-public-ip','ip-invalid'),('request-error','ip-http-error'),('ip-json-error','ip-json-error'),('numeric-public-ip','ip-numeric'),('zero-public-ip','ip-zero'),('whole-array-retains-lock','ip-array-fatal')]:steps.append(run_case(name,mode))
steps.append(run_case('fresh-cache',seed='fresh'))
steps.append(run_case('stale-coalesced','slow-success',seed='stale'))
steps.append(run_case('cold-coalesced','slow-success'))
steps.append(run_case('failed-recheck-retains-known','ip-http-error',seed='stale'))
steps.append(run_case('locked-cold',lock_present=True))
steps.append(run_case('database-disabled',enabled=False))
steps.append(run_case('env-disabled',env_enabled=False))
steps.append(run_case('legacy-enabled-value',enabled='yes'))
cache.delete(CACHE);cache.delete(LOCK);cache.delete(CoreSettings.group_cache_key('system_settings'))
print(json.dumps({'user_id':user.pk,'username':user.username,'steps':steps}))
