"""Execute unchanged original ASGI app in a disposable migrated fixture only."""
import datetime
import hashlib
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import time
from urllib.parse import urlencode

sys.path.insert(0,'/app')
os.environ.setdefault('DJANGO_SETTINGS_MODULE','dispatcharr.settings')
import django
django.setup()
from django.core.cache import cache
from apps.accounts.models import User
from rest_framework_simplejwt.tokens import AccessToken,RefreshToken
from core.utils import _gevent_ws_send
from core.utils import RedisClient
from ws_client import Client
REPORT={'cases':[]}


def main():
    accounts=json.load(sys.stdin);clients=[];cases=REPORT['cases'];port=8001
    if not accounts:
        for role,level,active in [('admin',10,True),('standard',1,True),('viewer',0,True),('inactive',10,False)]:
            row=User.objects.create(username='ws-source-'+role,user_level=level,is_active=active,api_key=os.urandom(24).hex())
            row.set_unusable_password();row.save();refresh=RefreshToken.for_user(row)
            accounts[role]={'id':row.pk,'access':str(refresh.access_token),'refresh':str(refresh),'api_key':row.api_key}
    process=subprocess.Popen(['/dispatcharrpy/bin/daphne','-b','127.0.0.1','-p',str(port),'dispatcharr.asgi:application'],cwd='/app',stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
    def connect(query,**kwargs):
        client=Client('127.0.0.1',port,'/ws/?'+urlencode(query),**kwargs);clients.append(client);return client
    def user(role):
        client=connect([('token',accounts[role]['access'])]);assert client.status==101
        initial=client.receive()['message'];assert initial=={'type':'connection_established','data':{'success':True,'message':'WebSocket connection established successfully'}}
        return client
    def emit(event):_gevent_ws_send('updates',event)
    try:
        deadline=time.monotonic()+20
        while True:
            try:
                with socket.create_connection(('127.0.0.1',port),timeout=.2):break
            except OSError:
                if process.poll() is not None or time.monotonic()>deadline:raise RuntimeError('Source Daphne startup failed')
                time.sleep(.1)
        cache.delete('dispatcharr:ip_lookup_result')
        token=AccessToken();token['user_id']=-999999
        auth_cases=[('absent',[],None,403),('invalid',[('token','invalid')],None,403),
            ('refresh',[('token',accounts['admin']['refresh'])],None,403),
            ('inactive',[('token',accounts['inactive']['access'])],None,403),('missing-user',[('token',str(token))],None,403),
            ('apikey-query',[('token',accounts['admin']['api_key'])],None,403),
            ('header-only',[],{'Authorization':'Bearer '+accounts['admin']['access']},403),
            ('wrong-case',[('Token',accounts['admin']['access'])],None,403),
            ('invalid-first',[('token','invalid'),('token',accounts['admin']['access'])],None,403),
            ('blank-first',[('token',''),('token',accounts['admin']['access'])],None,101),
            ('valid-first',[('token',accounts['admin']['access']),('token','invalid')],None,101)]
        for name,query,headers,expected in auth_cases:
            client=connect(query,headers=headers);row={'case':'auth-'+name,'status':client.status};assert client.status==expected
            if expected==101:row['initial']=client.receive()['message']
            row['close']=client.close();cases.append(row)
        for role in ['admin','standard','viewer']:
            client=user(role);cases.append({'case':'role-'+role,'status':client.status,'close':client.close()})
        for origin in [None,'null','https://cross-origin.invalid']:
            client=connect([('token',accounts['standard']['access'])],origin=origin);assert client.status==101
            cases.append({'case':'origin','origin':origin,'status':client.status,'initial':client.receive()['message'],'close':client.close()})
        for value in [None,{},False,0,'',['invalid'],{'public_ip':'192.0.2.10','country_code':'ZZ','verified_at':1234.5},
                      {'type':'fixture_type_override','verified_at':12}]:
            if value is None:cache.delete('dispatcharr:ip_lookup_result')
            else:cache.set('dispatcharr:ip_lookup_result',value,60)
            client=user('admin');extra=client.optional();expected=None
            if isinstance(value,dict) and value:expected={'message':{'data':{'type':'ip_lookup_complete',**{k:v for k,v in value.items() if k!='verified_at'}}}}
            assert extra==expected
            assert cache.get('dispatcharr:ip_lookup_result')==value
            cases.append({'case':'cached-ip','input':value,'extra':extra,'close':client.close()})
        cache.delete('dispatcharr:ip_lookup_result')
        admin=user('admin');standard=user('standard');viewer=user('viewer')
        for number,kind in enumerate(['channel_stats','vod_stats','timeshift_stats','vod_started','vod_stopped','future_event',None]):
            data={'type':kind,'fixture_probe':number};event={'type':'update','data':data};emit(event)
            got=[client.optional(.3) for client in [admin,standard,viewer]]
            expected=[{'message':event}]*3
            if kind in ['channel_stats','vod_stats','timeshift_stats','vod_started','vod_stopped']:expected[1:]=[None,None]
            assert got==expected;cases.append({'case':'event-filter','data':data,'received':got})
        for flag in [True,1,'false',[False],False,0,'',[],{}]:
            event={'type':'update','data':{'type':'system_notification','notification':{'admin_only':flag}}};emit(event)
            got=[client.optional(.25) for client in [admin,standard,viewer]];expected=[{'message':event}]*3
            if flag:expected[1:]=[None,None]
            assert got==expected;cases.append({'case':'notification-filter','flag':flag,'received':got})
        for data in [None,False,0,'',[],{}]:
            event={'type':'update','data':data};emit(event);got=[client.receive() for client in [admin,standard,viewer]];assert got==[{'message':event}]*3
            cases.append({'case':'falsy-update','data':data,'received':got})
        # Live principals remain the values captured at authentication time.
        User.objects.filter(pk=accounts['admin']['id']).update(user_level=1,is_active=False)
        User.objects.filter(pk=accounts['standard']['id']).update(user_level=10)
        event={'type':'update','data':{'type':'channel_stats','fixture_probe':'captured-role'}};emit(event)
        assert admin.receive()=={'message':event} and standard.optional(.2) is None
        denied=connect([('token',accounts['admin']['access'])]);assert denied.status==403;denied.close()
        cases.append({'case':'captured-role-and-active','old-admin-receives':True,'old-standard-restricted':True,'new-inactive-status':403})
        User.objects.filter(pk=accounts['admin']['id']).update(user_level=10,is_active=True)
        User.objects.filter(pk=accounts['standard']['id']).update(user_level=1)
        admin.close();standard.close();viewer.close()
        short=AccessToken.for_user(User.objects.get(pk=accounts['admin']['id']));short.set_exp(lifetime=datetime.timedelta(seconds=2))
        old=connect([('token',str(short))]);assert old.status==101;old.receive();time.sleep(2.2)
        event={'type':'update','data':{'type':'channel_stats','fixture_probe':'expired-token'}};emit(event);assert old.receive()=={'message':event}
        new=connect([('token',str(short))]);assert new.status==403;new.close();old.close();cases.append({'case':'token-expiry-after-handshake','old-stays-connected':True,'new-status':403})
        time.sleep(.1)
        REPORT['group_members_before_malformed']=RedisClient.get_buffer().zcard('asgi:group:updates')
        for name,opcode,payload in [('invalid-json',1,b'{'),('missing-type',1,b'{}'),('array',1,b'[]'),('binary',2,b'fixture')]:
            client=user('admin');client.send(opcode,payload);result=client.receive();assert 'closed' in result
            cases.append({'case':'inbound-'+name,**result});client.close()
        client=user('standard');client.json({'type':'m3u_profile_test','url':'fixture','search':'f','replace':'x'});assert client.optional(.3) is None;client.close()
        cases.append({'case':'standard-preview-ignored','passed':True})
        client=user('admin');client.send(9,b'fixture-ping');assert client.receive()=={'pong':b'fixture-ping'.hex()};client.close();cases.append({'case':'ping-pong','passed':True})
        for client in clients:client.close()
        redis=RedisClient.get_buffer();deadline=time.monotonic()+3
        while redis.zcard('asgi:group:updates'):
            if time.monotonic()>deadline:break
            time.sleep(.05)
        members=redis.zcard('asgi:group:updates');ttl=redis.ttl('asgi:group:updates')
        paths=['dispatcharr/consumers.py','dispatcharr/jwt_ws_auth.py','dispatcharr/asgi.py','dispatcharr/routing.py','core/utils.py']
        return {'scope':'actual pinned Source Daphne handshake, Origin, close, initial JSON and event visibility','passed':True,'application_performance_verified':False,'milestone_complete':False,'cases':cases,'case_count':len(cases),'group_members_before_malformed':REPORT['group_members_before_malformed'],'group_members_after_close':members,'group_key_ttl_seconds_after_close':ttl,'source_group_cleanup_passed':members==0,'cleanup_scope':'wire assertions passed; stale Source members after exceptional consumer termination are recorded independently, not normalized to success','source_sha256':{path:hashlib.sha256((Path('/app')/path).read_bytes()).hexdigest() for path in paths}}
    finally:
        for client in clients:client.close()
        process.terminate()
        try:process.wait(timeout=5)
        except subprocess.TimeoutExpired:process.kill();process.wait(timeout=3)


if __name__=='__main__':
    try:print(json.dumps(main()))
    except Exception as error:
        print(json.dumps({**REPORT,'passed':False,'failure':type(error).__name__,'failure_message':str(error)}));raise
