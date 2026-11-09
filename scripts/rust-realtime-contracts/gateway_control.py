"""Original Django control peer for native WS tests in an owned fixture only.

JWTs and fixture API keys are returned to the parent process in memory; callers
must never write the protocol stdout to evidence or command logs. Original
Dispatcharr authors retain full application credit.
"""
import hashlib
import inspect
import json
import os
from pathlib import Path
import sys
import threading
import requests

sys.path.insert(0,'/app')
os.environ.setdefault('DJANGO_SETTINGS_MODULE','dispatcharr.settings')
import django
django.setup()
from apps.accounts.models import User
from core.utils import RedisClient,_gevent_ws_send
from django.core.cache import cache
from rest_framework_simplejwt.tokens import AccessToken,RefreshToken

accounts={}
playbacks={}


def command(request):
    operation=request['op']
    if operation=='seed':
        if accounts:raise ValueError('already seeded')
        for role,level,active in [('admin',10,True),('standard',1,True),('viewer',0,True),('inactive',10,False)]:
            row=User.objects.create(username='native-ws-'+role,user_level=level,is_active=active,api_key=os.urandom(24).hex())
            row.set_unusable_password();row.save();refresh=RefreshToken.for_user(row)
            accounts[role]={'id':row.pk,'access':str(refresh.access_token),'refresh':str(refresh),'api_key':row.api_key}
        cache.delete('dispatcharr:ip_lookup_result')
        return {'accounts':accounts}
    if operation=='cache':
        value=request.get('value')
        if value is None:cache.delete('dispatcharr:ip_lookup_result')
        else:cache.set('dispatcharr:ip_lookup_result',value,60)
        return {'value':cache.get('dispatcharr:ip_lookup_result')}
    if operation=='emit':
        _gevent_ws_send('updates',request['message']);return {'sent':True}
    if operation=='user':
        role=request['role'];fields=request['fields']
        if not set(fields)<=set(['is_active','user_level']):raise ValueError('invalid field')
        User.objects.filter(pk=accounts[role]['id']).update(**fields);return {'updated':True}
    if operation=='token':
        token=AccessToken();token['user_id']=accounts[request['role']]['id'] if 'role' in request else request.get('user_id',-999999)
        if 'seconds' in request:
            import datetime
            token.set_exp(lifetime=datetime.timedelta(seconds=int(request['seconds'])))
        return {'access':str(token)}
    if operation=='groups':
        redis=RedisClient.get_buffer()
        return {'members':redis.zcard('asgi:group:updates'),'ttl':redis.ttl('asgi:group:updates')}
    if operation=='play_start':
        if playbacks:raise ValueError('playback already active')
        seed=json.loads(Path('/evidence/seed.json').read_text());path=seed['channels'][0]['path']
        stopped=threading.Event();state={'bytes':0,'error':None,'response':None,'stop':stopped}
        def read():
            try:
                with requests.get('http://127.0.0.1:5656'+path,stream=True,timeout=(5,15)) as response:
                    state['response']=response;state['status']=response.status_code
                    for data in response.iter_content(65536):
                        state['bytes']+=len(data)
                        if stopped.is_set():break
            except Exception as error:state['error']=type(error).__name__
        thread=threading.Thread(target=read,daemon=True);state['thread']=thread;playbacks['live']=state;thread.start()
        return {'started':True,'channel_id':seed['channels'][0]['channel_uuid']}
    if operation=='play_state':
        return {key:value for key,value in playbacks.get('live',{}).items() if key in ['bytes','error','status']}
    if operation=='play_stop':
        state=playbacks.pop('live');state['stop'].set()
        response=state['response']
        if response is not None:response.close()
        state['thread'].join(timeout=3)
        return {'stopped':not state['thread'].is_alive(),'bytes':state['bytes']}
    if operation=='status_get':
        with requests.get('http://127.0.0.1:5656/proxy/ts/status',headers={'Authorization':'Bearer '+accounts['admin']['access']},timeout=5) as response:
            return {'status':response.status_code,'body':response.json()}
    if operation=='source_info':
        from daphne.server import Server
        defaults={name:parameter.default for name,parameter in inspect.signature(Server).parameters.items()
                  if parameter.default is not inspect.Parameter.empty and isinstance(parameter.default,(int,float,str,bool,type(None)))}
        paths=['dispatcharr/consumers.py','dispatcharr/jwt_ws_auth.py','dispatcharr/asgi.py','dispatcharr/routing.py','core/utils.py']
        from daphne.ws_protocol import WebSocketProtocol
        return {'daphne_defaults':defaults,'daphne_timeout_source':inspect.getsource(WebSocketProtocol.check_timeouts),'daphne_close_source':inspect.getsource(WebSocketProtocol.serverClose),'source_sha256':{path:hashlib.sha256((Path('/app')/path).read_bytes()).hexdigest() for path in paths}}
    if operation=='close':return {'closed':True}
    raise ValueError('unknown operation')


if __name__=='__main__':
    print(json.dumps({'ready':True}),flush=True)
    while True:
        line=sys.stdin.buffer.readline(25*1024*1024)
        if not line:break
        if len(line)>24*1024*1024 or not line.endswith(b'\n'):raise ValueError('control request bound')
        request=json.loads(line)
        response=command(request)
        print(json.dumps(response),flush=True)
        if request['op']=='close':break
