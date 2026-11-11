"""Actual Original Django TCP response with explicit post-INCR scheduling hook.
No task/SQL/provider mutation is replaced. Not a withheld Redis transport proof.
"""
from gevent import monkey;monkey.patch_all()
import json,os,sys,time
sys.path.insert(0,'/app');sys.argv.append('shell');os.environ.setdefault('DJANGO_SETTINGS_MODULE','dispatcharr.settings')
import django;django.setup()
from dispatcharr.wsgi import application as original
from core.utils import RedisClient
from gevent.event import Event
from gevent.local import local
from gevent.pywsgi import WSGIServer
state={'armed':False,'reached':False};released=Event();context=local();client=RedisClient.get_client();real_incr=client.incr

def incr(name,*args,**kwargs):
 value=real_incr(name,*args,**kwargs)
 if state['armed']and not state['reached']and name==state['profile_key']and getattr(context,'agent',None)==state['target_agent']:
  state['reached_monotonic']=time.monotonic();state['reached']=True
  if not released.wait(timeout=12):raise TimeoutError('Owned actualTCP post-INCR gate')
 return value
client.incr=incr

def application(environ,start_response):
 path=environ.get('PATH_INFO','')
 if path.startswith('/__owned_fixture/'):
  if environ.get('REMOTE_ADDR')!='127.0.0.1':raise RuntimeError('Owned loopback control only')
  if path.endswith('/arm'):
   size=int(environ.get('CONTENT_LENGTH')or 0);assert 0<=size<=4096;data=json.loads(environ['wsgi.input'].read(size));assert set(data)=={'profile_key','target_agent'};state.update(data,armed=True,reached=False,reached_monotonic=None,released_monotonic=None);released.clear()
  elif path.endswith('/release'):state['released_monotonic']=time.monotonic();released.set();state['armed']=False
  elif not path.endswith('/state'):raise RuntimeError('Unknown gate')
  payload=json.dumps({'reached':state['reached'],'armed':state['armed'],'reached_monotonic':state.get('reached_monotonic'),'released_monotonic':state.get('released_monotonic')}).encode();start_response('200 OK',[('Content-Type','application/json'),('Content-Length',str(len(payload)))]);return[payload]
 context.agent=environ.get('HTTP_USER_AGENT')
 return original(environ,start_response)
WSGIServer(('127.0.0.1',5657),application,log=None,error_log=None).serve_forever()
