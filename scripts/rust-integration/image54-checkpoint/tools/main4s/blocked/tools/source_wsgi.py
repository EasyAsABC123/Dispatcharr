"""Genuine Original Django/gevent TCP application; no Redis/task method replaced."""
from gevent import monkey;monkey.patch_all()
import json,os,sys
from pathlib import Path
if os.environ.get('PENDING_MAIN_FIXTURE')!='playback-fixture-only':raise RuntimeError('OwnedSourceMainRequired')
sys.path.insert(0,'/app');sys.argv.append('shell');os.environ.setdefault('DJANGO_SETTINGS_MODULE','dispatcharr.settings')
import django;django.setup()
from core.utils import RedisClient
from django.conf import settings
client=RedisClient.get_client();params=client.connection_pool.connection_kwargs
if params.get('port')!=6383 or params.get('socket_timeout')!=60 or params.get('socket_connect_timeout')!=5 or params.get('retry_on_timeout')is not True:raise RuntimeError('GenuineSourceProxyTimeoutBinding')
Path('/evidence/Source-client-config.json').write_text(json.dumps({'socket_timeout':params['socket_timeout'],'connect_timeout':params['socket_connect_timeout'],'retry_on_timeout':params['retry_on_timeout'],'proxy_port':params['port'],'settings_socket_timeout':settings.REDIS_SOCKET_TIMEOUT,'method_replaced':False,'scope':'actual Original Source WSGI client configuration'},indent=2)+'\n')
from dispatcharr.wsgi import application
from request_observer import install
observer=install(json.loads(Path('/evidence/pending-main-private.json').read_text()),'/evidence')
def observer_status(environ,start_response):
 if environ.get('REMOTE_ADDR')!='127.0.0.1'or environ.get('REQUEST_METHOD')!='GET'or environ.get('PATH_INFO')!='/state':
  start_response('404 Not Found',[('Content-Length','0')]);return[b'']
 raw=json.dumps(observer.journal.snapshot()).encode()
 if len(raw)>4096:raise ValueError('ObserverStatusCapacity')
 start_response('200 OK',[('Content-Type','application/json'),('Content-Length',str(len(raw)))]);return[raw]
from gevent import spawn
from gevent.pool import Pool

from gevent.pywsgi import WSGIServer
status_server=WSGIServer(('127.0.0.1',6385),observer_status,spawn=Pool(2),log=None,error_log=None)
spawn(status_server.serve_forever)
WSGIServer(('127.0.0.1',5657),application,log=None,error_log=None).serve_forever()
