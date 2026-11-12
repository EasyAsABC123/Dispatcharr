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
from gevent.pywsgi import WSGIServer
WSGIServer(('127.0.0.1',5657),application,log=None,error_log=None).serve_forever()
