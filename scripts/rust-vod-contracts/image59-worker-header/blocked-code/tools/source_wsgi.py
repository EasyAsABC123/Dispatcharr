"""Owned unmodified Original WSGI, direct Redis; no Source function replacement."""
from gevent import monkey
monkey.patch_all()
import os,sys,json
from pathlib import Path
if os.environ.get('PENDING_MAIN_FIXTURE')!='playback-fixture-only':raise RuntimeError('OwnedHeaderSourceRequired')
sys.path.insert(0,'/app');sys.argv.append('shell');os.environ.setdefault('DJANGO_SETTINGS_MODULE','dispatcharr.settings')
import django;django.setup()
from core.utils import RedisClient
params=RedisClient.get_client().connection_pool.connection_kwargs
if params.get('port')!=6390 or params.get('socket_timeout')!=60 or params.get('socket_connect_timeout')!=5:raise RuntimeError('UnchangedSourceClientConfiguration')
Path('/evidence/Source-client-config.json').write_text(json.dumps({'port':params['port'],'socket_timeout':params['socket_timeout'],'socket_connect_timeout':params['socket_connect_timeout'],'Source_methods_replaced':False})+'\n')
from dispatcharr.wsgi import application
from gevent.pywsgi import WSGIServer
WSGIServer(('0.0.0.0',5657),application,log=None,error_log=None).serve_forever()
