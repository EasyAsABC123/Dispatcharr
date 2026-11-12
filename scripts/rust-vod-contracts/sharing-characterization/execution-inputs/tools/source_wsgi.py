"""Blocked owned Original WSGI adapter; no Source function replacement."""
from gevent import monkey;monkey.patch_all()
import json,os,sys
from pathlib import Path
if os.environ.get('NORMAL_REVIEWED_EXECUTION')!='one-normal-max2-pair':raise RuntimeError('RootCueRequiredForNormalSource')
# Future activation requires reviewed parent claim; current run entry is blocked.
from normal_guard import child_guard
child_guard()
sys.path.insert(0,'/app');sys.argv.append('shell');os.environ.setdefault('DJANGO_SETTINGS_MODULE','dispatcharr.settings')
import django;django.setup()
from core.utils import RedisClient
client=RedisClient.get_client();params=client.connection_pool.connection_kwargs
if params.get('port')!=6390 or params.get('socket_timeout')!=60 or params.get('socket_connect_timeout')!=5:raise ValueError('ActualNormalSourceClient')
Path('/evidence/Source-client-config.json').write_text(json.dumps({'actual_port':params['port'],'socket_timeout':params['socket_timeout'],'connect_timeout':params['socket_connect_timeout'],'retry_on_timeout':params.get('retry_on_timeout'),'Redis_methods_replaced':False})+'\n')
from normal_signals import install
observer=install(json.loads(Path('/evidence/pending-main-private.json').read_text()),'/evidence')
from dispatcharr.wsgi import application
from gevent import spawn
from gevent.pool import Pool
from gevent.pywsgi import WSGIServer

from status_app import app_for
status=app_for(observer,os.environ['NORMAL_FIXTURE_NONCE'])
spawn(WSGIServer(('127.0.0.1',6385),status,spawn=Pool(2),log=None,error_log=None).serve_forever)
WSGIServer(('127.0.0.1',5657),application,log=None,error_log=None).serve_forever()
