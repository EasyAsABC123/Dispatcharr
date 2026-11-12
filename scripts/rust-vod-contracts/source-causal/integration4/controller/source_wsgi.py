"""Owned Original WSGI startup: causal hooks installed before application import."""
from gevent import monkey;monkey.patch_all()
import json,os,sys
from pathlib import Path
if os.environ.get('PENDING_MAIN_FIXTURE')!='playback-fixture-only'or os.environ.get('CAUSAL_REVIEWED_EXECUTION')!='one-fixed-Source-busy-cohort':raise RuntimeError('OwnedReviewedSourceRequired')
# The parent executable requires a fresh reviewed cue and claims it before any
# Compose dispatch. This additional startup guard binds the actual mounted tools.
from activation import child_guard
child_guard('/evidence/execution-cue.json','/pending-tools')
sys.path.insert(0,'/app');sys.argv.append('shell');os.environ.setdefault('DJANGO_SETTINGS_MODULE','dispatcharr.settings')
import source_install
source_install.EXECUTION_BLOCKED=False
binding=json.loads(Path('/pending-tools/redis-library.json').read_text())
observer=source_install.install(json.loads(Path('/evidence/pending-main-private.json').read_text()),'busy','/evidence',binding['files'],6383)
# Nothing above imports Django app modules; django.core.signals import happens
# inside installation only AFTER tracer construction. The original app setup and
# WSGI callable are unchanged, and every six delayed callback code remains original.
import django;django.setup()
from core.utils import RedisClient
from django.conf import settings
client=RedisClient.get_client();params=client.connection_pool.connection_kwargs
if params.get('port')!=6383 or params.get('socket_timeout')!=60 or params.get('socket_connect_timeout')!=5 or params.get('retry_on_timeout')is not True:raise RuntimeError('GenuineSourceProxyTimeoutBinding')
Path('/evidence/Source-client-config.json').write_text(json.dumps({'socket_timeout':params['socket_timeout'],'connect_timeout':params['socket_connect_timeout'],'retry_on_timeout':params['retry_on_timeout'],'proxy_port':params['port'],'method_replaced':False},indent=2)+'\n')
from dispatcharr.wsgi import application
from request_observer import install
signals=install(json.loads(Path('/evidence/pending-main-private.json').read_text()),'/evidence')
def observer_status(environ,start_response):
 if environ.get('REMOTE_ADDR')!='127.0.0.1'or environ.get('REQUEST_METHOD')!='GET'or environ.get('PATH_INFO')!='/state':
  start_response('404 Not Found',[('Content-Length','0')]);return[b'']
 raw=json.dumps({'causal':observer.snapshot(),'signals':signals.journal.snapshot()}).encode()
 if len(raw)>8192:observer.journal.fail('CombinedStatusCapacity');raise ValueError('CombinedStatusCapacity')
 start_response('200 OK',[('Content-Type','application/json'),('Content-Length',str(len(raw)))]);return[raw]
from gevent import spawn
from gevent.pool import Pool
from gevent.pywsgi import WSGIServer
spawn(WSGIServer(('127.0.0.1',6385),observer_status,spawn=Pool(2),log=None,error_log=None).serve_forever)
# Terminal read-only status includes bounded rule hits. No forced GC/closer calls.
def terminal_status(environ,start_response):
 if environ.get('REMOTE_ADDR')!='127.0.0.1'or environ.get('REQUEST_METHOD')!='GET'or environ.get('PATH_INFO')!='/terminal':
  start_response('404 Not Found',[('Content-Length','0')]);return[b'']
 raw=json.dumps(observer.terminal()).encode()
 if len(raw)>65536:observer.journal.fail('TerminalStatusCapacity');raise ValueError('TerminalStatusCapacity')
 start_response('200 OK',[('Content-Type','application/json'),('Content-Length',str(len(raw)))]);return[raw]
spawn(WSGIServer(('127.0.0.1',6386),terminal_status,spawn=Pool(1),log=None,error_log=None).serve_forever)
WSGIServer(('127.0.0.1',5657),application,log=None,error_log=None).serve_forever()
