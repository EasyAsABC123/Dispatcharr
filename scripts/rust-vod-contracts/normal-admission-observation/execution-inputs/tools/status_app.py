"""Bounded nonce-scoped status WSGI app, independent of main application."""
import json,re,time

def app_for(observer,nonce):
 if type(nonce)is not str or not re.fullmatch('[0-9a-f]{32}',nonce):raise ValueError('OwnedStatusNonce')
 def app(environ,start_response):
  if environ.get('REMOTE_ADDR')!='127.0.0.1'or environ.get('REQUEST_METHOD')!='GET'or environ.get('PATH_INFO')!='/state'or environ.get('HTTP_X_FIXTURE_NONCE')!=nonce:
   start_response('403 Forbidden',[('Content-Length','0')]);return[b'']
  value={**observer.journal.snapshot(),'monotonic':time.monotonic()};raw=json.dumps(value).encode()
  if len(raw)>4096:
   observer.journal.fail('NormalStatusCapacity');raw=b'{"invalid":["NormalStatusCapacity"]}'
  start_response('200 OK',[('Content-Type','application/json'),('Content-Length',str(len(raw)))]);return[raw]
 return app
