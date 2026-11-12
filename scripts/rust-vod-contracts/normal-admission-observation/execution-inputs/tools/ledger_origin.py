"""Owned deterministic MP4 origin; scalar bounded physical/request ledger.
Original fixture media/range algorithms retain Dispatcharr contributor credit.
"""
import hashlib,json,os,re,signal,socket,threading,time,uuid
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlsplit
from range_http import serve_file
ALIASES={'/movie/fixture/fixture/fixture.mp4':'shared_movie','/movie/fixture/fixture/fixture-idle-other.mp4':'other_movie','/movie/fixture/fixture/fixture-normal-probe1.mp4':'probe1_movie','/movie/fixture/fixture/fixture-normal-probe2.mp4':'probe2_movie'}
FILE_SHA='4f3236e89fd6d9fee4f18bb1cd1e9759113a204dc64b74efa7a704fd4c6fc11c'
class Ledger:
 def __init__(self,nonce,aliases=None):
  if type(nonce)is not str or not re.fullmatch('[0-9a-f]{32}',nonce):raise ValueError('OwnedOriginNonce')
  self.aliases=dict(ALIASES if aliases is None else aliases)
  if self.aliases!=ALIASES or len(set(self.aliases.values()))!=4:raise ValueError('ExactUniqueOwnedMediaPaths')
  self.nonce=nonce;self.epoch=uuid.uuid4().hex;self.lock=threading.RLock();self.invalid=[];self.connections={};self.requests={};self.events=[];self.next_connection=0;self.next_request=0
 def fail(self,reason):
  if reason not in self.invalid and len(self.invalid)<16:self.invalid.append(reason)
 def event(self,kind,**values):
  if len(self.events)>=512:self.fail('OriginEventCapacity');return
  self.events.append({'kind':kind,'at':time.monotonic(),**values})
 def opened(self,sock):
  with self.lock:
   if sum(not row['socket_closed']for row in self.connections.values())>=64 or len(self.connections)>=128:self.fail('OriginConnectionCapacity');raise RuntimeError('OriginConnectionCapacity')
   self.next_connection+=1;identity=self.next_connection;self.connections[identity]={'id':identity,'opened':time.monotonic(),'socket_closed':False,'peer_eof_observed':False,'requests':0,'fd':sock.fileno()};self.event('accepted_TCP',connection=identity);return identity
 def request(self,identity,method,path,preflight=False):
  with self.lock:
   if len(self.requests)>=64:self.fail('OriginRequestCapacity');raise RuntimeError('OriginRequestCapacity')
   self.next_request+=1;number=self.next_request;media=path in self.aliases
   kind='preflight_GET'if preflight and method=='GET'and media else'stream_GET'if method=='GET'and media else'media_HEAD'if method=='HEAD'and media else'control'if path in['/health','/stats','/ledger']else'unknown'
   self.connections[identity]['requests']+=1;self.requests[number]={'id':number,'connection':identity,'method':method,'path_role':self.aliases.get(path),'kind':kind,'began':time.monotonic(),'in_flight':True,'bytes_written':0,'returned':None,'error':None};self.event('request_begin',connection=identity,request=number,request_kind=kind);return number
 def wrote(self,number,count):
  with self.lock:
   if type(count)is not int or not 0<=count<=65536:self.fail('OriginWriteFrame');return
   self.requests[number]['bytes_written']+=count
   if self.requests[number]['bytes_written']>8388608:self.fail('OriginWireCapacity')
 def returned(self,number,error=None):
  with self.lock:
   row=self.requests[number];row.update(in_flight=False,returned=time.monotonic(),error=error);self.event('handler_return',request=number,connection=row['connection'])
 def peer_eof(self,identity,local_stop=False):
  with self.lock:self.connections[identity]['peer_eof_observed']=not local_stop;self.event('request_read_EOF',connection=identity,local_shutdown_active=local_stop)
 def closed(self,identity,fd_after):
  with self.lock:
   if type(fd_after)is not int or fd_after!=-1:self.fail('OriginSocketCloseUnacknowledged')
   self.connections[identity].update(socket_closed=fd_after==-1,closed=time.monotonic(),fd_after=fd_after);self.event('server_socket_shutdown_ack',connection=identity,fd_after=fd_after)
 def snapshot(self):
  with self.lock:
   rows=list(self.connections.values());requests=list(self.requests.values());value={'epoch':self.epoch,'nonce_sha256':hashlib.sha256(self.nonce.encode()).hexdigest(),'invalid':list(self.invalid),'accepted_TCP_total':len(rows),'actual_open_TCP':sum(not row['socket_closed']for row in rows),'actual_stream_GET_inflight':sum(row['in_flight']and row['kind']=='stream_GET'for row in requests),'media_HEAD_total':sum(row['kind']=='media_HEAD'for row in requests),'control_total':sum(row['kind']=='control'for row in requests),'transient_TCP_without_request':sum(not row['requests']for row in rows),'connections':rows,'requests':requests,'events':list(self.events),'semantics':'TCP lifetime, in-flight HTTP, bytes written and peer EOF are separate; none denotes viewer/generator progress'}
   if len(json.dumps(value).encode())>65536:self.fail('OriginStatusCapacity');return{'invalid':list(self.invalid),'epoch':self.epoch}
   return json.loads(json.dumps(value))
class Writer:
 def __init__(self,raw,rate,ledger,request):self.raw=raw;self.rate=rate;self.ledger=ledger;self.request=request;self.sent=0;self.began=time.monotonic()
 def __getattr__(self,name):return getattr(self.raw,name)
 def write(self,data):
  if len(data)>65536:raise ValueError('OriginWriteFrame')
  if self.rate:
   delay=self.began+(self.sent+len(data))/self.rate-time.monotonic()
   if delay>0:time.sleep(delay)
  value=self.raw.write(data);actual=len(data)if value is None else value;self.sent+=actual;self.ledger.wrote(self.request,actual);return value
class Server(ThreadingHTTPServer):
 daemon_threads=False;block_on_close=True
 def __init__(self,address,media,nonce,rate=32768):
  self.media=Path(media);self.file_sha=hashlib.sha256(self.media.read_bytes()).hexdigest();self.rate=rate;self.stopping=False;self.ledger=Ledger(nonce);self.owned={};self.owned_lock=threading.Lock();super().__init__(address,Handler)
 def get_request(self):
  sock,peer=super().get_request()
  try:identity=self.ledger.opened(sock)
  except BaseException:sock.close();raise
  with self.owned_lock:self.owned[id(sock)]=(sock,identity)
  return sock,peer
 def shutdown_request(self,request):
  with self.owned_lock:owned=self.owned.get(id(request))
  try:super().shutdown_request(request)
  finally:
   if owned:self.ledger.closed(owned[1],request.fileno())
   with self.owned_lock:self.owned.pop(id(request),None)
 def stop_owned(self):
  self.stopping=True
  with self.owned_lock:sockets=[row[0]for row in self.owned.values()]
  for sock in sockets:
   try:sock.shutdown(socket.SHUT_RDWR)
   except OSError:pass
  self.shutdown();self.server_close()
  if self.ledger.snapshot().get('actual_open_TCP')!=0:raise RuntimeError('OriginPhysicalStopIncomplete')
class Handler(BaseHTTPRequestHandler):
 protocol_version='HTTP/1.1'
 def log_message(self,*args):pass
 def handle_one_request(self):
  try:return super().handle_one_request()
  finally:
   if getattr(self,'raw_requestline',None)==b'':self.server.ledger.peer_eof(self.connection_id(),self.server.stopping)
 def connection_id(self):
  with self.server.owned_lock:return self.server.owned[id(self.connection)][1]
 def json(self,value,status=200):
  raw=json.dumps(value).encode()
  if len(raw)>65536:raise ValueError('OriginStatusCapacity')
  self.send_response(status);self.send_header('Content-Type','application/json');self.send_header('Content-Length',str(len(raw)));self.end_headers();self.wfile.write(raw)
 def dispatch(self,method):
  if isinstance(self.wfile,Writer):self.wfile=self.wfile.raw
  path=urlsplit(self.path).path;nonce=self.headers.get('X-Fixture-Nonce');number=self.server.ledger.request(self.connection_id(),method,path,nonce==self.server.ledger.nonce and self.headers.get('X-Fixture-Preflight')=='1');self._media_request=number if path in ALIASES and method=='GET'else None
  error=None
  try:
   if method not in['GET','HEAD']:self.send_error(405);return
   if path=='/ledger':
    if nonce!=self.server.ledger.nonce:return self.json({'error':'owned nonce required'},403)
    return self.json(self.server.ledger.snapshot())
   if path in['/health','/stats']:
    return self.json({'vod':{'fixture_sha256':self.server.file_sha,'fixture_bytes':self.server.media.stat().st_size,'etag':'"'+self.server.file_sha+'"'}})
   if path not in ALIASES:self.send_error(404);return
   return serve_file(self,self.server.media,'"'+self.server.file_sha+'"',head=method=='HEAD')
  except(OSError,ValueError)as failure:error=type(failure).__name__;self.close_connection=True
  finally:self.server.ledger.returned(number,error)
 def end_headers(self):
  super().end_headers()
  if getattr(self,'_media_request',None)is not None:self.wfile=Writer(self.wfile,self.server.rate,self.server.ledger,self._media_request)
 def do_GET(self):return self.dispatch('GET')
 def do_HEAD(self):return self.dispatch('HEAD')
 def do_POST(self):return self.dispatch('POST')

def main():
 import argparse
 parser=argparse.ArgumentParser();parser.add_argument('--media',type=Path,default=Path('/tmp/fixture.mp4'));parser.add_argument('--port',type=int,default=8080);parser.add_argument('--terminal',type=Path,required=True);args=parser.parse_args()
 nonce=os.environ.get('NORMAL_FIXTURE_NONCE','')
 if os.environ.get('PENDING_MAIN_FIXTURE')!='playback-fixture-only':raise RuntimeError('OwnedNormalOriginRequired')
 if hashlib.sha256(args.media.read_bytes()).hexdigest()!=FILE_SHA:raise ValueError('ExactFixtureMedia')
 server=Server(('0.0.0.0',args.port),args.media,nonce);stop=threading.Event()
 for event in[signal.SIGINT,signal.SIGTERM]:signal.signal(event,lambda *_:stop.set())
 thread=threading.Thread(target=server.serve_forever);thread.start()
 try:stop.wait()
 finally:
  server.stop_owned();thread.join(10);result=server.ledger.snapshot();result['server_thread_settled']=not thread.is_alive();args.terminal.write_text(json.dumps(result,indent=2)+'\n')
  if thread.is_alive()or result['invalid']:raise RuntimeError('OriginTerminalIncomplete')
if __name__=='__main__':main()
