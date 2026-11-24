"""Owned fixed redirect validation Origin; no app/bootstrap/socket on import."""
import argparse,http.client,http.server,json,os,re,signal,socket,threading,time
from pathlib import Path
def main():
 parser=argparse.ArgumentParser();parser.add_argument('--state',required=True);parser.add_argument('--upstream',required=True);parser.add_argument('--media-path',required=True);args=parser.parse_args();
 if args.upstream not in ('11.254.251.3','11.254.252.3')or not re.fullmatch(r'/live/[0-9]{1,20}\.ts',args.media_path):raise ValueError('OwnedStreamingOriginOnly')
 root=Path(args.state);stop=threading.Event();lock=threading.Lock();active=set();gate=threading.BoundedSemaphore(4);entries=[];held=set()
 def save(name,value):
  p=root/(name+'.tmp');p.write_text(json.dumps(value)+'\n');p.replace(root/(name+'.json'))
 class Handler(http.server.BaseHTTPRequestHandler):
  def log_message(self,*unused):pass
  def do_HEAD(self):self.requested()
  def do_GET(self):self.requested()
  def requested(self):
   try:
    match=re.fullmatch(r'/held/([a-f0-9]{32})',self.path)
    with lock:
     if len(entries)>=128:raise RuntimeError('OriginJournal128Bound')
     entries.append({'method':self.command,'path':self.path,'monotonic':time.monotonic()});save('requests',entries)
    ordered=re.fullmatch(r'/redirect-order/([a-f0-9]{32})/(peer|first|middle|last)[.]ts',self.path)
    if ordered:
     ident,target=ordered.groups()
     if target in ('peer','middle') and self.command=='HEAD':
      save(ident+'-'+target+'-order-entered',{'method':'HEAD','role':target})
      release=root/(ident+'.'+target+'-order-release')
      until=time.monotonic()+4.5
      while not stop.is_set() and not release.exists() and time.monotonic()<until:stop.wait(.01)
      released=release.exists()
      save(ident+'-'+target+'-order-finished',{'released_by_fixture':released})
      code=(204 if target=='peer' else 405) if released else 502
     elif target in ('first','last') and self.command=='HEAD':code=204
     else:code=502
     self.send_response(code);self.send_header('Content-Length','0');self.send_header('Connection','close');self.end_headers()
     return
    single=re.fullmatch(r'/same-slot/([a-f0-9]{32})/(primary|fallback)[.]ts',self.path)
    if single:
     ident,scenario=single.groups()
     if scenario=='primary':
      code=405 if self.command=='HEAD' else 502
     elif self.command=='HEAD':
      save(ident+'-same-slot-entered',{'method':'HEAD','backup':True})
      until=time.monotonic()+4.5
      while not stop.is_set() and not(root/(ident+'.same-slot-release')).exists() and time.monotonic()<until:stop.wait(.01)
      released=(root/(ident+'.same-slot-release')).exists()
      save(ident+'-same-slot-finished',{'released_by_fixture':released})
      code=204 if released else 502
     else:code=404
     self.send_response(code);self.send_header('Content-Length','0');self.send_header('Connection','close');self.end_headers()
     return
    pooled=re.fullmatch(r'/pooled-frame/([a-f0-9]{32})/(primary[.]ts[?]Policy=owned%2Fsample~&Signature=abc[+]def%2Bgh%3D|fallback[.]ts)',self.path)
    if pooled and pooled[2].startswith('primary'):
     if self.command=='HEAD':
      self.send_response(405);self.send_header('Content-Length','0');self.send_header('Connection','close');self.end_headers()
      return
     self.send_response(200);self.send_header('Content-Length','1880');self.send_header('Connection','close');self.end_headers()
     self.wfile.write(b'x'*32);self.wfile.flush()
     ident=pooled[1]
     save(ident+'-pooled-entered',{'method':'GET','initial_bytes':32,'declared_bytes':1880})
     until=time.monotonic()+4.5
     while not stop.is_set() and not(root/(ident+'.pooled-release')).exists() and time.monotonic()<until:stop.wait(.01)
     save(ident+'-pooled-closed',{'released_by_fixture':(root/(ident+'.pooled-release')).exists()})
     return
    if pooled and pooled[2]=='fallback.ts':
     self.send_response(200);self.send_header('Content-Length','0');self.send_header('Connection','close');self.end_headers()
     return
    if match and self.command=='HEAD':
     ident=match.group(1)
     with lock:
      initial=ident not in held
      held.add(ident)
     if initial:
      save(ident+'-entered',{'method':self.command,'pid':os.getpid(),'first_validation_request':True})
      until=time.monotonic()+4.5
      while not stop.is_set()and not(root/(ident+'.release')).exists()and time.monotonic()<until:stop.wait(.01)
    if match and self.command=='GET':
     upstream=http.client.HTTPConnection(args.upstream,8080,timeout=3)
     try:
      headers={'Host':'origin:8080','Connection':'close'}
      agent=self.headers.get('User-Agent')
      if agent is not None:
       if len(agent)>4096:raise OSError('OwnedUserAgentBound')
       headers['User-Agent']=agent
      upstream.request('GET',args.media_path,headers=headers);reply=upstream.getresponse()
      if reply.status!=200:raise OSError('OwnedMediaOriginNotReady')
      self.send_response(200);self.send_header('Content-Type','video/mp2t');self.send_header('Connection','close');self.end_headers()
      total=0;until=time.monotonic()+120
      while not stop.is_set()and time.monotonic()<until:
       data=reply.read1(65536)
       if not data:break
       total+=len(data)
       if total>64*1024*1024:raise OSError('OwnedMedia64MiBBound')
       self.wfile.write(data);self.wfile.flush()
     finally:upstream.close()
     return
    if match or self.path=='/ok':code,body=200,b'owned redirect validation useful byte'
    elif self.path=='/empty':code,body=200,b''
    else:code,body=404,b''
    self.send_response(code);self.send_header('Content-Length',str(len(body)));self.send_header('Connection','close');self.end_headers()
    if self.command=='GET':self.wfile.write(body)
   except (OSError,RuntimeError):pass
 class Server(http.server.ThreadingHTTPServer):
  daemon_threads=False
  def process_request(self,request,address):
   if not gate.acquire(False):request.close();return
   request.settimeout(2)
   with lock:active.add(request)
   try:super().process_request(request,address)
   except BaseException:
    with lock:active.discard(request)
    gate.release();raise
  def process_request_thread(self,request,address):
   try:super().process_request_thread(request,address)
   finally:
    with lock:active.discard(request)
    gate.release()
 server=Server(('127.0.0.1',18099),Handler);server.timeout=.25
 signal.signal(signal.SIGTERM,lambda *unused:stop.set());signal.signal(signal.SIGINT,lambda *unused:stop.set())
 save('ready',{'pid':os.getpid(),'listener':server.socket.getsockname()})
 until=time.monotonic()+300
 try:
  while not stop.is_set()and time.monotonic()<until:server.handle_request()
 finally:
  stop.set()
  with lock:remaining=list(active)
  for conn in remaining:
   try:conn.shutdown(socket.SHUT_RDWR)
   except OSError:pass
   conn.close()
  server.server_close();save('final',{'active':len(active),'threads_joined':not active,'requests':entries})
  if active:raise RuntimeError('ActualOriginHandlerNotJoined')
if __name__=='__main__':main()
