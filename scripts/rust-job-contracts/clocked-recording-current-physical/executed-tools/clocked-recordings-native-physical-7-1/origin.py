"""Owned synthetic TS origin. No listener/media process starts on import."""
import hashlib,http.server,json,os,signal,socket,threading,time
from pathlib import Path
from urllib.parse import urlsplit
from bounded_process import Collected
from process_observer import starttime
from seed_spec import INPUT_SECONDS,ORIGIN_MAX_REQUESTS,ORIGIN_BODY_LIMIT,expected_url_path
def target(path,channel_uuid):
 parsed=urlsplit(path)
 return parsed.scheme==parsed.netloc==parsed.fragment==parsed.query==''and parsed.path==expected_url_path(channel_uuid)
def write(path,value):
 data=(json.dumps(value,indent=2,allow_nan=False)+'\n').encode()
 if len(data)>8388608:raise ValueError('OriginEvidence8MiB')
 temporary=path.with_suffix(path.suffix+'.pending');temporary.write_bytes(data);os.replace(temporary,path)
def main():
 root=Path('/evidence');nonce=os.environ['CLOCKED_NONCE']
 if len(nonce)!=32 or any(c not in'0123456789abcdef'for c in nonce):raise ValueError('OwnedNonce')
 media=Path('/tmp/owned-clocked-input.ts');stop=threading.Event();lock=threading.RLock();rows=[];invalid=[];sockets=set();handlers=set();slots=threading.BoundedSemaphore(2)
 def halted():stop.set()
 argv=['ffmpeg','-nostdin','-hide_banner','-loglevel','error','-f','lavfi','-i','testsrc2=size=160x90:rate=25','-t',str(INPUT_SECONDS),'-an','-c:v','mpeg2video','-b:v','256k','-threads','1','-f','mpegts',str(media)]
 generation=Collected(argv,dict(os.environ),1048576,halted,stdout_path=root/'origin-generate.stdout',stderr_path=root/'origin-generate.stderr');began=time.monotonic();original_start=starttime(generation.process.pid)
 try:generation.wait(45)
 finally:
  write(root/'origin-generation.json',{'argv':argv,'pid':generation.process.pid,'starttime':original_start,'actual_exit':generation.poll(),'physically_settled':generation.physically_settled,'pipes_settled':not any(t.is_alive()for t in generation.threads),'invalid':generation.invalid,'began_monotonic':began,'finished_monotonic':time.monotonic()})
 if not(media.is_file()and 0<media.stat().st_size<=ORIGIN_BODY_LIMIT):raise ValueError('OriginInputSize')
 data=media.read_bytes();rate=len(data)/INPUT_SECONDS
 (root/'owned-origin-input.ts').write_bytes(data)
 class Server(http.server.ThreadingHTTPServer):
  daemon_threads=False;block_on_close=False
  def process_request_thread(self,request,address):
   with lock:handlers.add(threading.current_thread())
   try:super().process_request_thread(request,address)
   finally:
    with lock:handlers.discard(threading.current_thread())
 class Handler(http.server.BaseHTTPRequestHandler):
  protocol_version='HTTP/1.1'
  def log_message(self,*unused):pass
  def do_GET(self):
   if not slots.acquire(blocking=False):self.send_error(503);return
   with lock:sockets.add(self.connection)
   self.connection.settimeout(3)
   try:
    if self.path=='/__owned_state__'and self.headers.get('X-Owned-Nonce')==nonce:
     with lock:body=json.dumps({'rows':rows,'invalid':invalid,'media_bytes':len(data),'media_sha256':hashlib.sha256(data).hexdigest()}).encode()
     if len(body)>1048576:raise ValueError('OriginState1MiB')
     self.send_response(200);self.send_header('Content-Length',str(len(body)));self.send_header('Content-Type','application/json');self.send_header('Connection','close');self.end_headers();self.close_connection=True;self.wfile.write(body);return
    config=json.loads((root/'origin-target.json').read_text())
    headers=list(self.headers.items())
    if len(headers)>128 or sum(len(k)+len(v)for k,v in headers)>65536:raise ValueError('OriginHeaderCapacity')
    row={'method':'GET','path':self.path,'headers':headers,'peer':list(self.client_address),'began_monotonic':time.monotonic(),'bytes_sent':0,'status':None,'terminal':None}
    with lock:
     if len(rows)>=ORIGIN_MAX_REQUESTS:raise ValueError('OriginRequestCount4')
     rows.append(row)
    if not target(self.path,config['channel_uuid']):
     with lock:invalid.append({'path_sha256':hashlib.sha256(self.path.encode()).hexdigest(),'reason':'UnknownOwnedTarget'})
     with lock:row['status']=404
     self.send_error(404);return
    self.send_response(200);self.send_header('Content-Type','video/mp2t');self.send_header('Connection','close');self.end_headers()
    with lock:row['status']=200
    self.close_connection=True;started=time.monotonic();offset=0
    try:
     while not stop.is_set()and time.monotonic()-started<45 and offset<len(data):
      chunk=data[offset:offset+1316];self.wfile.write(chunk);self.wfile.flush();offset+=len(chunk)
      with lock:row['bytes_sent']=offset
      if offset>ORIGIN_BODY_LIMIT:raise ValueError('OriginBodyCapacity')
      wait=started+offset/rate-time.monotonic()
      if wait>0:stop.wait(wait)
     with lock:row['terminal']='owned_duration_or_input_limit'
    except(BrokenPipeError,ConnectionResetError,socket.timeout):
     with lock:row['terminal']='client_closed_or_write_timeout'
    finally:
     with lock:row['finished_monotonic']=time.monotonic()
   except BaseException as error:
    with lock:invalid.append({'error':type(error).__name__,'text':str(error)[:256]})
   finally:
    with lock:
     sockets.discard(self.connection);write(root/'origin-state-final.json',{'rows':rows,'invalid':invalid,'stopped':stop.is_set()})
    slots.release()
 server=Server(('0.0.0.0',8080),Handler);thread=threading.Thread(target=server.serve_forever);thread.start()
 write(root/'origin-ready.json',{'passed':True,'actual_generated_input_bytes':len(data),'sha256':hashlib.sha256(data).hexdigest(),'input_seconds':INPUT_SECONDS,'pace_bytes_per_second':rate,'listener':8080,'pid':os.getpid()})
 for signum in(signal.SIGTERM,signal.SIGINT):signal.signal(signum,lambda *_:stop.set())
 try:
  while not stop.wait(.2):
   if not thread.is_alive():raise RuntimeError('OriginListenerUnexpectedExit')
 finally:
  stop.set();server.shutdown();server.server_close()
  with lock:active=list(sockets)
  for value in active:
   try:value.shutdown(socket.SHUT_RDWR)
   except OSError:pass
  deadline=time.monotonic()+5;thread.join(max(0,deadline-time.monotonic()))
  with lock:pending=list(handlers)
  for handler in pending:handler.join(max(0,deadline-time.monotonic()))
  write(root/'origin-close.json',{'listener_settled':not thread.is_alive(),'handler_connections':len(sockets),'handler_threads_settled':not handlers,'rows':rows,'invalid':invalid})
 if thread.is_alive()or sockets or handlers or invalid:raise RuntimeError('OriginUnsettledOrInvalid')
if __name__=='__main__':main()
