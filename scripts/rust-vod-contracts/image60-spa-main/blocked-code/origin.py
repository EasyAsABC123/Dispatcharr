"""No-media trap. Actual control/TCP coverage, finite journal and graceful close."""
import hashlib,json,os,re,signal,threading,time
from pathlib import Path
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer

class Trap:
 def __init__(self,nonce,root,host='0.0.0.0',port=8080):
  if re.fullmatch('[a-f0-9]{32}',nonce)is None:raise ValueError('OwnedNonce')
  self.nonce=nonce;self.root=Path(root);self.lock=threading.RLock();self.rows=[];self.invalid=[];self.connections=[];self.sockets={};self.active=set();self.stop=threading.Event();trap=self
  class Handler(BaseHTTPRequestHandler):
   def log_message(self,*args):pass
   def do_GET(self):
    control=self.path=='/__capture_state__'and self.headers.get('X-Fixture-Nonce')==trap.nonce
    with trap.lock:
     row=trap.sockets[self.request];row['requests']+=1;row['controls']+=int(control)
     if not control:
      if len(trap.rows)>=128:trap.invalidate('OriginJournalCapacity')
      else:trap.rows.append({'method':self.command,'path_sha256':hashlib.sha256(self.path.encode()).hexdigest(),'connection':row['id']})
    raw=json.dumps(trap.snapshot()).encode()if control else b''
    self.send_response(200 if control else 503);self.send_header('Content-Length',str(len(raw)));self.send_header('Connection','close');self.end_headers()
    if raw:self.wfile.write(raw)
   do_HEAD=do_GET;do_POST=do_GET;do_PUT=do_GET;do_PATCH=do_GET;do_DELETE=do_GET;do_OPTIONS=do_GET;do_TRACE=do_GET
  class Server(ThreadingHTTPServer):
   daemon_threads=False;block_on_close=True;request_queue_size=8
   slots=threading.BoundedSemaphore(8)
   def get_request(self):
    sock,address=super().get_request();sock.settimeout(3)
    with trap.lock:
     if len(trap.connections)>=128:trap.invalidate('OriginTCPJournalCapacity');sock.close();raise OSError('OriginTCPJournalCapacity')
     row={'id':len(trap.connections)+1,'accepted_monotonic':time.monotonic(),'socket_closed':False,'requests':0,'controls':0};trap.connections.append(row);trap.sockets[sock]=row
    return sock,address
   def process_request(self,request,address):
    if not self.slots.acquire(False):trap.invalidate('OriginConcurrencyCapacity');self.shutdown_request(request);return
    try:super().process_request(request,address)
    except BaseException:self.slots.release();raise
   def process_request_thread(self,request,address):
    thread=threading.current_thread()
    with trap.lock:trap.active.add(thread)
    try:super().process_request_thread(request,address)
    finally:
     with trap.lock:trap.active.discard(thread);trap.sockets[request]['socket_closed']=request.fileno()==-1
     self.slots.release()
  self.server=Server((host,port),Handler);self.worker=threading.Thread(target=self.server.serve_forever,name='owned-origin-listener')
 def invalidate(self,kind):
  with self.lock:
   if not self.invalid:self.invalid.append(kind)
 def snapshot(self):
  with self.lock:
   return {'rows':list(self.rows),'invalid':list(self.invalid),'nonce_sha256':hashlib.sha256(self.nonce.encode()).hexdigest(),'PID':os.getpid(),'starttime':Path('/proc/self/stat').read_text().rsplit(')',1)[1].split()[19],'listener_ready_actual':True,'active_handler_threads':len(self.active),'connections':[dict(r)for r in self.connections],'accepted_TCP_total':len(self.connections),'control_requests':sum(r['controls']for r in self.connections),'unclassified_connections':sum(r['requests']==0 for r in self.connections)}
 def save(self,name,value):
  raw=(json.dumps(value,indent=2)+'\n').encode()
  if len(raw)>65536:raise ValueError('OriginJournalByteCapacity')
  p=self.root/name;p.write_bytes(raw);p.chmod(0o644)
 def start(self):self.worker.start();self.save('origin-ready.json',self.snapshot())
 def close(self):
  self.server.shutdown();self.worker.join(5);self.server.server_close();value=self.snapshot();value.update(shutdown_acknowledged=True,socket_listener_closed=self.server.socket.fileno()==-1,handler_threads_joined=not self.worker.is_alive()and not self.active);self.save('origin-terminal.json',value)
  if self.worker.is_alive()or self.active:raise RuntimeError('OriginPhysicalStopIncomplete')
  return value

def main():
 if os.environ.get('SPA_FIXTURE_AUTHORITY')!='owned-native-spa-get-only':raise RuntimeError('OwnedOriginAuthority')
 # Guest UID65532 must read every minimal exported dependency before listening.
 inputs=json.loads(Path('/spa-tools/guest-inputs.json').read_text());readability=[]
 for name,want in inputs['files'].items():
  p=Path('/spa-tools')/name
  if not p.is_file()or p.is_symlink()or hashlib.sha256(p.read_bytes()).hexdigest()!=want:raise RuntimeError('ExactGuestReadableInput:'+name)
  readability.append(name)
 trap=Trap(os.environ['CAPTURE_NONCE'],Path('/origin-evidence'));trap.save('origin-guest-readable.json',{'UID':os.getuid(),'files':readability,'all_exact_readable':True});serve(trap)
def serve(trap):
 for sig in [signal.SIGTERM,signal.SIGINT]:signal.signal(sig,lambda *_:trap.stop.set())
 trap.start()
 try:trap.stop.wait()
 finally:trap.close()
if __name__=='__main__':main()
