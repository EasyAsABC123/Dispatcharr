"""No-media owned provider-contact trap; bounded journal, no application requests."""
import hashlib,json,os,threading
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
nonce=os.environ['CAPTURE_NONCE'];rows=[];lock=threading.Lock();invalid=[]
class Handler(BaseHTTPRequestHandler):
 def log_message(self,*args):pass
 def do_GET(self):
  if self.path=='/__capture_state__'and self.headers.get('X-Fixture-Nonce')==nonce:
   with lock:body=json.dumps({'rows':list(rows),'invalid':list(invalid),'nonce_sha256':hashlib.sha256(nonce.encode()).hexdigest()}).encode()
   self.send_response(200);self.send_header('Content-Length',str(len(body)));self.end_headers();self.wfile.write(body);return
  with lock:
   if len(rows)>=128:invalid.append('OriginJournalCapacity')if not invalid else None
   else:rows.append({'method':self.command,'path_sha256':hashlib.sha256(self.path.encode()).hexdigest()})
  self.send_response(503);self.send_header('Content-Length','0');self.end_headers()
 do_HEAD=do_GET
 do_POST=do_GET
 do_PUT=do_GET
 do_PATCH=do_GET
 do_DELETE=do_GET
 do_OPTIONS=do_GET
class Server(ThreadingHTTPServer):
 daemon_threads=True
 request_queue_size=8
 slots=threading.BoundedSemaphore(8)
 def get_request(self):
  sock,address=super().get_request();sock.settimeout(3);return sock,address
 def process_request(self,request,address):
  if not self.slots.acquire(False):
   with lock:
    if not invalid:invalid.append('OriginConnectionCapacity')
   self.shutdown_request(request);return
  try:super().process_request(request,address)
  except BaseException:self.slots.release();raise
 def process_request_thread(self,request,address):
  try:super().process_request_thread(request,address)
  finally:self.slots.release()
Server(('0.0.0.0',8080),Handler).serve_forever()
