"""Bounded fixture-only image origin; never contacts external services.
Original Dispatcharr contributors retain application credit.
"""
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
from collections import Counter
import gzip,json,threading,time
from urllib.parse import urlsplit
counts=Counter();lock=threading.Lock()
png=b'\x89PNG\r\n\x1a\nfixture-image';svg=b'\xef\xbb\xbf <!--fixture-->\n<svg xmlns="http://www.w3.org/2000/svg"/>'
class Handler(BaseHTTPRequestHandler):
 def log_message(self,*args):pass
 def do_GET(self):
  path=urlsplit(self.path).path
  if path=='/counts':
   with lock:body=json.dumps(dict(counts)).encode()
   self.send_response(200);self.send_header('Content-Length',str(len(body)));self.end_headers();self.wfile.write(body);return
  if path=='/reset':
   with lock:counts.clear()
   self.send_response(204);self.end_headers();return
  with lock:counts[path]+=1
  try:self.respond(path)
  except (BrokenPipeError,ConnectionResetError):pass
 def respond(self,path):
  if path=='/headers-stall':time.sleep(6)
  if path.startswith('/redirect/'):
   value=path.removeprefix('/redirect/');self.send_response(302)
   if value=='unsafe':location='http://127.0.0.1/fixture-secret'
   elif value.isdigit():location='/redirect/'+str(int(value)-1)if int(value)>1 else '/image/png'
   elif value=='missing':location=None
   else:location='/image/'+value
   if location is not None:self.send_header('Location',location)
   self.send_header('Content-Length','0');self.end_headers();return
  bodies={'png':png,'jpeg':b'\xff\xd8\xfffixture-jpeg','gif':b'GIF89afake-gif','webp':b'RIFF1234WEBPdata','ico':b'\x00\x00\x01\x00ico','bmp':b'BM1234','svg':svg,'html':b'<html><svg/></html>','unknown':b'not-an-image','empty':b'','agent':png+self.headers.get('User-Agent','').encode()}
  kind=path.rsplit('/',1)[-1];body=bodies.get(kind,png);headers={}
  if path=='/limit/exact':body=png+b'x'*(5*1024*1024-len(png))
  if path=='/limit/over':body=png+b'x'*(5*1024*1024-len(png)+1)
  if path=='/gzip':body=gzip.compress(png);headers['Content-Encoding']='gzip'
  if path=='/bad-gzip':body=b'not-gzip';headers['Content-Encoding']='gzip'
  if path=='/truncated':headers['Content-Length']='999'
  if path=='/status/404':self.send_response(404)
  else:self.send_response(200)
  if path=='/drip':body=png+b'x'*(8192*6-len(png))
  self.send_header('Content-Type','text/html');self.send_header('Cache-Control','public, max-age=123');self.send_header('Last-Modified','Tue, 14 Nov 2023 22:13:20 GMT')
  self.send_header('Content-Length',headers.pop('Content-Length',str(len(body))))
  for k,v in headers.items():self.send_header(k,v)
  self.end_headers()
  if path=='/stall':self.wfile.write(body[:8]);self.wfile.flush();time.sleep(6);self.wfile.write(body[8:]);return
  if path=='/drip':
   for offset in range(0,len(body),8192):time.sleep(2);self.wfile.write(body[offset:offset+8192]);self.wfile.flush()
   return
  self.wfile.write(body);self.close_connection=True
ThreadingHTTPServer(('0.0.0.0',8080),Handler).serve_forever()
