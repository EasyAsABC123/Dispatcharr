"""Owned decode-capable artwork origin; never forwards a provider request.
Original Dispatcharr authors retain credit. Private counts contain no credentials.
"""
import json,sys,threading
from pathlib import Path
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
counts=Path(sys.argv[1]); lock=threading.Lock(); observations=[]
class Handler(BaseHTTPRequestHandler):
 def do_GET(self):
  allowed=self.path in ['/poster-a.svg','/poster-b.svg','/episode-a.svg','/episode-b.svg']
  with lock:
   observations.append({'method':self.command,'path':self.path,'allowed':allowed})
   counts.write_text(json.dumps(observations))
  body=('<svg xmlns="http://www.w3.org/2000/svg" width="2" height="3"><rect width="2" height="3" fill="'+('red'if self.path.endswith('a.svg')else'blue')+'"/></svg>').encode() if allowed else b'Forbidden provider contact'
  self.send_response(200 if allowed else 503);self.send_header('Content-Type','image/svg+xml' if allowed else 'text/plain');self.send_header('Content-Length',str(len(body)));self.end_headers();self.wfile.write(body)
 def do_POST(self):self.do_GET()
 def log_message(self,*args):pass
ThreadingHTTPServer(('0.0.0.0',18881),Handler).serve_forever()
