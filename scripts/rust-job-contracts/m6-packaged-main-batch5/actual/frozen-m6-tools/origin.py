"""Owned fixed-response dependency only; no Original application execution."""
import base64,http.server,json,os,signal,threading,time
from pathlib import Path
ROOT=Path('/evidence');stop=threading.Event();lock=threading.Lock();rows=[]
class Handler(http.server.BaseHTTPRequestHandler):
 def do_GET(self):
  self.connection.settimeout(5);config=json.loads((ROOT/'version-response.json').read_text());body=base64.b64decode(config['body_base64'],validate=True);status=config['status']
  if self.path!='/owned-version'or len(body)>8192:status,body=400,b'outside owned route'
  row={'epoch':time.time(),'path':self.path,'headers':list(self.headers.items()),'status':status,'body_base64':base64.b64encode(body).decode(),'attempted':True,'completed':False}
  try:self.send_response(status);self.send_header('Content-Type','application/json');self.send_header('Content-Length',str(len(body)));self.end_headers();self.wfile.write(body);self.wfile.flush();row['completed']=True
  finally:
   with lock:
    rows.append(row)
    with(ROOT/'origin-requests.jsonl').open('ab')as f:f.write((json.dumps(row)+'\n').encode());f.flush();os.fsync(f.fileno())
 def log_message(self,*args):pass
server=http.server.ThreadingHTTPServer(('127.0.0.1',9193),Handler);server.daemon_threads=False
for sig in(signal.SIGTERM,signal.SIGINT):signal.signal(sig,lambda *_:stop.set())
thread=threading.Thread(target=server.serve_forever);thread.start();(ROOT/'origin-ready').write_text('owned HTTP dependency ready')
stop.wait();server.shutdown();server.server_close();thread.join(5);(ROOT/'origin-stop.json').write_text(json.dumps({'thread_settled':not thread.is_alive(),'actual_requests':len(rows)}));raise SystemExit(0 if not thread.is_alive()else 1)
