"""Owned HTTP M3U input in the existing milestone namespace; no app task replay."""
import hashlib,json,os,signal
from http.server import BaseHTTPRequestHandler,HTTPServer
from pathlib import Path
PORT=18098;COUNT=6001
class Server(HTTPServer):
    phase=0
    requests=[]
class Handler(BaseHTTPRequestHandler):
    def log_message(self,*args):pass
    def do_GET(self):
        if self.path=='/snapshot':
            raw=json.dumps({'phase':self.server.phase,'requests':self.server.requests}).encode()
        elif self.path=='/regex.m3u':
            names=['Auto 00001','Auto 00002','foo','Straße','a'*511+'b']
            lines=['#EXTM3U']
            for i,name in enumerate(names,1):
                lines.extend([f'#EXTINF:-1 group-title="Owned Regex",{name}',f'http://127.0.0.1:{PORT}/unrequested-regex/{i}.ts'])
            raw=('\n'.join(lines)+'\n').encode()
            self.server.requests.append({'method':'GET','path':self.path,'bytes':len(raw),'sha256':hashlib.sha256(raw).hexdigest()})
        elif self.path=='/owned.m3u':
            count=COUNT if self.server.phase==0 else 5001
            lines=['#EXTM3U']
            for i in range(1,count+1):
                lines.extend([f'#EXTINF:-1 tvg-id="auto-epg-{i%3}" tvg-chno="{20000+i}" group-title="Owned Auto",Auto {i:05d}',f'http://127.0.0.1:{PORT}/unrequested-media/{i}.ts'])
            raw=('\n'.join(lines)+'\n').encode()
            self.server.requests.append({'method':'GET','path':self.path,'phase':self.server.phase,'bytes':len(raw),'sha256':hashlib.sha256(raw).hexdigest()})
        else:self.send_error(404);return
        self.send_response(200);self.send_header('Content-Length',str(len(raw)));self.end_headers();self.wfile.write(raw)
    def do_POST(self):
        if self.path!='/phase1':self.send_error(404);return
        self.server.phase=1;self.send_response(204);self.send_header('Content-Length','0');self.end_headers()
def main():
    server=Server(('127.0.0.1',PORT),Handler)
    server.timeout=.25
    running=[True]
    def stop(*_):running[0]=False
    signal.signal(signal.SIGTERM,stop);signal.signal(signal.SIGINT,stop)
    root=Path(os.environ['AUTO_SYNC_DATA']);ready=root/'auto-origin.ready';ready.write_text('actual listener bound');ready.chmod(0o444)
    try:
        while running[0]:server.handle_request()
    finally:
        server.server_close();(root/'auto-origin-stop.json').write_text(json.dumps({'actual_signal':not running[0],'server_closed':True,'requests':server.requests}))
if __name__=='__main__':main()
