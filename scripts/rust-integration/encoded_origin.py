"""Paced real H264 TS with streaming HTTP encodings and redirect-cookie fixtures.

Original application credit remains with Dispatcharr's authors. The FFmpeg
fixture command is imported unchanged from the shared baseline tool.
"""
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
import json
from pathlib import Path
import re
import subprocess
import sys
import threading
import time
import urllib.parse
import zlib
sys.path.insert(0,'/verification')
from origin import fixture_command,file_sha256,RATE,PACKET_CHUNK
LOCK=threading.Lock();MODES={};CLIP=Path('/tmp/fixture.ts')
STATS={'active':0,'maximum_active':0,'connections':0,'disconnections':0,'requests':[],'paths':{},'decoded_bytes':0,'wire_bytes':0}


class Handler(BaseHTTPRequestHandler):
    protocol_version='HTTP/1.1'
    def log_message(self,*_):pass
    def json(self,value,status=200):
        data=json.dumps(value).encode();self.send_response(status);self.send_header('Content-Type','application/json');self.send_header('Content-Length',str(len(data)));self.end_headers();self.wfile.write(data)
    def do_POST(self):
        size=int(self.headers.get('Content-Length','0'))
        if self.path!='/control' or not 0<size<=4096:return self.json({'error':'bad control'},400)
        value=json.loads(self.rfile.read(size));path=value.get('path','');mode=value.get('mode')
        if not re.fullmatch(r'/live/\d+\.ts',path) or mode not in ('cookie-gzip','identity-clean','deflate','raw','stacked'):return self.json({'error':'bad control'},400)
        with LOCK:MODES[path]=mode
        self.json({'applied':True})
    def do_GET(self):
        path=urllib.parse.urlsplit(self.path).path
        if path in ('/health','/stats'):
            with LOCK:snapshot=json.loads(json.dumps(STATS))
            return self.json(snapshot)
        original=re.fullmatch(r'/live/(\d+)\.ts',path);encoded=re.fullmatch(r'/encoded/([^/]+)/(\d+)\.ts',path)
        if not original and not encoded:return self.json({'error':'not found'},404)
        canonical='/live/'+(original.group(1) if original else encoded.group(2))+'.ts'
        with LOCK:
            mode=MODES.get(canonical,'identity-clean')
            if len(STATS['requests'])>=256:return self.json({'error':'request bound'},503)
            row={'path':path,'canonical':canonical,'mode':mode,'cookie_present':bool(self.headers.get('Cookie')),'cookie_valid':self.headers.get('Cookie')=='stream_fixture=fixed','user_agent_present':bool(self.headers.get('User-Agent'))}
            STATS['requests'].append(row)
        if mode=='cookie-gzip' and original:
            self.send_response(302);self.send_header('Set-Cookie','stream_fixture=fixed; Path=/; HttpOnly');self.send_header('Location','/encoded/gzip/'+original.group(1)+'.ts');self.send_header('Content-Length','0');self.end_headers();return
        if mode=='cookie-gzip' and not row['cookie_valid']:return self.json({'error':'redirect cookie required'},403)
        if mode=='identity-clean' and row['cookie_present']:return self.json({'error':'prior cookie leaked'},403)
        encoding={'cookie-gzip':'gzip','deflate':'deflate','raw':'deflate','stacked':'gzip, deflate'}.get(mode)
        compressors=[]
        if mode=='cookie-gzip':compressors=[zlib.compressobj(wbits=31)]
        elif mode=='deflate':compressors=[zlib.compressobj()]
        elif mode=='raw':compressors=[zlib.compressobj(wbits=-15)]
        elif mode=='stacked':compressors=[zlib.compressobj(wbits=31),zlib.compressobj()]
        with LOCK:
            STATS['active']+=1;STATS['connections']+=1;STATS['maximum_active']=max(STATS['maximum_active'],STATS['active'])
            counters=STATS['paths'].setdefault(canonical,{'connections':0,'active':0});counters['connections']+=1;counters['active']+=1
        self.send_response(200);self.send_header('Content-Type','video/mp2t');self.send_header('Transfer-Encoding','chunked')
        if encoding:self.send_header('Content-Encoding',encoding)
        self.end_headers()
        try:
            with CLIP.open('rb') as stream:
                began=time.monotonic();sent=0
                while chunk:=stream.read(PACKET_CHUNK):
                    raw=len(chunk)
                    for compressor in compressors:chunk=compressor.compress(chunk)+compressor.flush(zlib.Z_SYNC_FLUSH)
                    if chunk:self.wfile.write(format(len(chunk),'x').encode()+b'\r\n'+chunk+b'\r\n')
                    with LOCK:STATS['decoded_bytes']+=raw;STATS['wire_bytes']+=len(chunk)
                    sent+=raw;time.sleep(max(0,began+sent/RATE-time.monotonic()))
                tail=b''
                for compressor in compressors:tail=compressor.compress(tail)+compressor.flush(zlib.Z_FINISH)
                if tail:self.wfile.write(format(len(tail),'x').encode()+b'\r\n'+tail+b'\r\n')
                self.wfile.write(b'0\r\n\r\n')
        except (BrokenPipeError,ConnectionResetError):pass
        finally:
            with LOCK:STATS['active']-=1;STATS['disconnections']+=1;counters['active']-=1


if __name__=='__main__':
    subprocess.run(fixture_command(CLIP,120,'h264'),check=True,timeout=60)
    STATS['fixture_sha256']=file_sha256(CLIP);STATS['prefix_sha256']=__import__('hashlib').sha256(CLIP.read_bytes()[:188*4096]).hexdigest();STATS['prefix_bytes']=188*4096
    ThreadingHTTPServer(('0.0.0.0',8080),Handler).serve_forever()
