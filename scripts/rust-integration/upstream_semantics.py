#!/usr/bin/env python3
"""Capture unchanged original HTTPStreamReader transport semantics in its image.

Full original Dispatcharr author/contributor credit retained. The AST-loaded
class body is unchanged; only get_logger is stubbed. Loopback transport fixtures
are byte-contract checks, not media-decoder or performance claims.
"""
import argparse
import ast
import gzip
import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import select
import shutil
import subprocess
import tempfile
import threading
import time
import urllib.parse
import zlib

ROOT=Path(__file__).resolve().parents[2] if len(Path(__file__).resolve().parents)>2 else Path("/")
BASELINE='1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430'
IMAGE='ghcr.io/dispatcharr/dispatcharr@sha256:34f9011514edd31ef0ff111b5f256d2e37614fcb08a6ee1ed9eccec9251a856f'
SOURCE='apps/proxy/live_proxy/input/http_streamer.py'


def inside(source,output):
    import requests
    from requests.adapters import HTTPAdapter
    from urllib3.util.request import ACCEPT_ENCODING
    import urllib3
    class Log:
        def __getattr__(self,_):return lambda *args,**kwargs:None
    tree=ast.parse(source.read_text());node=next(x for x in tree.body if isinstance(x,ast.ClassDef) and x.name=='HTTPStreamReader')
    namespace={'threading':threading,'os':os,'requests':requests,'HTTPAdapter':HTTPAdapter,'logger':Log()}
    exec(compile(ast.Module(body=[node],type_ignores=[]),str(source),'exec'),namespace)
    Reader=namespace['HTTPStreamReader']
    payload=(bytes([0x47,0x1f,0xff,0x10])+bytes(184))*128
    expected=hashlib.sha256(payload).hexdigest();observed=[];lock=threading.Lock()
    encoded={'identity':payload,'gzip':gzip.compress(payload),'deflate':zlib.compress(payload)}
    compressor=zlib.compressobj(wbits=-zlib.MAX_WBITS);encoded['raw-deflate']=compressor.compress(payload)+compressor.flush()
    encoded['stacked-gzip-deflate']=zlib.compress(gzip.compress(payload))
    try:
        import brotli
        encoded['br']=brotli.compress(payload)
    except ImportError:pass
    try:
        import zstandard
        encoded['zstd']=zstandard.ZstdCompressor().compress(payload)
    except ImportError:pass
    class Handler(BaseHTTPRequestHandler):
        protocol_version='HTTP/1.1'
        def log_message(self,*_):pass
        def do_GET(self):
            path=urllib.parse.urlsplit(self.path).path
            auth=self.headers.get('Authorization','')
            row={'path':path,'http_version':self.request_version,'headers':{name:self.headers.get(name) for name in ('Host','User-Agent','Accept','Accept-Encoding','Connection','Cookie')},'authorization_present':bool(auth),'authorization_scheme':auth.partition(' ')[0] if auth else None}
            with lock:observed.append(row)
            status=200;data=payload;headers={}
            if path.startswith('/body/'):
                coding=path.rsplit('/',1)[1];data=encoded[coding]
                if coding!='identity':headers['Content-Encoding']={'raw-deflate':'deflate','stacked-gzip-deflate':'gzip, deflate'}.get(coding,coding)
            elif path.startswith('/status/'):status=int(path.rsplit('/',1)[1]);data=b'' if status in (204,304) else payload
            elif path.startswith('/redirect/code/'):
                status=int(path.rsplit('/',1)[1]);data=b'';headers['Location']='/body/identity'
            elif path.startswith('/redirect/chain/'):
                remaining=int(path.rsplit('/',1)[1])
                if remaining:status=302;data=b'';headers['Location']='/redirect/chain/'+str(remaining-1)
            elif path in ('/cookie/start','/cookie/cross-host'):
                status=302;data=b'';headers['Set-Cookie']='attempt_cookie=fixed; Path=/; HttpOnly'
                headers['Location']='/cookie/final' if path=='/cookie/start' else 'http://127.0.0.2:'+str(self.server.server_port)+'/cookie/check'
            elif path=='/cookie/final' and self.headers.get('Cookie')!='attempt_cookie=fixed':status=403;data=b''
            elif path in ('/auth/same-host','/auth/cross-host'):
                status=302;data=b'';headers['Location']='/auth/final' if path=='/auth/same-host' else 'http://127.0.0.2:'+str(self.server.server_port)+'/auth/final'
            self.send_response(status);self.send_header('Content-Type','video/mp2t');self.send_header('Content-Length',str(len(data)))
            for name,value in headers.items():self.send_header(name,value)
            self.end_headers()
            if data:
                try:self.wfile.write(data)
                except (BrokenPipeError,ConnectionResetError):pass
    server=ThreadingHTTPServer(('0.0.0.0',0),Handler);server.daemon_threads=True
    thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start();base='http://127.0.0.1:'+str(server.server_port)
    cases=[]
    def read(label,path,agent=None,auth=False):
        began=time.monotonic();before=len(observed);url=base+path
        if auth:url=url.replace('http://','http://fixture-user:fixture-pass@',1)
        reader=Reader(url,user_agent=agent);fd=reader.start();os.set_blocking(fd,False);data=bytearray();eof=False
        try:
            while time.monotonic()-began<5:
                if not select.select([fd],[],[],.1)[0]:continue
                block=os.read(fd,65536)
                if not block:eof=True;break
                data.extend(block)
                if len(data)>128*1024:raise RuntimeError('fixture output exceeds128KiB')
        finally:reader.stop();os.close(fd)
        row={'case':label,'bytes':len(data),'sha256':hashlib.sha256(data).hexdigest(),'decoded_payload_equal':hashlib.sha256(data).hexdigest()==expected,'eof':eof,'seconds':time.monotonic()-began,'response_status':reader.response.status_code if reader.response is not None else None,'request_count':len(observed)-before,'requests':observed[before:]}
        cases.append(row)
    try:
        read('default-request-headers','/body/identity');read('custom-user-agent','/body/identity','Dispatcharr fixture agent')
        for label,agent in [('empty',''),('latin1','é'),('internal-tab','Agent\tvalue'),('leading-space',' Agent'),('crlf','Agent\r\nInjected:x'),('emoji','emoji😀')]:read('ua-'+label,'/body/identity',agent)
        for coding in encoded:
            if coding!='identity':read('content-encoding-'+coding,'/body/'+coding)
        for status in (200,201,202,203,204,205,206,300,304,404,503):read('response-status-'+str(status),'/status/'+str(status))
        for status in (301,302,303,307,308,300,304,305,306):read('redirect-status-'+str(status),'/redirect/code/'+str(status))
        for count in (29,30,31):read('redirect-chain-'+str(count),'/redirect/chain/'+str(count))
        read('redirect-cookie-within-attempt','/cookie/start');read('cookie-fresh-attempt','/cookie/check');read('cookie-required-fresh-attempt','/cookie/final');read('cookie-cross-host','/cookie/cross-host')
        read('url-basic-auth-same-host','/auth/same-host',auth=True);read('url-basic-auth-cross-host','/auth/cross-host',auth=True)
    finally:server.shutdown();server.server_close();thread.join(2)
    for row in cases:
        name=row['case'];valid=row['eof']
        if name.startswith('response-status-'):
            status=int(name.rsplit('-',1)[1]);valid=valid and row['response_status']==status and (row['decoded_payload_equal'] if status==200 else row['bytes']==0)
        elif name.startswith('redirect-status-'):
            status=int(name.rsplit('-',1)[1]);follow=status in (301,302,303,307,308)
            valid=valid and row['request_count']==(2 if follow else 1) and (row['decoded_payload_equal'] if follow else row['bytes']==0)
        elif name.startswith('redirect-chain-'):
            count=int(name.rsplit('-',1)[1]);valid=valid and row['request_count']==min(count+1,31) and (row['decoded_payload_equal'] if count<=30 else row['bytes']==0)
        elif name in ('ua-leading-space','ua-crlf','ua-emoji'):valid=valid and row['bytes']==0 and row['request_count']==0
        elif name=='cookie-required-fresh-attempt':valid=valid and row['response_status']==403 and row['bytes']==0
        else:valid=valid and row['decoded_payload_equal']
        if name=='redirect-cookie-within-attempt':valid=valid and row['requests'][-1]['headers']['Cookie']=='attempt_cookie=fixed'
        if name in ('cookie-fresh-attempt','cookie-cross-host'):valid=valid and row['requests'][-1]['headers']['Cookie'] is None
        if name=='url-basic-auth-same-host':valid=valid and row['requests'][-1]['authorization_present']
        if name=='url-basic-auth-cross-host':valid=valid and not row['requests'][-1]['authorization_present']
        row['fixture_contract_passed']=bool(valid)
    result={'scope':'unchanged original HTTPStreamReader loopback transport characterization','passed':all(row['fixture_contract_passed'] for row in cases),'baseline_commit':BASELINE,'baseline_image':IMAGE,'source_sha256':hashlib.sha256(source.read_bytes()).hexdigest(),'class_ast_sha256':hashlib.sha256(ast.dump(node,include_attributes=False).encode()).hexdigest(),'requests_version':requests.__version__,'urllib3_version':urllib3.__version__,'urllib3_accept_encoding':ACCEPT_ENCODING,'payload_sha256':expected,'payload_bytes':len(payload),'fixture_proxy_environment':'HTTP(S)/ALL_PROXY cleared, NO_PROXY=*; Docker network none, loopback only','application_performance_verified':False,'milestone_complete':False,'cases':cases}
    (output/'upstream-semantics.json').write_text(json.dumps(result,indent=2)+'\n')
    return 0 if result['passed'] else 1


def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--inside',action='store_true');parser.add_argument('--source',type=Path);parser.add_argument('--output',type=Path,required=True);parser.add_argument('--docker',default='docker');args=parser.parse_args()
    if args.inside:return inside(args.source,args.output)
    output=args.output.resolve();output.mkdir(parents=True,exist_ok=True)
    if any(output.iterdir()):parser.error('output must be empty')
    docker=shutil.which(args.docker)
    if not docker:parser.error('Docker not found')
    raw=subprocess.check_output(['git','show',BASELINE+':'+SOURCE],cwd=ROOT)
    with tempfile.TemporaryDirectory(prefix='dispatcharr-upstream-source-') as temporary:
        source=Path(temporary)/'http_streamer.py';source.write_bytes(raw)
        command=[docker,'run','--rm','--network','none','--read-only','--tmpfs','/tmp:rw,size=33554432','--pids-limit','64','--memory','256m',
            '-e','HTTP_PROXY=','-e','HTTPS_PROXY=','-e','ALL_PROXY=','-e','http_proxy=','-e','https_proxy=','-e','all_proxy=','-e','NO_PROXY=*','-e','no_proxy=*',
            '-v',str(source)+':/source/http_streamer.py:ro','-v',str(Path(__file__).resolve().parent)+':/verification:ro','-v',str(output)+':/evidence',
            '--entrypoint','/dispatcharrpy/bin/python',IMAGE,'/verification/upstream_semantics.py','--inside','--source','/source/http_streamer.py','--output','/evidence']
        run=subprocess.run(command,text=True,capture_output=True,timeout=120)
        (output/'command.log').write_text(run.stdout+run.stderr)
        result=json.loads((output/'upstream-semantics.json').read_text()) if (output/'upstream-semantics.json').exists() else {'passed':False}
        result['tool_sha256']=hashlib.sha256(Path(__file__).read_bytes()).hexdigest();result['command_exit_code']=run.returncode;result['source_sha256']=hashlib.sha256(raw).hexdigest()
        (output/'upstream-semantics.json').write_text(json.dumps(result,indent=2)+'\n')
        return 0 if run.returncode==0 and result['passed'] else 1


if __name__=='__main__':raise SystemExit(main())
