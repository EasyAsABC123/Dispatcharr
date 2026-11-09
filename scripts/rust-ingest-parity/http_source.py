"""Real Requests fetch_m3u_lines oracle in the original isolated runtime.

Full Dispatcharr application/algorithm credit belongs to its original authors.
DB status/progress sinks are replaced only to isolate network/cache behavior.
"""
import argparse
import ast
import gzip
import hashlib
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
import json
import logging
import os
from pathlib import Path
import socket
import subprocess
import tempfile
import threading
import time
import types
import zlib

ROOT=Path(__file__).resolve().parents[2]
PLAYLIST=b'#EXTM3U\n#EXTINF:-1,Fixture\nhttp://fixture/live\n'
CASES=['plain','partial','unusual-success','gzip','zlib','raw-deflate','stacked','cookie','fresh','empty','html','error','broken-body']
class Origin(BaseHTTPRequestHandler):
    def log_message(self,*args):pass
    def do_GET(self):
        mode=self.path.lstrip('/')
        self.server.records.append({'mode':mode,'cookie':self.headers.get('Cookie'),'agent':self.headers.get('User-Agent'),
                                    'accept_encoding':self.headers.get('Accept-Encoding')})
        body=PLAYLIST;status=200;encoding=None
        if mode=='cookie':
            self.send_response(302);self.send_header('Location','/cookie-final');self.send_header('Set-Cookie','fixture=present; Path=/');self.send_header('Content-Length','0');self.end_headers();return
        if mode=='cookie-final' and self.headers.get('Cookie')!='fixture=present':status=403;body=b'missing cookie'
        elif mode=='fresh' and self.headers.get('Cookie'):status=409;body=b'leaked cookie'
        elif mode=='partial':status=206
        elif mode=='unusual-success':status=299
        elif mode=='gzip':body=gzip.compress(body);encoding='gzip'
        elif mode=='zlib':body=zlib.compress(body);encoding='deflate'
        elif mode=='raw-deflate':body=zlib.compress(body)[2:-4];encoding='deflate'
        elif mode=='stacked':body=zlib.compress(gzip.compress(body));encoding='gzip, deflate'
        elif mode=='empty':body=b''
        elif mode=='html':body=b'<html>unauthorized'
        elif mode=='error':status=403;body=b'denied'
        self.send_response(status);self.send_header('Content-Type','text/plain')
        self.send_header('Content-Length',str(len(body)+(100 if mode=='broken-body' else 0)))
        if encoding:self.send_header('Content-Encoding',encoding)
        self.end_headers();self.wfile.write(body);self.wfile.flush()
        if mode=='broken-body':self.connection.shutdown(socket.SHUT_RDWR)

def server():
    instance=ThreadingHTTPServer(('127.0.0.1',0),Origin);instance.records=[]
    threading.Thread(target=instance.serve_forever,daemon=True).start()
    return instance,'http://127.0.0.1:'+str(instance.server_address[1])

def original(base,directory):
    import requests
    logger=logging.getLogger('source-fetch');logger.disabled=True
    tree=ast.parse((ROOT/'apps/m3u/tasks.py').read_text());function=next(node for node in tree.body if isinstance(node,ast.FunctionDef) and node.name=='fetch_m3u_lines')
    messages=[]
    scope={'os':os,'time':time,'requests':requests,'logger':logger,'m3u_dir':directory,
           'M3UAccount':types.SimpleNamespace(Status=types.SimpleNamespace(FETCHING='fetching',ERROR='error')),
           'send_m3u_update':lambda *args,**kwargs:None,
           '_set_m3u_account_status':lambda account_id,status,message,**kwargs:messages.append(message)}
    exec(compile(ast.Module(body=[function],type_ignores=[]),str(ROOT/'apps/m3u/tasks.py'),'exec'),scope)
    results=[]
    for mode in CASES:
        path=Path(directory)/'1.m3u';path.write_bytes(b'previous cache');messages.clear()
        account=types.SimpleNamespace(id=1,name='fixture',server_url=base+'/'+mode,file_path=None,get_user_agent_string=lambda:'Café',save=lambda **kwargs:None)
        _,success=scope['fetch_m3u_lines'](account)
        error_class=None if success else ('status' if mode=='error' else ('network' if mode=='broken-body' else 'cache'))
        status=({'partial':206,'unusual-success':299}.get(mode,200)) if success else (403 if mode=='error' else None)
        data=path.read_bytes()
        results.append({'case':mode,'expected':{'success':success,'error_class':error_class,
            'error_message':messages[-1].replace(base,'<origin>') if error_class=='cache' else None,'status':status,
            'cache_bytes':len(data),'cache_sha256':hashlib.sha256(data).hexdigest(),'temporary_exists':Path(str(path)+'.tmp').exists()}})
    return results

def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--output',type=Path,required=True)
    parser.add_argument('--goldens',type=Path);parser.add_argument('--binary',type=Path)
    args=parser.parse_args();instance,base=server()
    try:
        if not args.goldens:
            import requests,sys
            with tempfile.TemporaryDirectory(prefix='source-fetch-') as directory:cases=original(base,directory)
            report={'scope':'original-real-HTTP-download-and-cache-goldens','python':sys.version,'requests':requests.__version__,
                    'source_sha256':hashlib.sha256((ROOT/'apps/m3u/tasks.py').read_bytes()).hexdigest(),
                    'capture_tool_sha256':hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),'cases':cases,'requests_observed':instance.records}
        else:
            goldens=json.loads(args.goldens.read_text())
            if goldens['source_sha256']!=hashlib.sha256((ROOT/'apps/m3u/tasks.py').read_bytes()).hexdigest():raise RuntimeError('Source hash mismatch')
            inputs=[{'base':base,'url':base+'/'+case['case']} for case in goldens['cases']]
            process=subprocess.run([str(args.binary.resolve())],input=''.join(json.dumps(value)+'\n' for value in inputs),text=True,capture_output=True,check=True,timeout=60)
            actual=[json.loads(line) for line in process.stdout.splitlines()]
            if len(actual)!=len(inputs):raise RuntimeError('Incomplete native fixture results')
            failures=[{'case':case['case'],'expected':case['expected'],'actual':response} for case,response in zip(goldens['cases'],actual) if case['expected']!=response]
            if instance.records!=goldens['requests_observed']:failures.append({'request_headers_or_cookie_scope':instance.records,'expected':goldens['requests_observed']})
            report={'scope':'original-vs-Rust-real-HTTP-download-and-cache','passed':not failures,'cases':len(inputs),'failures':failures,
                    'goldens_sha256':hashlib.sha256(args.goldens.read_bytes()).hexdigest(),'binary_sha256':hashlib.sha256(args.binary.read_bytes()).hexdigest(),
                    'capture_tool_sha256':goldens['capture_tool_sha256'],'comparison_tool_sha256':hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
                    'original_runtime':{key:goldens[key] for key in ['python','requests','source_sha256']},'application_performance_verified':False,'ingestion_jobs_migrated':False}
        args.output.parent.mkdir(parents=True,exist_ok=True);args.output.write_text(json.dumps(report,indent=2)+'\n')
        print(json.dumps({key:report[key] for key in ['scope','passed','cases'] if key in report}));return not report.get('passed',True)
    finally:instance.shutdown();instance.server_close()

if __name__=='__main__':raise SystemExit(main())
