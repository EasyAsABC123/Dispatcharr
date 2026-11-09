"""Private deterministic IP-provider fixture; never performs external requests."""
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
import json
import threading
from urllib.parse import urlsplit,parse_qs
lock=threading.Lock();release=threading.Event();state={'mode':'success','calls':[]}
class Handler(BaseHTTPRequestHandler):
    def log_message(self,*args):pass
    def send(self,status,value,raw=False):
        body=value if raw else json.dumps(value).encode();self.send_response(status);self.send_header('Content-Type','application/json');self.send_header('Content-Length',str(len(body)));
        if urlsplit(self.path).path=='/ipify':self.send_header('Set-Cookie','ip-fixture=one; Path=/')
        self.end_headers();self.wfile.write(body)
    def do_GET(self):
        path=urlsplit(self.path).path
        if path=='/control':
            mode=parse_qs(urlsplit(self.path).query).get('mode',['success'])[-1]
            with lock:state.update(mode=mode,calls=[])
            release.clear();return self.send(200,{'configured':True})
        if path=='/release':release.set();return self.send(200,{'released':True})
        if path=='/stats':
            with lock:value={'mode':state['mode'],'calls':list(state['calls'])}
            return self.send(200,value)
        if path=='/health':return self.send(200,{'ready':True})
        with lock:mode=state['mode'];state['calls'].append({'path':path,'cookie_present':bool(self.headers.get('Cookie')),'user_agent':self.headers.get('User-Agent')})
        if path=='/ipify':
            if mode.startswith('slow'):release.wait(8)
            if mode=='ip-http-error':return self.send(503,b'unavailable',True)
            if mode=='ip-json-error':return self.send(200,b'not-json',True)
            if mode=='ip-array-fatal':return self.send(200,[])
            return self.send(200,{'ip':{'ip-invalid':'invalid','ip-numeric':123,'ip-zero':0}.get(mode,'198.51.100.10')})
        if path.startswith('/geo/'):
            if mode=='fallback':return self.send(503,b'unavailable',True)
            if mode=='geo-json-error':return self.send(200,b'not-json',True)
            if mode=='geo-array':return self.send(200,[])
            return self.send(200,{'country_code':'TS','country_name':'Testland','city':'Fixture City'})
        if path=='/fallback':return self.send(200,{'countryCode':'FB','country':'Fallback Country','city':'Fallback City'})
        return self.send(404,{'error':'unknown fixture path'})
ThreadingHTTPServer(('0.0.0.0',8080),Handler).serve_forever()
