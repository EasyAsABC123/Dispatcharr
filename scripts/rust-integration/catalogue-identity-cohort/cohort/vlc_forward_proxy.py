"""Owned fixture HTTP proxy: fixed media Origin only; no DNS/client target dialing.
No service starts on import. Application/profile parameters are unchanged.
"""
import argparse,http.client,http.server,json,os,re,signal,socket,threading,time
from pathlib import Path
TARGET=re.compile(r'http://origin:8080(/live/[0-9]{1,20}\.ts)',re.ASCII)
def media_path(method,target,headers):
    if method not in ('GET','HEAD'):raise ValueError('MethodDenied')
    found=TARGET.fullmatch(target)
    if not found:raise ValueError('TargetDenied')
    hosts=headers.get_all('Host',[])
    if hosts!=['origin:8080']:raise ValueError('HostDenied')
    if headers.get('Transfer-Encoding')is not None or headers.get('Expect')is not None:raise ValueError('BodyDenied')
    lengths=headers.get_all('Content-Length',[])
    if lengths and lengths!=['0']:raise ValueError('BodyDenied')
    for key,value in headers.items():
        if '\r'in value or '\n'in value:raise ValueError('HeaderDenied')
    return found.group(1)
def main():
    parser=argparse.ArgumentParser();parser.add_argument('--upstream',required=True);parser.add_argument('--state',required=True);parser.add_argument('--seconds',type=float,required=True);args=parser.parse_args()
    if args.upstream not in ('11.254.251.3','11.254.252.3')or not 1<=args.seconds<=300:raise ValueError('OwnedConfiguration')
    state=Path(args.state);state.mkdir(mode=0o755,exist_ok=True)
    stop=threading.Event();lock=threading.Lock();sockets=set();gate=threading.BoundedSemaphore(4)
    counts={'accepted':0,'denied':0,'completed':0,'upstream_errors':0,'bytes':0,'active':0,'maximum_active':0,'bounded_stream_limit':134217728,'fixed_upstream':args.upstream,'fixed_port':8080,'listener':'127.0.0.1:18109'}
    def save(name,value):
        tmp=state/(name+'.tmp');tmp.write_text(json.dumps(value,sort_keys=True)+'\n');tmp.replace(state/(name+'.json'))
    def add_socket(value):
        with lock:sockets.add(value)
    def remove_socket(value):
        with lock:sockets.discard(value)
    class Handler(http.server.BaseHTTPRequestHandler):
        protocol_version='HTTP/1.0'
        def log_message(self,*values):pass
        def handle_one_request(self):
            self.connection.settimeout(3);add_socket(self.connection)
            try:super().handle_one_request()
            finally:remove_socket(self.connection)
        def deny(self):
            with lock:counts['denied']+=1
            self.send_error(403,'Owned media destination required')
        def do_CONNECT(self):self.deny()
        def do_POST(self):self.deny()
        def do_GET(self):self.forward()
        def do_HEAD(self):self.forward()
        def forward(self):
            try:path=media_path(self.command,self.path,self.headers)
            except ValueError:self.deny();return
            if stop.is_set():self.send_error(503);return
            upstream=http.client.HTTPConnection(args.upstream,8080,timeout=3);total=0;transport=None
            try:
                headers={'Host':'origin:8080','Connection':'close'}
                agent=self.headers.get('User-Agent')
                if agent is not None and len(agent)<=4096:headers['User-Agent']=agent
                for key in ('Range','Accept','Icy-MetaData'):
                    value=self.headers.get(key)
                    if value is not None and len(value)<=4096:headers[key]=value
                upstream.connect();transport=upstream.sock;add_socket(transport)
                upstream.request(self.command,path,headers=headers);response=upstream.getresponse()
                self.send_response(response.status)
                for key in ('Content-Type','Content-Length','Content-Range','Accept-Ranges'):
                    value=response.getheader(key)
                    if value is not None:self.send_header(key,value)
                self.send_header('Connection','close');self.end_headers()
                with lock:counts['accepted']+=1
                if self.command=='GET':
                    until=time.monotonic()+min(args.seconds,180)
                    while not stop.is_set()and time.monotonic()<until:
                        data=response.read1(min(65536,134217728-total+1))
                        if not data:break
                        if total+len(data)>134217728:raise ValueError('StreamByteBound')
                        self.wfile.write(data);self.wfile.flush();total+=len(data)
                with lock:counts['completed']+=1
            except (OSError,ValueError,http.client.HTTPException):
                with lock:counts['upstream_errors']+=1
            finally:
                if transport is not None:remove_socket(transport)
                upstream.close()
                with lock:counts['bytes']+=total
                self.close_connection=True
    class Server(http.server.ThreadingHTTPServer):
        daemon_threads=False;block_on_close=True;request_queue_size=4
        def process_request(self,request,address):
            if not gate.acquire(False):request.close();return
            with lock:
                counts['active']+=1;counts['maximum_active']=max(counts['maximum_active'],counts['active'])
            try:super().process_request(request,address)
            except BaseException:
                gate.release()
                with lock:counts['active']-=1
                raise
        def process_request_thread(self,request,address):
            try:super().process_request_thread(request,address)
            finally:
                with lock:counts['active']-=1
                gate.release()
    server=Server(('127.0.0.1',18109),Handler);server.timeout=.25
    def halt(*unused):stop.set()
    signal.signal(signal.SIGTERM,halt);signal.signal(signal.SIGINT,halt)
    save('ready',{'pid':os.getpid(),'actual_listener':server.socket.getsockname(),'upstream':args.upstream,'port':8080,'maximum_handlers':4})
    until=time.monotonic()+args.seconds
    try:
        while not stop.is_set()and time.monotonic()<until:server.handle_request()
    finally:
        stop.set()
        with lock:remaining=list(sockets)
        for conn in remaining:
            try:conn.shutdown(socket.SHUT_RDWR)
            except OSError:pass
            conn.close()
        server.server_close()
        with lock:final=dict(counts,active_sockets=len(sockets),all_handler_threads_joined=counts['active']==0)
        save('final',final)
        if not final['all_handler_threads_joined']:raise RuntimeError('ProxyHandlerNotJoined')
if __name__=='__main__':main()
