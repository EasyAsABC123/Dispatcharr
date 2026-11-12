"""Bounded disposable-loopback RESP reply hold; no workload/resource metrics.
Never records AUTH arguments. A hold occurs only after the real Redis reply.
"""
import socket,socketserver,threading
MAX=1048576
def frame(stream):
 line=stream.readline(MAX+1)
 if not line:raise EOFError
 assert len(line)<=MAX and line.endswith(b'\r\n');tag=line[:1];body=line[1:-2]
 if tag==b'*':
  n=int(body);assert -1<=n<=256;parts=[];values=[]
  for _ in range(max(0,n)):
   raw,value=frame(stream);parts.append(raw);values.append(value)
  return line+b''.join(parts),values if n>=0 else None
 if tag==b'$':
  n=int(body);assert -1<=n<=MAX
  if n<0:return line,None
  data=stream.read(n+2);assert len(data)==n+2 and data.endswith(b'\r\n');return line+data,data[:-2]
 if tag==b':':return line,int(body)
 assert tag in [b'+',b'-'];return line,body
class Proxy:
 def __init__(self,mode,parent,child):
  self.mode=mode;self.parent=parent.encode();self.child=child.encode();self.held=threading.Event();self.release=threading.Event();self.count=0;self.errors=[]
  owner=self
  class Handler(socketserver.BaseRequestHandler):
   def handle(self):
    try:
     with socket.create_connection(('127.0.0.1',6379),timeout=5)as remote:
      remote.settimeout(None);incoming=self.request.makefile('rb');response=remote.makefile('rb')
      while True:
       raw,command=frame(incoming);assert isinstance(command,list);remote.sendall(raw);reply,value=frame(response);owner.count+=1
       key=b'celery-tasks:celery-task-meta-'+(owner.parent if mode=='parent'else owner.child)
       lua=command[0].upper()in [b'EVAL',b'EVALSHA'];target=(lua and key in command and isinstance(value,list)and len(value)==2 and value[0]==1)if mode=='parent'else(lua and value==1 and (b'celery'in command if mode=='claim'else key in command))
       if mode=='events':target=lua and b'asgispecific.rust_owned_m3u_replies!'in command and isinstance(value,int)
       if mode=='cache':target=lua and b'epg_content_generation'in command and isinstance(value,int)
       if target and not owner.held.is_set():
        owner.held.set();assert owner.release.wait(20),'Owned reply hold cleanup deadline'
       self.request.sendall(reply)
    except (EOFError,ConnectionResetError,BrokenPipeError):pass
    except Exception as error:owner.errors.append(type(error).__name__+':'+str(error)[:256])
  class Server(socketserver.ThreadingTCPServer):allow_reuse_address=True;daemon_threads=True
  self.server=Server(('127.0.0.1',{'parent':61601,'claim':61602,'result':61603,'events':61604,'cache':61605}[mode]),Handler);self.thread=threading.Thread(target=self.server.serve_forever,daemon=True);self.thread.start()
 def close(self):self.release.set();self.server.shutdown();self.server.server_close();self.thread.join(timeout=2)
