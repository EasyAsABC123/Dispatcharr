"""DRAFT, UNEXECUTED bounded owned result-reply observer; never logs AUTH data.
Only fixture loopback clients, finite 8 sockets/512KiB RESP2 frames. No task changes.
Original Redis/Celery/Kombu authors retain protocol credit.
"""
import socket,threading
class OwnedResultProxy:
 def __init__(self,target,hold=False,mode="result"):
  self.target=target.encode();self.hold=hold;self.mode=mode;self.armed=threading.Event();self.ack_tag=None;self.release=threading.Event();self.blocked=threading.Event();self.denied=threading.Event();self.stop=threading.Event();self.lock=threading.Lock();self.sockets=[];self.threads=[]
  self.listener=socket.socket();bound=False
  for port in range(61000,61128):
   try:self.listener.bind(('127.0.0.1',port));bound=True;break
   except OSError:continue
  if not bound:raise RuntimeError('Owned proxy port range exhausted')
  self.port=self.listener.getsockname()[1];self.listener.listen(8);self.listener.settimeout(.2)
  self.thread=threading.Thread(target=self.accept,daemon=True);self.thread.start()
 def frame(self,file,depth=0,budget=None):
  if budget is None:budget=[524288]
  if depth>8:raise ValueError('Owned RESP depth cap')
  line=file.readline(min(budget[0]+1,524289));budget[0]-=len(line)
  if not line or budget[0]<0 or len(line)>1024 or not line.endswith(b'\r\n'):raise ValueError('Owned RESP line cap/EOF')
  kind=line[:1]
  if kind in[b'+',b'-',b':']:return line,line[1:-2]
  if kind==b'$':
   count=int(line[1:-2]);assert -1<=count<=budget[0]-2
   if count==-1:return line,None
   raw=file.read(count+2);budget[0]-=len(raw);assert len(raw)==count+2 and raw.endswith(b'\r\n');return line+raw,raw[:-2]
  if kind==b'*':
   count=int(line[1:-2]);assert -1<=count<=128
   if count==-1:return line,None
   parts=[self.frame(file,depth+1,budget)for _ in range(count)];return line+b''.join(x[0]for x in parts),[x[1]for x in parts]
  raise ValueError('Owned RESP2 only')
 def accept(self):
  while not self.stop.is_set():
   try:client,_=self.listener.accept()
   except socket.timeout:continue
   except OSError:return
   with self.lock:
    if len(self.threads)>=8:client.close();continue
    self.sockets.append(client);thread=threading.Thread(target=self.connection,args=(client,),daemon=True);self.threads.append(thread)
   thread.start()
 def connection(self,client):
  server=None
  try:
   server=socket.create_connection(('127.0.0.1',6379),timeout=5);server.settimeout(5)
   with self.lock:self.sockets.append(server)
   cf=client.makefile('rb');sf=server.makefile('rb');touched=False
   while not self.stop.is_set():
    raw,command=self.frame(cf);assert isinstance(command,list)and command and isinstance(command[0],bytes)
    name=command[0].upper()
    if name==b'MULTI':touched=False
    own=(self.mode=='result'and name in[b'SET',b'SETEX',b'PSETEX']and len(command)>1 and command[1]==self.target)or(self.mode=='ack'and self.armed.is_set()and name==b'HDEL'and len(command)==3 and command[1]==b'unacked')
    if own and self.mode=='ack':self.ack_tag=command[2].decode()
    if self.mode=='native_result':own=name==b'EVALSHA'and len(command)==5 and command[2]==b'1'and command[3]==self.target
    if self.mode=='native_ack':
     own=name==b'EVALSHA'and len(command)==8 and command[2:6]==[b'3',b'celery',b'unacked',b'unacked_index']
     if own:self.ack_tag=command[6].decode()
    touched=touched or own;server.sendall(raw);reply,parsed=self.frame(sf)
    if own and reply.startswith(b'-'):self.denied.set()
    if self.hold and touched and ((name in[b'EXEC',b'SET',b'SETEX',b'PSETEX',b'HDEL']and not reply.startswith(b'+QUEUED'))or(self.mode.startswith('native_')and name==b'EVALSHA'and reply==b':1\r\n')):
     self.blocked.set()
     while not self.release.wait(.1):
      if self.stop.is_set():return
    client.sendall(reply)
    if name in[b'EXEC',b'DISCARD']:touched=False
  except (OSError,ValueError,AssertionError):pass
  finally:
   client.close()
   if server:server.close()
 def close(self):
  self.stop.set();self.release.set();self.listener.close()
  with self.lock:sockets=list(self.sockets);threads=list(self.threads)
  for value in sockets:
   try:value.shutdown(socket.SHUT_RDWR)
   except OSError:pass
   value.close()
  self.thread.join(timeout=2)
  for thread in threads:thread.join(timeout=2)
