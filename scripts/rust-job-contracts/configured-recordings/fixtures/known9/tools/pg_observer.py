"""Unexecuted bounded transparent owned-PG observation; no held/error frames.
Same disposable loopback scope as qualified M3U proxies. TLS-decline is an
explicit fixture transport control; actual database payloads are forwarded.
"""
import base64,socket,socketserver,struct,threading,time

def exact(stream,n):
 if not 0<=n<=2097152:raise ValueError('PGFrameCapacity2MiB')
 data=stream.read(n)
 if len(data)!=n:raise EOFError
 return data
class Observer:
 def __init__(self,port=61651):
  if port!=61651:raise ValueError('FixedOwnedPort')
  self.lock=threading.Lock();self.frames=[];self.used=0;self.errors=[];self.active=set();self.sockets=set();self.serial=0;self.closed=False
  owner=self
  def capture(ident,direction,kind,payload):
   with owner.lock:
    if owner.used+len(payload)>16777216:raise ValueError('PGTranscriptCapacity16MiB')
    owner.used+=len(payload);owner.frames.append({'connection':ident,'direction':direction,'kind_hex':kind.hex(),'payload_base64':base64.b64encode(payload).decode(),'epoch':time.time()})
  class Handler(socketserver.BaseRequestHandler):
   def handle(self):
    remote=None;up=None
    with owner.lock:owner.serial+=1;ident=owner.serial;owner.active.add(ident);owner.sockets.add(self.request)
    try:
     stream=self.request.makefile('rb')
     while True:
      head=exact(stream,4);size=struct.unpack('!I',head)[0];payload=exact(stream,size-4)
      if size==8 and payload==struct.pack('!I',80877103):
       capture(ident,'fixture-TLS-decline',b'',payload);self.request.sendall(b'N');continue
      break
     remote=socket.create_connection(('127.0.0.1',5432),timeout=5);remote.settimeout(None)
     with owner.lock:owner.sockets.add(remote)
     capture(ident,'frontend-startup',b'',payload);remote.sendall(head+payload)
     def upstream():
      try:
       while True:
        kind=exact(stream,1);header=exact(stream,4);payload=exact(stream,struct.unpack('!I',header)[0]-4);capture(ident,'frontend',kind,payload);remote.sendall(kind+header+payload)
      except (EOFError,OSError):pass
      except BaseException as error:
       with owner.lock:owner.errors.append({'connection':ident,'direction':'frontend','error':type(error).__name__+':'+str(error)[:256]})
      finally:
       try:remote.shutdown(socket.SHUT_WR)
       except OSError:pass
     up=threading.Thread(target=upstream);up.start();back=remote.makefile('rb')
     while True:
      kind=exact(back,1);header=exact(back,4);payload=exact(back,struct.unpack('!I',header)[0]-4);capture(ident,'backend',kind,payload);self.request.sendall(kind+header+payload)
    except (EOFError,OSError):pass
    except BaseException as error:
     with owner.lock:owner.errors.append({'connection':ident,'direction':'backend','error':type(error).__name__+':'+str(error)[:256]})
    finally:
     for sock in [self.request,remote]:
      if sock:
       try:sock.shutdown(socket.SHUT_RDWR)
       except OSError:pass
       sock.close()
     if up:up.join(10)
     with owner.lock:
      if up and up.is_alive():owner.errors.append({'connection':ident,'error':'upstream-unsettled'})
      owner.active.discard(ident);owner.sockets.discard(self.request);owner.sockets.discard(remote)
  class Server(socketserver.ThreadingMixIn,socketserver.TCPServer):allow_reuse_address=True;daemon_threads=True
  self.server=Server(('127.0.0.1',port),Handler);self.thread=threading.Thread(target=self.server.serve_forever);self.thread.start()
 def close(self):
  if self.closed:raise ValueError('ObserverClosedTwice')
  self.closed=True;self.server.shutdown();self.server.server_close();self.thread.join(10)
  with self.lock:sockets=list(self.sockets)
  for sock in sockets:
   try:sock.shutdown(socket.SHUT_RDWR)
   except OSError:pass
   sock.close()
  deadline=time.monotonic()+10
  while time.monotonic()<deadline:
   with self.lock:active=list(self.active)
   if not active:break
   time.sleep(.01)
  with self.lock:
   return {'frames':self.frames,'captured_payload_bytes':self.used,'errors':self.errors,'active_connections':list(self.active),'observer_thread_settled':not self.thread.is_alive(),'physical_close_verified':not self.active and not self.thread.is_alive()}
