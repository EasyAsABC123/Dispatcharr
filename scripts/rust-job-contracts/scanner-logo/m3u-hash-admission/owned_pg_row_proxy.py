"""Owned copied-PG relay: forward real ERROR, hold its trailing ReadyForQuery.
No fabricated server frames. SSL request is declined; credential payloads are
forwarded but never retained/logged. Only one explicit P0001 is held.
"""
import socket,socketserver,threading,struct
def exact(stream,n):
 assert 0<=n<=2097152
 raw=stream.read(n)
 if len(raw)!=n:raise EOFError
 return raw
class Proxy:
 def __init__(self):
  self.record_data=False;self.max_data_row_bytes=0;self.data_rows=0;self.error=threading.Event();self.held=threading.Event();self.release=threading.Event();self.errors=[];self.backend_pid=None;self.error_code=None
  owner=self
  class Handler(socketserver.BaseRequestHandler):
   def handle(self):
    remote=None
    try:
     stream=self.request.makefile('rb')
     while True:
      header=exact(stream,4);n=struct.unpack('!I',header)[0];body=exact(stream,n-4)
      if n==8 and body==struct.pack('!I',80877103):self.request.sendall(b'N');continue
      break
     remote=socket.create_connection(('127.0.0.1',5432),timeout=5);remote.settimeout(None);remote.sendall(header+body)
     def upstream():
      try:
       while True:
        data=stream.read1(65536)
        if not data:break
        remote.sendall(data)
      except (OSError,EOFError):pass
      finally:
       try:remote.shutdown(socket.SHUT_WR)
       except OSError:pass
     thread=threading.Thread(target=upstream,daemon=True);thread.start();response=remote.makefile('rb');own_error=False
     while True:
      kind=exact(response,1);header=exact(response,4);body=exact(response,struct.unpack('!I',header)[0]-4)
      if kind==b'D'and owner.record_data:owner.max_data_row_bytes=max(owner.max_data_row_bytes,len(body)+5);owner.data_rows+=1
      if kind==b'K':owner.backend_pid=struct.unpack('!I',body[:4])[0]
      if kind==b'E':
       fields=body.rstrip(b'\0').split(b'\0');values={v[:1]:v[1:]for v in fields if v}
       if values.get(b'C')==b'P0001':owner.error_code='P0001';owner.error.set();own_error=True
      if kind==b'Z'and own_error and not owner.held.is_set():owner.held.set();assert owner.release.wait(25),'Owned Ready hold deadline'
      self.request.sendall(kind+header+body)
    except (EOFError,ConnectionResetError,BrokenPipeError):pass
    except Exception as error:owner.errors.append(type(error).__name__+':'+str(error)[:256])
    finally:
     if remote:remote.close()
  class Server(socketserver.ThreadingTCPServer):allow_reuse_address=True;daemon_threads=True
  self.server=Server(('127.0.0.1',61651),Handler);self.thread=threading.Thread(target=self.server.serve_forever,daemon=True);self.thread.start()
 def close(self):self.release.set();self.server.shutdown();self.server.server_close();self.thread.join(timeout=2)
