"""Owned PG relay; real frames only, no credential payload recording.
Frontend gate defers one real capped SELECT before forwarding it. Separate
response accounting records actual DataRow sizes/fields, not SQL semantics.
"""
import base64,socket,socketserver,threading,struct

def exact(s,n):
 if not 0<=n<=2097152:raise ValueError('Owned frame capacity')
 data=s.read(n)
 if len(data)!=n:raise EOFError
 return data

def fields(body):
 count=struct.unpack('!H',body[:2])[0];at=2;out=[]
 for _ in range(count):
  size=struct.unpack('!i',body[at:at+4])[0];at+=4
  if size==-1:out.append(None)
  else:out.append(base64.b64encode(body[at:at+size]).decode());at+=size
 return out
class Proxy:
 def __init__(self):
  self.record_data=False;self.max_data_row_bytes=0;self.data_rows=0;self.target_rows=[];self.errors=[];self.gate=False;self.held=threading.Event();self.release=threading.Event();self.snapshot_queries=0;self.mutations=0;self.backend_pids=[]
  owner=self
  class Handler(socketserver.BaseRequestHandler):
   def handle(self):
    remote=None
    try:
     stream=self.request.makefile('rb')
     while True:
      head=exact(stream,4);n=struct.unpack('!I',head)[0];body=exact(stream,n-4)
      if n==8 and body==struct.pack('!I',80877103):self.request.sendall(b'N');continue
      break
     remote=socket.create_connection(('127.0.0.1',5432),timeout=5);remote.settimeout(None);remote.sendall(head+body)
     def upstream():
      try:
       while True:
        kind=exact(stream,1);header=exact(stream,4);payload=exact(stream,struct.unpack('!I',header)[0]-4)
        if kind==b'P':
         query=payload.split(b'\0',2)[1]
         if b'AS logo_bounded'in query:
          owner.snapshot_queries+=1
          if owner.gate and not owner.held.is_set():owner.held.set();assert owner.release.wait(25),'Owned query gate25s'
         if query.startswith(b'INSERT INTO dispatcharr_channels_stream')or query.startswith(b'UPDATE dispatcharr_channels_stream AS target'):owner.mutations+=1
        remote.sendall(kind+header+payload)
      except (OSError,EOFError):pass
      except Exception as error:owner.errors.append(type(error).__name__+':'+str(error)[:256])
      finally:
       try:remote.shutdown(socket.SHUT_WR)
       except OSError:pass
     thread=threading.Thread(target=upstream,daemon=True);thread.start();response=remote.makefile('rb')
     while True:
      kind=exact(response,1);header=exact(response,4);body=exact(response,struct.unpack('!I',header)[0]-4)
      if kind==b'D'and owner.record_data:
       owner.data_rows+=1;owner.max_data_row_bytes=max(owner.max_data_row_bytes,len(body)+5)
       decoded=fields(body)
       if len(decoded)==16:owner.target_rows.append({'bytes':len(body)+5,'fields_base64':decoded})
      if kind==b'K':owner.backend_pids.append(struct.unpack('!I',body[:4])[0])
      self.request.sendall(kind+header+body)
    except (EOFError,ConnectionResetError,BrokenPipeError):pass
    except Exception as error:owner.errors.append(type(error).__name__+':'+str(error)[:256])
    finally:
     if remote:remote.close()
  class Server(socketserver.ThreadingTCPServer):allow_reuse_address=True;daemon_threads=True
  self.server=Server(('127.0.0.1',61651),Handler);self.thread=threading.Thread(target=self.server.serve_forever,daemon=True);self.thread.start()
 def reset(self):self.data_rows=0;self.max_data_row_bytes=0;self.target_rows=[];self.snapshot_queries=0;self.mutations=0;self.held.clear();self.release.clear()
 def close(self):self.release.set();self.server.shutdown();self.server.server_close();self.thread.join(timeout=2)
