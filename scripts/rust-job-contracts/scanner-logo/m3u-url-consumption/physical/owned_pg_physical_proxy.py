"""Disposable PG relay: real errors/data and controlled socket loss, no synthetic frames.
Original Dispatcharr/dependency authors retain code and algorithm credit.
"""
import socket,socketserver,threading,struct,time

def exact(stream,n):
 if not 0<=n<=2097152:raise ValueError('Owned PG capacity')
 value=stream.read(n)
 if len(value)!=n:raise EOFError
 return value
def ready_for_hold(mode,target,own_error,rows,commands):
 # Parse/Describe/Sync Ready is not actual successful SELECT completion.
 return target and ((rows==1 and commands==1)if mode=='snapshot-success-DataRow-Ready'else own_error)
class Proxy:
 def __init__(self,mode):
  self.mode=mode;self.held=threading.Event();self.release=threading.Event();self.error=threading.Event();self.errors=[];self.error_codes=[];self.queries=[];self.data_rows=0;self.command_complete=0;self.backend_pids=[];self.timeline=[];self.disconnections=0
  owner=self
  def hit(query):
   q=query.strip().upper()
   if mode=='preACK-support-SQL-Error-Ready':return b'SELECT NOT EXISTS(SELECT 1 FROM DISPATCHARR_CHANNELS_STREAM'in q
   if mode.startswith('snapshot-'):return b'AS LOGO_BOUNDED'in q and b'FROM DISPATCHARR_CHANNELS_STREAM'in q
   if mode=='transaction-BEGIN-loss':return q==b'BEGIN'
   if mode=='rollback-loss':return q==b'ROLLBACK'
   if mode=='COMMIT-P0001-Ready':return q==b'COMMIT'
   if mode=='mutation-P0001-rollback':return q.startswith(b'INSERT INTO DISPATCHARR_CHANNELS_STREAM')or q.startswith(b'UPDATE DISPATCHARR_CHANNELS_STREAM AS TARGET')
   return False
  class Handler(socketserver.BaseRequestHandler):
   def handle(self):
    remote=None;state={'target':False,'rows':0,'commands':0,'identity':None}
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
        query=payload.split(b'\0',2)[1]if kind==b'P'else(payload[:-1]if kind==b'Q'else None)
        if query is not None:
         owner.queries.append(query.decode(errors='replace'));state.update(target=hit(query),rows=0,commands=0,identity=query.decode(errors='replace')if hit(query)else None)
         if state['target']and mode in ['transaction-BEGIN-loss','rollback-loss']and not owner.held.is_set():
          owner.timeline.append({'stage':'before-real-command-loss','epoch':time.time()});owner.held.set();assert owner.release.wait(25);owner.disconnections+=1;remote.shutdown(socket.SHUT_RDWR);self.request.shutdown(socket.SHUT_RDWR);return
        remote.sendall(kind+header+payload)
      except (OSError,EOFError):pass
      except Exception as e:owner.errors.append(type(e).__name__+':'+str(e)[:256])
      finally:
       try:remote.shutdown(socket.SHUT_WR)
       except OSError:pass
     threading.Thread(target=upstream,daemon=True).start();response=remote.makefile('rb');own_error=False
     while True:
      kind=exact(response,1);head=exact(response,4);body=exact(response,struct.unpack('!I',head)[0]-4)
      if kind==b'K':owner.backend_pids.append(struct.unpack('!I',body[:4])[0])
      if kind==b'D'and state['target']:owner.data_rows+=1;state['rows']+=1
      if kind==b'C'and state['target']:owner.command_complete+=1;state['commands']+=1
      if kind==b'E':
       values={p[:1]:p[1:]for p in body.rstrip(b'\0').split(b'\0')if p};code=values.get(b'C',b'').decode();owner.error_codes.append(code);owner.error.set();own_error=True
      target=ready_for_hold(mode,state['target'],own_error,state['rows'],state['commands'])
      if kind==b'Z'and target and not owner.held.is_set():owner.timeline.append({'stage':'real-Ready-held','epoch':time.time(),'query_identity':state['identity'],'matching_DataRows':state['rows'],'matching_CommandComplete':state['commands']});owner.held.set();assert owner.release.wait(25)
      self.request.sendall(kind+head+body)
    except (EOFError,ConnectionResetError,BrokenPipeError):pass
    except OSError as e:
     if not owner.disconnections:owner.errors.append(type(e).__name__+':'+str(e)[:256])
    except Exception as e:owner.errors.append(type(e).__name__+':'+str(e)[:256])
    finally:
     if remote:remote.close()
  class Server(socketserver.ThreadingTCPServer):allow_reuse_address=True;daemon_threads=True
  self.server=Server(('127.0.0.1',61651),Handler);self.thread=threading.Thread(target=self.server.serve_forever,daemon=True);self.thread.start()
 def close(self):self.release.set();self.server.shutdown();self.server.server_close();self.thread.join(timeout=2)
