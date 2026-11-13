"""Unexecuted owned copied-PG relay, actual bytes only; no SQL values/auth logs.
The two explicit gates observe frontend identity AND actual backend completion.
No import-time sockets. Original Dispatcharr/dependency authors retain credit.
"""
import base64,hashlib,json,socket,socketserver,struct,threading,time
from datetime import datetime,timezone
from pg_preflight_gate import Gate

def utc():return datetime.now(timezone.utc).isoformat()
def exact(stream,n):
 if not 0<=n<=2097152:raise ValueError('OwnedPGFrameCapacity')
 value=stream.read(n)
 if len(value)!=n:raise EOFError
 return value

class Proxy:
 def __init__(self,policy,hold_preflight=False,hold_error=False):
  self.policy=policy;self.hold_preflight=hold_preflight;self.hold_error=hold_error
  self.preflight_held=threading.Event();self.preflight_release=threading.Event();self.error_held=threading.Event();self.error_release=threading.Event()
  self.errors=[];self.timeline=[];self.queries=[];self.frames=[];self.backend_pids=[];self.active={};self.workers=[];self.handlers=[];self.lock=threading.RLock();self.slots=threading.BoundedSemaphore(4);self.next_id=0
  owner=self
  def event(kind,**values):
   with owner.lock:
    if len(owner.timeline)>=512:raise ValueError('PGTimelineCapacity')
    owner.timeline.append({'kind':kind,'epoch':time.time(),'utc':utc(),**values})
  class Handler(socketserver.BaseRequestHandler):
   def handle(self):
    remote=None;up=None;client_stream=None;response=None;identity=None
    if not owner.slots.acquire(blocking=False):owner.errors.append('PGActiveConnectionCapacity');return
    guard=Gate(policy['guard_sql_sha256']);statements=set();portals=set();state={'create':False,'error':False}
    try:
     with owner.lock:
      if owner.next_id>=256:raise ValueError('PGHistoricalConnectionCapacity')
      owner.next_id+=1;identity=owner.next_id;owner.active[identity]=[self.request];owner.handlers.append(threading.current_thread())
     client_stream=self.request.makefile('rb')
     while True:
      head=exact(client_stream,4);size=struct.unpack('!I',head)[0];payload=exact(client_stream,size-4)
      if size==8 and payload==struct.pack('!I',80877103):self.request.sendall(b'N');continue
      break
     # Startup credentials forwarded but never retained in frame/query receipts.
     remote=socket.create_connection(('127.0.0.1',5432),timeout=5);remote.settimeout(None);remote.sendall(head+payload)
     with owner.lock:owner.active[identity].append(remote)
     def upstream():
      try:
       while True:
        kind=exact(client_stream,1);header=exact(client_stream,4);body=exact(client_stream,struct.unpack('!I',header)[0]-4)
        guard.frontend(kind,body)
        if kind==b'P':
         name,query,_=body.split(b'\0',2)
         if len(query)>65536:raise ValueError('PGSQLTextCapacity')
         digest=hashlib.sha256(query).hexdigest()
         with owner.lock:
          if len(owner.queries)>=512:raise ValueError('PGQueryCountCapacity')
          owner.queries.append({'connection':identity,'kind':'Parse','sql':query.decode('utf-8'),'sql_sha256':digest,'utc':utc()})
         statements.discard(name)
         if digest==policy['create_sql_sha256']:statements.add(name)
        elif kind==b'B':
         portal,statement,_=body.split(b'\0',2);portals.discard(portal)
         if statement in statements:portals.add(portal)
        elif kind==b'E':
         portal,_=body.split(b'\0',1)
         with owner.lock:state.update(create=portal in portals,error=False)
        elif kind==b'Q':
         query=body[:-1]
         if len(query)>65536:raise ValueError('PGSimpleSQLCapacity')
         with owner.lock:
          if len(owner.queries)>=512:raise ValueError('PGQueryCountCapacity')
          owner.queries.append({'connection':identity,'kind':'Simple','sql':query.decode('utf-8'),'sql_sha256':hashlib.sha256(query).hexdigest(),'utc':utc()})
        if len(statements)>4 or len(portals)>4:raise ValueError('PGOwnedStatementCapacity')
        remote.sendall(kind+header+body)
      except (OSError,EOFError):pass
      except Exception as e:owner.errors.append('frontend:'+type(e).__name__+':'+str(e)[:256])
      finally:
       try:remote.shutdown(socket.SHUT_WR)
       except OSError:pass
     up=threading.Thread(target=upstream);up.start()
     with owner.lock:owner.workers.append(up)
     response=remote.makefile('rb')
     while True:
      kind=exact(response,1);header=exact(response,4);body=exact(response,struct.unpack('!I',header)[0]-4)
      if kind==b'K':
       with owner.lock:owner.backend_pids.append(struct.unpack('!I',body[:4])[0])
      if kind==b'E' and state['create']:
       if len(body)>65536:raise ValueError('OwnedErrorReceiptCapacity')
       fields={item[:1]:item[1:]for item in body.rstrip(b'\0').split(b'\0')if item}
       state['error']=fields.get(b'C')==b'P0001' and fields.get(b'V',fields.get(b'S'))==b'ERROR'
       event('actual_create_ErrorResponse',connection=identity,sqlstate=fields.get(b'C',b'').decode('ascii'),error_sha256=hashlib.sha256(body).hexdigest(),raw_base64=base64.b64encode(body).decode())
      matching_guard=guard.backend(kind,body)
      if matching_guard and owner.hold_preflight and not owner.preflight_held.is_set():
       event('matching_guard_success_Ready_held',connection=identity,guard=list(guard.events));owner.preflight_held.set()
       if not owner.preflight_release.wait(25):raise TimeoutError('OwnedGuardReady25s')
       event('matching_guard_Ready_released',connection=identity)
      if kind==b'Z' and state['create'] and state['error'] and owner.hold_error and not owner.error_held.is_set():
       event('actual_create_error_Ready_held',connection=identity,ready_state=body.decode('ascii'));owner.error_held.set()
       if not owner.error_release.wait(25):raise TimeoutError('OwnedErrorReady25s')
       event('actual_create_error_Ready_released',connection=identity)
      self.request.sendall(kind+header+body)
      if kind==b'Z':state.update(create=False,error=False)
    except (EOFError,ConnectionResetError,BrokenPipeError):pass
    except Exception as e:owner.errors.append('backend:'+type(e).__name__+':'+str(e)[:256])
    finally:
     for stream in (self.request,remote):
      if stream:
       try:stream.shutdown(socket.SHUT_RDWR)
       except OSError:pass
     for stream in (client_stream,response,remote):
      if stream:
       try:stream.close()
       except OSError:pass
     if up:up.join(2)
     if up and up.is_alive():owner.errors.append('PGUpstreamThreadNotSettled')
     with owner.lock:owner.active.pop(identity,None)
     owner.slots.release()
  class Server(socketserver.ThreadingTCPServer):allow_reuse_address=True;daemon_threads=False;block_on_close=False
  self.server=Server(('127.0.0.1',61651),Handler);self.thread=threading.Thread(target=self.server.serve_forever);self.thread.start()
 def snapshot(self):
  with self.lock:return {'errors':list(self.errors),'timeline':list(self.timeline),'queries':list(self.queries),'backend_pids':list(self.backend_pids),'active_connection_ids':sorted(self.active),'upstream_threads_alive':sum(t.is_alive()for t in self.workers),'server_thread_alive':self.thread.is_alive(),'handler_threads_alive':sum(t.is_alive()for t in self.handlers)}
 def close(self):
  self.preflight_release.set();self.error_release.set();self.server.shutdown()
  with self.lock:sockets=[sock for values in self.active.values()for sock in values]
  for sock in sockets:
   try:sock.shutdown(socket.SHUT_RDWR)
   except OSError:pass
  self.server.server_close();self.thread.join(2)
  for thread in self.workers+self.handlers:thread.join(2)
  result=self.snapshot();result['physical_close_verified']=not self.thread.is_alive() and not result['active_connection_ids'] and not result['upstream_threads_alive'] and not result['handler_threads_alive']
  return result
