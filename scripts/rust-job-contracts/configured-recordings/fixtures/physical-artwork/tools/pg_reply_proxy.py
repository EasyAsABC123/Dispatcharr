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
def target_query(mode,query,transaction):
 q=query.strip().upper()
 if mode=='settings-error-Ready':return b'FROM CORE_CORESETTINGS WHERE KEY='in q
 if mode=='programme-success-Ready':return b'FROM EPG_PROGRAMDATA WHERE EPG_ID='in q and b'ORDER BY START_TIME LIMIT 33'in q
 if mode=='Recording-INSERT-P0001-Ready':return q.startswith(b'INSERT INTO DISPATCHARR_CHANNELS_RECORDING ')
 if mode=='Clocked-COMMIT-Ready':return q==b'COMMIT'and'Clocked'in transaction
 if mode=='Periodic-COMMIT-Ready':return q==b'COMMIT'and'Periodic'in transaction
 return False
def ready_for_hold(mode,state):
 if not state['target']:return False
 if mode=='programme-success-Ready':return state['rows']==state.get('expected_rows',1) and state['commands']==[('SELECT '+str(state.get('expected_rows',1))).encode()+b'\0']
 if mode.endswith('COMMIT-Ready'):return state['commands']==[b'COMMIT\0']
 return state['error']
def error_fields(payload):
 if type(payload)is not bytes or len(payload)>65536:raise ValueError('BoundedActualPGErrorPayload')
 fields={};position=0
 while position<len(payload):
  if payload[position:position+1]==b'\0':
   if position!=len(payload)-1:raise ValueError('PGErrorTrailingBytes')
   return fields
  key=payload[position:position+1].decode('ascii');end=payload.find(b'\0',position+1)
  if end<0 or key in fields:raise ValueError('PGErrorTruncatedOrDuplicateField')
  fields[key]=payload[position+1:end].decode('utf-8');position=end+1
 raise ValueError('PGErrorMissingTerminator')
class Exchanges:
 """Bounded frontend/backend correspondence, not a PostgreSQL emulator.
 Parse/Describe Sync has no execution. A following Execute and an error's
 queued ping have distinct response groups; matching Ready never uses latest SQL.
 """
 def __init__(self,mode,expected_rows=1):
  self.mode=mode;self.expected_rows=expected_rows;self.statements={};self.portals={};self.groups=[];self.building=None;self.transaction=set();self.serial=0
 def group(self):
  if self.building is None:
   if len(self.groups)>=128:raise ValueError('PGPendingExchangeCapacity128')
   self.serial+=1;self.building={'exchange':self.serial,'target':False,'rows':0,'commands':[],'error':False,'identity':None,'executions':[],'synced':False,'expected_rows':self.expected_rows};self.groups.append(self.building)
  return self.building
 def execute(self,query):
  group=self.group()
  if len(group['executions'])>=32:raise ValueError('PGExchangeExecutionCapacity32')
  group['executions'].append(query)
  q=query.strip().upper()
  if q==b'BEGIN':self.transaction.clear()
  if q.startswith(b'INSERT INTO DJANGO_CELERY_BEAT_CLOCKEDSCHEDULE '):self.transaction.add('Clocked')
  if q.startswith(b'INSERT INTO DJANGO_CELERY_BEAT_PERIODICTASK '):self.transaction.add('Periodic')
  group.update(target=target_query(self.mode,query,self.transaction),identity=query.decode('utf-8'))
 def frontend(self,kind,payload):
  query=None
  if kind==b'P':
   name,query=payload.split(b'\0',2)[:2]
   if name not in self.statements and len(self.statements)>=64:raise ValueError('PGStatementIdentityCapacity64')
   self.statements[name]=query;self.group()
  elif kind==b'B':
   portal,statement=payload.split(b'\0',2)[:2]
   if portal not in self.portals and len(self.portals)>=64:raise ValueError('PGPortalIdentityCapacity64')
   if statement not in self.statements:raise ValueError('PGUnknownBoundStatement')
   self.portals[portal]=self.statements[statement];self.group()
  elif kind==b'E':
   portal=payload.split(b'\0',1)[0]
   if portal not in self.portals:raise ValueError('PGUnknownExecutedPortal')
   query=self.portals[portal];self.execute(query)
  elif kind==b'S':
   self.group()['synced']=True;self.building=None
  elif kind==b'Q':
   if self.building is not None:raise ValueError('PGSimpleQueryInsideUnfinishedExchange')
   query=payload[:-1];self.execute(query);self.building['synced']=True;self.building=None
  return query
 def backend(self,kind,payload):
  if not self.groups:return None
  group=self.groups[0]
  if kind==b'D':group['rows']+=1
  if kind==b'C':group['commands'].append(payload)
  if kind==b'E':group['error']=True
  if kind==b'Z'and group['synced']and len(group['executions'])==1 and ready_for_hold(self.mode,group):return dict(group)
  return None
 def forwarded_ready(self):
  if self.groups:self.groups.pop(0)
class Proxy:
 def __init__(self,mode,port=61651,expected_rows=1):
  if port!=61651:raise ValueError('FixedOwnedPort')
  if type(expected_rows)is not int or not 1<=expected_rows<=32:raise ValueError('ExpectedBoundedDataRows')
  if mode not in ('settings-error-Ready','programme-success-Ready','Recording-INSERT-P0001-Ready','Clocked-COMMIT-Ready','Periodic-COMMIT-Ready'):raise ValueError('FiniteConfiguredPGMode')
  self.mode=mode;self.held=threading.Event();self.release=threading.Event();self.before_insert=threading.Event();self.insert_release=threading.Event();self.timeline=[]
  self.lock=threading.Lock();self.frames=[];self.used=0;self.errors=[];self.active=set();self.sockets=set();self.serial=0;self.closed=False
  owner=self
  def capture(ident,direction,kind,payload):
   with owner.lock:
    if owner.used+len(payload)>16777216:raise ValueError('PGTranscriptCapacity16MiB')
    owner.used+=len(payload);owner.frames.append({'connection':ident,'direction':direction,'kind_hex':kind.hex(),'payload_base64':base64.b64encode(payload).decode(),'epoch':time.time()})
  class Handler(socketserver.BaseRequestHandler):
   def handle(self):
    remote=None;up=None;exchanges=Exchanges(mode,expected_rows)
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
        kind=exact(stream,1);header=exact(stream,4);payload=exact(stream,struct.unpack('!I',header)[0]-4);capture(ident,'frontend',kind,payload)
        with owner.lock:query=exchanges.frontend(kind,payload)
        # Install the real owned trigger before the first target Parse reaches
        # PostgreSQL; doing it after Bind can deadlock on table locks.
        if query is not None and kind==b'P'and mode=='Recording-INSERT-P0001-Ready'and target_query(mode,query,set())and not owner.before_insert.is_set():
         with owner.lock:owner.timeline.append({'stage':'actual-tagged-INSERT-before-forward','epoch':time.time(),'query':query.decode('utf-8')})
         owner.before_insert.set()
         if not owner.insert_release.wait(20):raise TimeoutError('OwnedINSERTInjection20s')
        remote.sendall(kind+header+payload)
      except (EOFError,OSError):pass
      except BaseException as error:
       with owner.lock:owner.errors.append({'connection':ident,'direction':'frontend','error':type(error).__name__+':'+str(error)[:256]})
      finally:
       try:remote.shutdown(socket.SHUT_WR)
       except OSError:pass
     up=threading.Thread(target=upstream);up.start();back=remote.makefile('rb')
     while True:
      kind=exact(back,1);header=exact(back,4);payload=exact(back,struct.unpack('!I',header)[0]-4);capture(ident,'backend',kind,payload)
      with owner.lock:
       matching=exchanges.backend(kind,payload)
       hold=matching is not None and not owner.held.is_set()
       if hold:owner.timeline.append({'stage':'actual-matching-Ready-held','epoch':time.time(),'connection':ident,'exchange':matching['exchange'],'identity':matching['identity'],'matching_rows':matching['rows'],'matching_commands_hex':[x.hex()for x in matching['commands']],'actual_error':matching['error'],'executions':len(matching['executions']),'frontend_Sync_or_simpleQuery_observed':matching['synced']})
      if hold:
       owner.held.set()
       if not owner.release.wait(20):raise TimeoutError('OwnedPGReady20s')
      self.request.sendall(kind+header+payload)
      if kind==b'Z':
       with owner.lock:exchanges.forwarded_ready()
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
  self.release.set();self.insert_release.set();self.closed=True;self.server.shutdown();self.server.server_close();self.thread.join(10)
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
   return {'mode':self.mode,'timeline':self.timeline,'frames':self.frames,'captured_payload_bytes':self.used,'errors':self.errors,'active_connections':list(self.active),'observer_thread_settled':not self.thread.is_alive(),'physical_close_verified':not self.active and not self.thread.is_alive()}
