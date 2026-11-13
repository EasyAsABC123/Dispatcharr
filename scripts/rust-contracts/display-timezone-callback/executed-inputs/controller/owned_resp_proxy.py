"""UNEXECUTED private backup callback RESP2 duplex physical-reply gate.
No app/runtime mutation, response replay, credential/argv dump or import activity.
"""
import argparse,collections,hashlib,json,re,signal,socket,socketserver,threading,time,sys
from pathlib import Path
class Frame:
 def __init__(self,sock,stopped):self.sock=sock;self.stopped=stopped;self.buffer=bytearray()
 def line(self,start):
  end=self.buffer.find(b'\r\n',start)
  return None if end<0 else(end,bytes(self.buffer[start:end]))
 def end(self,start=0,depth=0):
  if depth>16:raise ValueError('RESPDepth')
  if start>=len(self.buffer):return None
  line=self.line(start+1)
  if line is None:return None
  at,text=line;after=at+2;tag=self.buffer[start:start+1]
  if len(text)>8192:raise ValueError('RESPLineCapacity')
  if tag in[b'+',b'-']:return after
  if tag==b':':
   if not re.fullmatch(rb'-?[0-9]+',text):raise ValueError('RESPInteger')
   return after
  if tag==b'_':
   if text:raise ValueError('RESPNull')
   return after
  number=int(text)
  if number<0:
   if number!=-1 or tag not in[b'$',b'*']:raise ValueError('RESPNegativeLength')
   return after
  if tag==b'$':
   if number>1048576:raise ValueError('RESPBulkCapacity')
   end=after+number+2
   if end>len(self.buffer):return None
   if self.buffer[end-2:end]!=b'\r\n':raise ValueError('RESPBulkTerminator')
   return end
  if tag==b'*':
   if number>1024:raise ValueError('RESPArrayCapacity')
   for _ in range(number):
    after=self.end(after,depth+1)
    if after is None:return None
   return after
  raise ValueError('RESPProtocolUnsupported')
 def read(self):
  while not self.stopped.is_set():
   end=self.end()
   if end is not None:
    result=bytes(self.buffer[:end]);del self.buffer[:end];return result
   try:data=self.sock.recv(4096)
   except socket.timeout:continue
   if not data:raise EOFError('RESPPeerBodyEOF')
   if len(self.buffer)+len(data)>1048576:raise ValueError('RESPFrameCapacity')
   self.buffer.extend(data)
  raise EOFError('OwnedProxyStopped')
def arguments(raw):
 if not raw.startswith(b'*'):return[]
 at=raw.index(b'\r\n');count=int(raw[1:at]);at+=2;values=[]
 if not 0<=count<=1024:raise ValueError('RESPArgCount')
 for _ in range(count):
  if raw[at:at+1]!=b'$':raise ValueError('RESPRequestShape')
  end=raw.index(b'\r\n',at);size=int(raw[at+1:end]);at=end+2
  if size<0 or size>1048576 or raw[at+size:at+size+2]!=b'\r\n':raise ValueError('RESPRequestBulk')
  values.append(raw[at:at+size]);at+=size+2
 if at!=len(raw):raise ValueError('RESPRequestTrailingBytes')
 return values
GATE_SAFETY_SECONDS=45
class Gate:
 def __init__(self,contracts,evidence):
  self.contracts,self.evidence=contracts,evidence;self.lock=threading.RLock();self.release=threading.Event();self.armed=False;self.seen=0;self.selected=None;self.invalid=[];self.connections=0;self.rows=[];self.sockets=set();self.closed=False
 def trace(self,kind,**fields):
  with self.lock:
   if len(self.rows)>=2048:self.invalid.append('TraceCapacity');raise ValueError('TraceCapacity')
   row={'kind':kind,'monotonic':time.monotonic(),'seen':self.seen,**fields};self.rows.append(row)
   if self.evidence is not None:
    raw=json.dumps(row,allow_nan=False)+'\n'
    if self.evidence.tell()+len(raw.encode())>8388608:raise ValueError('LedgerCapacity')
    self.evidence.write(raw);self.evidence.flush()
 def snapshot(self):
  with self.lock:return{'armed':self.armed,'selected':self.selected,'seen':self.seen,'connections':self.connections,'invalid':list(self.invalid),'rows':list(self.rows)}
 def arm(self,case):
  if case not in self.contracts:raise ValueError('DeclaredFiniteCase')
  with self.lock:
   if self.armed or self.selected is not None:raise ValueError('OnceArmOnly')
   self.selected=case;self.armed=True;self.seen=0;self.release.clear()
 def target(self,args):
  if not args:return False
  with self.lock:
   if not self.armed:return False
   want=self.contracts[self.selected]
   if want['kind']=='get':return args==[b'GET',bytes.fromhex(want['key_hex'])]
   if want['kind']!='lua':raise ValueError('DeclaredGateKind')
   if len(args)<3:return False
   command=args[0].upper()
   if 'commands_hex'in want and command.hex()not in want['commands_hex']:return False
   sha=args[1].decode('ascii',errors='ignore')if command==b'EVALSHA'else hashlib.sha1(args[1]).hexdigest()if command==b'EVAL'else None
   if sha!=want['sha1']or args[2]!=str(len(want['keys_hex'])).encode():return False
   return args[3:3+len(want['keys_hex'])]==[bytes.fromhex(k)for k in want['keys_hex']]
 def response(self,target,raw):
  if not target or raw.startswith(b'-NOSCRIPT'):return
  with self.lock:
   if not self.armed:return
   self.seen+=1;first=self.seen==1
  if not first:raise ValueError('DuplicateTargetReply')
  self.trace('backend-target-reply-read-held',reply_hex=raw.hex(),reply_sha256=hashlib.sha256(raw).hexdigest())
  if not self.release.wait(GATE_SAFETY_SECONDS):
   with self.lock:self.invalid.append('GateSafety45Expired')
   raise TimeoutError('OwnedGateSafety45')
  self.trace('backend-target-reply-released')
class Proxy(socketserver.ThreadingTCPServer):
 allow_reuse_address=True;daemon_threads=False
class Handler(socketserver.BaseRequestHandler):
 def handle(self):
  gate=self.server.gate
  with gate.lock:
   if gate.closed:return
   if gate.connections>=64:gate.invalid.append('ConnectionCapacity');return
   gate.connections+=1;gate.sockets.add(self.request)
  stop=threading.Event();server=None;reader=None;queue=collections.deque();lock=threading.Lock()
  try:
   server=socket.create_connection(('127.0.0.1',self.server.backend_port),timeout=5);server.settimeout(.5);self.request.settimeout(.5)
   with gate.lock:gate.sockets.add(server)
   def replies():
    try:
     framer=Frame(server,stop)
     while not stop.is_set():
      raw=framer.read()
      with lock:target=queue.popleft()if queue else False
      gate.response(target,raw);self.request.sendall(raw);gate.trace('reply-forwarded',target=target,reply_sha256=hashlib.sha256(raw).hexdigest())
    except(EOFError,OSError):pass
    except Exception as error:
     with gate.lock:gate.invalid.append(type(error).__name__)
    finally:
     stop.set()
     try:self.request.shutdown(socket.SHUT_RDWR)
     except OSError:pass
   reader=threading.Thread(target=replies);reader.start();framer=Frame(self.request,stop)
   while not stop.is_set():
    raw=framer.read();args=arguments(raw);target=gate.target(args)
    gate.trace('request-read',command_hex=args[0].hex()if args else None,request_sha256=hashlib.sha256(raw).hexdigest(),target=target,target_arguments_hex=[v.hex()for v in args]if target else None)
    with lock:
     if len(queue)>=128:raise ValueError('ConnectionPendingCapacity')
     queue.append(target)
    server.sendall(raw);gate.trace('request-forwarded',request_sha256=hashlib.sha256(raw).hexdigest(),target=target)
  except(EOFError,OSError):pass
  except Exception as error:
   with gate.lock:gate.invalid.append(type(error).__name__)
  finally:
   stop.set()
   for connection in[self.request,server]:
    if connection:
     try:connection.shutdown(socket.SHUT_RDWR)
     except OSError:pass
     connection.close()
   if reader:
    reader.join(46)
    if reader.is_alive():
     with gate.lock:gate.invalid.append('ReplyThreadNotSettled')
   with gate.lock:
    gate.connections-=1;gate.sockets.discard(self.request);gate.sockets.discard(server)
class Control(socketserver.StreamRequestHandler):
 def handle(self):
  if self.client_address[0]!='127.0.0.1':raise ValueError('OwnedLoopbackControl')
  self.request.settimeout(3);raw=self.rfile.readline(4097)
  if len(raw)>4096:raise ValueError('ControlCapacity')
  value=json.loads(raw);gate=self.server.gate
  if value['action']=='arm':gate.arm(value['case'])
  elif value['action']=='release':
   with gate.lock:gate.armed=False
   gate.release.set()
  elif value['action']!='state':raise ValueError('UnknownControl')
  self.wfile.write((json.dumps(gate.snapshot())+'\n').encode())
def main():
 parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--contracts',type=Path,required=True);parser.add_argument('--case',required=True);parser.add_argument('--evidence',type=Path,required=True);parser.add_argument('--backend-port',type=int,required=True);args=parser.parse_args()
 if not 1<=args.backend_port<=65535:raise ValueError('DeclaredBackendPort')
 contracts=json.loads(args.contracts.read_text())
 if not contracts or len(contracts)>3:raise ValueError('FiniteContracts')
 ledger=args.evidence.open('x');gate=Gate(contracts,ledger);gate.arm(args.case);proxy=Proxy(('127.0.0.1',0),Handler);proxy.gate=gate;proxy.backend_port=args.backend_port
 thread=threading.Thread(target=proxy.serve_forever,name='resp-listener');thread.start();closed=False
 def close():
  nonlocal closed
  gate.release.set()
  with gate.lock:gate.closed=True;active=list(gate.sockets)
  for connection in active:
   try:connection.shutdown(socket.SHUT_RDWR)
   except OSError:pass
  proxy.shutdown();proxy.server_close();thread.join(3);closed=True
  result=gate.snapshot();result['listener_joined']=not thread.is_alive();return result
 print(json.dumps({'ready':True,'port':proxy.server_address[1]}),flush=True)
 try:
  while True:
   raw=sys.stdin.buffer.readline(65537)
   if not raw:break
   if len(raw)>65536 or not raw.endswith(b'\n'):raise ValueError('ControlCapacity')
   value=json.loads(raw);action=value['action']
   if action=='release':
    with gate.lock:gate.armed=False
    gate.release.set()
   elif action=='stop':result=close()
   elif action!='stats':raise ValueError('UnknownControl')
   if action!='stop':result=gate.snapshot()
   response=json.dumps({'id':value['id'],'result':result},allow_nan=False)
   if len(response.encode())>4194304:raise ValueError('ReplyCapacity')
   print(response,flush=True)
   if closed:break
 finally:
  if not closed:close()
  ledger.close()
if __name__=='__main__':main()
