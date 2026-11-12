"""UNEXECUTED owned RESP2 physical-reply gate for future Main fixture.
No app/runtime mutation, response replay, credential/argv dump or import activity.
"""
import argparse,collections,hashlib,json,re,signal,socket,socketserver,threading,time
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
class Gate:
 def __init__(self,sha,evidence):
  self.sha,self.evidence=sha,evidence;self.lock=threading.Lock();self.release=threading.Event();self.armed=False;self.seen=0;self.profile=None;self.invalid=[];self.connections=0;self.rows=[];self.sockets=set();self.closed=False;self.expected_reply=1
 def trace(self,kind):
  with self.lock:
   if len(self.rows)>=1024:self.invalid.append('TraceCapacity');raise ValueError('TraceCapacity')
   self.rows.append({'kind':kind,'monotonic':time.monotonic(),'seen':self.seen})
 def snapshot(self):
  with self.lock:return{'armed':self.armed,'seen':self.seen,'connections':self.connections,'invalid':list(self.invalid),'rows':list(self.rows)}
 def arm(self,profile,expected_reply=1):
  if not re.fullmatch(r'profile_connections:[0-9]{1,16}',profile):raise ValueError('OwnedProfileShape')
  if type(expected_reply)is not int or expected_reply not in[1,-2]:raise ValueError('DeclaredReserveReply')
  with self.lock:
   if self.armed:raise ValueError('AlreadyArmed')
   self.profile=profile.encode();self.armed=True;self.seen=0;self.expected_reply=expected_reply;self.release.clear()
 def target(self,args):
  if len(args)<2:return False
  sha=args[1].decode('ascii',errors='ignore')if args[0].upper()==b'EVALSHA'else hashlib.sha1(args[1]).hexdigest()if args[0].upper()==b'EVAL'else None
  with self.lock:return self.armed and sha==self.sha and self.profile in args
 def response(self,target,raw):
  if not target or raw.startswith(b'-NOSCRIPT'):return
  with self.lock:
   if not self.armed:return
   if raw!=(':'+str(self.expected_reply)+'\r\n').encode():
    self.invalid.append('UnexpectedReserveReplySchema');raise ValueError('UnexpectedReserveReplySchema')
   self.seen+=1;first=self.seen==1
  if first:
   self.trace('backend-target-reply-read-held')
   if not self.release.wait(10):
    with self.lock:self.invalid.append('HeldReplyDeadline')
    raise TimeoutError('OwnedHeldReplyDeadline')
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
      gate.response(target,raw);self.request.sendall(raw)
    except(EOFError,OSError):pass
    except Exception as error:
     with gate.lock:gate.invalid.append(type(error).__name__)
    finally:
     stop.set()
     try:self.request.shutdown(socket.SHUT_RDWR)
     except OSError:pass
   reader=threading.Thread(target=replies);reader.start();framer=Frame(self.request,stop)
   while not stop.is_set():
    raw=framer.read();target=gate.target(arguments(raw))
    with lock:
     if len(queue)>=128:raise ValueError('ConnectionPendingCapacity')
     queue.append(target)
    server.sendall(raw)
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
    reader.join(11)
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
  if value['action']=='arm':gate.arm(value['profile_key'],value['expected_reply'])
  elif value['action']=='release':
   with gate.lock:gate.armed=False
   gate.release.set()
  elif value['action']!='state':raise ValueError('UnknownControl')
  self.wfile.write((json.dumps(gate.snapshot())+'\n').encode())
def main():
 parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--script-sha1',required=True);parser.add_argument('--evidence',type=Path,required=True);parser.add_argument('--backend-port',type=int,choices=[6379,6391],default=6391);args=parser.parse_args()
 if not re.fullmatch('[0-9a-f]{40}',args.script_sha1):raise ValueError('DeclaredScriptSHA')
 if not str(args.evidence).startswith('/evidence/'):raise ValueError('OwnedEvidencePath')
 gate=Gate(args.script_sha1,args.evidence);proxy=Proxy(('0.0.0.0',6381),Handler);control=Proxy(('127.0.0.1',6382),Control);proxy.gate=control.gate=gate;proxy.backend_port=args.backend_port;stop=threading.Event()
 for event in[signal.SIGINT,signal.SIGTERM]:signal.signal(event,lambda *_:stop.set())
 threads=[threading.Thread(target=server.serve_forever)for server in[proxy,control]]
 for thread in threads:thread.start()
 try:stop.wait()
 finally:
  gate.release.set()
  with gate.lock:gate.closed=True;active=list(gate.sockets)
  for connection in active:
   try:connection.shutdown(socket.SHUT_RDWR)
   except OSError:pass
  for server in[control,proxy]:server.shutdown();server.server_close()
  for thread in threads:thread.join(15)
  args.evidence.write_text(json.dumps(gate.snapshot(),indent=2)+'\n')
if __name__=='__main__':main()
