"""Fixture-only transparent RESP mirror. No listener/activity on import.
Bounded parser/lifecycle derived from qualified Main reply gate; credit retained.
Telemetry failure invalidates evidence but never rewrites/replays Redis traffic.
"""
import collections,socket,socketserver,threading,time
from qualified_resp import Frame,arguments,Gate
from causal_probe import RedisEffects
from raw_redis import RedisFailure
EXECUTION_BLOCKED=True

def decode(raw,start=0,depth=0):
 if depth>16:raise ValueError('ReplyDepth')
 end=raw.index(b'\r\n',start);tag=raw[start:start+1];text=raw[start+1:end];at=end+2
 if tag==b'+':return text,at
 if tag==b'-':return RedisFailure(),at
 if tag==b':':return int(text),at
 if tag==b'_':return None,at
 size=int(text)
 if size==-1:return None,at
 if tag==b'$':return raw[at:at+size],at+size+2
 if tag==b'*':
  if not 0<=size<=1024:raise ValueError('ReplyArrayCapacity')
  result=[]
  for _ in range(size):value,at=decode(raw,at,depth+1);result.append(value)
  return result,at
 raise ValueError('ReplyType')

class State:
 def __init__(self,journal,profile_key):
  self.j=journal;self.profile=profile_key.encode();self.effects=RedisEffects(journal);self.lock=threading.RLock();self.next=0
 def opened(self,peer,local):
  with self.lock:self.next+=1;identity=self.next;self.j.append({'kind':'wire_connection_open','at':time.monotonic(),'connection':identity,'peer_port':peer[1],'local_port':local[1]});return identity
 def command(self,identity,argv):
  with self.lock:
   family=argv[0].upper()if argv else b'';role='profile_counter'if self.profile in argv[1:]else None
   known={b'MULTI',b'EXEC',b'DISCARD',b'WATCH',b'UNWATCH',b'GET',b'INCR',b'DECR',b'DEL'}
   if family in[b'INCRBY',b'DECRBY']and len(argv)==3 and argv[2]==b'1':family=b'INCR'if family==b'INCRBY'else b'DECR'
   if role=='profile_counter':
    supported=(family in[b'INCR',b'DECR',b'GET']and len(argv)==2 and argv[1]==self.profile)or(family==b'WATCH'and len(argv)>=2)or(family in[b'INCR',b'DECR']and len(argv)==3 and argv[1]==self.profile and argv[2]==b'1')
    if not supported:self.j.fail('UnsupportedOwnedCounterCommand')
   name=family.decode('ascii')if family in known else'OTHER';self.effects.command(identity,name,role)
 def reply(self,identity,raw):
  with self.lock:
   value,end=decode(raw)
   if end!=len(raw):self.j.fail('ReplyTrailingData');return
   self.effects.reply(identity,value)
 def closed(self,identity):
  with self.lock:self.effects.close(identity);self.j.append({'kind':'wire_connection_close','at':time.monotonic(),'connection':identity})

class Server(socketserver.ThreadingTCPServer):
 allow_reuse_address=True;daemon_threads=False
class Handler(socketserver.BaseRequestHandler):
 def handle(self):
  gate=self.server.gate;state=self.server.state;identity=None;server=None;reader=None;stop=threading.Event();queue=collections.deque();queue_lock=threading.Lock()
  with gate.lock:
   if gate.closed:return
   if gate.connections>=64:gate.invalid.append('ConnectionCapacity');return
   gate.connections+=1;gate.sockets.add(self.request)
  try:
   identity=state.opened(self.request.getpeername(),self.request.getsockname());server=socket.create_connection(('127.0.0.1',self.server.backend_port),timeout=5);server.settimeout(.5);self.request.settimeout(.5)
   with gate.lock:gate.sockets.add(server)
   def replies():
    try:
     framer=Frame(server,stop)
     while not stop.is_set():
      raw=framer.read()
      with queue_lock:
       if not queue:raise ValueError('ReplyWithoutCommand')
       target=queue.popleft()
      try:state.reply(identity,raw)
      except Exception:state.j.fail('PassiveReplyObserverFailure')
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
    raw=framer.read();argv=arguments(raw);target=gate.target(argv)
    with queue_lock:
     if len(queue)>=128:raise ValueError('ConnectionPendingCapacity')
     queue.append(target)
    try:state.command(identity,argv)
    except Exception:state.j.fail('PassiveCommandObserverFailure')
    server.sendall(raw)
  except(EOFError,OSError):pass
  except Exception as error:
   with gate.lock:gate.invalid.append(type(error).__name__)
  finally:
   stop.set()
   for connection in[self.request,server]:
    if connection is not None:
     try:connection.shutdown(socket.SHUT_RDWR)
     except OSError:pass
     connection.close()
   if reader is not None:
    reader.join(11)
    if reader.is_alive():
     with gate.lock:gate.invalid.append('ReplyThreadNotSettled')
   if identity is not None:
    try:state.closed(identity)
    except Exception:state.j.fail('PassiveCloseObserverFailure')
   with gate.lock:gate.connections-=1;gate.sockets.discard(self.request);gate.sockets.discard(server)

class Relay:
 def __init__(self,journal,profile_key,backend_port,gate=None):
  self.server=Server(('127.0.0.1',0),Handler);self.server.backend_port=backend_port;self.server.state=State(journal,profile_key);self.server.gate=gate or Gate('local-no-hold',None);self.thread=threading.Thread(target=self.server.serve_forever);self.port=self.server.server_address[1];self.stopped=False
 def start(self):self.thread.start()
 def stop(self):
  if self.stopped:return self.server.gate.snapshot()
  self.stopped=True
  gate=self.server.gate
  with gate.lock:gate.closed=True;sockets=list(gate.sockets)
  gate.release.set()
  for connection in sockets:
   try:connection.shutdown(socket.SHUT_RDWR)
   except OSError:pass
  self.server.shutdown();self.server.server_close();self.thread.join(15)
  state=gate.snapshot()
  if self.thread.is_alive()or state['connections']or state['invalid']:raise RuntimeError('LocalPassiveRelayUndrained')
  return state

def main():
 import argparse,json,os,signal
 from pathlib import Path
 from causal_probe import Journal
 from qualified_resp import Proxy,Control
 from source_resp_gate import SourceGate
 from activation import child_guard
 if os.environ.get('CAUSAL_REVIEWED_EXECUTION')!='one-fixed-Source-busy-cohort':raise RuntimeError('SourcePassiveRelayRequiresRootReview')
 child_guard('/evidence/execution-cue.json','/pending-tools')
 parser=argparse.ArgumentParser();parser.add_argument('--evidence',type=Path,required=True);args=parser.parse_args()
 if str(args.evidence)!='/evidence/source-proxy-private.json':raise ValueError('ExactOwnedRelayEvidence')
 journal=Journal('/evidence/Source-wire.jsonl',max_rows=1024,max_bytes=2097152);gate=SourceGate(args.evidence);state=State(journal,'profile_connections:'+str(json.loads(Path('/evidence/pending-main-private.json').read_text())['provider_profile_id']))
 server=Server(('0.0.0.0',6383),Handler);server.backend_port=6390;server.gate=gate;server.state=state
 control=Proxy(('127.0.0.1',6384),Control);control.gate=gate;stop=threading.Event()
 for event in[signal.SIGINT,signal.SIGTERM]:signal.signal(event,lambda *_:stop.set())
 threads=[threading.Thread(target=service.serve_forever)for service in[server,control]]
 for thread in threads:thread.start()
 try:stop.wait()
 finally:
  gate.release.set()
  with gate.lock:gate.closed=True;sockets=list(gate.sockets)
  for connection in sockets:
   try:connection.shutdown(socket.SHUT_RDWR)
   except OSError:pass
  for service in[control,server]:service.shutdown();service.server_close()
  for thread in threads:thread.join(15)
  report={**gate.snapshot(),'journal':journal.status(),'threads_settled':all(not thread.is_alive()for thread in threads)}
  args.evidence.write_text(json.dumps(report,indent=2)+'\n')
  if report['connections']or report['invalid']or report['journal']['invalid']or not report['threads_settled']:raise RuntimeError('PassiveSourceRelayIncomplete')
if __name__=='__main__':main()
