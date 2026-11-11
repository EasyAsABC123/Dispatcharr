"""Independent physical authority: no method-return wrapper; fixed2.4s reply holds."""
import argparse,json,signal,threading,time
from pathlib import Path
from resp_gate import Gate,Proxy,Handler
class PhysicalGate(Gate):
 def __init__(self,sha,source_profile):
  super().__init__(sha,None);self.source_key=('profile_connections:'+source_profile).encode();self.native_key=b'profile_connections:987654321';self.counts={'source':0,'native':0};self.armed=True
 def target(self,args):
  if len(args)>=2 and args[0].upper()==b'INCRBY'and args[1]==self.source_key and args[2:]==[b'1']:return'source'
  if len(args)==2 and args[0].upper()==b'INCR'and args[1]==self.source_key:return'source'
  return'native'if super().target(args)and self.native_key in args else False
 def response(self,target,raw):
  if not target or raw.startswith(b'-NOSCRIPT'):return
  with self.lock:
   self.counts[target]+=1;index=self.counts[target]
  expected=index if target=='source'else 1
  if target=='source'and index>2 or target=='native'and index>1 or raw!=(':'+str(expected)+'\r\n').encode():
   self.invalid.append('AuthorityUnexpectedReply');raise ValueError('AuthorityUnexpectedReply')
  began=time.monotonic();self.trace(target+'-actual-reply-read-held');time.sleep(2.4);ended=time.monotonic();self.trace(target+'-actual-reply-released')
  if not 2.4<=ended-began<=2.75:self.invalid.append('AuthorityHoldBound');raise ValueError('AuthorityHoldBound')
def main():
 p=argparse.ArgumentParser();p.add_argument('--sha',required=True);p.add_argument('--source-profile',required=True);a=p.parse_args();g=PhysicalGate(a.sha,a.source_profile)
 # Native target uses default Gate.profile membership plus its known immutable script.
 g.profile=g.native_key
 server=Proxy(('0.0.0.0',6381),Handler);server.gate=g;server.backend_port=6391;stop=threading.Event()
 for sig in[signal.SIGTERM,signal.SIGINT]:signal.signal(sig,lambda *_:stop.set())
 thread=threading.Thread(target=server.serve_forever);thread.start()
 try:stop.wait()
 finally:
  with g.lock:g.closed=True;sockets=list(g.sockets)
  for sock in sockets:
   try:sock.shutdown(2)
   except OSError:pass
  server.shutdown();server.server_close();thread.join(15)
  Path('/evidence/physical-proxy-private.json').write_text(json.dumps({**g.snapshot(),'counts':g.counts},indent=2)+'\n')
if __name__=='__main__':main()
