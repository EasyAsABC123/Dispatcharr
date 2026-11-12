"""Source-only actual INCR reply gate. No Original client/method is replaced."""
import argparse,json,os,re,signal,socket,threading
from pathlib import Path
from resp_gate import Gate,Proxy,Handler,Control
class SourceGate(Gate):
 def __init__(self,evidence):super().__init__('unused-Source-command',evidence)
 def arm(self,profile,expected_reply=1):
  if not re.fullmatch(r'profile_connections:[0-9]{1,16}',profile):raise ValueError('OwnedProfileShape')
  if type(expected_reply)is not int or expected_reply not in[1,2]:raise ValueError('DeclaredSourceINCRReply')
  with self.lock:
   if self.armed:raise ValueError('AlreadyArmed')
   self.profile=profile.encode();self.armed=True;self.seen=0;self.expected_reply=expected_reply;self.release.clear()
 def target(self,args):
  with self.lock:
   if not self.armed:return False
   return len(args)==2 and args[0].upper()==b'INCR'and args[1]==self.profile or len(args)==3 and args[0].upper()==b'INCRBY'and args[1]==self.profile and args[2]==b'1'
 def response(self,target,raw):
  with self.lock:
   if target and self.armed and raw!=(':'+str(self.expected_reply)+'\r\n').encode():
    self.invalid.append('UnexpectedSourceINCRReplySchema');raise ValueError('UnexpectedSourceINCRReplySchema')
  return super().response(target,raw)
 def snapshot(self):return{**super().snapshot(),'witness':'actual Original Redis INCR reply held','expected_reply':self.expected_reply}
def main():
 if os.environ.get('PENDING_MAIN_FIXTURE')!='playback-fixture-only':raise RuntimeError('OwnedSourceProxyRequired')
 p=argparse.ArgumentParser();p.add_argument('--evidence',type=Path,required=True);a=p.parse_args()
 if not str(a.evidence).startswith('/evidence/'):raise ValueError('OwnedEvidencePath')
 gate=SourceGate(a.evidence);server=Proxy(('0.0.0.0',6383),Handler);control=Proxy(('127.0.0.1',6384),Control);server.gate=control.gate=gate;server.backend_port=6390;stop=threading.Event()
 for event in[signal.SIGINT,signal.SIGTERM]:signal.signal(event,lambda *_:stop.set())
 threads=[threading.Thread(target=v.serve_forever)for v in[server,control]]
 for thread in threads:thread.start()
 try:stop.wait()
 finally:
  gate.release.set()
  with gate.lock:gate.closed=True;active=list(gate.sockets)
  for connection in active:
   try:connection.shutdown(socket.SHUT_RDWR)
   except OSError:pass
  for service in[control,server]:service.shutdown();service.server_close()
  for thread in threads:thread.join(15)
  a.evidence.write_text(json.dumps(gate.snapshot(),indent=2)+'\n')
if __name__=='__main__':main()
