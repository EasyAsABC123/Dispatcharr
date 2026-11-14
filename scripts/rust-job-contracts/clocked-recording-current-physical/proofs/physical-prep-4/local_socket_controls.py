"""Bounded loopback-only OS socket controls; no app/PG/Redis/media workload."""
import json,os,socket,sys,time,unittest
from pathlib import Path
ROOT=Path(__file__).resolve().parent;sys.path.insert(0,str(ROOT/'tools'));from owned_nodelay import configure
ACTUAL=[]
class Controls(unittest.TestCase):
 def pair(self):
  listener=socket.socket(socket.AF_INET,socket.SOCK_STREAM);listener.settimeout(1);listener.bind(('127.0.0.1',0));listener.listen(1);self.addCleanup(listener.close)
  client=socket.socket(socket.AF_INET,socket.SOCK_STREAM);client.settimeout(1);self.addCleanup(client.close);client.connect(listener.getsockname());accepted,_=listener.accept();accepted.settimeout(1);self.addCleanup(accepted.close);return client,accepted
 def test_actual_four_roles_readback_one(self):
  client,accepted=self.pair()
  for role,sock in [('native-downstream',accepted),('postgres-upstream',client),('redis-downstream',accepted),('redis-upstream',client)]:
   record=configure(sock,role);self.assertEqual(type(record['actual_TCP_NODELAY_after']),int);self.assertEqual(record['actual_TCP_NODELAY_after'],1);ACTUAL.append(record)
  client.sendall(b'owned-control');self.assertEqual(accepted.recv(32),b'owned-control')
 def test_unknown_role_before_socket_mutation(self):
  client,_=self.pair();client.setsockopt(socket.IPPROTO_TCP,socket.TCP_NODELAY,0)
  with self.assertRaises(ValueError):configure(client,'arbitrary-role')
  self.assertEqual(client.getsockopt(socket.IPPROTO_TCP,socket.TCP_NODELAY),0)
 def test_unconnected_never_reports_acknowledged(self):
  with socket.socket(socket.AF_INET,socket.SOCK_STREAM)as sock:
   with self.assertRaises(OSError):configure(sock,'postgres-upstream')
   self.assertEqual(sock.getsockopt(socket.IPPROTO_TCP,socket.TCP_NODELAY),0)
 def test_unix_socket_not_promoted(self):
  left,right=socket.socketpair();self.addCleanup(left.close);self.addCleanup(right.close)
  with self.assertRaises(ValueError):configure(left,'native-downstream')
 def test_failed_readback_refuses(self):
  class FailedReadback:
   family=socket.AF_INET;type=socket.SOCK_STREAM
   def getpeername(self):return('127.0.0.1',5432)
   def getsockname(self):return('127.0.0.1',12345)
   def getsockopt(self,*unused):return 0
   def setsockopt(self,*unused):pass
  with self.assertRaises(ValueError):configure(FailedReadback(),'postgres-upstream')
 def test_foreign_address_refuses_before_options(self):
  class Foreign:
   family=socket.AF_INET;type=socket.SOCK_STREAM
   def getpeername(self):return('192.0.2.1',5432)
   def getsockname(self):return('127.0.0.1',12345)
   def getsockopt(self,*unused):raise AssertionError('must not inspect foreign options')
  with self.assertRaises(ValueError):configure(Foreign(),'postgres-upstream')
if __name__=='__main__':
 began=time.monotonic();result=unittest.TextTestRunner().run(unittest.defaultTestLoader.loadTestsFromTestCase(Controls));elapsed=time.monotonic()-began
 receipt={'actual_exit':0 if result.wasSuccessful()else 1,'tests_run':result.testsRun,'elapsed_seconds':elapsed,'pid':os.getpid(),'original_starttime':Path('/proc/self/stat').read_text().rsplit(')',1)[1].split()[19],'actual_owned_socket_readbacks':ACTUAL,'scope':'bounded owned IPv4 loopback socket pair only; no app/PG/Redis/media/task; no latency benchmark or historical causal proof','kernel_sockets_closed_before_receipt':True}
 (ROOT/'socket-controls.json').write_text(json.dumps(receipt,indent=2)+'\n');raise SystemExit(receipt['actual_exit'])
