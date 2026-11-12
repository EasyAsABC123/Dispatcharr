"""Owned local helpers only: no application, Docker, workload or resource sampler.
RESP peer is a protocol fixture, NOT real Redis counter/Source acceptance.
"""
import ctypes,json,os,select,socket,subprocess,sys,threading,time,unittest
from unittest import mock
import metrics,redis_session,workload,pathlib,tempfile
HELPER="import ctypes,sys; r=ctypes.CDLL(None,use_errno=True).prctl(4,int(sys.argv[1]),0,0,0); assert r==0;print('READY',flush=True);sys.stdin.readline()"
class LocalKernel(unittest.TestCase):
 def child(self,dumpable):
  p=subprocess.Popen([sys.executable,'-c',HELPER,str(dumpable)],stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
  def cleanup():
   if p.poll()is None:
    p.stdin.close()
    try:p.wait(timeout=2)
    except subprocess.TimeoutExpired:p.kill();p.wait(timeout=2)
   for f in[p.stdin,p.stdout,p.stderr]:
    if not f.closed:f.close()
   self.assertIsNotNone(p.poll())
  self.addCleanup(cleanup)
  self.assertTrue(select.select([p.stdout],[],[],2)[0]);self.assertEqual(p.stdout.readline(),b'READY\n');return p
 def test_real_nondumpable_memory_and_exe_denial_then_complete_helper(self):
  denied=self.child(0);ident=[os.geteuid(),os.getegid()];row=metrics.collect_pid(denied.pid,ident)
  self.assertFalse(row['coverage_complete']);self.assertIn('memory',row['denied_fields']);self.assertIsNone(row['pss_bytes']);self.assertTrue(row['stable']);denied_row=row
  denied.stdin.close();denied.wait(timeout=2)
  missing=metrics.collect_pid(denied.pid,ident);self.assertTrue(missing['raced_process']);self.assertFalse(missing['coverage_complete'])
  good=self.child(1);row=metrics.collect_pid(good.pid,ident);self.assertTrue(row['coverage_complete']);self.assertTrue(row['stable']);self.assertGreater(row['rss_bytes'],0);self.assertGreater(row['pss_bytes'],0)
  self.assertNotEqual(denied.pid,good.pid) # real exit/race; same-number reuse remains injected control.
  print(json.dumps({'local_kernel_only':True,'denied':denied_row,'exited_process':missing,'complete_after_helper_exit':row,'same_numeric_PID_reuse_observed':False},sort_keys=True))
class LocalRESP(unittest.TestCase):
 def exercise(self,held):
  listener=socket.socket();listener.bind(('127.0.0.1',0));listener.listen(1);listener.settimeout(2);port=listener.getsockname()[1];ready=threading.Event();eof=threading.Event();errors=[]
  def server():
   conn=None
   try:
    conn,_=listener.accept();conn.settimeout(4);reader=conn.makefile('rb')
    def command():
     line=reader.readline(129)
     if not line:return None
     if not line.startswith(b'*'):raise ValueError('ExpectedRESPArray')
     result=[]
     for _ in range(int(line[1:])):
      length=reader.readline(129);size=int(length[1:]);raw=reader.read(size+2)
      if raw[-2:]!=b'\r\n':raise ValueError('RESPTerminator')
      result.append(raw[:-2])
     return result
    self.assertEqual(command(),[b'AUTH',b'local-fixture']);conn.sendall(b'+OK\r\n')
    for index in range(2):
     self.assertEqual(command(),[b'INFO']);body=('run_id:'+'a'*40+'\r\ntotal_commands_processed:'+str(index+1)+'\r\ntotal_net_input_bytes:100\r\ntotal_net_output_bytes:200\r\n').encode();conn.sendall(b'$'+str(len(body)).encode()+b'\r\n')
     if held and index==1:
      ready.set();self.assertEqual(reader.read(1),b'');eof.set();break
     conn.sendall(body+b'\r\n')
    if not held:self.assertEqual(reader.read(1),b'');eof.set()
    reader.close()
   except BaseException as error:errors.append(type(error).__name__)
   finally:
    if conn is not None:conn.close()
    listener.close()
  thread=threading.Thread(target=server,name='owned-m0-local-RESP',daemon=True);thread.start();session=redis_session.RedisInfoSession('127.0.0.1',port,b'local-fixture');client=None;client_errors=[]
  try:
   with mock.patch.object(redis_session,'EXECUTION_BLOCKED',False):session.open()
   if held:
    def sample():
     try:session.sample('before-reader')
     except BaseException as error:client_errors.append(type(error).__name__)
    client=threading.Thread(target=sample,name='owned-m0-local-INFO',daemon=True);client.start();self.assertTrue(ready.wait(2));client.join(.05);self.assertTrue(client.is_alive());self.assertGreaterEqual(session.socket.fileno(),0);session.close();client.join(2);self.assertFalse(client.is_alive());self.assertTrue(client_errors);self.assertEqual(len(session.records),1)
   else:session.sample('before-reader');session.close();self.assertEqual(len(session.records),2)
   self.assertTrue(eof.wait(2));thread.join(2);self.assertFalse(thread.is_alive());self.assertEqual(errors,[]);self.assertEqual(session.socket.fileno(),-1);print(json.dumps({'local_protocol_peer_NOT_Redis':True,'held_reply':held,'peer_EOF':eof.is_set(),'owner_socket_fd':session.socket.fileno(),'server_thread_drained':not thread.is_alive(),'client_error_types':client_errors,'accepted_INFO_snapshots':len(session.records)},sort_keys=True))
  finally:
   session.close();listener.close()
   if client is not None:client.join(2)
   thread.join(5)
   self.assertFalse(thread.is_alive())
 def test_actual_local_RESP_complete_then_peerEOF(self):self.exercise(False)
 def test_actual_local_held_reply_owned_until_close_then_error_and_peerEOF(self):self.exercise(True)
class LocalHTTP(unittest.TestCase):
 def test_actual_reader_physical_close_and_origin_peerEOF(self):
  listener=socket.socket();listener.bind(('127.0.0.1',0));listener.listen(1);listener.settimeout(2);port=listener.getsockname()[1];release=threading.Event();peerEOF=threading.Event();errors=[];ready=threading.Event();stop=threading.Event()
  def server():
   conn=None
   try:
    conn,_=listener.accept();conn.settimeout(4);raw=bytearray()
    while not raw.endswith(b'\r\n\r\n'):
     raw.extend(conn.recv(1))
     if len(raw)>4096:raise ValueError('LocalHTTPHeaderCapacity')
    conn.sendall(b'HTTP/1.1 200 OK\r\nConnection: close\r\n\r\nabc');self.assertTrue(release.wait(2));conn.sendall(b'next');self.assertEqual(conn.recv(1),b'');peerEOF.set()
   except BaseException as error:errors.append(type(error).__name__)
   finally:
    if conn is not None:conn.close()
    listener.close()
  server_thread=threading.Thread(target=server,name='owned-m0-local-HTTP',daemon=True);server_thread.start();result=[]
  with tempfile.TemporaryDirectory()as folder:
   shared={'stop':stop,'release':threading.Event(),'prefix_limit':2097152}
   def reader():result.append(workload.reader('http://127.0.0.1:'+str(port),'/','fixture',shared,ready,pathlib.Path(folder)/'0.ts',2))
   client=threading.Thread(target=reader,name='owned-m0-local-reader',daemon=True);client.start()
   try:self.assertTrue(ready.wait(2));stop.set();release.set();client.join(3);server_thread.join(3);self.assertFalse(client.is_alive());self.assertFalse(server_thread.is_alive());self.assertTrue(peerEOF.is_set());self.assertEqual(errors,[]);self.assertTrue(result[0]['physical_close']['socket_closed']);self.assertEqual(result[0]['errors'],[]);print(json.dumps({'local_HTTP_fixture_NOT_Source':True,'reader_close_receipt':result[0]['physical_close'],'independent_origin_peer_EOF':peerEOF.is_set(),'both_threads_drained':True},sort_keys=True))
   finally:stop.set();release.set();listener.close();client.join(3);server_thread.join(5)
if __name__=='__main__':unittest.main()
