"""Pure/local owned socket and child controls. No Source/Django application."""
import io,json,os,socket,sys,tempfile,threading,time,types,unittest,urllib.request,urllib.error
from pathlib import Path
from reader import Reader
from ledger_origin import Ledger,Server,ALIASES
from local_command import LocalCommand
from bounded_process import CollectionFailure
class Response:
 def __init__(self,sock):
  self.sock=sock;self.file=sock.makefile('rb');self.raw=types.SimpleNamespace(read=self.file.read,_original_response=types.SimpleNamespace(fp=types.SimpleNamespace(raw=types.SimpleNamespace(_sock=sock))))
 def close(self):self.file.close();self.sock.close()
class Readers(unittest.TestCase):
 def test_retention_cap_keeps_draining_without_closing(self):
  with tempfile.TemporaryDirectory()as directory:
   client,producer=socket.socketpair();client.settimeout(2);response=Response(client);reader=Reader(response,Path(directory)/'body','other',cap=16384,wire_cap=65536);producer.sendall(b'x'*49152);reader.await_prefix();end=time.monotonic()+2
   while reader.snapshot()['received']<49152 and time.monotonic()<end:time.sleep(.01)
   state=reader.snapshot();self.assertEqual(state['retained'],16384);self.assertEqual(state['received'],49152);self.assertFalse(state['reader_done']);self.assertGreaterEqual(client.fileno(),0);self.assertEqual((Path(directory)/'body').stat().st_size,16384);receipt=reader.close();self.assertTrue(receipt['reader_joined']);self.assertEqual(receipt['fd_after'],-1);producer.close()
 def test_initial8192_is_retained_once_and_decoder_starts_at_zero(self):
  with tempfile.TemporaryDirectory()as directory:
   client,producer=socket.socketpair();client.settimeout(2);reader=Reader(Response(client),Path(directory)/'body','other',cap=16384,wire_cap=32768,initial=b'a'*8192);producer.sendall(b'b'*16384);reader.await_prefix();end=time.monotonic()+2
   while reader.snapshot()['received']<24576 and time.monotonic()<end:time.sleep(.01)
   self.assertEqual((Path(directory)/'body').read_bytes(),b'a'*8192+b'b'*8192);self.assertEqual(reader.snapshot()['received'],24576);reader.close();producer.close()
 def test_literal_eof_does_not_extend_fixture(self):
  with tempfile.TemporaryDirectory()as directory:
   client,producer=socket.socketpair();client.settimeout(2);reader=Reader(Response(client),Path(directory)/'body','other',cap=8192,wire_cap=16384);producer.sendall(b'x'*8192);producer.shutdown(1);reader.await_prefix();reader.thread.join(2);self.assertTrue(reader.snapshot()['eof']);reader.close();producer.close()
 def test_wire_overflow_stays_owned_until_physical_close(self):
  with tempfile.TemporaryDirectory()as directory:
   client,producer=socket.socketpair();client.settimeout(2);reader=Reader(Response(client),Path(directory)/'body','other',cap=8192,wire_cap=8192);producer.sendall(b'x'*16384);reader.thread.join(2);self.assertIn('ReaderWireCapacity',reader.snapshot()['invalid']);self.assertGreaterEqual(client.fileno(),0);reader.close();producer.close()
class Origins(unittest.TestCase):
 def test_nonce_paths_and_connection_bounds(self):
  for nonce in['','nothex','a'*31]:
   with self.assertRaises(ValueError):Ledger(nonce)
  with self.assertRaises(ValueError):Ledger('a'*32,{'/wrong':'other'})
  ledger=Ledger('a'*32)
  class Sock:
   def fileno(self):return 3
  for _ in range(64):ledger.opened(Sock())
  with self.assertRaises(RuntimeError):ledger.opened(Sock())
  self.assertIn('OriginConnectionCapacity',ledger.snapshot()['invalid'])
 def test_event_overflow_sticky_and_local_stop_not_peer_ack(self):
  ledger=Ledger('a'*32)
  class Sock:
   def fileno(self):return 3
  identity=ledger.opened(Sock());ledger.peer_eof(identity,local_stop=True);self.assertFalse(ledger.snapshot()['connections'][0]['peer_eof_observed'])
  for _ in range(513):ledger.event('synthetic')
  self.assertIn('OriginEventCapacity',ledger.snapshot()['invalid']);self.assertLessEqual(len(ledger.events),512)
 def test_real_origin_HEAD_controls_stream_and_socket_shutdown_are_distinct(self):
  with tempfile.TemporaryDirectory()as directory:
   media=Path(directory)/'media';media.write_bytes(b'x'*65536);server=Server(('127.0.0.1',0),media,'a'*32,32768);thread=threading.Thread(target=server.serve_forever);thread.start();base='http://127.0.0.1:'+str(server.server_port);response=None
   try:
    with urllib.request.urlopen(urllib.request.Request(base+next(iter(ALIASES)),method='HEAD'),timeout=3)as r:self.assertEqual(r.status,200)
    with self.assertRaises(urllib.error.HTTPError):urllib.request.urlopen(base+'/ledger',timeout=3)
    response=urllib.request.urlopen(base+next(iter(ALIASES)),timeout=3)
    with urllib.request.urlopen(urllib.request.Request(base+'/ledger',headers={'X-Fixture-Nonce':'a'*32}),timeout=3)as r:state=json.loads(r.read(65537))
    self.assertEqual(state['media_HEAD_total'],1);self.assertEqual(state['actual_stream_GET_inflight'],1);self.assertGreater(state['accepted_TCP_total'],state['actual_stream_GET_inflight']);self.assertTrue(any(row['kind']=='control'for row in state['requests']))
   finally:
    if response:response.close()
    server.stop_owned();thread.join(10)
   state=server.ledger.snapshot();self.assertFalse(thread.is_alive());self.assertEqual(state['actual_open_TCP'],0);self.assertTrue(all(row['fd_after']==-1 for row in state['connections']));self.assertFalse(state['invalid'])
class Commands(unittest.TestCase):
 def test_real_stdout_stderr_overflow_stops_reaps_and_drains(self):
  job=LocalCommand([sys.executable,'-c',"import os; [(os.write(1,b'x'*65536),os.write(2,b'y'*65536)) for _ in range(64)]"],limit=32768)
  with self.assertRaises(CollectionFailure):job.wait(5)
  self.assertIsNotNone(job.process.poll());self.assertTrue(all(not t.is_alive()for t in job.threads));self.assertLessEqual(job.used,32768)
 def test_real_abort_retains_physical_child_until_reaped(self):
  job=LocalCommand([sys.executable,'-c','import time;time.sleep(10)']);job.abort()
  with self.assertRaises(CollectionFailure):job.wait(5)
  self.assertIsNotNone(job.process.poll());self.assertTrue(all(not t.is_alive()for t in job.threads))
if __name__=='__main__':unittest.main(verbosity=2)
