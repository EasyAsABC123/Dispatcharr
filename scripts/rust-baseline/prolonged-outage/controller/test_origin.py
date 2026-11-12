"""Offline lifecycle/identity controls; no Source, HTTP server or socket opened."""
import importlib.util,sys,tempfile,threading,types,unittest
from pathlib import Path
from unittest import mock
from http_adapter import HTTPAdapter
spec=importlib.util.spec_from_file_location('outage_origin_private',Path(__file__).with_name('origin_fixture.py'))
origin=importlib.util.module_from_spec(spec)
with mock.patch.dict(sys.modules,{'range_http':types.SimpleNamespace(serve_file=None)}):spec.loader.exec_module(origin)
class Origin(unittest.TestCase):
 def state(self,folder):
  clip=Path(folder)/'clip';clip.write_bytes(b'fixture')
  return origin.FixtureState(clip,600)
 def record(self,state):
  sock=mock.Mock();sock.fileno.return_value=42;event=mock.Mock();event.wait.return_value=False
  record={'id':1,'path':'/live/0.ts','peer':['172.20.0.2',1234],'socket':sock,'closed':event};state.input_connections[1]=record
  return record
 def test_wrong_latch_does_not_mutate_or_close(self):
  with tempfile.TemporaryDirectory()as folder:
   s=self.state(folder);r=self.record(s)
   with self.assertRaisesRegex(ValueError,'LatchMismatch'):s.control('/live/0.ts','fail',expected_connection_ids=[])
   self.assertEqual(s.controls,{});r['socket'].shutdown.assert_not_called()
 def test_close_call_is_not_acknowledged_without_server_completion(self):
  with tempfile.TemporaryDirectory()as folder:
   s=self.state(folder);r=self.record(s)
   with self.assertRaisesRegex(RuntimeError,'Unacknowledged'):s.control('/live/0.ts','fail',expected_connection_ids=[1])
   self.assertTrue(s.controls['/live/0.ts']['fail']);self.assertEqual(s.closed_inputs,[])
 def test_only_completed_socket_cleanup_acknowledges_and_wait_is_outside_lock(self):
  with tempfile.TemporaryDirectory()as folder:
   s=self.state(folder);r=self.record(s)
   def finish(timeout):
    self.assertTrue(s.lock.acquire(False));s.lock.release()
    r['socket'].fileno.return_value=-1;s.physical_closed(r['socket']);return True
   r['closed'].wait.side_effect=finish
   receipt=s.control('/live/0.ts','fail',expected_connection_ids=[1]);self.assertTrue(receipt['all_closed']);self.assertEqual(receipt['closed_ids'],[1]);self.assertEqual(s.input_connections,{})
   self.assertEqual(s.closed_inputs[0]['socket_fileno'],-1)
 def test_open_descriptor_cannot_be_marked_closed(self):
  with tempfile.TemporaryDirectory()as folder:
   s=self.state(folder);r=self.record(s)
   with self.assertRaisesRegex(RuntimeError,'StillOpen'):s.physical_closed(r['socket'])
   r['closed'].set.assert_not_called()
 def test_warmed_input_is_nonvacuous_exact_and_source_owned(self):
  a=HTTPAdapter('http://127.0.0.1','http://127.0.0.1','/','token',{'source_IPs':['172.20.0.2']},None)
  item={'id':1,'path':'/live/0.ts','peer':['172.20.0.2',1234]}
  def check(items):return a.require_existing_owned_input({'origin_socket_witness':{'active_input_connections':items}},['/live/0.ts','/live/100.ts'])
  self.assertEqual(check([item]),[1])
  for items in [[],[item,item],[dict(item,peer=['foreign',1])],[dict(item,path='/other.ts')]]:
   with self.assertRaisesRegex(ValueError,'WarmedOwned'):check(items)
 def test_close_journal_capacity_cannot_overflow_or_ack(self):
  with tempfile.TemporaryDirectory()as folder:
   s=self.state(folder);r=self.record(s);r['socket'].fileno.return_value=-1;s.closed_inputs=[{}]*4096
   with self.assertRaisesRegex(ValueError,'JournalCapacity'):s.physical_closed(r['socket'])
   self.assertEqual(len(s.closed_inputs),4096);r['closed'].set.assert_not_called()
 def test_origin_entry_blocked_before_asset_generation(self):
  with mock.patch.object(origin.subprocess,'run',side_effect=AssertionError('no process')):
   with self.assertRaisesRegex(RuntimeError,'ExecutionBlocked'):origin.main()
if __name__=='__main__':unittest.main()
