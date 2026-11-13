"""Actual finite Python child/pipe controls, no Native/app/network fixture."""
import os,sys,unittest
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'tools'))
from interactive_peer import Peer,CollectionFailure
PROGRAM="import sys,json;\nfor line in sys.stdin:\n v=json.loads(line);print('NATIVE_CONFIGURED_RECORDINGS_JSON='+json.dumps(v),flush=True)"
class Interactive(unittest.TestCase):
 def make(self,program=PROGRAM):
  state={}
  def stop():
   child=state.get('peer')
   if child and child.poll()is None:child.process.terminate()
  p=Peer([sys.executable,'-u','-c',program],dict(os.environ),stop);state['peer']=p;return p
 def test_actual_echo_pipe_waits_are_joined(self):
  p=self.make()
  try:
   p.send({'first':None});self.assertEqual(p.receive(2),{'first':None});p.send({'second':[1,2]});self.assertEqual(p.receive(2),{'second':[1,2]});p.finish(2)
   self.assertTrue(p.physically_settled);self.assertTrue(all(not t.is_alive()for t in p.threads));self.assertFalse(p.writer.is_alive());self.assertIsNone(p.invalid)
  finally:
   if p.poll()is None:p.abort();p.finish(2)
 def test_early_eof_is_not_json_null(self):
  p=self.make('pass')
  try:
   with self.assertRaises(CollectionFailure):p.receive(2)
  finally:p.finish(2)
 def test_input_cap_does_not_truncate_to_valid_command(self):
  p=self.make()
  try:
   with self.assertRaises(ValueError):p.send({'x':'a'*8192})
  finally:p.finish(2)
if __name__=='__main__':unittest.main()
