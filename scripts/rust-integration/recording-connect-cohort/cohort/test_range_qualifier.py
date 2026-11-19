"""Offline false-acceptance regressions; synthetic input is NOT runtime evidence."""
import json,tempfile,unittest
from pathlib import Path
from run import paired_exceptional_ranges
class RangeQualification(unittest.TestCase):
 def setUp(self):
  self.directory=tempfile.TemporaryDirectory(prefix='dispatcharr-range-qualifier-');self.addCleanup(self.directory.cleanup);self.output=Path(self.directory.name)
 def qualify(self):
  binding = {'retained_Source_playback': str(self.output / 'Original' / 'overlap-completed-playback.json')}
  return paired_exceptional_ranges(self.output, binding)['status']
 def write(self,backend,size,bad=False,timeout=False):
  path=self.output/backend;path.mkdir(exist_ok=True);cells=[]
  for name,start,end,length in [('range-reversed-transport',7,2,-4),('range-outside-transport',size+100,size-1,-100)]:
   cells.append({'cell':name,'actual_header_bytes':f'HTTP/1.1 206 Partial Content\r\nContent-Length: {length if not bad else 0}\r\nContent-Range: bytes {start}-{end}/{size}','actual_peer_EOF':True,'actual_body_bytes':0,**({'transport_failure':'TimeoutError'}if timeout else{})})
  (path/'overlap-completed-playback.json').write_text(json.dumps({'actual_cells':cells}))
 def test_distinct_real_object_sizes_use_same_range_relationship(self):
  self.write('Original',100);self.write('Native',200);self.assertEqual(self.qualify(),'passed')
 def test_wrong_negative_length_is_failure(self):
  self.write('Original',100);self.write('Native',200,bad=True);self.assertEqual(self.qualify(),'failed')
 def test_timeout_is_never_transport_parity(self):
  self.write('Original',100,timeout=True);self.write('Native',200,timeout=True);self.assertEqual(self.qualify(),'failed')
 def test_missing_backend_is_blocked(self):
  self.write('Original',100);self.assertEqual(self.qualify(),'blocked')
if __name__=='__main__':unittest.main()
