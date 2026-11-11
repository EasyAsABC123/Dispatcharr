import json,tempfile,unittest
from pathlib import Path
from unittest import mock
import reconstruct
class ReconstructionTests(unittest.TestCase):
 def test_exact_whole_source(self):
  with tempfile.TemporaryDirectory(prefix='dispatcharr-m1-reconstruct-')as d:
   result=reconstruct.reconstruct(Path.cwd(),Path(d)/'fresh');self.assertTrue(result['passed']);self.assertGreater(result['files'],600)
 def test_nonempty_refused(self):
  with tempfile.TemporaryDirectory()as d:
   (Path(d)/'existing').write_text('retain')
   with self.assertRaisesRegex(ValueError,'Fresh empty'):reconstruct.reconstruct(Path.cwd(),d)
   self.assertEqual((Path(d)/'existing').read_text(),'retain')
 def test_changed_overlay_refused(self):
  original=reconstruct.sha
  def bad(p):return '0'*64 if 'source-overlays'in p.parts else original(p)
  with tempfile.TemporaryDirectory()as d,mock.patch.object(reconstruct,'sha',side_effect=bad):
   with self.assertRaisesRegex(ValueError,'overlay changed'):reconstruct.reconstruct(Path.cwd(),Path(d)/'fresh')
if __name__=='__main__':unittest.main()
