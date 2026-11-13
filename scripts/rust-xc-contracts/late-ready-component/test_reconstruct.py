import shutil,tempfile,unittest
from pathlib import Path
from unittest.mock import patch
import reconstruct as R
class Reconstruction(unittest.TestCase):
 def test_tampered_map_fails_before_output(self):
  with tempfile.TemporaryDirectory()as d:
   p=Path(d);(p/'current-union').mkdir();shutil.copyfile(R.HERE/'current-union/source-inputs.json',p/'current-union/source-inputs.json');(p/'current-union/source-inputs.json').write_text('{}')
   with patch.object(R,'HERE',p),self.assertRaisesRegex(ValueError,'PinnedSourceMap'):R.reconstruct(p,p/'out')
   self.assertFalse((p/'out').exists())
 def test_tampered_manifest_fails_before_output(self):
  with tempfile.TemporaryDirectory()as d:
   p=Path(d);(p/'current-union').mkdir();shutil.copyfile(R.HERE/'current-union/source-inputs.json',p/'current-union/source-inputs.json');(p/'current-union/selected-hunks.json').write_text('{}')
   with patch.object(R,'HERE',p),self.assertRaisesRegex(ValueError,'PinnedOverlayManifest'):R.reconstruct(p,p/'out')
   self.assertFalse((p/'out').exists())
 def test_existing_output_refused_before_git(self):
  with tempfile.TemporaryDirectory()as d:
   p=Path(d)
   with self.assertRaisesRegex(ValueError,'FreshOutputOnly'):R.reconstruct(p,p)
if __name__=='__main__':unittest.main()
