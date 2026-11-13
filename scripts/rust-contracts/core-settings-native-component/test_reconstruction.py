"""Offline source corruption controls; no compiler or application work."""
import json,shutil,tempfile,unittest
from pathlib import Path
from unittest.mock import patch
import reconstruct as R
class Reconstruction(unittest.TestCase):
 def setUp(self):
  self.tmp=tempfile.TemporaryDirectory();self.root=Path(self.tmp.name)
  shutil.copyfile(R.HERE/'historical-source-inputs.json',self.root/'historical-source-inputs.json');shutil.copytree(R.HERE/'historical-source-overlay',self.root/'historical-source-overlay')
 def tearDown(self):self.tmp.cleanup()
 def test_actual_historical_inputs(self):self.assertEqual(len(R.load_inputs()['after_inputs']),846)
 def test_changed_manifest(self):
  p=self.root/'historical-source-inputs.json';d=json.loads(p.read_text());d['Rust_inputs']=842;p.write_text(json.dumps(d))
  with patch.object(R,'HERE',self.root),self.assertRaises(ValueError):R.load_inputs()
 def test_modified_overlay(self):
  p=next((self.root/'historical-source-overlay').rglob('Cargo.lock'));p.write_bytes(p.read_bytes()+b'changed')
  with patch.object(R,'HERE',self.root),self.assertRaises(ValueError):R.load_inputs()
 def test_missing_overlay(self):
  next((self.root/'historical-source-overlay').rglob('Cargo.lock')).unlink()
  with patch.object(R,'HERE',self.root),self.assertRaises(ValueError):R.load_inputs()
 def test_extra_overlay(self):
  (self.root/'historical-source-overlay/extra').write_text('unselected')
  with patch.object(R,'HERE',self.root),self.assertRaises(ValueError):R.load_inputs()
if __name__=='__main__':unittest.main()
