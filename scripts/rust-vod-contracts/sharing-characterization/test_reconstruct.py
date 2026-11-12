import json,shutil,tempfile,unittest
from pathlib import Path
from reconstruct import HERE,reconstruct
class Controls(unittest.TestCase):
 def test_exact_blocked_and_historical_restore(self):
  with tempfile.TemporaryDirectory()as tmp:
   for mode in ['blocked','executed-historical']:
    out=Path(tmp)/mode;self.assertEqual(reconstruct(out,mode)['restored_files'],87)
    self.assertIn('EXECUTION_BLOCKED='+('True'if mode=='blocked'else'False'),(out/'tools/run.py').read_text())
 def test_existing_output_rejected(self):
  with tempfile.TemporaryDirectory()as tmp:
   with self.assertRaises(ValueError):reconstruct(Path(tmp))
 def test_corruption_and_selection_mismatch_fail_before_output(self):
  with tempfile.TemporaryDirectory()as tmp:
   src=Path(tmp)/'src';shutil.copytree(HERE,src);out=Path(tmp)/'out';(src/'execution-inputs/tools/grants.py').write_text('corrupt')
   with self.assertRaises(ValueError):reconstruct(out,source=src)
   self.assertFalse(out.exists());(src/'execution-selection.json').write_text('{}')
   with self.assertRaises(ValueError):reconstruct(out,source=src)
 def test_symlink_input_rejected(self):
  with tempfile.TemporaryDirectory()as tmp:
   src=Path(tmp)/'src';shutil.copytree(HERE,src);file=src/'execution-inputs/tools/grants.py';target=Path(tmp)/'target';file.rename(target);file.symlink_to(target)
   with self.assertRaises(ValueError):reconstruct(Path(tmp)/'out',source=src)
if __name__=='__main__':unittest.main(verbosity=2)
