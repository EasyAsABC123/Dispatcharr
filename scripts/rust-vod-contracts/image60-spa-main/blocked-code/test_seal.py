"""Local activation normalization and exact dependency closure; no dispatch."""
import hashlib,json,tempfile,unittest
from pathlib import Path
from unittest.mock import patch
import run as R
class Seal(unittest.TestCase):
 def fixture(self,root):
  (root/'run.py').write_text('before\nHARD_BLOCKED=True\nafter\n');(root/'helper.py').write_text('x=1\n');s={'state':'BLOCKED_IMAGE60_MAIN_SPA_ROOT_REVIEW','files':{p.name:hashlib.sha256(p.read_bytes()).hexdigest()for p in root.iterdir()},'external_inputs':{}};f=root/'seal.json';f.write_text(json.dumps(s));return f
 def test_flag_only_copy_accepts_both_and_other_change_refused(self):
  with tempfile.TemporaryDirectory()as d:
   root=Path(d);f=self.fixture(root)
   with patch.object(R,'HERE',root):
    R.check_seal(f);run=root/'run.py';run.write_bytes(run.read_bytes().replace(b'HARD_BLOCKED=True',b'HARD_BLOCKED=False'));R.check_seal(f);run.write_bytes(run.read_bytes()+b'behavior_change=True\n')
    with self.assertRaises(ValueError):R.check_seal(f)
 def test_missing_or_added_helper_refused(self):
  with tempfile.TemporaryDirectory()as d:
   root=Path(d);f=self.fixture(root);(root/'helper.py').unlink()
   with patch.object(R,'HERE',root):
    with self.assertRaises(ValueError):R.check_seal(f)
if __name__=='__main__':unittest.main()
