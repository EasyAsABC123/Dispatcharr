"""Retained HEAD1 integrity/corruption checks; no Source requests."""
import json,shutil,tempfile,unittest,hashlib
from pathlib import Path
from source_head_reference import validate_frozen_head_reference
ROOT=Path(__file__).resolve().parents[1]
class HeadReference(unittest.TestCase):
 def test_actual_one_reference(self):self.assertEqual(validate_frozen_head_reference(ROOT)['status'],401)
 def test_changed_raw_rejected(self):
  with tempfile.TemporaryDirectory()as folder:
   target=Path(folder)/'controller';shutil.copytree(ROOT,target,ignore=shutil.ignore_patterns('__pycache__'));p=target/'retained-head/Source-method-private.json';v=json.loads(p.read_text());v['cases'][0]['status']=405;p.write_text(json.dumps(v))
   with self.assertRaises(ValueError):validate_frozen_head_reference(target)
 def test_changed_projection_rejected(self):
  with tempfile.TemporaryDirectory()as folder:
   target=Path(folder)/'controller';shutil.copytree(ROOT,target,ignore=shutil.ignore_patterns('__pycache__'));p=target/'Source-HEAD1-reference.json';v=json.loads(p.read_text());v['observed']['status']=405;p.write_text(json.dumps(v))
   with self.assertRaises(ValueError):validate_frozen_head_reference(target)
 def test_missing_reference_rejected(self):
  with tempfile.TemporaryDirectory()as folder:
   target=Path(folder)/'controller';shutil.copytree(ROOT,target,ignore=shutil.ignore_patterns('__pycache__'));(target/'retained-head/agent-independent-review.json').unlink()
   with self.assertRaises(FileNotFoundError):validate_frozen_head_reference(target)
