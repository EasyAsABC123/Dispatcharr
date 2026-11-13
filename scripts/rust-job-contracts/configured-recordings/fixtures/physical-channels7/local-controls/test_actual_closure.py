import hashlib,json,sys,unittest
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT/'tools'))
import run_physical

def closed(names,hashes):
 if set(hashes)!=run_physical.REQUIRED or set(names)!=run_physical.REQUIRED:raise ValueError('Exact18ToolClosure')
 for name in run_physical.REQUIRED:
  if names[name]!=hashes[name]:raise ValueError('ChangedBoundTool')
 return True
class ActualClosure(unittest.TestCase):
 def actual(self):return {p.name:hashlib.sha256(p.read_bytes()).hexdigest()for p in(ROOT/'tools').iterdir()if p.is_file()},json.loads((ROOT/'proposed-inputs-channels-onward.json').read_text())['tools_sha256']
 def test_real_closed18(self):self.assertTrue(closed(*self.actual()))
 def test_required_missing_omitted_not_silently_projected(self):
  for name in ('support-inputs.json','reference.json','physical-policy.json'):
   names,expected=self.actual();names.pop(name)
   with self.assertRaisesRegex(ValueError,'Exact18ToolClosure'):closed(names,expected)
 def test_stale_hash_refused(self):
  names,expected=self.actual();names['physical-policy.json']='0'*64
  with self.assertRaisesRegex(ValueError,'ChangedBoundTool'):closed(names,expected)
