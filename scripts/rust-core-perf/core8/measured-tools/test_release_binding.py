import copy,json,unittest
from pathlib import Path
import m1_imports
binding=m1_imports.load('release_binding')
HERE=Path(__file__).resolve().parent.parent
class ReleaseBindingControls(unittest.TestCase):
 def setUp(self):self.record=json.loads((HERE/'release-record.json').read_text());self.binary=HERE/'native-core-peer'
 def test_actual_binding(self):self.assertEqual(binding.validate(self.record,self.binary)['production_base_commit'],binding.BASE)
 def test_wrong_source_map(self):
  self.record['source_map'].popitem()
  with self.assertRaises(ValueError):binding.validate(self.record,self.binary)
 def test_wrong_optimized_profile(self):
  self.record['profile']['debug_assertions']=True
  with self.assertRaises(ValueError):binding.validate(self.record,self.binary)
 def test_wrong_production_base(self):
  self.record['production_base']='0'*40
  with self.assertRaises(ValueError):binding.validate(self.record,self.binary)
 def test_wrong_ELF(self):
  self.record['binary_sha256']='0'*64
  with self.assertRaises(ValueError):binding.validate(self.record,self.binary)
if __name__=='__main__':unittest.main()
