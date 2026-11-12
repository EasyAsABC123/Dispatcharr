"""Actual retained data positive + corruption controls; no Docker/network/build."""
import copy,importlib.util,json,tempfile,unittest
from pathlib import Path
here=Path(__file__).parent;s=importlib.util.spec_from_file_location('_audit_actual_acceptance',here/'run_pair.py');m=importlib.util.module_from_spec(s);s.loader.exec_module(m)
actual=json.loads((here/'native-audit-data-4.json').read_text())
class Acceptance(unittest.TestCase):
 def bad(self,fn,code=0):
  value=copy.deepcopy(actual);fn(value)
  with self.assertRaises(ValueError):m.accept_capture(code,value)
 def test_actual_positive(self):self.assertTrue(m.accept_capture(0,actual)['passed'])
 def test_nonzero_exit(self):self.bad(lambda v:None,7)
 def test_boolean_exit(self):self.bad(lambda v:None,False)
 def test_native_flag_missing(self):self.bad(lambda v:v.pop('native_verified'))
 def test_failed_gate(self):self.bad(lambda v:v['checks'][0].update(passed=False))
 def test_duplicate_gate(self):self.bad(lambda v:v['checks'][1].update(name=v['checks'][0]['name']))
 def test_missing_case(self):self.bad(lambda v:v['cases'].pop())
 def test_case_order(self):self.bad(lambda v:v['cases'].reverse())
 def test_native_count(self):self.bad(lambda v:v['cases'][0]['native_ids'].pop())
 def test_refused_single_not_native(self):self.bad(lambda v:v['cases'][4].update(source_single_fallback=False))
 def test_explicit_null_traceback_required(self):
  self.bad(lambda v:v['cases'][0]['results'][v['cases'][0]['native_ids'][0]]['metadata'].pop('traceback'))
 def test_native_parent_required(self):
  self.bad(lambda v:v['cases'][0]['results'][v['cases'][0]['native_ids'][0]]['metadata'].update(parent_id='wrong'))
 def test_native_state(self):
  self.bad(lambda v:v['cases'][0]['results'][v['cases'][0]['native_ids'][0]]['metadata'].update(status='PENDING'))
 def test_Original_consumer_observation_required(self):
  self.bad(lambda v:v['cases'][0]['results'][v['cases'][0]['native_ids'][0]].pop('Original_get_type'))
 def test_reference_corruption(self):
  with tempfile.TemporaryDirectory()as raw:
   path=Path(raw)/'bad.json';path.write_text('{}')
   with self.assertRaises(ValueError):m.verify_reference(path)
if __name__=='__main__':unittest.main()
