"""Actual guard report positive/corruption controls, no workload execution."""
import copy,importlib.util,json,tempfile,unittest
from pathlib import Path
here=Path(__file__).parent;s=importlib.util.spec_from_file_location('_hash_admission_acceptance',here/'run_pair.py');m=importlib.util.module_from_spec(s);s.loader.exec_module(m);actual=json.loads((here/'native-guard-data-3.json').read_text())
class Acceptance(unittest.TestCase):
 def bad(self,fn,code=0):
  value=copy.deepcopy(actual);fn(value)
  with self.assertRaises(ValueError):m.accept_capture(code,value)
 def test_actual_positive(self):self.assertTrue(m.accept_capture(0,actual)['passed'])
 def test_nonzero_exit(self):self.bad(lambda v:None,7)
 def test_bool_exit(self):self.bad(lambda v:None,False)
 def test_failed_check(self):self.bad(lambda v:v['checks'][0].update(passed=False))
 def test_missing_case(self):self.bad(lambda v:v['cases'].pop())
 def test_refusal_became_completion(self):self.bad(lambda v:v['cases'][4]['native_outputs'][0].update(run='Ok(Completed(fake))'))
 def test_source_fallback_not_hidden(self):self.bad(lambda v:v['cases'][4].pop('Source_fallback'))
 def test_large_text_bulk_transferred(self):self.bad(lambda v:v['cases'][-1]['projected_rows'].update(max_data_row_bytes=262144))
 def test_exact8192_not_observed(self):self.bad(lambda v:v['cases'][-3]['projected_rows'].update(max_data_row_bytes=50))
 def test_reference_corruption(self):
  with tempfile.TemporaryDirectory()as raw:
   f=Path(raw)/'bad';f.write_text('{}')
   with self.assertRaises(ValueError):m.verify_reference(f)
if __name__=='__main__':unittest.main()
