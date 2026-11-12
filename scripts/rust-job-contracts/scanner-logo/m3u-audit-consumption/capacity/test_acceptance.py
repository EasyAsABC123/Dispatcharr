"""Actual native capacity boundary rejection controls, no workload execution."""
import copy,importlib.util,json,unittest
from pathlib import Path
here=Path(__file__).parent;s=importlib.util.spec_from_file_location('_audit_capacity_acceptance',here/'run_pair.py');m=importlib.util.module_from_spec(s);s.loader.exec_module(m);actual=json.loads((here/'native-capacity-data-1.json').read_text())
class Acceptance(unittest.TestCase):
 def bad(self,fn,code=0):
  value=copy.deepcopy(actual);fn(value)
  with self.assertRaises(ValueError):m.accept_capture(code,value)
 def test_actual_positive(self):self.assertTrue(m.accept_capture(0,actual)['passed'])
 def test_nonzero_exit(self):self.bad(lambda v:None,7)
 def test_bool_exit(self):self.bad(lambda v:None,False)
 def test_case_missing(self):self.bad(lambda v:v['cases'].pop())
 def test_false_gate(self):self.bad(lambda v:v['checks'][0].update(passed=False))
 def test_owned_extra_row_rejected(self):self.bad(lambda v:v['cases'][0].update(history_after=10000))
 def test_initial_overflow_accepted(self):self.bad(lambda v:v['cases'][1]['native_observation'].update(run='Ok(Completed(fake))'))
 def test_oldest_removed(self):self.bad(lambda v:v['cases'][0].update(oldest_id_unchanged=False))
if __name__=='__main__':unittest.main()
