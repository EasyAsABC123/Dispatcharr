"""Actual physical report corruption controls; no fake successful factory run."""
import copy,importlib.util,json,unittest
from pathlib import Path
here=Path(__file__).parent;s=importlib.util.spec_from_file_location('_audit_physical_acceptance',here/'run_pair.py');m=importlib.util.module_from_spec(s);s.loader.exec_module(m);actual=json.loads((here/'native-physical-data-2.json').read_text())
class Acceptance(unittest.TestCase):
 def bad(self,fn,code=0):
  value=copy.deepcopy(actual);fn(value)
  with self.assertRaises(ValueError):m.accept_capture(code,value)
 def test_actual_positive(self):self.assertTrue(m.accept_capture(0,actual)['passed'])
 def test_nonzero_exit(self):self.bad(lambda v:None,7)
 def test_bool_exit(self):self.bad(lambda v:None,False)
 def test_native_missing(self):self.bad(lambda v:v.pop('native_verified'))
 def test_failed_check(self):self.bad(lambda v:v['checks'][0].update(passed=False))
 def test_missing_case(self):self.bad(lambda v:v['cases'].pop())
 def test_wrong_code(self):self.bad(lambda v:v['cases'][0].update(actual_code='P0002'))
 def test_not_ERROR(self):self.bad(lambda v:v['cases'][0].update(actual_severity='FATAL'))
 def test_repeated_statement(self):self.bad(lambda v:v['cases'][0].update(error_calls=2))
 def test_unknown_not_fake_SUCCESS(self):self.bad(lambda v:v['cases'][2].update(actual_result_state='SUCCESS'))
 def test_caller_released_early(self):self.bad(lambda v:v['cases'][0]['observed_retention'].update(caller_retained=False))
if __name__=='__main__':unittest.main()
