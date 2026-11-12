"""Actual physical report negative controls; no Docker/network/build."""
import copy,importlib.util,json,unittest
from pathlib import Path
here=Path(__file__).parent;s=importlib.util.spec_from_file_location('_hash_fence_acceptance',here/'run_pair.py');m=importlib.util.module_from_spec(s);s.loader.exec_module(m);actual=json.loads((here/'native-physical-data-2.json').read_text())
class Acceptance(unittest.TestCase):
 def bad(self,fn,code=0):
  value=copy.deepcopy(actual);fn(value)
  with self.assertRaises(ValueError):m.accept_capture(code,value)
 def test_actual_positive(self):self.assertTrue(m.accept_capture(0,actual)['passed'])
 def test_nonzero_exit(self):self.bad(lambda v:None,7)
 def test_failed_check(self):self.bad(lambda v:v['checks'][0].update(passed=False))
 def test_missing_case(self):self.bad(lambda v:v['cases'].pop())
 def test_caller_released_early(self):self.bad(lambda v:v['cases'][0]['retention'].update(caller_retained=False))
 def test_ACK_claimed(self):self.bad(lambda v:v['cases'][0].update(no_ACK=False))
 def test_false_metadata(self):self.bad(lambda v:v['cases'][0].update(no_result=False))
 def test_cache_mutation(self):self.bad(lambda v:v['cases'][0].update(account_cache_unchanged=False))
if __name__=='__main__':unittest.main()
