"""Public aggregate rejection controls, no Docker/network/compilation."""
import copy,importlib.util,unittest
from pathlib import Path
spec=importlib.util.spec_from_file_location('_owned_child_capture_runner',Path(__file__).with_name('run_pair.py'));mod=importlib.util.module_from_spec(spec);spec.loader.exec_module(mod)
class Controls(unittest.TestCase):
 def good(self):return {'passed':True,'native_verified':True,'checks':[{'name':str(n),'passed':True}for n in range(150)],'cases':[{'name':v}for v in ['epg-empty-active','epg-programme-active','physical-parent','physical-claim','physical-result']],'predeclared_cases':['epg-empty-active','epg-programme-active','physical-parent','physical-claim','physical-result']}
 def bad(self,change,code=0):
  x=self.good();change(x)
  with self.assertRaises(ValueError):mod.accept_capture(code,x)
 def test_positive(self):self.assertTrue(mod.accept_capture(0,self.good())['passed'])
 def test_passed_true_nonzero_exit(self):self.bad(lambda x:None,7)
 def test_boolean_exit(self):self.bad(lambda x:None,False)
 def test_native_required(self):self.bad(lambda x:x.pop('native_verified'))
 def test_missing_case(self):self.bad(lambda x:x['cases'].pop())
 def test_reordered_cases(self):self.bad(lambda x:x['cases'].reverse())
 def test_metadata_predeclaration(self):self.bad(lambda x:x['predeclared_cases'].pop())
 def test_failed_gate(self):self.bad(lambda x:x['checks'][0].update(passed=False))
 def test_duplicate_gate(self):self.bad(lambda x:x['checks'][1].update(name='0'))
if __name__=='__main__':unittest.main()
