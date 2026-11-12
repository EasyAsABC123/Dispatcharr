"""Reject false successful host aggregates; no Docker/Cargo/network."""
import copy,importlib.util,unittest
from pathlib import Path
spec=importlib.util.spec_from_file_location('_mapped_parent_runner',Path(__file__).with_name('run_pair.py'));m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
def valid():return {'passed':True,'native_verified':True,'checks':[{'passed':True}for _ in range(614)],'predeclared_cases':[str(n)for n in range(17)],'cases':[{'name':str(n)}for n in range(17)],'physical_tests':8,'physical_cases':[{'exit':0}for _ in range(8)]}
class Controls(unittest.TestCase):
 def test_valid_complete(self):self.assertTrue(m.acceptance(valid(),0,True))
 def test_passed_true_nonzero_exit_rejected(self):self.assertFalse(m.acceptance(valid(),75,True))
 def test_missing_native_verification(self):r=valid();r.pop('native_verified');self.assertFalse(m.acceptance(r,0,True))
 def test_failed_inner_check_rejected(self):r=valid();r['checks'][3]['passed']=False;self.assertFalse(m.acceptance(r,0,True))
 def test_reordered_or_missing_case_rejected(self):r=valid();r['cases'].reverse();self.assertFalse(m.acceptance(r,0,True))
 def test_missing_physical_control_rejected(self):r=valid();r['physical_cases'].pop();self.assertFalse(m.acceptance(r,0,True))
 def test_failed_physical_exit_rejected(self):r=valid();r['physical_cases'][0]['exit']=1;self.assertFalse(m.acceptance(r,0,True))
 def test_wrongly_typed_exit_rejected(self):self.assertFalse(m.acceptance(valid(),False,True))
if __name__=='__main__':unittest.main()
