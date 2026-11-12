"""Pure host-exit/coverage/temporal/refusal negative controls; no actual fixture."""
import copy,importlib.util,json,unittest
from pathlib import Path
HERE=Path(__file__).parent;spec=importlib.util.spec_from_file_location('_url_native_host',HERE/'run_pair.py');m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
P=json.loads((HERE/'predeclared-qa-plan.json').read_text())
def positive():
 return {'passed':True,'checks':[{'passed':True}],'cases':[{'label':v['label'],'comparison':{'passed':True,'timestamp_fields_dropped':0},'native_refusal':v['native_expectation'].startswith('preACK'),'frames':[{}],'results':{'parent':{},'child':{}}}for v in P['Source_derived_case_order']]}
class Controls(unittest.TestCase):
 def test_exactpositive(self):self.assertTrue(m.accept_capture(0,positive())['passed'])
 def test_nonzero_cannot_pass(self):
  with self.assertRaises(ValueError):m.accept_capture(1,positive())
 def test_bool_exit_is_not_int(self):
  with self.assertRaises(ValueError):m.accept_capture(False,positive())
 def test_missingcase(self):
  p=positive();p['cases'].pop()
  with self.assertRaises(ValueError):m.accept_capture(0,p)
 def test_wrongorder(self):
  p=positive();p['cases'].reverse()
  with self.assertRaises(ValueError):m.accept_capture(0,p)
 def test_temporalfields_cannot_drop(self):
  p=positive();p['cases'][0]['comparison']['timestamp_fields_dropped']=1
  with self.assertRaises(ValueError):m.accept_capture(0,p)
 def test_mappedcase_cannot_silently_accept(self):
  p=positive();next(v for v in p['cases']if v['label']=='mapped-own-update')['native_refusal']=False
  with self.assertRaises(ValueError):m.accept_capture(0,p)
 def test_failedcheck(self):
  p=positive();p['checks'][0]['passed']=False
  with self.assertRaises(ValueError):m.accept_capture(0,p)
if __name__=='__main__':unittest.main()
