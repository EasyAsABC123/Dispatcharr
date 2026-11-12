import copy,importlib.util,json,unittest
from pathlib import Path
spec=importlib.util.spec_from_file_location("store14_host",Path(__file__).with_name("run_pair.py"));host=importlib.util.module_from_spec(spec);spec.loader.exec_module(host)
class Acceptance(unittest.TestCase):
 def fixture(self):
  plan=json.loads(Path(__file__).with_name("predeclared-qa-plan.json").read_text());return {"passed":True,"cases":[{"label":v["label"]}for v in plan["Store_projection_and_refusal_order"]],"checks":[{"passed":True}]}
 def test_positive(self):host.accept_capture(0,self.fixture())
 def test_nonzero_even_passed(self):
  with self.assertRaises(ValueError):host.accept_capture(1,self.fixture())
 def test_missing_case(self):
  v=self.fixture();v["cases"].pop()
  with self.assertRaises(ValueError):host.accept_capture(0,v)
 def test_reordered(self):
  v=self.fixture();v["cases"].reverse()
  with self.assertRaises(ValueError):host.accept_capture(0,v)
 def test_failed_check(self):
  v=self.fixture();v["checks"][0]["passed"]=False
  with self.assertRaises(ValueError):host.accept_capture(0,v)
 def test_no_checks(self):
  v=self.fixture();v["checks"]=[]
  with self.assertRaises(ValueError):host.accept_capture(0,v)
 def test_false_report(self):
  v=self.fixture();v["passed"]=False
  with self.assertRaises(ValueError):host.accept_capture(0,v)
 def test_bool_exit(self):
  with self.assertRaises(ValueError):host.accept_capture(False,self.fixture())
if __name__=="__main__":unittest.main()
