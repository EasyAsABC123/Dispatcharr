"""Real source AST checks before any Django/import/Docker workload."""
import importlib.util,json,unittest
from pathlib import Path
here=Path(__file__).parent;spec=importlib.util.spec_from_file_location('_audit_case_runner',here/'run_pair.py');m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
source=(here/'source_pair.py').read_text();cases=json.loads((here/'predeclared-plan.json').read_text())['cases']
class Cases(unittest.TestCase):
 def test_actual_source_list(self):self.assertEqual(m.validate_plan(source,cases),cases)
 def test_literal_string_loop_rejected(self):
  with self.assertRaises(ValueError):m.validate_plan(source.replace("for label in report['predeclared_cases']:",'for label in "+repr(names)+":'),cases)
 def test_changed_plan_rejected(self):
  with self.assertRaises(ValueError):m.validate_plan(source,cases[:-1])
 def test_duplicate_plan_rejected(self):
  x=cases.copy();x[1]=x[0]
  with self.assertRaises(ValueError):m.validate_plan(source,x)
 def test_different_iterable_rejected(self):
  with self.assertRaises(ValueError):m.validate_plan(source.replace("for label in report['predeclared_cases']:","for label in sorted(report['predeclared_cases']):"),cases)
 def test_result_shadow_rejected(self):
  with self.assertRaises(ValueError):m.validate_plan(source.replace("sql_result='NEW'","result='NEW'"),cases)
 def test_result_SQL_use_rejected(self):
  with self.assertRaises(ValueError):m.validate_plan(source.replace('+sql_result+','+result+'),cases)
if __name__=='__main__':unittest.main()
