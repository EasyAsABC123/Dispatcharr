"""Real Source-script pre-launch controls; no Django/Docker imports."""
import importlib.util,json,unittest
from pathlib import Path
here=Path(__file__).parent;s=importlib.util.spec_from_file_location('_hash_source_plan',here/'run_pair.py');m=importlib.util.module_from_spec(s);s.loader.exec_module(m);source=(here/'source_pair.py').read_text();names=[v['name']for v in json.loads((here/'predeclared-plan.json').read_text())['factory_cases']]
class Plan(unittest.TestCase):
 def test_actual_source(self):self.assertEqual(len(m.validate_source(source,names)),22)
 def test_wrong_loop(self):
  with self.assertRaises(ValueError):m.validate_source(source.replace('for item in FACTORY:',"for item in 'FACTORY':"),names)
 def test_missing_case(self):
  with self.assertRaises(ValueError):m.validate_source(source,names[:-1])
 def test_wrong_case_name(self):
  bad=names.copy();bad[0]='wrong'
  with self.assertRaises(ValueError):m.validate_source(source,bad)
if __name__=='__main__':unittest.main()
