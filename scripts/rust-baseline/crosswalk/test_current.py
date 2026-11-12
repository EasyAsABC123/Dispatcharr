import json,shutil,tempfile,unittest
from pathlib import Path
import validate_current
HERE=Path(__file__).parent
class Current(unittest.TestCase):
 def setUp(self):
  self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup);self.root=Path(self.tmp.name)/'scripts/rust-baseline/crosswalk';self.root.mkdir(parents=True)
  for name in ['crosswalk.json','current-reference-appendix.json']:shutil.copyfile(HERE/name,self.root/name)
  repository=HERE.parents[2];relative='scripts/rust-baseline/supplemental-metrics/source2/verified-source2.json';target=Path(self.tmp.name)/relative;target.parent.mkdir(parents=True);shutil.copyfile(repository/relative,target)
 def bad(self,fn):
  p=self.root/'current-reference-appendix.json';v=json.loads(p.read_text());fn(v);p.write_text(json.dumps(v))
  with self.assertRaises(AssertionError):validate_current.validate(self.root)
 def test_static_positive(self):self.assertEqual(validate_current.validate(self.root)['historical_scenarios_unchanged'],8)
 def test_old_capture_unchanged(self):
  p=self.root/'crosswalk.json';v=json.loads(p.read_text());v['scenarios'][-1]['execution']='PASS';p.write_text(json.dumps(v))
  with self.assertRaises(AssertionError):validate_current.validate(self.root)
 def test_literal_method_not_resolved(self):self.bad(lambda v:v['current_reference_records'][0].update(registered_method='GET'))
 def test_Source_growth_negative_not_green(self):self.bad(lambda v:v['current_reference_records'][1].update(Source_budget_acceptance=True))
 def test_exit1_retained(self):self.bad(lambda v:v['current_reference_records'][2].update(process_exit=0))
 def test_Source2_actual_rows_not_erased(self):self.bad(lambda v:v['current_reference_records'][5].update(actual_RSS_PSS_rows=None))
 def test_M1_not_promoted(self):self.bad(lambda v:v.update(M1_formal_pass=True))
 def test_missing_public_Source2_authority_no_fallback(self):
  p=Path(self.tmp.name)/'scripts/rust-baseline/supplemental-metrics/source2/verified-source2.json';p.unlink()
  with self.assertRaisesRegex(ValueError,'AuthorityAbsent'):validate_current.validate(self.root)
 def test_mutated_public_Source2_authority_no_fallback(self):
  p=Path(self.tmp.name)/'scripts/rust-baseline/supplemental-metrics/source2/verified-source2.json';p.write_text('{}')
  with self.assertRaisesRegex(ValueError,'AuthorityAbsent'):validate_current.validate(self.root)
 def test_absent_ratio_not_fabricated(self):self.bad(lambda v:v['current_reference_records'][1]['required_budget_fields'].update(CPU_ratio_ceiling=.9))
if __name__=='__main__':unittest.main()
