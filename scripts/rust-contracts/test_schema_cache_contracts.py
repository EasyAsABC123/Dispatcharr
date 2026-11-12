"""Reject unsupported scope/identity claims on retained Original schema tests."""
import copy
from pathlib import Path
import tempfile
import unittest
import schema_cache_contracts as module
ROOT=Path(__file__).resolve().parents[2]
class SchemaCacheControls(unittest.TestCase):
 def setUp(self):self.value=module.build(ROOT)
 def reject(self,change):
  value=copy.deepcopy(self.value);change(value)
  with self.assertRaises((AssertionError,KeyError)):module.validate(ROOT,value)
 def test_exact_original_three_successes(self):self.assertEqual(module.validate(ROOT,self.value)['Original_named_successes'],3)
 def test_no_invented_get_declaration(self):self.reject(lambda v:v['contracts'][0].update(method='GET'))
 def test_wrong_route_or_alias(self):self.reject(lambda v:v['contracts'][0].update(pattern_exact='api/swagger.json'))
 def test_no_real_schema_or_TCP_claim(self):self.reject(lambda v:v['contracts'][0]['result'].update(Native_or_TCP_acceptance=True))
 def test_no_complete_roles(self):self.reject(lambda v:v['contracts'][0]['result'].update(complete_role_matrix=True))
 def test_no_unobserved_authenticated_role(self):self.reject(lambda v:v['contracts'][0].update(observed_roles=['admin']))
 def test_no_removed_cache_mock_scope(self):self.reject(lambda v:v['contracts'][0]['source_role_specs']['anonymous'].update(schema_generator_mocked=False))
 def test_no_missing_named_test(self):self.reject(lambda v:v['contracts'][0]['source_test_methods'].pop())
 def test_wrong_raw_suite_log_rejected(self):
  with tempfile.TemporaryDirectory()as d:
   p=Path(d)/'tests.log';p.write_text('invented ok\n')
   with self.assertRaisesRegex(AssertionError,'ActualRawFullSuiteHash'):module.authority(ROOT,p)
if __name__=='__main__':unittest.main()
