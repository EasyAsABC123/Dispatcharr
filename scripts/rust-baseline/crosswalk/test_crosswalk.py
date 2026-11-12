import json
from pathlib import Path
import unittest
D=json.loads(Path(__file__).with_name('crosswalk.json').read_text())
class CrosswalkControls(unittest.TestCase):
 def test_registry_cardinality_and_fully_zero_preserved(self):
  self.assertEqual(D['registry']['counts']['runtime_routes'],592);self.assertEqual(D['registry']['counts']['runtime_operation_work_items'],798);self.assertEqual(D['registry']['counts']['fully_verified_route_operations'],0)
 def test_unresolved_ids_not_fabricated_GET_POST(self):
  for x in D['HTTP_observations']:
   if x['registered_method']is None:self.assertTrue(x['literal_contract_id'].endswith(':METHOD_UNRESOLVED'))
   else:self.assertEqual(x['registered_method'],x['observed_method'])
 def test_methods_roles_remain_separate(self):
  switch=[x for x in D['HTTP_observations']if'change_stream'in x['observed_path']];self.assertEqual([(x['observed_method'],x['statuses'])for x in switch],[('POST',[200]),('GET',[405]),('POST',[401])])
 def test_no_endpoint_or_native_completion(self):
  self.assertFalse(D['M0_complete']);self.assertFalse(D['Native_acceptance']);self.assertTrue(all(x['fully_verified']is False and x['Native_acceptance']is False for x in D['HTTP_observations']+D['task_observations']))
 def test_literal_16FD_not_zero(self):
  idle=D['scenarios'][0]['literal_declared_growth_budgets'];self.assertEqual(idle['post_idle_fd_growth'],16);self.assertEqual(idle['post_idle_socket_growth'],16);self.assertEqual(idle['post_idle_process_growth'],0);self.assertFalse(D['scenarios'][0]['Source_budget_acceptance'])
 def test_absent_ceilings_remain_null(self):
  for x in D['scenarios'][1:-1]:self.assertIsNone(x['absolute_CPU_ceiling']);self.assertIsNone(x['absolute_memory_ceiling']);self.assertIsNone(x['growth_ceiling'])
 def test_fixture_control_not_source_POST_contract(self):self.assertIn('origin /control POST',D['fixture_endpoints_not_application_contracts'])
 def test_proposed_resource_not_existing_acceptance(self):
  x=D['scenarios'][-1];self.assertIn('HARD BLOCKED',x['execution']);self.assertIsNone(x['Source_budget_acceptance']);self.assertEqual(x['proposed_budgets_not_yet_approved_for_four_roles']['whole_stack_cpu_percent_one_core'],500)
if __name__=='__main__':unittest.main()
