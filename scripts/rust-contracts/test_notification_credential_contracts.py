"""Reject scope, identity and evidence changes in three real-credential bindings."""
import copy,json,pathlib,unittest
import notification_credential_contracts as M
ROOT=pathlib.Path(__file__).resolve().parents[2]
class CredentialSourceControls(unittest.TestCase):
 def setUp(self):self.value=M.generate(ROOT)
 def reject(self,mutate):
  value=copy.deepcopy(self.value);mutate(value)
  with self.assertRaises(AssertionError):M.validate(ROOT,value)
 def test_exact_three_partial(self):self.assertEqual(M.validate(ROOT,self.value)['operations'],3)
 def test_wrong_source_ordinal(self):self.reject(lambda v:v['contracts'][0]['cases'][0].update(source_ordinal=3))
 def test_wrong_resolved_action(self):self.reject(lambda v:v['contracts'][1].update(source_action='count'))
 def test_wrong_observed_actor(self):self.reject(lambda v:v['contracts'][0]['source_role_specs'][v['contracts'][0]['observed_roles'][0]].update(actual_authenticated_user_id=1))
 def test_wrong_backend(self):self.reject(lambda v:v['contracts'][0]['cases'][0]['observation_projection']['actual_auth_observation'].update(backend_class='forced'))
 def test_SQL_effect_erased(self):self.reject(lambda v:v['contracts'][2]['cases'][0]['observation_projection'].pop('after_audit'))
 def test_no_new_runs(self):self.reject(lambda v:v.update(new_Source_runs=20))
 def test_no_TCP_or_Native_promotion(self):self.reject(lambda v:v['contracts'][0]['result'].update(Native_or_TCP_acceptance=True))
 def test_no_fully_verified(self):self.reject(lambda v:v['contracts'][0].update(fully_verified=True))
 def test_no_role_matrix_completion(self):self.reject(lambda v:v['contracts'][0]['result'].update(complete_role_matrix=True))
 def test_projection_not_original(self):self.reject(lambda v:v['fixtures'][v['contracts'][0]['fixture_id']]['projection'].update(original_raw_sha256='0'*64))
if __name__=='__main__':unittest.main()
