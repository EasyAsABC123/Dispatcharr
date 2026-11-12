"""Portable rejection controls for exact retained correct-route Source capture."""
import copy,json,hashlib,unittest,tempfile,shutil
from pathlib import Path
import notification_source_contracts as subject
ROOT=Path(__file__).resolve().parents[2]
class NotificationSourceControls(unittest.TestCase):
    @classmethod
    def setUpClass(cls):cls.value=json.loads((ROOT/subject.INPUT).read_text())
    def rejected(self,mutate):
        value=copy.deepcopy(self.value);mutate(value)
        with self.assertRaises((AssertionError,ValueError,KeyError,StopIteration)):subject.validate(ROOT,value)
    def test_exact3_actual_routes8cases(self):
        self.assertTrue(subject.validate(ROOT,self.value)['passed']);self.assertEqual(sum(len(c['cases'])for c in self.value['contracts']),8);self.assertEqual(self.value['unique_actual_Source_observations'],8)
    def test_wrong_method(self):self.rejected(lambda v:v['contracts'][0].update(method='HEAD'))
    def test_wrong_route_alias(self):self.rejected(lambda v:v['contracts'][0].update(pattern_exact='api/system/^notifications/$'))
    def test_format_alias_not_assigned(self):self.rejected(lambda v:v['contracts'][0].update(contract_id='route-b76d75e9c24d41b6517e:GET'))
    def test_unobserved_action(self):self.rejected(lambda v:v['contracts'][0].update(source_action='unread_count'))
    def test_changed_view(self):self.rejected(lambda v:v['contracts'][0].update(source_handler='core.views.SystemNotificationViewSet'))
    def test_case_index_swap(self):self.rejected(lambda v:v['contracts'][0]['cases'][0].update(source_index=7))
    def test_wrong_captured_status(self):self.rejected(lambda v:v['contracts'][0]['cases'][0]['actual_observation'].update(actual_status=200))
    def test_case_body_rewritten(self):self.rejected(lambda v:v['contracts'][0]['cases'][0]['actual_observation'].update(body_hex='7b7d'))
    def test_Source_clock_not_normalized(self):self.rejected(lambda v:v['contracts'][2]['cases'][0]['actual_observation']['after']['dismissals'][0].update(dismissed_at='<clock>'))
    def test_real_credential_scope_not_invented(self):self.rejected(lambda v:v['contracts'][1]['source_role_specs']['admin'].update(credential_backend_bypassed=False))
    def test_actor_level_not_promoted(self):self.rejected(lambda v:v['contracts'][0]['source_role_specs']['standard'].update(Source_user_level=10))
    def test_negative_controller_cannot_pass(self):self.rejected(lambda v:v['contracts'][0]['result'].update(Source_controller_passed=True))
    def test_negative_exit_cannot_zero(self):self.rejected(lambda v:v['contracts'][0]['result'].update(Source_controller_exit=0))
    def test_effect_count_not_inflated(self):self.rejected(lambda v:v['contracts'][0]['result'].update(offline_whole_fixture_effect_checks=214))
    def test_Source_run_not_invented(self):self.rejected(lambda v:v['contracts'][0]['result'].update(new_Source_runs_this_mapping=1))
    def test_whole_endpoint_not_verified(self):self.rejected(lambda v:v['contracts'][0].update(fully_verified=True))
    def test_setup_input_closure_not_shortened(self):self.rejected(lambda v:next(iter(v['fixtures'].values()))['setup'].update(sealed_inputs=25))
    def test_recovered_cleanup_not_hidden(self):self.rejected(lambda v:v['contracts'][0]['result'].update(independent_cleanup_recovered=False))
    def test_wrong_role_coverage(self):self.rejected(lambda v:v['contracts'][2].update(observed_roles=['admin','standard']))
    def test_extra_nonHTTP_forbidden(self):self.rejected(lambda v:v['non_HTTP_contracts'].append({'candidate_id':'invented'}))
if __name__=='__main__':unittest.main()
