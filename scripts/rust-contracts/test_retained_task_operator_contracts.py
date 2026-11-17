"""Portable corruption controls, no Django, worker, Docker or media execution.
Original Dispatcharr/dependency authors retain all application/algorithm credit.
"""
import copy,json,tempfile,unittest
from pathlib import Path
import retained_task_operator_contracts as subject
ROOT=Path(__file__).resolve().parents[2]
VALUE=json.loads((ROOT/subject.BINDING_PATH).read_text())
class RetainedTaskOperatorControls(unittest.TestCase):
 def reject(self,change):
  value=copy.deepcopy(VALUE);change(value)
  with self.assertRaises((AssertionError,KeyError,ValueError)):subject.validate(ROOT,value)
 def test_exact_seven_typed_partial_assignments(self):
  result=subject.validate(ROOT,VALUE);self.assertEqual(result['nonHTTP_assignments'],7);self.assertEqual(result['actual_Original_worker_observations'],31);self.assertEqual(result['actual_Original_command_subprocesses'],17)
  self.assertEqual([len(x['observations'])for x in VALUE['non_HTTP_contracts']],[11,5,7,4,4,6,11]);self.assertEqual(VALUE['contracts'],[])
 def test_bool_cannot_replace_actual_CLI_exit_zero(self):
  def mutate(v):
   case=next(x for x in v['non_HTTP_contracts'][-1]['observations']if x['exit_code']==0);case['exit_code']=False
  self.reject(mutate)
 def test_wrong_owning_milestone_rejected(self):self.reject(lambda v:v['non_HTTP_contracts'][0].update(owning_milestone='M0'))
 def test_wrong_stable_ID_rejected(self):self.reject(lambda v:v['non_HTTP_contracts'][0].update(candidate_id='invented'))
 def test_wrong_source_coordinate_rejected(self):self.reject(lambda v:v['non_HTTP_contracts'][0]['source'].update(line=0))
 def test_wrong_task_alias_rejected(self):self.reject(lambda v:v['non_HTTP_contracts'][3].update(task_name='apps.epg.sd_tasks.fetch_schedules_direct_stations'))
 def test_invented_full_actor_or_role_rejected(self):self.reject(lambda v:v['fixtures']['matching']['observed_actor'].update(complete_role_matrix=True))
 def test_changed_case_or_argument_rejected(self):self.reject(lambda v:v['non_HTTP_contracts'][0]['observations'][0].update(decoded_arguments=[999]))
 def test_changed_actual_backend_result_rejected(self):self.reject(lambda v:v['non_HTTP_contracts'][0]['observations'][0]['actual_backend'].update(result='invented'))
 def test_fake_worker_received_signal_rejected(self):self.reject(lambda v:v['non_HTTP_contracts'][0]['observations'][0].update(received_signal_retained=True))
 def test_fake_native_or_new_Source_rejected(self):self.reject(lambda v:v.update(new_Source_execution=True,Native_coverage_increased=True))
 def test_mutated_CLI_failure_exit_rejected(self):
  def mutate(v):
   case=next(x for x in v['non_HTTP_contracts'][-1]['observations']if x['exit_code']==1);case['exit_code']=0
  self.reject(mutate)
 def test_changed_setup_or_fixture_rejected(self):self.reject(lambda v:v['non_HTTP_contracts'][0].update(fixture_ids=['operator']))
 def test_full_verification_rejected(self):self.reject(lambda v:v['non_HTTP_contracts'][0].update(fully_verified=True))
 def test_extra_HTTP_contract_rejected(self):self.reject(lambda v:v['contracts'].append({'contract_id':'invented'}))
 def test_actual_task_UUID_binding_rejected(self):
  row=copy.deepcopy(VALUE['non_HTTP_contracts'][0]['observations'][0]);source={'frame':row['actual_frame'],'backend':row['actual_backend']};source['backend']['task_id']='foreign'
  with self.assertRaisesRegex(AssertionError,'identity'):subject.validate_task_observation(source,row['task_name'])
 def test_actual_task_name_rejected(self):
  row=copy.deepcopy(VALUE['non_HTTP_contracts'][0]['observations'][0]);source={'frame':row['actual_frame'],'backend':row['actual_backend']};source['frame']['headers']['task']='foreign'
  with self.assertRaisesRegex(AssertionError,'identity'):subject.validate_task_observation(source,row['task_name'])
 def test_absent_authority_fails_closed(self):
  with tempfile.TemporaryDirectory()as temporary:
   with self.assertRaisesRegex(AssertionError,'authority absent'):subject.validate(Path(temporary),VALUE)
 def test_public_projection_preserves_observed_failures(self):
  workers=[r for c in VALUE['non_HTTP_contracts'][:5]for r in c['observations']];self.assertTrue(any(x['actual_backend']['status']=='FAILURE'for x in workers))
  commands=[r for c in VALUE['non_HTTP_contracts'][-2:]for r in c['observations']];self.assertEqual(sorted({r['exit_code']for r in commands}),[0,1,2]);self.assertFalse(VALUE['new_Source_execution'])
if __name__=='__main__':unittest.main()
