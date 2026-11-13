"""Source-self synthetic collector models; never Native execution evidence."""
import base64,copy,json,sys,unittest
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT/'tools'))
import pair_contract as c
REFERENCE=json.loads((ROOT/'tools/reference.json').read_text())
def model():
 r=copy.deepcopy(REFERENCE['Source']);r.update(Native_verified=True,Source_verified=False,Source_reference_verified=True,lifecycle_began_utc=REFERENCE['Source_lifecycle_began_utc'],native_publications=[])
 r['worker_exit_receipts']=[v for v in r['worker_exit_receipts']if v['role']!='artwork'];r['all_prerun']=[v for v in r['all_prerun']if v['task']!=c.ARTWORK]
 case=r['cases'][2];case['Source_task_prerun_fabricated']=False;case['native_processes']=[]
 mid=copy.deepcopy(case['before']);first_id=case['ids'][0]
 for table in c.TABLES:mid[table]=copy.deepcopy(case['after'][table])
 for field in ['Channels_raw_base64','Channels_decoded']:mid[field]=copy.deepcopy(case['after'][field])
 mid['queues'][c.QUEUES['artwork']]=mid['queues'][c.QUEUES['artwork']][:-1]
 for row in mid['broker']:
  if base64.b64decode(row['key_base64']).decode()==c.QUEUES['artwork']:row['list_base64']=copy.deepcopy(mid['queues'][c.QUEUES['artwork']])
 mid['backend'][first_id]=copy.deepcopy(case['after']['backend'][first_id])
 key=base64.b64decode(mid['backend'][first_id]['key_base64'])
 mid['result_backend_raw'] += [copy.deepcopy(row)for row in case['after']['result_backend_raw']if base64.b64decode(row['key_base64'])==key]

 for index,task in enumerate(case['tasks']):
  begin=task['prerun'][0]['utc'];end=task['postrun'][0]['utc'];ident=task['id'];frame=next(c.frame(raw)for raw in case['before']['queues'][c.QUEUES['artwork']]if c.frame(raw)['headers']['id']==ident)
  process={'spawn_state':'actual_child_started','exit':0,'settled':True,'pipes_settled':True,'writer_settled':True,'invalid':None,'began_utc':begin,'finished_utc':end};task['native_process']=process;case['native_processes'].append(process.copy())
  task['native_responses']=[{'prepared':True,'route':c.QUEUES['artwork'],'task_id':ident,'root':frame['headers']['root_id'],'parent':frame['headers']['parent_id'],'captured_ETA':frame['headers']['eta'],'provider_fallback':False},{'action':'run','ok':True,'typed_result':{'state':'Completed','task_id':ident,'changed':index==0},'caller_released':True,'available_slots':1,'began_utc':begin,'finished_utc':end},{'closed':True}]
  task['prerun']=[];task['postrun']=[];task['sql']=[];task['before']=copy.deepcopy(case['before']if index==0 else mid);task['after']=copy.deepcopy(mid if index==0 else case['after'])
 return r
class Pair(unittest.TestCase):
 def test_source_self_model_is_only_a_validator_control(self):self.assertTrue(c.validate(0,model(),REFERENCE)['passed'])
 def reject(self,change):
  r=model();change(r)
  with self.assertRaises((ValueError,KeyError,TypeError,IndexError)):c.validate(0,r,REFERENCE)
 def test_nonzero_native_process(self):self.reject(lambda r:r['cases'][2]['tasks'][0]['native_process'].update(exit=1))
 def test_false_settlement(self):self.reject(lambda r:r['cases'][2]['tasks'][0]['native_process'].update(settled=False))
 def test_live_output_pipe(self):self.reject(lambda r:r['cases'][2]['tasks'][0]['native_process'].update(pipes_settled=False))
 def test_missing_explicit_parent(self):self.reject(lambda r:r['cases'][2]['tasks'][0]['result']['metadata'].pop('parent_id'))
 def test_short_ttl(self):self.reject(lambda r:r['cases'][2]['tasks'][0]['result'].update(ttl=1))
 def test_wrong_raw_metadata_projection(self):self.reject(lambda r:r['cases'][2]['tasks'][0]['result']['metadata'].update(result='other'))
 def test_wrong_get_type(self):self.reject(lambda r:r['cases'][2]['tasks'][0].update(Original_get_type='builtins.dict'))
 def test_duplicate_reports_changed(self):self.reject(lambda r:r['cases'][2]['tasks'][1]['native_responses'][1]['typed_result'].update(changed=True))
 def test_changed_not_typed_bool(self):self.reject(lambda r:r['cases'][2]['tasks'][0]['native_responses'][1]['typed_result'].update(changed=1))
 def test_wrong_completed_id(self):self.reject(lambda r:r['cases'][2]['tasks'][0]['native_responses'][1]['typed_result'].update(task_id='wrong'))
 def test_fabricated_Source_SQL(self):self.reject(lambda r:r['cases'][2]['tasks'][0]['sql'].append({'sql':'fake'}))
 def test_unreleased_caller(self):self.reject(lambda r:r['cases'][2]['tasks'][0]['native_responses'][1].update(caller_released=False))
 def test_missing_SQL_clock_field(self):self.reject(lambda r:r['cases'][1]['before']['sources'][0].pop('created_at'))
 def test_bool_not_equal_SQL_integer(self):self.reject(lambda r:r['cases'][0]['before']['channels'][0].update(user_level=False))
 def test_default_descendant_removed(self):self.reject(lambda r:r['final']['queues']['celery'].pop())
 def test_HTTP_fallback_attempt(self):self.reject(lambda r:r['all_HTTP_attempts'].append({'host':'unqualified.invalid'}))
 def test_missing_raw_credential_field_not_null(self):self.reject(lambda r:r['cases'][1]['before']['sources'][0].pop('password'))
 def test_before_ETA_run_cannot_pass(self):self.reject(lambda r:r['cases'][2]['tasks'][0]['native_responses'][0].update(captured_ETA='2099-01-01T00:00:00+00:00'))
 def test_dropped_row_effect(self):self.reject(lambda r:r['final']['recordings'][0]['custom_properties'].pop('poster_logo_id'))
 def test_prepared_root_cannot_disagree_with_raw(self):self.reject(lambda r:r['cases'][2]['tasks'][0]['native_responses'][0].update(root='wrong'))
 def test_prepared_parent_cannot_disagree_with_raw(self):self.reject(lambda r:r['cases'][2]['tasks'][0]['native_responses'][0].update(parent='wrong'))
 def test_early_prepared_ETA_cannot_replace_raw(self):self.reject(lambda r:r['cases'][2]['tasks'][0]['native_responses'][0].update(captured_ETA='1900-01-01T00:00:00+00:00'))
 def test_case_process_cannot_hide_unsettled_task(self):self.reject(lambda r:r['cases'][2]['native_processes'][0].update(exit=1))
 def test_reference_corruption(self):
  ref=copy.deepcopy(REFERENCE);ref['Source']['seed']['Movie_id']=999
  with self.assertRaises(ValueError):c.validate(0,model(),ref)
if __name__=='__main__':unittest.main()
