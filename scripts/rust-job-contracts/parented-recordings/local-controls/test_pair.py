"""Source-self synthetic collector models; never Native execution evidence."""
import base64,copy,json,sys,unittest
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT/'tools'))
import pair_contract as c
REFERENCE=json.loads((ROOT/'tools/reference.json').read_text())
def model():
 r=copy.deepcopy(REFERENCE['Source']);r['Native_verified']=True;r['Source_verified']=False;r['Source_reference_verified']=True;r['lifecycle_began_utc']=REFERENCE['Source_lifecycle_began_utc'];r['worker_exit_receipts']=[v for v in r['worker_exit_receipts']if v['role']!='series'];r['all_prerun']=[v for v in r['all_prerun']if v['task']!=c.SERIES]
 for case in r['cases']:
  if case['role']!='series':continue
  window=c.source_window(case);ident=case['ids'][0];case['Source_task_prerun_fabricated']=False
  case['native_process']={'spawn_state':'actual_child_started','exit':0,'settled':True,'pipes_settled':True,'writer_settled':True,'invalid':None}
  case['native_responses']=[{'prepared':True,'parent_route':c.QUEUES['series'],'child_route':c.QUEUES['artwork']},{'action':'run','ok':True,'result':'Ok(Completed('+ident+'))','caller_released':True,'available_slots':1,'began_utc':window[0],'finished_utc':window[1]},{'closed':True}]
  for task in case['tasks']:task['prerun']=[];task['postrun']=[];task['sql']=[]
 pubs=[];replacements={}
 for raw in reversed(r['cases'][1]['after']['queues'][c.QUEUES['artwork']]):
  frame=c.frame(raw);frame['headers']['origin']='rust@configured-recordings';encoded=base64.b64encode(json.dumps(frame,separators=(',',':')).encode()).decode();replacements[raw]=encoded;pubs.append({'frame':frame,'raw_base64':encoded,'captured_utc':r['cases'][1]['finished_utc'],'source':'synthetic-model-not-execution'})
 for case in r['cases']:
  for snapshot in [case['before'],case['after']]:
   snapshot['queues'][c.QUEUES['artwork']]=[replacements.get(v,v)for v in snapshot['queues'][c.QUEUES['artwork']]]
   for row in snapshot['broker']:
    if 'list_base64'in row:row['list_base64']=[replacements.get(v,v)for v in row['list_base64']]
 r['native_publications']=pubs
 return r
class Pair(unittest.TestCase):
 def test_source_self_model_is_not_an_execution(self):self.assertTrue(c.validate(0,model(),REFERENCE)['passed'])
 def reject(self,change):
  r=model();change(r)
  with self.assertRaises((ValueError,KeyError,TypeError)):c.validate(0,r,REFERENCE)
 def test_missing_explicit_parent(self):self.reject(lambda r:r['cases'][1]['tasks'][0]['result']['metadata'].pop('parent_id'))
 def test_raw_metadata_cannot_disagree_with_projection(self):self.reject(lambda r:r['cases'][1]['tasks'][0]['result']['metadata'].update(result={'scheduled':False,'details':[]}))
 def test_both_runtimes_cannot_claim_same_execution(self):self.reject(lambda r:r.update(Source_verified=True))
 def test_native_process_nonzero(self):self.reject(lambda r:r['cases'][1]['native_process'].update(exit=1))
 def test_false_native_settlement(self):self.reject(lambda r:r['cases'][1]['native_process'].update(settled=False))
 def test_unclosed_native_writer(self):self.reject(lambda r:r['cases'][1]['native_process'].update(writer_settled=False))
 def test_wrong_native_completed_id(self):self.reject(lambda r:r['cases'][1]['native_responses'][1].update(result='Ok(Empty)'))
 def test_fabricated_source_prerun(self):self.reject(lambda r:r['cases'][1]['tasks'][0]['prerun'].append({'task':'fake'}))
 def test_artwork_native_frame_must_exist_on_broker(self):self.reject(lambda r:r['cases'][1]['after']['queues'][c.QUEUES['artwork']].clear())
 def test_wrong_publication_root(self):
  def change(r):
   p=r['native_publications'][0];p['frame']['headers']['root_id']=p['frame']['headers']['parent_id'];p['raw_base64']=base64.b64encode(json.dumps(p['frame']).encode()).decode()
  self.reject(change)
 def test_untouched_default_frame_cannot_be_deleted(self):self.reject(lambda r:r['cases'][1]['after']['queues']['celery'].clear())
 def test_poster_change_required(self):self.reject(lambda r:r['cases'][2]['after']['recordings'][0]['custom_properties'].pop('poster_logo_id'))
 def test_all_table_fields_present(self):self.reject(lambda r:r['cases'][1]['after']['recordings'][0].pop('task_id'))
 def test_unrelated_business_field_change_cannot_pass(self):self.reject(lambda r:r['cases'][1]['after']['channels'][0].update(name='altered'))
 def test_generated_clock_outside_command(self):self.reject(lambda r:r['cases'][1]['after']['clocked'][0].update(clocked_time='1990-01-01T00:00:00+00:00'))
 def test_unchanged_row_clock_cannot_silently_change(self):self.reject(lambda r:r['cases'][4]['after']['VOD_movies'][0].update(created_at=r['cases'][4]['began_utc']))
 def test_changed_row_clock_cannot_silently_stay_unchanged(self):self.reject(lambda r:r['cases'][3]['after']['sources'][0].update(updated_at=r['cases'][3]['before']['sources'][0]['updated_at']))
 def test_source_auth_null_cannot_go_missing(self):self.reject(lambda r:r['cases'][1]['before']['sources'][0].pop('password'))
 def test_source_integer_cannot_be_bool(self):self.reject(lambda r:r['cases'][0]['before']['channels'][0].update(user_level=False))
 def test_actual_artwork_sql_duplicate_update_is_rejected(self):self.reject(lambda r:r['cases'][2]['tasks'][1]['sql'].append({'sql':'UPDATE "dispatcharr_channels_recording" SET custom_properties=1'}))
 def test_wrong_artwork_get_type(self):self.reject(lambda r:r['cases'][2]['tasks'][0].update(Original_get_type='builtins.dict'))
 def test_unexpected_http_dependency_attempt(self):self.reject(lambda r:r['all_HTTP_attempts'].append({'host':'example.invalid'}))
 def test_extra_event_field_cannot_be_ignored(self):self.reject(lambda r:r['cases'][1]['after']['Channels_decoded'][-1].update(extra='unqualified'))
 def test_generated_event_clock_outside_own_window(self):
  def change(r):
   item=next(v for v in r['cases'][0]['after']['Channels_decoded']if 'updated_at'in v.get('data',{}));item['data']['updated_at']='1990-01-01T00:00:00+00:00'
  self.reject(change)
 def test_residual_lease_cannot_pass(self):
  def change(r):r['cases'][1]['after']['cache'].append({'key_base64':base64.b64encode(b'task_lock_evaluate_series_rules_all').decode(),'type':'string','DUMP_base64':'eA==','ttl':300,'value_base64':'b3duZWQ='})
  self.reject(change)
 def test_group_cache_null_cannot_equal_missing(self):
  def change(r):r['cases'][1]['after']['cache']=[v for v in r['cases'][1]['after']['cache']if base64.b64decode(v['key_base64'])!=b':1:coresettings:group:dvr_settings']
  self.reject(change)
 def test_group_cache_unsafe_pickle_never_imports(self):
  def change(r):
   row=next(v for v in r['cases'][1]['after']['cache']if base64.b64decode(v['key_base64'])==b':1:coresettings:group:dvr_settings');row['value_base64']=base64.b64encode(b'cos\nsystem\n.').decode()
  self.reject(change)
 def test_broker_projection_cannot_silently_diverge(self):
  def change(r):
   row=next(v for v in r['cases'][1]['after']['broker']if base64.b64decode(v['key_base64']).decode()==c.QUEUES['artwork']);row['list_base64']=[]
  self.reject(change)
 def test_short_task_result_ttl_is_not_accepted(self):self.reject(lambda r:r['cases'][1]['tasks'][0]['result'].update(ttl=1))
 def test_false_slot_is_not_integer_one(self):self.reject(lambda r:r['cases'][1]['native_responses'][1].update(available_slots=True))
 def test_mime_or_extra_envelope_field_cannot_be_ignored(self):
  def change(r):r['native_publications'][0]['frame']['content-type']='application/x-python-serialize'
  self.reject(change)
 def test_late_sql_change_cannot_be_omitted(self):self.reject(lambda r:r['final']['recordings'][0]['custom_properties'].update(extra='late'))
 def test_reference_wrapper_cannot_be_mutated(self):
  w=copy.deepcopy(REFERENCE);w['Source']['seed']['channel_id']=999
  with self.assertRaises(ValueError):c.validate(0,model(),w)
if __name__=='__main__':unittest.main()
