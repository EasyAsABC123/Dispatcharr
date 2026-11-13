"""Unexecuted offline corruption controls; synthetic models are NOT actual runs."""
import base64,copy,json,sys,unittest
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'tools'))
from compare_full import compare_case,Clocks
from run_pair import accept_capture,accept_physical
ROOT=Path(__file__).resolve().parents[1]/'tools'
REFERENCE=json.loads((ROOT/'reference.json').read_text())
POLICY=json.loads((ROOT/'qa-policy.json').read_text())

def model(index):
 source=REFERENCE['cases'][index];actual=copy.deepcopy(source)
 actual['source_fallback']=False
 actual['native_ids']=[v['id']for v in source['postruns']if v['task'].startswith('apps.m3u.')]
 actual['native_fileposts']=[copy.deepcopy(v)for v in source['fileposts']if v['id']in actual['native_ids']]
 window={'began_utc':'2000-01-01T00:00:00+00:00','finished_utc':'2100-01-01T00:00:00+00:00'}
 actual['case_window']=window;actual['producer_window']=window;actual['command_windows']={ident:window for ident in actual['native_ids']}
 keys={'task_lock_refresh_m3u_account_groups_'+str(c['after']['accounts'][0]['id'])for c in REFERENCE['cases']if c['name']in ('m3u-missing-local-file','m3u-group-SQL-P0001')}
 for phase in ('before','after'):
  for key,row in actual[phase]['locks'].items():
   if key in keys:row['raw_base64']=base64.b64encode(b'rust:00000000-0000-4000-8000-000000000001').decode()
 for row in actual['results'].values():
  if row['metadata']['status']=='FAILURE':row['metadata']['traceback']='Native Dispatcharr M3U groups diagnostic [synthetic-control]'
  else:row['Original_get_value']=copy.deepcopy(row['metadata']['result'])
 return source,actual,keys

class ContractControls(unittest.TestCase):
 def test_exact_eight_models_including_inactive_one_child(self):
  clocks=Clocks();counts=[]
  for index in range(8):
   source,actual,keys=model(index);counts.append(len(compare_case(source,actual,clocks,keys)))
  self.assertEqual(counts,[1,2,2,2,2,2,2,2])
 def test_missing_explicit_null_field_is_not_equal(self):
  source,actual,keys=model(0);del actual['after']['accounts'][0]['updated_at']
  with self.assertRaisesRegex(AssertionError,'FieldPresence'):compare_case(source,actual,Clocks(),keys)
 def test_static_nested_bool_does_not_equal_integer(self):
  source,actual,keys=model(0);actual['after']['accounts'][0]['is_active']=0
  with self.assertRaisesRegex(AssertionError,'ExactField'):compare_case(source,actual,Clocks(),keys)
 def test_result_not_null_string_and_errno_unchanged(self):
  source,actual,keys=model(3)
  task=next(v for v in actual['postruns']if v['task'].endswith('refresh_single_m3u_account'))
  actual['results'][task['id']]['metadata']['result']=None
  with self.assertRaisesRegex(AssertionError,'TypedMetadata'):compare_case(source,actual,Clocks(),keys)
 def test_source_fallback_cannot_become_native(self):
  source,actual,keys=model(3);actual['source_fallback']=True
  with self.assertRaisesRegex(AssertionError,'NoSourceFallback'):compare_case(source,actual,Clocks(),keys)
 def test_lost_residual_key_not_hidden_by_raw_token_difference(self):
  source,actual,keys=model(3);actual['after']['locks']={}
  with self.assertRaisesRegex(AssertionError,'ExactLockKeys'):compare_case(source,actual,Clocks(),keys)
 def test_foreign_token_change_not_an_owner_token_exception(self):
  source,actual,keys=model(4)
  key=next(k for k in actual['after']['locks']if k not in keys);actual['after']['locks'][key]['raw_base64']=base64.b64encode(b'changed').decode()
  with self.assertRaisesRegex(AssertionError,'ForeignLockRawPreserved'):compare_case(source,actual,Clocks(),keys)
 def test_generation_cache_data_must_match(self):
  source,actual,keys=model(2);actual['after']['output_cache']['generation_ascii']='999'
  with self.assertRaisesRegex(AssertionError,'OutputCache'):compare_case(source,actual,Clocks(),keys)
 def test_metadata_outside_actual_native_interval_rejected(self):
  source,actual,keys=model(0);actual['command_windows'][actual['native_ids'][0]]={'began_utc':'2000-01-01T00:00:00+00:00','finished_utc':'2001-01-01T00:00:00+00:00'}
  with self.assertRaisesRegex(AssertionError,'ActualMetadataCommandWindow'):compare_case(source,actual,Clocks(),keys)
 def test_corrupt_actual_original_normal_decoder_value_rejected(self):
  source,actual,keys=model(3)
  ident=next(v['id']for v in actual['postruns']if v['task'].endswith('refresh_single_m3u_account'))
  actual['results'][ident]['Original_get_value']=None
  with self.assertRaisesRegex(AssertionError,'ActualOriginalNormalValue'):compare_case(source,actual,Clocks(),keys)
 def test_missing_physical_check_cannot_be_hidden_by_passed_true(self):
  cases=[{'name':v,'peer_close':{'actual_exit':0,'process_settled':True}}for v in POLICY['physical_modes']]
  actual={'passed':True,'native_verified':True,'cases':cases,'checks':[{'name':v,'passed':True}for v in POLICY['physical_expected_check_names']][1:],'cleanup_errors':[],'Source_parent_exit_receipts':[{'actual_exit':0,'process_settled':True}for _ in POLICY['physical_modes']]}
  with self.assertRaisesRegex(ValueError,'ExactPhysicalChecksNoDeletion'):accept_physical(0,actual)
 def test_unsettled_source_parent_cannot_pass_physical(self):
  cases=[{'name':v,'peer_close':{'actual_exit':0,'process_settled':True}}for v in POLICY['physical_modes']]
  actual={'passed':True,'native_verified':True,'cases':cases,'checks':[{'name':v,'passed':True}for v in POLICY['physical_expected_check_names']],'cleanup_errors':[],'Source_parent_exit_receipts':[{'actual_exit':0,'process_settled':True}for _ in POLICY['physical_modes']]}
  actual['Source_parent_exit_receipts'][0]['process_settled']=False
  with self.assertRaisesRegex(ValueError,'ActualSourceParentSettlement'):accept_physical(0,actual)
 def test_nonzero_process_cannot_pass_even_with_passed_true(self):
  with self.assertRaises(ValueError):accept_capture(1,{'passed':True,'native_verified':True})

if __name__=='__main__':unittest.main()
