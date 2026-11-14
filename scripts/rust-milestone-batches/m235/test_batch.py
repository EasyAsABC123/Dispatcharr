"""Batch development checks; no application/Docker/Cargo/media execution."""
import copy,json,tempfile,unittest
from pathlib import Path
from unittest.mock import patch
import batch
class Matrix(unittest.TestCase):
 def setUp(self):self.matrix=batch.load_matrix()
 def test_shared_registry_is_exact28_and_not_duplicate_pool(self):
  names=[n['id']for n in self.matrix['nodes']];self.assertEqual(len(names),len(set(names)));self.assertEqual(len([n for n in names if n.startswith('package/')]),28);self.assertIn('one Root/global dispatcher'.lower(),batch.__doc__.lower())
 def test_all_unbound_are_blocked_never_passed(self):
  r=batch.readiness({});self.assertEqual(r['ready'],0);self.assertEqual(r['blocked'],len(self.matrix['nodes']));self.assertFalse(r['milestone_passed'])
 def test_missing_required_cells_prevent_whole_milestone(self):self.assertFalse(batch.aggregate([])['full_batch_passed'])
 def test_component_cannot_fill_Main(self):
  result={'id':'Main/core-settings-workflow','status':'passed','entrypoint':'real Core+RealtimeBus component','actual_exit':0,'cleanup_verified':True,'semantic_assertions_passed':True,'candidate_image_id':'sha256:'+'a'*64}
  with self.assertRaises(ValueError):batch.aggregate([result])
 def test_501_cannot_be_relabelled_pass(self):
  result={'id':'Main/active-profile-status-probes','status':'passed','entrypoint':'production-Main HTTP','actual_exit':0,'cleanup_verified':True,'semantic_assertions_passed':True,'candidate_image_id':'sha256:'+'a'*64,'known_unimplemented_required_cells':['active-profile501']}
  with self.assertRaises(ValueError):batch.aggregate([result])
 def test_exit0_alone_not_semantic_pass(self):
  with self.assertRaises(ValueError):batch.aggregate([{'id':'package/baseline-all','status':'passed','actual_exit':0}])
 def test_duplicate_result_rejected(self):
  with self.assertRaises(ValueError):batch.aggregate([{'id':'package/baseline-all','status':'failed'}]*2)
 def test_extra_result_rejected(self):
  with self.assertRaises(ValueError):batch.aggregate([{'id':'unknown','status':'failed'}])
 def test_old_candidate_map_cannot_fill_current_Main(self):
  with tempfile.TemporaryDirectory()as d:
   p=Path(d)/'proof';p.write_bytes(b'{}');import hashlib
   f={'path':str(p),'sha256':hashlib.sha256(p.read_bytes()).hexdigest()};c={'manifest':f,'archive':f,'runtime_map':f,'credit_frontend_UID_review':f,'image_id':'sha256:'+'a'*64,'production_binary_attested':True}
   with self.assertRaises(ValueError):batch.candidate_ready({'candidate':c,'integration_source_map_sha256':'b'*64},Path(d))
 def test_global4_refused_without_starting_child(self):
  job={'ready':True,'exclusive_heavy':False};ctx={'batch':self.matrix['batch'],'node_id':'source/cache-and-profile-contexts','active_functional_fixtures':4}
  with patch.object(batch,'resolve_node',return_value=job),self.assertRaises(ValueError):batch.run_node(ctx['node_id'],{},ctx,lambda j:self.fail('child launched'))
 def test_heavy_refused_when_another_fixture_active(self):
  job={'ready':True,'exclusive_heavy':True};ctx={'batch':self.matrix['batch'],'node_id':'package/sharedTS18','active_functional_fixtures':2,'heavy_exclusive':False}
  with patch.object(batch,'resolve_node',return_value=job),self.assertRaises(ValueError):batch.run_node(ctx['node_id'],{},ctx,lambda j:self.fail('child launched'))
 def test_failed_clean_node_does_not_fake_pass(self):
  job={'ready':True,'exclusive_heavy':False};id='source/cache-and-profile-contexts';ctx={'batch':self.matrix['batch'],'node_id':id,'active_functional_fixtures':1,'attempt':1,'claim_persisted':True};r={'status':'failed','actual_exit':1,'cleanup_verified':True}
  with patch.object(batch,'resolve_node',return_value=job):self.assertEqual(batch.run_node(id,{},ctx,lambda j:r),r)
 def test_Source_node_runs_with_explicitly_unbound_candidate(self):
  job={'ready':True,'exclusive_heavy':False};id='source/cache-and-profile-contexts';ctx={'batch':self.matrix['batch'],'node_id':id,'active_functional_fixtures':1,'attempt':1,'claim_persisted':True};r={'status':'failed','actual_exit':1,'cleanup_verified':True}
  with patch.object(batch,'resolve_node',return_value=job):self.assertEqual(batch.run_node(id,{'candidate':None},ctx,lambda j:r),r)
 def test_no_second_attempt_under_same_batch(self):
  job={'ready':True,'exclusive_heavy':False};id='source/cache-and-profile-contexts';ctx={'batch':self.matrix['batch'],'node_id':id,'active_functional_fixtures':1,'attempt':2,'claim_persisted':True}
  with patch.object(batch,'resolve_node',return_value=job),self.assertRaises(ValueError):batch.run_node(id,{},ctx,lambda j:self.fail('child launched'))
 def test_status_probes_have_fixed_paths_and_hls302(self):
  from status_probes import requests
  rows=requests({'owned_channel_UUID':'0bd6295a-8e4a-4fa4-afd3-5df8f8aebd30','owned_active_OutputProfile_id':3});self.assertEqual([r[2]for r in rows],[200,302]);self.assertEqual(len(rows),2);self.assertIn('output_format=hls',rows[1][1])
 def test_foreign_or_invalid_probe_identity_refused(self):
  from status_probes import requests
  for v in ['', '../foreign','UPPER']:
   with self.assertRaises(ValueError):requests({'owned_channel_UUID':v,'owned_active_OutputProfile_id':3})
if __name__=='__main__':unittest.main()
