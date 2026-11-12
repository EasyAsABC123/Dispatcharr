"""Offline corruption tests over retained projection, no fixtures/network."""
import copy,json,unittest
from pathlib import Path
from validate import preserved,reanalyze
class Controls(unittest.TestCase):
 def setUp(self):self.data=json.loads(Path(__file__).with_name('retained-input.json').read_text())
 def check(self,before,after):return preserved(before,after,self.data['expected_profile'],self.data['expected_content'])
 def test_actual_typed_namespaces_and_full_equality(self):
  b=self.data['native_other_before'];a=self.data['native_other_after'];self.assertNotEqual(b['owner'],b['metadata']['rust_owner_token']);self.assertTrue(self.check(b,a)['preserved'])
 def test_missing_snapshot_or_fields_fail(self):
  for key in ['owner','owner_confirm','ledger','metadata']:
   b=copy.deepcopy(self.data['native_other_before']);b.pop(key)
   with self.assertRaises(ValueError):self.check(b,b)
  for kind,fields in [('ledger',['token','owner','profile','reserved','fence']),('metadata',['rust_owner_token','content_uuid','m3u_profile_id','active_streams','created_at'])]:
   for key in fields:
    b=copy.deepcopy(self.data['native_other_before']);b[kind].pop(key)
    with self.assertRaises(ValueError):self.check(b,b)
  for value in [None,{},False]:
   with self.assertRaises(ValueError):self.check(value,value)
 def test_typed_token_cannot_use_wrong_namespace_or_malformed_uuid(self):
  for target in ['lease','metadata']:
   for value in [None,'','foreign','rust:bad','rust-vod-metadata:bad','rust:00000000-0000-0000-0000-000000000000']:
    b=copy.deepcopy(self.data['native_other_before'])
    if target=='lease':b['owner']=b['owner_confirm']=b['ledger']['token']=value
    else:b['metadata']['rust_owner_token']=value
    with self.assertRaises(ValueError):self.check(b,b)
  b=copy.deepcopy(self.data['native_other_before']);b['metadata']['rust_owner_token']=b['owner']
  with self.assertRaises(ValueError):self.check(b,b)
 def test_fence_unreserved_wrong_owner_profile_content(self):
  for key,value in [('fence',None),('fence','0'),('fence','bad'),('fence','9223372036854775808'),('reserved','0'),('reserved',True),('owner','another-session'),('profile','profile_connections:999')]:
   b=copy.deepcopy(self.data['native_other_before']);b['ledger'][key]=value
   with self.assertRaises(ValueError):self.check(b,b)
  for key,value in [('content_uuid','wrong-content'),('m3u_profile_id','999'),('active_streams','0'),('created_at','nan')]:
   b=copy.deepcopy(self.data['native_other_before']);b['metadata'][key]=value
   with self.assertRaises(ValueError):self.check(b,b)
 def test_changed_welltyped_metadata_or_lease_or_fence_fail(self):
  b=copy.deepcopy(self.data['native_other_before'])
  for kind,key,value in [('metadata','rust_owner_token','rust-vod-metadata:31e51956-2399-436b-92d5-bb40db1a0e12'),('ledger','fence','3'),('metadata','created_at','1794473700')]:
   a=copy.deepcopy(b);a[kind][key]=value
   with self.assertRaises(ValueError):self.check(b,a)
  a=copy.deepcopy(b);a['owner']=a['owner_confirm']=a['ledger']['token']='rust:31e51956-2399-436b-92d5-bb40db1a0e12'
  with self.assertRaises(ValueError):self.check(b,a)
 def test_actual_negative_preserved_separate_offline_result(self):
  r=reanalyze(self.data);self.assertEqual(r['actual_exit_code_preserved'],1);self.assertFalse(r['actual_summary_qualification_preserved']);self.assertTrue(r['offline_normal_scenario_qualified']);self.assertFalse(r['actual_execution_reclassified']);self.assertFalse(r['full_M4_acceptance'])
 def test_no_source_zero_or_status_equality_required(self):
  d=copy.deepcopy(self.data);d['backends']['source']['probes'][0]['status']=429
  self.assertTrue(reanalyze(d)['offline_normal_scenario_qualified'])
if __name__=='__main__':unittest.main(verbosity=2)
