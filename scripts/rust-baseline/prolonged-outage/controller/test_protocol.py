"""Draft pure controls, NOT EXECUTED before Root review."""
import copy,unittest
from unittest import mock
import outage_protocol as p
class Protocol(unittest.TestCase):
 def test_exact_horizons(self):self.assertEqual(p.derive(dict(p.EXPECTED)),{'outage_seconds':102,'restore_observation_seconds':88,'single_path_horizon_seconds':40,'per_candidate_backoff_seconds':.75})
 def test_any_effective_setting_mismatch_rejects_without_adaptation(self):
  for key in p.EXPECTED:
   settings=dict(p.EXPECTED);settings[key]+=1
   with self.assertRaises(ValueError):p.derive(settings)
 def test_entry_block_precedes_all_adapter_calls(self):
  adapter=mock.Mock()
  with self.assertRaisesRegex(RuntimeError,'RootReviewRequired'):p.run(adapter,lambda:0,lambda _:None)
  self.assertEqual(adapter.mock_calls,[])
 def test_known_unused_retry_counters_are_not_horizon(self):
  self.assertLess(p.derive(dict(p.EXPECTED))['outage_seconds'],300);self.assertLess(p.derive(dict(p.EXPECTED))['outage_seconds'],1800)


class VirtualAdapter:
 def __init__(self):self.now=0;self.flags={};self.events=[];self.closed=False;self.fresh=False;self.retained=0
 def effective_settings_and_path(self):return dict(p.EXPECTED),{'loaded_Source_sha256':dict(p.PINNED),'profile_name':'Proxy','profile_locked':True,'metadata_profile_resolves_to_owned_locked_Proxy':True,'initial_owner_transcode':False,'actual_HTTP_reader_code_file':'/app/apps/proxy/live_proxy/input/http_streamer.py'}
 def exact_owned_candidate_paths(self):return ['/live/0.ts','/live/100.ts']
 def verify_warmed_original_body(self,seconds):self.now+=seconds
 def snapshot(self,label):return{'label':label,'old_EOF':True}
 def require_existing_owned_input(self,before,paths):return [1]
 def origin_control(self,path,action):self.flags[path]=action;return{'path':path,'action':action,'time':self.now,'physical_close':{'closed_ids':[1]if action=='fail'and path=='/live/0.ts'else[]}}
 def probe_all_unavailable(self,paths):return[{'path':x,'status':503 if self.flags[x]=='fail'else 200}for x in paths]
 def verify_origin_active_zero(self):pass
 def event(self,name,at,**kwargs):self.events.append((name,at))
 def retain_original_body_outcome(self):self.retained+=1
 def freeze_and_physically_close_original_body(self):self.closed=True;return{'EOF':True,'recovered':False}
 def one_fresh_postrestore_request(self):
  if not self.closed:raise AssertionError('old body not frozen')
  self.fresh=True;return{'status':200,'decoded':True}
 def retain_cleanup_result(self,errors):self.cleanup_errors=errors
 def close_owned_bodies_and_join(self):self.closed=True
 def same_run_retention(self,seconds):self.now+=seconds
class VirtualSchedule(unittest.TestCase):
 def run_schedule(self,adapter):
  def sleep(n):adapter.now+=n
  with mock.patch.object(p,'EXECUTION_BLOCKED',False):return p.run(adapter,lambda:adapter.now,sleep)
 def test_actual_unavailable_span_and_old_fresh_separation(self):
  a=VirtualAdapter();r=self.run_schedule(a);self.assertEqual(r['outage_elapsed_seconds'],102);self.assertTrue(r['original_body_outcome']['EOF']);self.assertFalse(r['original_body_outcome']['recovered']);self.assertTrue(r['fresh_body_outcome']['decoded']);self.assertIsNone(r['Source_budget_acceptance'])
 def test_restore_window_endpoints_and_receipt_are_recomputable(self):
  a=VirtualAdapter();r=self.run_schedule(a);self.assertEqual(r['primary_recover_receipt']['path'],'/live/0.ts');self.assertEqual(r['primary_recover_receipt']['action'],'recover');self.assertEqual(r['restore_observation_end_monotonic']-r['restored_monotonic'],88);self.assertEqual(r['restore_observation_elapsed_seconds'],88)
 def test_wrong_runtime_path_rejects_before_fault(self):
  a=VirtualAdapter();a.effective_settings_and_path=lambda:(dict(p.EXPECTED),{})
  with self.assertRaisesRegex(ValueError,'InputPath'):self.run_schedule(a)
  self.assertNotIn('fail',a.flags.values());self.assertTrue(a.closed)
 def test_warmed_socket_must_be_in_actual_closed_receipts(self):
  a=VirtualAdapter();a.require_existing_owned_input=lambda before,paths:[99]
  with self.assertRaisesRegex(RuntimeError,'NotPhysicallyClosed'):self.run_schedule(a)
  self.assertTrue(a.closed);self.assertFalse(a.fresh)
 def test_bad_actual_unavailability_restores_and_joins(self):
  a=VirtualAdapter();a.probe_all_unavailable=lambda paths:[{'path':x,'status':200}for x in paths]
  with self.assertRaisesRegex(RuntimeError,'NotEstablished'):self.run_schedule(a)
  self.assertTrue(a.closed);self.assertEqual(set(a.flags.values()),{'recover'});self.assertFalse(a.fresh)

if __name__=='__main__':unittest.main()
