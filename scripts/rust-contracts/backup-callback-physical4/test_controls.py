"""Portable retained-data corruptions only; no app, SQL, Docker or new request."""
import copy,json,unittest
from verify import observations,load_members
class Controls(unittest.TestCase):
 @classmethod
 def setUpClass(cls):cls.members=load_members()
 def corrupt(self,path,change):
  data=dict(self.members);value=json.loads(data[path]);change(value);data[path]=(json.dumps(value)+'\n').encode()
  with self.assertRaises((ValueError,KeyError,TypeError,AssertionError)):observations(data)
 def case(self,change,attempt=1,case=1):self.corrupt(f'attempt{attempt}/case-{case}/callback-case.json',change)
 def test_four_actual(self):self.assertEqual(len(observations(self.members)['actual_selected_cases']),4)
 def test_host1_preserved(self):self.corrupt('attempt1/integration.json',lambda v:v.__setitem__('actual_host_exit',0))
 def test_no_passing_replay(self):self.corrupt('attempt2/integration.json',lambda v:v['cases'].append(v['cases'][0]))
 def test_raw_recorder_false(self):self.case(lambda v:v.__setitem__('passed',True))
 def test_capture_only(self):self.case(lambda v:v.__setitem__('capture_only',False))
 def test_sourceflag(self):self.case(lambda v:v.__setitem__('Source_runs',1))
 def test_slots(self):self.case(lambda v:v.__setitem__('request_slots_while_held',4))
 def test_early_shutdown(self):self.case(lambda v:v.__setitem__('early_shutdown_returned',True))
 def test_final_shutdown(self):self.case(lambda v:v.__setitem__('final_shutdown_returned',False))
 def test_abort_not_FD(self):self.case(lambda v:v.__setitem__('HTTP_FD_loss_claimed',True))
 def test_abort_consumed(self):self.case(lambda v:v.__setitem__('caller_JoinHandle_abort_consumed',False))
 def test_SQL_after(self):self.case(lambda v:v['after']['tables']['accounts_user'].append({'id':99}))
 def test_hold_budget(self):self.case(lambda v:v.__setitem__('actual_physical_hold_seconds',26))
 def test_endpoint(self):self.case(lambda v:v['actual_target_endpoint_settlement'].__setitem__('original_socket_inodes_absent',False))
 def test_pool_reuse(self):self.case(lambda v:v.__setitem__('same_SQL_pool_reuse_after_target_physical_close',False))
 def test_prepared_same_backend(self):self.case(lambda v:v['actual_readonly_prepared_timeout_fixture'].__setitem__('backend_pid',999),2,2)
 def test_prepared_statement(self):self.case(lambda v:v['actual_readonly_prepared_timeout_fixture'].__setitem__('statement_prepared_before_lock',False),2,2)
 def test_HTTP_handler_inference(self):self.case(lambda v:v.__setitem__('handler_abort_claimed',True),2,3)
 def test_HTTP_write_inference(self):self.case(lambda v:v.__setitem__('HTTP_write_failure_or_response_status_inferred_from_client_loss',True),2,3)
 def test_HTTP_status_inference(self):self.case(lambda v:v['actual_HTTP_response_write_fate'].__setitem__('response_statuses',[500]),2,3)
 def test_cache_logical_status(self):self.case(lambda v:v['logical_response_before_physical_reply'].__setitem__('status',500),3,4)
 def test_pipeline_write_not_inferred(self):self.case(lambda v:v.__setitem__('later_pipeline_write_arrival_claimed',True),3,4)
 def test_partial_case2_staysnegative(self):self.case(lambda v:v.__setitem__('capture_complete',True),1,2)
 def test_partial_case4_staysnegative(self):self.case(lambda v:v.__setitem__('capture_complete',True),2,4)
 def test_proxy_forced(self):self.corrupt('attempt3/case-4/proxy-process.json',lambda v:v.__setitem__('forced',True))
 def test_default_literalbytes(self):
  data=dict(self.members);data['attempt3/default-after-private.json']+=b' '
  with self.assertRaises(ValueError):observations(data)
if __name__=='__main__':unittest.main()
