"""Socket/process-free guard corruptions; synthetic positives are not captures."""
import copy,math,os,unittest
from unittest import mock
import run as R
DB='rust_probe_backup_physical_0000000000'
def specimen(case):
 v={'case':case,'database':DB,'capture_complete':True,'capture_only':True,'passed':False,'Source_runs':0,'Main_runs':0,'before':{},'after':{},'before_credentials':{},'after_credentials':{},'request_slots_while_held':3,'early_shutdown_returned':False,'final_shutdown_returned':True,'actual_physical_hold_seconds':1.0,'cache_before_request':{'raw_DUMP_PTTL':{}},'cache_after_request':{'raw_DUMP_PTTL':{}}}
 if case in(1,2):v.update(caller_JoinHandle_abort_consumed=True,HTTP_FD_loss_claimed=False)
 if case==2:v.update(actual_readonly_prepared_timeout_fixture={'backend_pid':1234,'statement_prepared_before_lock':True,'held_reply_same_backend':True,'subject_pool_size_before_lock':1,'subject_pool_idle_before_lock':1,'Source_business_request':False},actual_gate_reached={'held':{'backend_pid':1234}})
 if case==3:v.update(actual_HTTP_FD_shutdown_and_drop=True,handler_abort_claimed=False,HTTP_server_graceful_wait0=True,actual_HTTP_response_write_fate={'accepted':1,'handler_started':1,'socket_dropped':True,'capacity_exceeded':False,'response_statuses':[],'handler_cancelled':1,'bytes_read':80,'bytes_written':0,'read_eof':True,'write_errors':[]},HTTP_write_failure_or_response_status_inferred_from_client_loss=False)
 if case==4:v.update(logical_response_before_physical_reply={'status':401},later_pipeline_write_arrival_claimed=False)
 else:v.update(actual_target_endpoint_settlement={'original_socket_inodes_absent':True,'actual_backend_absent':[],'actual_prior_client_FDs':[{'fd':7,'inode':'123'}]},same_SQL_pool_reuse_after_target_physical_close=True)
 return v
class Controller(unittest.TestCase):
 def test_hardblock_precedes_cli_and_env_cannot_enable(self):
  with mock.patch.dict(os.environ,{'EXECUTION_BLOCKED':'False','ALLOW_SOURCE_CAPTURE':'1'}):
   with self.assertRaisesRegex(RuntimeError,'ROOT_SEALED'):R.main()
 def test_synthetic_positives_are_only_controller_contract(self):
  for case in range(1,5):self.assertTrue(R.validate_native_capture(specimen(case),case,DB)['capture_valid'])
 def test_fixed_command_and_cleanup_count_reserve(self):
  self.assertEqual(R.command_budget(223,0,False),1048576);self.assertEqual(R.command_budget(224,0,True),1048576)
  for n,clean in[(224,False),(256,True)]:
   with self.assertRaises(ValueError):R.command_budget(n,0,clean)
 def test_fixed_output_and_cleanup_byte_reserve(self):
  with self.assertRaises(ValueError):R.command_budget(0,56*1048576,False)
  self.assertEqual(R.command_budget(0,56*1048576,True),1048576)
  with self.assertRaises(ValueError):R.command_budget(0,64*1048576,True)
 def test_typed_budget_cannot_accept_bool_or_negative(self):
  for args in[(True,0,False),(0,-1,False),(0,False,False),(0,0,1)]:
   with self.assertRaises(ValueError):R.command_budget(*args)
 def reject(self,key,value,case=1):
  v=specimen(case);v[key]=value
  with self.assertRaises((ValueError,KeyError,TypeError)):R.validate_native_capture(v,case,DB)
 def test_case_and_database_binding(self):
  self.reject('case',True);self.reject('database','dispatcharr')
 def test_capture_completion_and_negative_status_not_relabelled(self):
  self.reject('capture_complete',False);self.reject('passed',True);self.reject('capture_only',False)
 def test_native_only_scope(self):self.reject('Source_runs',1);self.reject('Main_runs',1)
 def test_sql_or_credential_mutations(self):self.reject('after',{'mutation':1});self.reject('after_credentials',{'event':1})
 def test_same_existing_slot_and_physical_drain(self):
  self.reject('request_slots_while_held',4);self.reject('early_shutdown_returned',True);self.reject('final_shutdown_returned',False)
 def test_nonfinite_negative_and_too_long_actual_hold(self):
  for n in[-1,float('nan'),float('inf'),25.01,True]:self.reject('actual_physical_hold_seconds',n)
 def test_actual_pool_endpoint_close_and_reuse(self):
  for key,value in [('original_socket_inodes_absent',False),('actual_backend_absent',[{'pid':7}]),('actual_prior_client_FDs',[])]:
   v=specimen(1);v['actual_target_endpoint_settlement'][key]=value
   with self.assertRaises(ValueError):R.validate_native_capture(v,1,DB)
  self.reject('same_SQL_pool_reuse_after_target_physical_close',False)
 def test_caller_abort_is_not_http_fd_loss(self):self.reject('caller_JoinHandle_abort_consumed',False);self.reject('HTTP_FD_loss_claimed',True)
 def test_http_loss_does_not_prove_handler_abort_or_response_failure(self):
  self.reject('actual_HTTP_FD_shutdown_and_drop',False,3);self.reject('handler_abort_claimed',True,3);self.reject('HTTP_write_failure_or_response_status_inferred_from_client_loss',True,3)
 def test_http_handler_response_and_cancel_are_mutually_exclusive(self):
  v=specimen(3);v['actual_HTTP_response_write_fate']['response_statuses']=[401]
  with self.assertRaises(ValueError):R.validate_native_capture(v,3,DB)
  v['actual_HTTP_response_write_fate']['handler_cancelled']=0;self.assertTrue(R.validate_native_capture(v,3,DB)['capture_valid'])
 def test_http_transport_must_be_physically_dropped_and_bounded(self):
  for key,val in [('accepted',2),('socket_dropped',False),('capacity_exceeded',True)]:
   v=specimen(3);v['actual_HTTP_response_write_fate'][key]=val
   with self.assertRaises(ValueError):R.validate_native_capture(v,3,DB)
 def test_cache_reply_requires_actual_logical_401_not_later_write_assumption(self):
  self.reject('logical_response_before_physical_reply',{'status':503},4);self.reject('later_pipeline_write_arrival_claimed',True,4)
 def test_cache_dump_values_cannot_mutate(self):self.reject('cache_after_request',{'raw_DUMP_PTTL':{'a':{'dump_hex':'01'}}})
 def test_ttl_decay_is_retained_not_rewritten_as_equal(self):
  v=specimen(1);v['cache_before_request']['raw_DUMP_PTTL']={'a':{'dump_hex':'01','pttl':300000}};v['cache_after_request']['raw_DUMP_PTTL']={'a':{'dump_hex':'01','pttl':298000}};self.assertTrue(R.validate_native_capture(v,1,DB)['capture_valid'])
class MoreGuards(unittest.TestCase):
 def test_nested_bool_integer_SQL_not_equivalent(self):
  v=specimen(1);v['before']={'flag':True};v['after']={'flag':1}
  with self.assertRaises(ValueError):R.validate_native_capture(v,1,DB)
 def test_source_counter_bool_is_not_zero(self):
  v=specimen(1);v['Source_runs']=False
  with self.assertRaises(ValueError):R.validate_native_capture(v,1,DB)
 def test_http_counter_bool_is_not_one(self):
  v=specimen(3);v['actual_HTTP_response_write_fate']['accepted']=True
  with self.assertRaises(ValueError):R.validate_native_capture(v,3,DB)
 def test_prepared_timeout_requires_same_actual_backend(self):
  v=specimen(2);v['actual_readonly_prepared_timeout_fixture']['backend_pid']=1235
  with self.assertRaises(ValueError):R.validate_native_capture(v,2,DB)
 def test_prepared_timeout_requires_single_idle_connection(self):
  for key,val in [('subject_pool_size_before_lock',2),('subject_pool_idle_before_lock',0),('subject_pool_size_before_lock',True)]:
   v=specimen(2);v['actual_readonly_prepared_timeout_fixture'][key]=val
   with self.assertRaises(ValueError):R.validate_native_capture(v,2,DB)
 def test_single_actual_Rust_result(self):
  text='test backup_files::callback_physical_tests::backup_callbacks_actual_sql_and_cache_ownership ... ok\n'+'test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 250 filtered out; finished in 1.2s\n'
  self.assertEqual(R.qualify_rust_result(text.encode())['actual_passed'],1)
  for bad in [text.replace('250 filtered','249 filtered'),text+text,text.replace('1.2s','91.0s'),text.replace('... ok','... ignored'),text.replace('1 passed','0 passed')]:
   with self.assertRaises(ValueError):R.qualify_rust_result(bad.encode())
if __name__=='__main__':unittest.main()

