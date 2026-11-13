"""Pure synthetic corruption controls; never application observations."""
import copy,json,pickle,unittest,sys,os,tempfile
from pathlib import Path
from synthetic import sample
from validate_native import compare,safe_cache,cache,literal_equal,raw_guard_redis
class Controls(unittest.TestCase):
 def reject(self,mutate):
  n,s,p=sample();mutate(n)
  with self.assertRaises((ValueError,KeyError,TypeError)):compare(n,s,p)
 def test_synthetic_positive(self):n,s,p=sample();self.assertTrue(compare(n,s,p)['passed'])
 def test_raw_false_preserved(self):self.reject(lambda n:n.update(passed=True))
 def test_missing_request(self):self.reject(lambda n:n['requests'].pop())
 def test_source_mode_wrong(self):self.reject(lambda n:n.update(Source_reference_mode='original-or-projected'))
 def test_source_digest_wrong(self):self.reject(lambda n:n.update(Source_reference_sha256='0'*64))
 def test_wrong_role_header(self):self.reject(lambda n:n['requests'][1]['raw_headers_private'].update({'x-api-key':'00'}))
 def test_wrong_status(self):self.reject(lambda n:n['requests'][1].update(status=200))
 def test_wrong_body_sha(self):self.reject(lambda n:n['requests'][1].update(body_sha256='0'*64))
 def test_wrong_target(self):self.reject(lambda n:n['requests'][4].update(path='/api/backups/status/wrong/'))
 def test_user_effect_omission(self):self.reject(lambda n:n['requests'][5]['after'].update(users=[]))
 def test_event_effect_omission(self):self.reject(lambda n:n['requests'][5]['after'].update(events=[{'id':99}]))
 def test_sequence_advance(self):self.reject(lambda n:n['requests'][5]['after']['SQL']['sequences']['core_systemnotification'].update(last_value=77))
 def test_drop_network_cache_after1(self):self.reject(lambda n:n['requests'][0]['after']['redis'][0]['rows'].pop())
 def test_prefilled_five_before1(self):self.reject(lambda n:n['requests'][0].update(before=copy.deepcopy(n['requests'][0]['after'])))
 def test_auth_budget_injected(self):
  def change(n):
   row=copy.deepcopy(n['requests'][1]['after']['redis'][0]['rows'][0]);row['key_hex']=b':1:throttle_api_key_auth_127.0.0.1'.hex();n['requests'][1]['after']['redis'][0]['rows'].append(row)
  self.reject(change)
 def test_initial_context_reset_hidden(self):self.reject(lambda n:n['Source_cache_context_seed'].update(auth_history_or_events_reset=True))
 def test_context_persistent_ttl_wrong(self):self.reject(lambda n:n['Source_cache_context_seed']['rows'][0].update(RESTORE_ttl=1))
 def test_context_value_hash_wrong(self):self.reject(lambda n:n['Source_cache_context_seed']['rows'][0].update(restored_dump_sha256='0'*64))
 def test_wrong_actual_queue_envelope(self):
  def change(n):
   row=n['requests'][3]['after']['redis'][1]['rows'][0];frame=json.loads(bytes.fromhex(row['list_hex'][0]));frame['properties']['delivery_info']['routing_key']='owned-only-list-change';row['list_hex']=[json.dumps(frame).encode().hex()]
  self.reject(change)
 def test_missing_real_backend_result(self):self.reject(lambda n:n['requests'][12]['after']['redis'][2].update(rows=[]))
 def test_result_clock_outside_worker(self):self.reject(lambda n:n['actual_result_metadata'].update(date_done='2020-01-01T00:00:00+00:00'))
 def test_worker_id_mismatch(self):self.reject(lambda n:n['actual_result_metadata'].update(task_id='00000000-0000-4000-8000-000000000000'))
 def test_archive_file_hash_mismatch(self):self.reject(lambda n:n['requests'][15]['after']['files'][0].update(sha256='0'*64))
 def test_guard_effect_event(self):self.reject(lambda n:n['native_only_family_guards']['after'].update(events=[{'id':99}]))
 def test_missing_service_drain(self):self.reject(lambda n:n.update(backups_shutdown=False))
 def test_pg_micro_quantization_predeclared(self):
  n,s,p=sample();n['seed_clock']='2026-11-13T04:00:00.000000400+00:00';self.assertTrue(compare(n,s,p)['passed'])
 def test_seed_clock_rewind_rejected(self):self.reject(lambda n:n.update(seed_clock='2026-11-13T04:00:00.000005+00:00'))
 def test_literal_pickle(self):self.assertEqual(safe_cache(pickle.dumps({'a':[1,True,None]})),{'a':[1,True,None]})
 def test_pickle_global_forbidden(self):
  with self.assertRaises(ValueError):safe_cache(b'cos\nsystem\n.')
 def test_pickle_trailing_bytes_forbidden(self):
  with self.assertRaises(ValueError):safe_cache(pickle.dumps({'a':1})+b'ignored')
class Revision2Controls(unittest.TestCase):
 def test_literal_bool_int_distinct(self):self.assertFalse(literal_equal({'nested':[True,False]},{'nested':[1,0]}))
 def test_nested_cache_bool_to_int_rejected(self):
  n,s,p=sample();row=next(r for r in n['requests'][0]['after']['redis'][0]['rows']if bytes.fromhex(r['key_hex']).decode()==':1:coresettings:group:system_settings');value=safe_cache(bytes.fromhex(row['value_hex']));key=next(k for k,v in value.items()if type(v)is bool);value[key]=int(value[key]);row['value_hex']=pickle.dumps(value).hex()
  with self.assertRaises(ValueError):compare(n,s,p)
 def test_guard_broker_key_drop_rejected(self):
  n,s,p=sample();n['native_only_family_guards']['after']['redis'][1]['rows']=[]
  with self.assertRaises(ValueError):compare(n,s,p)
 def test_guard_backend_value_change_rejected(self):
  n,s,p=sample();n['native_only_family_guards']['after']['redis'][2]['rows'][0]['value_hex']='7b7d'
  with self.assertRaises(ValueError):compare(n,s,p)
 def test_guard_dump_change_rejected(self):
  n,s,p=sample();n['native_only_family_guards']['after']['redis'][1]['rows'][0]['dump_hex']='04'
  with self.assertRaises(ValueError):compare(n,s,p)
 def test_guard_type_change_rejected(self):
  n,s,p=sample();n['native_only_family_guards']['after']['redis'][1]['rows'][0]['type']='hash'
  with self.assertRaises(ValueError):compare(n,s,p)
 def test_guard_finite_ttl_increase_rejected(self):
  n,s,p=sample();n['native_only_family_guards']['after']['redis'][2]['rows'][0]['pttl']+=1
  with self.assertRaises(ValueError):compare(n,s,p)
 def test_guard_persistent_ttl_change_rejected(self):
  n,s,p=sample();n['native_only_family_guards']['after']['redis'][1]['rows'][0]['pttl']=1000
  with self.assertRaises(ValueError):compare(n,s,p)
 def test_guard_clearing_both_sides_still_fails_continuity(self):
  n,s,p=sample()
  for side in('before','after'):n['native_only_family_guards'][side]['redis'][1]['rows']=[]
  with self.assertRaises(ValueError):compare(n,s,p)
 def test_guard_list_raw_bytes_rejected(self):
  n,s,p=sample();a=n['native_only_family_guards']['before'];b=copy.deepcopy(a);row={'key_hex':b'foreign'.hex(),'type':'list','dump_hex':'00','list_hex':['6162'],'pttl':-1};a['redis'][1]['rows'].append(row);b['redis'][1]['rows'].append(dict(row,list_hex=['6163']))
  with self.assertRaises(ValueError):raw_guard_redis(a,b)
 def test_finite_decreasing_ttl_accepted(self):
  n,s,p=sample();n['native_only_family_guards']['after']['redis'][2]['rows'][0]['pttl']-=1;n['final']['redis'][2]['rows'][0]['pttl']-=2;self.assertTrue(compare(n,s,p)['passed'])
 def test_cleanup90_boundary(self):
  from run import cleanup_window
  self.assertTrue(cleanup_window(100,190)['within_declared_90_seconds']);self.assertFalse(cleanup_window(100,190.001)['within_declared_90_seconds']);self.assertFalse(cleanup_window(100,99)['within_declared_90_seconds'])
 def test_cleanup_timer_precedes_all_postwork(self):
  from pathlib import Path
  text=(Path(__file__).resolve().parent/'run.py').read_text();start=text.index(" finally:\n  begin_cleanup()\n  capture=");self.assertLess(start,text.index("audit('cache','cache-after'",start));self.assertNotIn('cleanup_deadline=time.monotonic()+90',text)
 def stop(self,code,limit=1024,timeout=1,save=None):
  from bounded_stop import collect
  from pathlib import Path
  receipt={}
  with tempfile.TemporaryDirectory(prefix='stop-control-',dir=Path(__file__).resolve().parents[2])as folder:
   try:collect([sys.executable,'-c',code],dict(os.environ),folder,receipt,limit=limit,timeout=timeout,save=save)
   except BaseException:return receipt
  return receipt
 def test_bounded_stop_success_no_recursion(self):
  r=self.stop("print('owned stop ack')");self.assertTrue(r['acknowledged']);self.assertFalse(r['recursive_stop_callback']);self.assertTrue(r['pipes_settled'])
 def test_bounded_stop_overflow_retained_negative(self):
  r=self.stop("print('x'*2048)",limit=64);self.assertFalse(r['acknowledged']);self.assertFalse(r['capture_complete']);self.assertEqual(r['collection_invalid'],'CollectionCapacity')
 def test_bounded_stop_timeout_retained_negative(self):
  r=self.stop("import time;time.sleep(1)",timeout=.02);self.assertFalse(r['acknowledged']);self.assertEqual(r['collection_invalid'],'CollectionDeadline');self.assertIsNotNone(r['actual_exit'])
 def test_bounded_stop_nonzero_retained(self):
  r=self.stop('raise SystemExit(7)');self.assertFalse(r['acknowledged']);self.assertEqual(r['actual_exit'],7)
 def test_bounded_stop_receipt_failure_retained(self):
  def fail(*args):raise OSError('owned injected receipt failure')
  r=self.stop("print('ack')",save=fail);self.assertFalse(r['acknowledged']);self.assertIn('receipt_write_failure',r)
if __name__=='__main__':unittest.main()
