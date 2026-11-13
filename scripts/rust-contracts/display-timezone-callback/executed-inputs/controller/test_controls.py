"""Socket-free controller/effect/closure corruption controls; no app evidence."""
import copy,json,tempfile,unittest,pickle
from pathlib import Path
from synthetic_controls import native
import validate_native as V
import validate_physical as P
from runtime_closure import validate as closure,resolve_mount
from run import command_budget,cleanup_window
ROOT=Path(__file__).resolve().parents[1]
class Native(unittest.TestCase):
 def setUp(self):self.value=native()
 def reject(self,fn):
  fn(self.value)
  with self.assertRaises(ValueError):V.validate(self.value)
 def test_positive_synthetic_only(self):self.assertEqual(V.validate(self.value)['input_kind'],'synthetic-only-not-actual-observations')
 def test_version_import_signal_lie(self):self.reject(lambda d:d.update(Native_network_save_signal_executed=True))
 def test_warm_seed(self):self.reject(lambda d:d['natural_cache_before_explicit_import'].update(rows=[{}]))
 def test_seed_version_missing(self):self.reject(lambda d:d['cache_after_seed'].update(rows=[]))
 def test_system_group_removed(self):self.reject(lambda d:d['requests'][0]['after_cache']['rows'].pop(1))
 def test_nested_bool_to_int(self):self.reject(lambda d:d['requests'][0]['after_cache']['named_decoded'][V.SYS]['decoded'].update(auto_import_mapped_files=1))
 def test_version_mutation(self):self.reject(lambda d:d['requests'][0]['after_cache']['named_decoded'][V.VERSION].update(decoded=1))
 def test_version_expiry(self):
  def mutate(d):
   for r in d['requests'][0]['after_cache']['rows']:
    if bytes.fromhex(r['key_hex']).decode()==V.VERSION:r['pttl']=1
  self.reject(mutate)
 def test_group_persistent(self):
  def mutate(d):
   for r in d['requests'][0]['after_cache']['rows']:
    if bytes.fromhex(r['key_hex']).decode()==V.SYS:r['pttl']=-1
  self.reject(mutate)
 def test_raw_type(self):self.reject(lambda d:d['requests'][0]['after_raw_Redis'][1]['rows'][0].update(type='list'))
 def test_other_db_mutation(self):self.reject(lambda d:d['requests'][0]['after_raw_Redis'][2]['rows'].append({'key_hex':'01','dump_hex':'00','pttl':-1,'type':'string'}))
 def test_actor(self):self.reject(lambda d:d['requests'][0].update(actor='standard'))
 def test_status(self):self.reject(lambda d:d['requests'][0].update(status=500))
 def test_other_sequence(self):self.reject(lambda d:d['requests'][0]['after_SQL']['sequences']['core_coresettings'].update(last_value=900))
 def test_fullparity_claim(self):self.reject(lambda d:d.update(full_cache_parity_claimed=True))
class ClosureAndBudgets(unittest.TestCase):
 def test_actual_mounted_closure(self):self.assertTrue(closure()['passed'])
 def test_missing_body_dependency(self):
  # No sealed input edit: ask resolver to validate a nonexistent mounted path.
  self.assertFalse(resolve_mount('/native-inputs/bodies/not-present.bin',ROOT,ROOT/'source').exists())
 def test_path_traversal(self):
  with self.assertRaises(ValueError):resolve_mount('/native-inputs/../secret',ROOT,ROOT/'source')
 def test_command_reserve(self):
  with self.assertRaises(ValueError):command_budget(224,0,False)
  self.assertGreater(command_budget(224,0,True),0)
 def test_total_count_bound(self):
  with self.assertRaises(ValueError):command_budget(256,0,True)
 def test_aggregate_output(self):
  with self.assertRaises(ValueError):command_budget(0,64*1048576,True)
 def test_cleanup_elapsed(self):self.assertFalse(cleanup_window(0,90.1)['within_declared_90_seconds'])
 def test_cleanup_nan(self):self.assertFalse(cleanup_window(0,float('nan'))['within_declared_90_seconds'])
 def test_foreign_mount(self):
  with self.assertRaises(ValueError):resolve_mount('/undeclared/file',ROOT,ROOT/'source')
 def test_literal_schema_exact(self):self.assertEqual(json.loads((ROOT/'inputs/schema62-authority.json').read_text())['tables'].__len__(),62)
def physical(case):
 before={'tables':{k:[]for k in P.TABLES},'sequences':{k:{'last_value':1,'is_called':False}for k in P.TABLES}}
 held={'backend_pid':123,'sql':P.SYSTEM_SQL,'parameter_hex':['system_settings'.encode().hex()],'sync_seen':True,'errors':['57014']if case==2 else[],'rows':0 if case==2 else 1}
 final={'connections':0,'invalid':[],'listener_joined':True,'rows':[{'kind':'backend-target-reply-read-held'},{'kind':'backend-target-reply-released'}]}if case>=3 else{'errors':[],'listener_alive':False,'connections':[{'closed':True,'up_joined':True}]}
 d={'capture_complete':True,'passed':True,'capture_only':True,'case':case,'Source_runs':0,'Main_runs':0,'IANA_ZoneInfo_catalogue_equivalent':False,'dummy_owner':False,'real_existing_writer_budget':4,'before':before,'after':copy.deepcopy(before),'credentials_before':{},'credentials_after':{},'closed_pool_size':0,'closed_pool_idle_diagnostic':2,'while_ELF_alive_other_owned_SQL_backends':[],'writer_slots_after_settlement':4,'post_loss_or_logical_held_observations':[{'elapsed_monotonic_seconds':i,'original_owner_weak_alive':True,'writer_slots':3,'actual_cache_physical_operations':1 if case>=3 else 0}for i in(0.001,.252)],'weak_original_owner_while_held':True,'writer_slots_while_held':3,'weak_original_owner_after_settlement':False,'actual_target_hold_seconds':.01,'proxy_final':final,'proxy_naturally_drained_before_stop':copy.deepcopy(final),'held':{'held':held},'actual_observer_abort':True,'error_preserved_zone_and_pretry_debounce':True,'same_target_backend_before_cancel':[{'pid':123}],'HTTP_fate':{'accepted':1,'handler_started':1,'socket_dropped':True,'capacity_exceeded':False},'HTTP_FDloss_is_not_assumed_handler_abort':True,'actual_client_FD':9,'logical_observer_returned_before_physical_reply':True,'four_original_weak_alive':True,'four_real_reused_tokens_settled':True,'four_real_target_backends_held':[{}, {}, {}, {}],'four_prepared_actual_backend_PIDs':[1,2,3,4],'setter_invalid_previous_zone_and_real_debounce':True,'exact_4999_5000_boundary_authority':'compiled paused-clock unit test; no wallclock exactness claim','raw_cache_before':{'raw_DUMP_PTTL':{}},'raw_cache_after':{'raw_DUMP_PTTL':{}}}
 if case==3:
  raw=pickle.dumps({'time_zone':'Europe/London'},protocol=4);dump=b'\x00'+bytes([len(raw)])+raw+b'\x00'*10;d['raw_cache_after']['raw_DUMP_PTTL'][V.SYS.encode().hex()]={'dump_hex':dump.hex(),'pttl':290000}
 return d
class Physical(unittest.TestCase):
 def setUp(self):
  self.tmp=tempfile.TemporaryDirectory();self.out=Path(self.tmp.name);(self.out/'proxy-process.json').write_text(json.dumps({'forced':False,'exit':0,'wait':0,'proc_PID_absent_after_wait':True,'stdout_reader_joined':True,'starttime':'123'}))
 def tearDown(self):self.tmp.cleanup()
 def reject(self,fn,case=1):
  d=physical(case);fn(d)
  with self.assertRaises(ValueError):P.validate(d,case,self.out)
 def test_all_six_synthetic(self):
  for i in range(1,7):self.assertTrue(P.validate(physical(i),i,self.out)['passed'])
 def test_dummy_owner(self):self.reject(lambda d:d.update(dummy_owner=True))
 def test_fifth_permit(self):self.reject(lambda d:d.update(real_existing_writer_budget=5))
 def test_weak_missing(self):self.reject(lambda d:d.update(weak_original_owner_while_held=False))
 def test_weak_leak(self):self.reject(lambda d:d.update(weak_original_owner_after_settlement=True))
 def test_sustained_samples_missing(self):self.reject(lambda d:d.update(post_loss_or_logical_held_observations=[]))
 def test_sustained_weak_false(self):self.reject(lambda d:d['post_loss_or_logical_held_observations'][1].update(original_owner_weak_alive=False))
 def test_sustained_short_window(self):self.reject(lambda d:d['post_loss_or_logical_held_observations'][1].update(elapsed_monotonic_seconds=.1))
 def test_sustained_nan(self):self.reject(lambda d:d['post_loss_or_logical_held_observations'][1].update(elapsed_monotonic_seconds=float('nan')))
 def test_sustained_bool_clock(self):self.reject(lambda d:d['post_loss_or_logical_held_observations'][1].update(elapsed_monotonic_seconds=True))
 def test_sustained_cache_not_owned(self):self.reject(lambda d:d['post_loss_or_logical_held_observations'][1].update(actual_cache_physical_operations=0),4)
 def test_sustained_slot_lie(self):self.reject(lambda d:d['post_loss_or_logical_held_observations'][1].update(writer_slots=4))
 def test_pool_size(self):self.reject(lambda d:d.update(closed_pool_size=1))
 def test_pool_bool(self):self.reject(lambda d:d.update(closed_pool_size=False))
 def test_backend_live(self):self.reject(lambda d:d.update(while_ELF_alive_other_owned_SQL_backends=[{'pid':1}]))
 def test_error_other_backend(self):self.reject(lambda d:d['same_target_backend_before_cancel'][0].update(pid=456),2)
 def test_no_sync(self):self.reject(lambda d:d['held']['held'].update(sync_seen=False))
 def test_foreign_parameter(self):self.reject(lambda d:d['held']['held'].update(parameter_hex=['00']))
 def test_missing_fd(self):self.reject(lambda d:d['HTTP_fate'].update(accepted=0),3)
 def test_settle_lie(self):self.reject(lambda d:d.update(logical_observer_returned_before_physical_reply=False),4)
 def test_foreign_cache_change(self):self.reject(lambda d:d['raw_cache_after']['raw_DUMP_PTTL'].update({'aa':{'dump_hex':'00','pttl':-1}}))
 def test_hold_expired(self):self.reject(lambda d:d.update(actual_target_hold_seconds=26))
 def test_forced_stop(self):
  p=self.out/'proxy-process.json';d=json.loads(p.read_text());d['forced']=True;p.write_text(json.dumps(d));self.reject(lambda d:None)
if __name__=='__main__':unittest.main(verbosity=2)
