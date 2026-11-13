"""Socket-free synthetic corruption controls; no observations or business claims."""
import copy,json,tempfile,unittest
from pathlib import Path
from unittest.mock import patch
import run as D,validate_physical as V,owned_pg_proxy as P,bootstrap_env as B
ROOT=Path(__file__).resolve().parents[1]
NAMES={'accounts_user','m3u_m3uaccount','m3u_m3uaccountprofile','vod_movie','vod_m3umovierelation','vod_series','vod_episode','vod_m3uepisoderelation','core_coresettings','core_systemnotification','core_notificationdismissal'}
def fixture(case):
 stage='api_key_error_ready'if case==1 else'network_error_ready';c=json.loads((ROOT/'inputs/pg-contracts.json').read_text())[stage];held={'stage':stage,'backend_pid':12345,'sql':c['sql'],'parameter_hex':c['parameter_hex'],'errors':['57014'],'rows':0,'commands':[],'sync_seen':True,'tx':'I'};row=json.loads((ROOT/'inputs/imported-existing-version.json').read_text())['row'];before={'tables':{n:[]for n in NAMES},'sequences':{n:{'last_value':1,'is_called':False}for n in NAMES}};cache={'raw_DUMP_PTTL':{row['key_hex']:{'dump_hex':row['dump_hex'],'pttl':-1}}}
 return {'_synthetic_only':True,'case':case,'capture_only':True,'capture_complete':True,'passed':True,'Source_runs':0,'Main_runs':0,'dummy_owner':False,'real_existing_writer_budget':4,'pool_max_connections':4,'before':before,'after':copy.deepcopy(before),'credentials_before':{'users':[],'events':[]},'credentials_after':{'users':[],'events':[]},'raw_cache_before':cache,'raw_cache_after':copy.deepcopy(cache),'prepared_actual_backend_pid':12345,'same_target_backend_before_cancel':[{'pid':12345,'wait_event_type':'Lock'}],'held':{'held':held},'actual_direct_observer_abort':True,'actual_logical_deadline_returned_before_ready':True,'post_loss_or_deadline_held_observations':[{'elapsed_monotonic_seconds':n,'original_owner_weak_alive':True,'writer_slots':3,'same_backend_still_exists':True,'held':copy.deepcopy(held)}for n in(0,.25)],'original_owner_weak_after_settlement':False,'writer_slots_after_settlement':4,'actual_pool_reuse_value':424242,'closed_pool_size':0,'closed_pool_idle_diagnostic':2,'while_ELF_alive_subject_SQL_backends':[],'proxy_final':{'errors':[],'listener_alive':False,'events':[{'kind':'ready-held','stage':stage,'monotonic':1},{'kind':'ready-forwarded','stage':stage,'monotonic':1.5}]},'proxy_naturally_drained_before_stop':{'connections':[{'closed':True,'up_joined':True}]},'actual_target_hold_seconds':.5}
PROCESS={'forced':False,'exit':0,'wait':0,'proc_PID_absent_after_wait':True,'stdout_reader_joined':True,'starttime':'123'}
class Controls(unittest.TestCase):
 def check(self,value):
  with tempfile.TemporaryDirectory()as d:
   (Path(d)/'proxy-process.json').write_text(json.dumps(PROCESS));return V.validate(value,value['case'],d)
 def test_two_synthetic_baselines(self):
  for case in(1,2):self.assertTrue(self.check(fixture(case))['passed'])
 def test_control_imports_and_no_network_setup(self):
  import ast
  tree=ast.parse((ROOT/'controller/audit.py').read_text());imports=[n.module for n in ast.walk(tree)if isinstance(n,ast.ImportFrom)]+[x.name for n in ast.walk(tree)if isinstance(n,ast.Import)for x in n.names];self.assertTrue(all(not x.startswith('django')for x in imports));self.assertIn('psycopg',imports);self.assertNotIn('psycopg2',imports)
 def test_budget_normal_and_cleanup_reserves(self):
  self.assertGreater(D.command_budget(0,0,False),0)
  with self.assertRaises(ValueError):D.command_budget(224,0,False)
  with self.assertRaises(ValueError):D.command_budget(256,0,True)
  with self.assertRaises(ValueError):D.command_budget(0,56*1048576,False)
  with self.assertRaises(ValueError):D.command_budget(0,64*1048576,True)
 def test_cleanup90_exact_and_nonfinite(self):
  self.assertTrue(D.cleanup_window(1,91)['within_declared_90_seconds'])
  for x in(91.1,float('nan'),float('inf'),0):self.assertFalse(D.cleanup_window(1,x)['within_declared_90_seconds'])
 def test_pinned_environment_complete(self):self.assertTrue(B.audit_helpers(ROOT,B.storage_environment())['passed'])
 def test_all_required_environment_missing_refuse(self):
  for name in B.FIXED:
   env=B.storage_environment();del env[name]
   with self.assertRaises(ValueError,msg=name):B.validate_environment(env)
 def test_execute_vs_prepare_and_bound_parameter(self):
  want=json.loads((ROOT/'inputs/pg-contracts.json').read_text())['api_key_error_ready'];state={'rows':0,'sql':want['sql'],'execute':True,'commands':[],'errors':['57014'],'parameter_hex':want['parameter_hex'],'sync_seen':True};self.assertTrue(P.ready_match(want,state,'I'))
  for k,v in [('execute',False),('sync_seen',False),('parameter_hex',['bad']),('errors',[]),('rows',1),('sql','SELECT 1')]:
   bad=copy.deepcopy(state);bad[k]=v;self.assertFalse(P.ready_match(want,bad,'I'))
 def test_mounted_literal_path_closure_missing_authority_refuses(self):
  seal=json.loads((ROOT/'seal.json').read_text())
  with tempfile.TemporaryDirectory()as d:
   root=Path(d);(root/'inputs/pg-init').mkdir(parents=True);(root/'seal.json').write_text(json.dumps(seal))
   for name in('schema62-authority.json','imported-existing-version.json','pg-contracts.json','pg-init/01-user-setup.sh','pg-init/02-postgres.sh'):(root/'inputs'/name).write_bytes((ROOT/'inputs'/name).read_bytes())
   with patch.object(D,'ROOT',root):self.assertTrue(D.mounted_closure()['passed']);(root/'inputs/schema62-authority.json').unlink();self.assertRaises(ValueError,D.mounted_closure)
 def test_literal_expected_target_parameters(self):
  c=json.loads((ROOT/'inputs/pg-contracts.json').read_text());self.assertEqual(c['api_key_error_ready']['parameter_hex'],[b'owned-rest-read-key-not-production'.hex(),'00010000']);self.assertEqual(c['network_error_ready']['parameter_hex'],[b'network_access'.hex(),'00010000'])
 def test_full_readonly_closure(self):self.assertEqual(D.check()['state'],'COMPILED_INERT_REST_READ_PHYSICAL_REVIEW')
def add_negative(name,path,new,delete=False):
 def test(self):
  for case in(1,2):
   value=fixture(case);target=value
   for part in path[:-1]:target=target[part]
   if delete:del target[path[-1]]
   else:target[path[-1]]=new
   with self.assertRaises((ValueError,KeyError,TypeError)):self.check(value)
 setattr(Controls,'test_corruption_'+name,test)
for name,path,value in [
 ('incomplete',['capture_complete'],False),('source_run',['Source_runs'],1),('boolean_source_count',['Source_runs'],False),('new_budget',['real_existing_writer_budget'],5),('dummy_owner',['dummy_owner'],True),('changed_SQL',['after','tables','accounts_user'],[{'id':1}]),('changed_sequence',['after','sequences','accounts_user','last_value'],2),('bool_sequence',['after','sequences','accounts_user','is_called'],0),('credential_effect',['credentials_after','events'],[{}]),('cache_drop',['raw_cache_after','raw_DUMP_PTTL'],{}),('wrong_backend',['prepared_actual_backend_pid'],54321),('wrong_held_backend',['held','held','backend_pid'],54321),('wrong_sql',['held','held','sql'],'SELECT 1'),('wrong_binding',['held','held','parameter_hex'],['bad']),('prepare_not_execute',['held','held','sync_seen'],False),('not_error57014',['held','held','errors'],[]),('row_response',['held','held','rows'],1),('sample_weak_lost',['post_loss_or_deadline_held_observations',1,'original_owner_weak_alive'],False),('sample_short',['post_loss_or_deadline_held_observations',1,'elapsed_monotonic_seconds'],.249),('sample_nonfinite',['post_loss_or_deadline_held_observations',1,'elapsed_monotonic_seconds'],float('nan')),('sample_new_slot',['post_loss_or_deadline_held_observations',1,'writer_slots'],4),('sample_backend_gone',['post_loss_or_deadline_held_observations',1,'same_backend_still_exists'],False),('not_retired',['original_owner_weak_after_settlement'],True),('no_poolreuse',['actual_pool_reuse_value'],0),('pool_notclosed',['closed_pool_size'],1),('idle_negative',['closed_pool_idle_diagnostic'],-1),('idle_boolean',['closed_pool_idle_diagnostic'],False),('idle_exceedsmax',['closed_pool_idle_diagnostic'],5),('backend_stillalive',['while_ELF_alive_subject_SQL_backends'],[{'pid':1}]),('relay_error',['proxy_final','errors'],[{}]),('no_relay_drain',['proxy_naturally_drained_before_stop','connections'],[{'closed':False,'up_joined':True}]),('hold_over25',['actual_target_hold_seconds'],26)]:add_negative(name,path,value)
for name,path,value in [
 ('heldclock_nan',['proxy_final','events',0,'monotonic'],float('nan')),('heldclock_inf',['proxy_final','events',0,'monotonic'],float('inf')),('heldclock_boolean',['proxy_final','events',0,'monotonic'],True),('forwardclock_nan',['proxy_final','events',1,'monotonic'],float('nan')),('forwardclock_inf',['proxy_final','events',1,'monotonic'],float('inf')),('forwardclock_boolean',['proxy_final','events',1,'monotonic'],False),('forward_before_held',['proxy_final','events',1,'monotonic'],.5),('hold_nan',['actual_target_hold_seconds'],float('nan')),('hold_inf',['actual_target_hold_seconds'],float('inf')),('hold_bool',['actual_target_hold_seconds'],False),('hold_zero',['actual_target_hold_seconds'],0)]:add_negative(name,path,value)
def causal_negative(self,zero=False):
 for case in(1,2):
  value=fixture(case)
  if zero:value['proxy_final']['events'][0]['monotonic']=value['proxy_final']['events'][1]['monotonic']=0;value['actual_target_hold_seconds']=0
  else:value['proxy_final']['events'][1]['monotonic']=1.25;value['actual_target_hold_seconds']=.25;value['post_loss_or_deadline_held_observations'][1]['elapsed_monotonic_seconds']=.5
  with self.assertRaises(ValueError):self.check(value)
Controls.test_corruption_zero_event_duration_with_sustained_samples=lambda self:causal_negative(self,True)
Controls.test_corruption_hold_shorter_than_sustained_sample_span=causal_negative
add_negative('missing_samples',['post_loss_or_deadline_held_observations'],None,True)
if __name__=='__main__':unittest.main()
