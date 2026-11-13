"""Pure synthetic/source/ownership guards, no Docker/Main/Source/socket execution."""
import ast,copy,hashlib,json,math,unittest
from pathlib import Path
from urllib.parse import urlencode
import run as R
import main_http as H
ROOT=Path(__file__).resolve().parent

def synthetic():
 schema=R.read(ROOT/'schema.json');seed=R.read(ROOT/'retained-seed-private.json');golden=H.reference(R.read(ROOT/'retained-source26.json'));declared=[c for c in H.cases()if c['action']in H.ACTIONS];db='rust_capture_'+'a'*16
 snapshot={'database':db,'tables':{k:{'rows':0,'sha256':'0'*64}for k in schema['tables']},'sequences':{k:{'last_value':1,'is_called':False}for k in schema['sequences']}};rows=[]
 for i,(case,source)in enumerate(zip(declared,golden['cases'],strict=True)):
  before=copy.deepcopy(snapshot);begin=i+1.;query=H.query(case,seed);row={'name':case['name'],'role':case['role'],'action':case['action'],'method':'GET','request_query_private':query,'request_target_sha256':hashlib.sha256(('/player_api.php?'+urlencode(query)).encode()).hexdigest(),'Main_destination':'127.0.0.1:9192 shared baseline network namespace','actual_Host':'127.0.0.1:5657','began_monotonic':begin,'completed_monotonic':begin+.1,'status':source['status'],'headers':copy.deepcopy(source['headers']),'body_hex':source['body_hex'],'body_sha256':source['body_sha256'],'absolute_observation':{'completed':begin+.05,'deadline':begin+8,'timer_joined':True},'physical_close':{'timer_joined':True,'fd_after':-1,'original_inode_absent':True},'before':before,'after':copy.deepcopy(snapshot),'before_named11':H.named(before),'after_named11':H.named(snapshot),'before_raw_Redis':[{'database':n,'rows':[]}for n in range(4)],'after_raw_Redis':[{'database':n,'rows':[]}for n in range(4)]};rows.append(row)
 return {'_synthetic_only':True,'actual_production_Main':True,'info_actions_or_fixed_clock_hook':False,'declared_configured_XC_max_rows':50000,'capture_complete':True,'fixed_names':H.NAMES,'phase_started':0.,'phase_completed':30.,'cases':rows},db
class Effects(unittest.TestCase):
 def setUp(self):self.value,self.db=synthetic()
 def reject(self,f):
  f(self.value)
  with self.assertRaises((ValueError,KeyError)):R.qualify_http(self.value,self.db)
 def test_synthetic_positive_shape_only(self):self.assertTrue(R.qualify_http(self.value,self.db)['Main26_Source_body_status_CT_parity'])
 def test_complete_mismatch_is_negative_not_retry(self):
  row=self.value['cases'][0];row['status']=501
  self.assertFalse(R.qualify_http(self.value,self.db)['Main26_Source_body_status_CT_parity'])
 def test_body_mismatch_preserved(self):
  row=self.value['cases'][0];row['body_hex']=b'[]'.hex();row['body_sha256']=hashlib.sha256(b'[]').hexdigest();self.assertFalse(R.qualify_http(self.value,self.db)['Main26_Source_body_status_CT_parity'])
 def test_changed_body_hash(self):self.reject(lambda d:d['cases'][0].update(body_sha256='0'*64))
 def test_wrong_role(self):self.reject(lambda d:d['cases'][0].update(role='basic'))
 def test_wrong_password(self):self.reject(lambda d:d['cases'][0]['request_query_private'].update(password='other'))
 def test_host_change(self):self.reject(lambda d:d['cases'][0].update(actual_Host='candidate:9192'))
 def test_network_context_change(self):self.reject(lambda d:d['cases'][0].update(Main_destination='candidate:9192'))
 def test_info_hook(self):self.reject(lambda d:d.update(info_actions_or_fixed_clock_hook=True))
 def test_testpeer_claim(self):self.reject(lambda d:d.update(actual_production_Main=False))
 def test_production_cap(self):self.reject(lambda d:d.update(declared_configured_XC_max_rows=10))
 def test_float_cap(self):self.reject(lambda d:d.update(declared_configured_XC_max_rows=50000.))
 def test_missing_case(self):self.reject(lambda d:d['cases'].pop())
 def test_open_fd(self):self.reject(lambda d:d['cases'][0]['physical_close'].update(fd_after=5))
 def test_missing_timer_join(self):self.reject(lambda d:d['cases'][0]['physical_close'].update(timer_joined=False))
 def test_nonfinite_clock(self):self.reject(lambda d:d['cases'][0].update(began_monotonic=float('nan')))
 def test_global_phase_expired(self):self.reject(lambda d:d.update(phase_completed=90.1))
 def test_own_audit_narrow(self):self.reject(lambda d:d['cases'][0]['before']['tables'].pop(next(iter(d['cases'][0]['before']['tables']))))
 def test_sequence_bool(self):
  def f(d):d['cases'][0]['before']['sequences'][next(iter(d['cases'][0]['before']['sequences']))]['last_value']=True
  self.reject(f)
 def test_table_count_bool(self):
  def f(d):d['cases'][0]['before']['tables'][next(iter(d['cases'][0]['before']['tables']))]['rows']=False
  self.reject(f)
 def test_sql_mutation(self):
  def f(d):d['cases'][0]['after']['tables'][H.OWN11[0]]['rows']=1;d['cases'][0]['after_named11']=H.named(d['cases'][0]['after'])
  self.reject(f)
 def test_named_scope_lie(self):self.reject(lambda d:d['cases'][0].update(before_named11={}))
 def test_missing_rawRedis(self):self.reject(lambda d:d['cases'][0].update(before_raw_Redis=[]))
class CodeAndBudget(unittest.TestCase):
 def test_unchanged_pinned_seed_inputs(self):
  for name,h in R.read(ROOT/'reference-inputs.json')['private_inputs'].items():self.assertEqual(R.sha(ROOT/name),h)
 def test_loopback_source_policy_literal(self):
  rows=R.read(ROOT/'retained-rows-private.json');network=next(r['value']for r in rows['core_coresettings']if r['key']=='network_access');self.assertEqual(network,{'XC_API':'127.0.0.0/8'})
 def test_exact_actions_noinfo(self):self.assertEqual(H.ACTIONS,{'get_vod_categories','get_vod_streams','get_series_categories','get_series'});self.assertEqual(len(H.NAMES),26)
 def test_command_reserve(self):
  with self.assertRaises(ValueError):R.command_budget(224,0,False)
  self.assertGreater(R.command_budget(224,0,True),0)
 def test_total_count(self):
  with self.assertRaises(ValueError):R.command_budget(256,0,True)
 def test_output_reserve(self):
  with self.assertRaises(ValueError):R.command_budget(1,64*1048576,True)
 def test_shared_namespace_exact_baseline(self):
  R.shared_loopback('container:'+'a'*64,'a'*64)
  for actual in ['container:'+'b'*64,'host','container:']:
   with self.assertRaises(ValueError):R.shared_loopback(actual,'a'*64)
 def test_redis_successful_receipt(self):
  identity={'actual_identity_observed':True};receipt={k:True for k in ['stdout_pipe_closed','stderr_pipe_closed','pipe_reader_threads_settled','stdin_writer_settled','pipes_physically_settled']};receipt.update(actual_exit=0,collection_invalid=None,owned_stop_requested=False);R.redis_settlement(identity,receipt)
  for key,value in [('actual_exit',1),('actual_exit',False),('collection_invalid','CollectionCapacity'),('pipe_reader_threads_settled',False),('owned_stop_requested',True)]:
   bad=dict(receipt);bad[key]=value
   with self.assertRaises(ValueError):R.redis_settlement(identity,bad)
  with self.assertRaises(ValueError):R.redis_settlement({'actual_identity_observed':False},receipt)
 def test_long_job_counted_and_always_waited(self):
  text=(ROOT/'run.py').read_text();self.assertIn('used+(redis_job.used if redis_job is not None else 0)',text);self.assertIn('if job is redis_job or job.process.poll()is None:',text)
 def test_blocked_launcher(self):self.assertTrue(R.EXECUTION_BLOCKED)
 def test_real_startup_skip_order(self):
  source=(ROOT/'fixture_db.py').read_text();self.assertLess(source.index("sys.argv.append('shell')"),source.index('django.setup()'));self.assertIn('ManagementSkipBefore',source);self.assertIn('ManagementSkipAfter',source)
 def test_no_candidate_source_or_binary_mount(self):
  text=(ROOT/'run.py').read_text();self.assertIn("'volumes':['rust-data:/data']",text);self.assertIn("['/usr/local/bin/dispatcharr-rs']",text);self.assertNotIn('native-xc-tests',text)
 def test_post_request_cleanup_reserve_before_validation(self):
  text=(ROOT/'run.py').read_text();self.assertIn("allowed=(0,1));begin_cleanup();http=read",text)
if __name__=='__main__':unittest.main(verbosity=2)
