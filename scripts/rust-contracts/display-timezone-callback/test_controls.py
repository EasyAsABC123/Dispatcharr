"""Meaningful socket-free projection/physical-oracle corruption controls."""
import copy,gzip,importlib.util,json,sys,tempfile,unittest,os
from pathlib import Path
import reconstruct,verify
ROOT=Path(__file__).resolve().parent
REPO=Path(os.environ.get('DISPLAY_CALLBACK_GIT_REPO',str(ROOT.parents[2])))
class Controls(unittest.TestCase):
 @classmethod
 def setUpClass(cls):
  cls.tmp=tempfile.TemporaryDirectory(prefix='display-callback-controls-',dir=REPO/'artifacts/rust');cls.work=Path(cls.tmp.name);reconstruct.inputs(cls.work/'execution',REPO);sys.path.insert(0,str(cls.work/'execution/controller'))
  spec=importlib.util.spec_from_file_location('validate_native',cls.work/'execution/controller/validate_native.py');cls.native=importlib.util.module_from_spec(spec);sys.modules['validate_native']=cls.native;spec.loader.exec_module(cls.native)
  spec=importlib.util.spec_from_file_location('physical',cls.work/'execution/controller/validate_physical.py');cls.physical=importlib.util.module_from_spec(spec);spec.loader.exec_module(cls.physical);cls.raw=json.loads(gzip.decompress((ROOT/'actual/case1/callback-private.json.gz').read_bytes()));cls.directory=verify.lifecycle(1,cls.work/'case1')
 @classmethod
 def tearDownClass(cls):cls.tmp.cleanup()
 def reject(self,change):
  v=copy.deepcopy(self.raw);change(v)
  with self.assertRaises((ValueError,KeyError)):self.physical.validate(v,1,self.directory)
 def test_retained_physical_positive(self):self.assertTrue(self.physical.validate(self.raw,1,self.directory)['passed'])
 def test_projected_case0_positive(self):
  v=verify.restore_case0(json.loads((ROOT/'actual/case0/Native19-projection.json').read_text()));self.assertIs(v['passed'],False);self.native.validate(v)
 def test_projected_credential_digest_wrong(self):
  v=json.loads((ROOT/'actual/case0/Native19-projection.json').read_text());v['seed_users'][0]['api_key']['sha256']='0'*64
  with self.assertRaises(ValueError):verify.restore_case0(v)
 def test_projected_credential_length_bool(self):
  v=json.loads((ROOT/'actual/case0/Native19-projection.json').read_text());v['seed_users'][0]['api_key']['bytes']=True
  with self.assertRaises(ValueError):verify.restore_case0(v)
 def test_wrong_original_owner(self):self.reject(lambda v:v.update(weak_original_owner_while_held=False))
 def test_extra_budget(self):self.reject(lambda v:v.update(real_existing_writer_budget=5))
 def test_missing_sustained_samples(self):self.reject(lambda v:v.pop('post_loss_or_logical_held_observations'))
 def test_short_held_window(self):
  def bad(v):v['post_loss_or_logical_held_observations'][1]['elapsed_monotonic_seconds']=v['post_loss_or_logical_held_observations'][0]['elapsed_monotonic_seconds']+.1
  self.reject(bad)
 def test_false_held_sample(self):self.reject(lambda v:v['post_loss_or_logical_held_observations'][1].update(original_owner_weak_alive=False))
 def test_more_than_four_slots(self):self.reject(lambda v:v.update(writer_slots_after_settlement=5))
 def test_live_backend(self):self.reject(lambda v:v.update(while_ELF_alive_other_owned_SQL_backends=[{'pid':123}]))
 def test_closed_pool_size1(self):self.reject(lambda v:v.update(closed_pool_size=1))
 def test_idle_negative(self):self.reject(lambda v:v.update(closed_pool_idle_diagnostic=-1))
 def test_idle_over_compiled_max(self):self.reject(lambda v:v.update(closed_pool_idle_diagnostic=5))
 def test_idle_boolean(self):self.reject(lambda v:v.update(closed_pool_idle_diagnostic=False))
 def test_foreign_row_mutation(self):self.reject(lambda v:v['after']['tables']['accounts_user'].append({'id':1}))
 def test_sequence_mutation(self):
  def bad(v):v['after']['sequences']['accounts_user']['last_value']+=1
  self.reject(bad)
 def test_wrong_prepared_SQL(self):self.reject(lambda v:v['held']['held'].update(sql='SELECT 1'))
 def test_wrong_bind(self):self.reject(lambda v:v['held']['held'].update(parameter_hex=['wrong'.encode().hex()]))
 def test_sync_missing(self):self.reject(lambda v:v['held']['held'].update(sync_seen=False))
 def test_no_real_abort(self):self.reject(lambda v:v.update(actual_observer_abort=False))
 def test_catalogue_equivalence_claim(self):self.reject(lambda v:v.update(IANA_ZoneInfo_catalogue_equivalent=True))
 def test_main_claim(self):self.reject(lambda v:v.update(Main_runs=1))
 def test_dummy_owner_claim(self):self.reject(lambda v:v.update(dummy_owner=True))
 def test_wrong_case(self):self.reject(lambda v:v.update(case=2))
 def test_cache_addition(self):self.reject(lambda v:v['raw_cache_after']['raw_DUMP_PTTL'].update({'00':{'dump_hex':'','pttl':-1}}))
if __name__=='__main__':unittest.main(verbosity=2)
