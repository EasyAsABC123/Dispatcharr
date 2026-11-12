"""Pure ownership/nonce/cue/preflight corruption controls; no Source activity."""
import copy,datetime,hashlib,importlib.util,json,os,tempfile,time,unittest
from pathlib import Path
from grants import preserved
import normal_guard
class Controls(unittest.TestCase):
 def grant(self):return{'owner':'owned-token','owner_confirm':'owned-token','ledger':{'token':'owned-token','owner':'rust:coordination:vod:session:fixture-normal-other:owner','profile':'profile_connections:2','reserved':'1','fence':'7'},'metadata':{'content_uuid':'other','rust_owner_token':'owned-token','active_streams':'1'}}
 def test_other_grant_positive_fence_and_no_none_equality(self):
  before=self.grant();self.assertTrue(preserved(before,copy.deepcopy(before),'profile_connections:2','other')['preserved'])
  for value in[None,'0','-1','bad','9223372036854775808',True]:
   bad=self.grant();bad['ledger']['fence']=value
   with self.assertRaises(ValueError):preserved(bad,copy.deepcopy(bad),'profile_connections:2','other')
 def test_owner_change_is_not_counter_equality(self):
  before=self.grant();after=self.grant();after['owner']='foreign'
  with self.assertRaises(ValueError):preserved(before,after,'profile_connections:2','other')
 def test_strict_cue_time_boot_hash_and_boolean_controls(self):
  with tempfile.TemporaryDirectory()as directory:
   root=Path(directory);(root/'policy.json').write_text('{}');digest=normal_guard.digest;(root/'selection.json').write_text(json.dumps({'files':[{'path':'policy.json','sha256':digest(root/'policy.json')}]}));now=time.time();cue={'authorized':True,'action':normal_guard.ACTION,'backends':['source','native'],'repetitions':1,'resource_sampling':False,'performance_acceptance':False,'full_M4_acceptance':False,'boot_id':Path('/proc/sys/kernel/random/boot_id').read_text().strip(),'issued_epoch':now-1,'expires_epoch':now+100,'selection_sha256':digest(root/'selection.json'),'policy_sha256':digest(root/'policy.json')}
   for name in['issued','expires']:cue[name+'_utc']=datetime.datetime.fromtimestamp(cue[name+'_epoch'],datetime.timezone.utc).isoformat().replace('+00:00','Z')
   self.assertEqual(normal_guard.validate(cue,root),cue)
   for key,value in[('authorized',False),('repetitions',True),('boot_id','other'),('expires_epoch',float('nan')),('selection_sha256','0'*64),('issued_utc','yesterday')]:
    with self.assertRaises(ValueError):normal_guard.validate({**cue,key:value},root)
 def test_actual_fixture_prefix_guard_and_corruption(self):
  import run as normal_run
  source=Path(__file__).resolve().parent.parent/'fixture-preflight/qualification.json'
  self.assertTrue(normal_run.fixture_guard(source)['used_tool_subset_exact'])
  original=json.loads(source.read_text())
  for key,value in[('retained_bytes',8192),('frames',49),('retained_sha256','0'*64)]:
   record=copy.deepcopy(original);record['proof']['decoder'][key]=value
   with tempfile.TemporaryDirectory()as directory:
    path=Path(directory)/'proof';path.write_text(json.dumps(record))
    with self.assertRaises(ValueError):normal_run.fixture_guard(path)
 def test_full_pair_entry_blocks_before_preflight_candidate_or_source(self):
  import run as normal_run,sys
  from unittest import mock
  arguments=['run','--repo','/tmp','--candidate-image','unbound','--build-manifest','absent','--source-archive','absent','--fixture-preflight','absent','--cue','absent','--output','absent']
  with mock.patch.object(sys,'argv',arguments),mock.patch.object(normal_run,'execute')as execute:
   with self.assertRaisesRegex(RuntimeError,'RootReviewFreshPairCueRequired'):normal_run.main()
   execute.assert_not_called()
 def test_phase_and_Source_wsgi_block_before_Django(self):
  import runpy
  from unittest import mock
  for name,guard in[('phase.py','ReviewedNormalPairRequired'),('source_wsgi.py','RootCueRequiredForNormalSource')]:
   # Source_wsgi imports gevent first; use AST static guard in host-only tests.
   text=(Path(__file__).with_name(name)).read_text();self.assertIn(guard,text)
  with mock.patch.dict(os.environ,{'NORMAL_REVIEWED_EXECUTION':''}):
   with self.assertRaisesRegex(RuntimeError,'ReviewedNormalPairRequired'):runpy.run_path(str(Path(__file__).with_name('phase.py')))
if __name__=='__main__':unittest.main(verbosity=2)
