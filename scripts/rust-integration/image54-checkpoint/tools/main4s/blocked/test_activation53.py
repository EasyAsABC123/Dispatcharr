"""Local-only fixture/cue guards: dispatch mocked, no Docker/Source/native requests."""
import copy,datetime,hashlib,importlib.util,json,shutil,sys,tempfile,time,unittest
from pathlib import Path
from unittest import mock
HERE=Path(__file__).resolve().parent
spec=importlib.util.spec_from_file_location('launch52_test',HERE/'launch_once.py');M=importlib.util.module_from_spec(spec);spec.loader.exec_module(M)
ROOT=Path('/home/justin/github/Dispatcharr')
class Controls(unittest.TestCase):
 def setUp(self):
  self.temp=tempfile.TemporaryDirectory();self.repo=Path(self.temp.name);self.here=self.repo/'controller';shutil.copytree(HERE,self.here,ignore=shutil.ignore_patterns('__pycache__'));self.plan=json.loads((self.here/'execution-plan.json').read_text());self.binding=json.loads((self.here/'candidate-binding.json').read_text());self.now=time.time();self.cue=json.loads((self.here/'cue-template.json').read_text());self.cue.update(authorized=True,source_native_application_requests_authorized=True,issued_epoch=self.now-1,expires_epoch=self.now+100,boot_id=Path('/proc/sys/kernel/random/boot_id').read_text().strip());self.utc()
  for key,rel in [('selection_sha256','selection.json'),('plan_sha256','execution-plan.json'),('policy_sha256','policy.json'),('binding_sha256','candidate-binding.json')]:self.cue[key]=M.digest(self.here/rel)
  for rel in [self.binding['build_manifest'],self.binding['source_archive']]:
   p=self.repo/rel;p.parent.mkdir(parents=True,exist_ok=True);p.symlink_to(ROOT/rel)
  self.preflight=self.repo/self.plan['Root_quiet_preflight'];self.preflight.parent.mkdir(parents=True,exist_ok=True);self.preflight.write_text(json.dumps({'passed':True,'boot_id':self.cue['boot_id'],'checked_epoch':self.now-2}));self.cue['Root_quiet_preflight_sha256']=M.digest(self.preflight)
  self.proof={'image_id':self.binding['candidate_image'],'git_revision':self.binding['git_revision'],'source_tree_sha256':self.binding['source_tree_sha256']}
 def tearDown(self):self.temp.cleanup()
 def utc(self):
  for name in ['issued','expires']:
   value=self.cue[name+'_epoch']
   if type(value)in[int,float]and value==value and abs(value)<1e12:self.cue[name+'_utc']=datetime.datetime.fromtimestamp(value,datetime.timezone.utc).isoformat().replace('+00:00','Z')
 def check(self):
  with mock.patch.object(M,'guard_candidate',return_value=self.proof):return M.validate(self.repo,self.here,self.cue,self.now)
 def fail(self):
  with self.assertRaises((ValueError,FileNotFoundError)):self.check()
  self.assertFalse((self.repo/self.plan['new_output']).exists())
 def test_exact_cue_read_only_validation(self):self.check();self.assertFalse((self.repo/self.plan['new_output']).exists())
 def test_unconditional_launcher_block_before_any_cue_read(self):
  with mock.patch.object(M,'dispatch')as d:
   with self.assertRaisesRegex(RuntimeError,'ROOT_ACTIVATION'):M.main()
   d.assert_not_called()
 def test_underlying_protocol_entry_unconditionally_blocked(self):
  spec=importlib.util.spec_from_file_location('blocked52',self.here/'tools/run.py');m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
  with mock.patch.object(sys,'argv',['run.py','--repo',str(self.repo),'--candidate-image','none','--build-manifest','none','--source-archive','none','--output','none']),mock.patch.object(m,'execute')as d:
   with self.assertRaisesRegex(RuntimeError,'EXECUTION_BLOCKED'):m.main()
   d.assert_not_called()
 def test_scope_action_and_explicit_authorization(self):
  for key,value in [('authorized',1),('source_native_application_requests_authorized',False),('action','old51'),('cases',['success']),('resource_sampling',True),('performance_acceptance',True),('full_M4_acceptance',True),('Root_reviewed_Rust_input_count',792.0),('new_output','old')]:
   old=self.cue[key];self.cue[key]=value;self.fail();self.cue[key]=old
 def test_expired_future_nan_bool_and_overlong_cues(self):
  for changes in [{'expires_epoch':self.now-1},{'issued_epoch':self.now+1},{'issued_epoch':float('nan')},{'issued_epoch':True},{'expires_epoch':self.now+2000}]:
   old=copy.deepcopy(self.cue);self.cue.update(changes);self.utc();self.fail();self.cue=old
 def test_UTC_epoch_and_boot(self):
  for key,value in [('issued_utc','2020-01-01T00:00:00Z'),('expires_utc','bad'),('boot_id','wrong')]:
   old=self.cue[key];self.cue[key]=value;self.fail();self.cue[key]=old
 def test_every_binding_and_seal_mismatch(self):
  for key in ['selection_sha256','policy_sha256','binding_sha256','plan_sha256','build_manifest_sha256','candidate_tag','candidate_image','git_revision','source_tree_sha256','source_archive_sha256','Root_reviewed_input_map_sha256']:
   old=self.cue[key];self.cue[key]='wrong';self.fail();self.cue[key]=old
 def test_missing_corrupt_stale_quiet_preflight(self):
  raw=self.preflight.read_bytes();self.preflight.unlink();self.fail();self.preflight.write_bytes(raw);self.preflight.write_text('{}');self.fail();self.cue['Root_quiet_preflight_sha256']=M.digest(self.preflight);self.fail()
 def test_input_and_nonflag_launcher_edits_reject(self):
  for rel in ['tools/phase.py','launch_once.py']:
   p=self.here/rel;raw=p.read_bytes();p.write_bytes(raw+b'\n#unreviewed\n');self.fail();p.write_bytes(raw)
 def test_only_flag_activation_preserves_seal(self):
  p=self.here/'launch_once.py';p.write_text(p.read_text().replace('\nEXECUTION_BLOCKED=True\n','\nEXECUTION_BLOCKED=False\n'));self.check()
 def test_existing_output_and_symlink_output_rejected(self):
  p=self.repo/self.plan['new_output'];p.mkdir()
  with self.assertRaises(ValueError):self.check()
  self.assertTrue(p.is_dir());p.rmdir();p.symlink_to(self.repo/'missing')
  with self.assertRaises(ValueError):self.check()
  self.assertTrue(p.is_symlink())
 def test_runtime_proof_mismatch(self):self.proof['image_id']='wrong';self.fail()
 def test_claim_before_dispatch_and_cannot_replay(self):
  cuepath=self.repo/self.plan['cue_path'];cuepath.write_text(json.dumps(self.cue))
  def dispatch(*args):self.assertTrue(cuepath.with_name(cuepath.stem+'.claimed.json').exists());self.assertFalse((self.repo/self.plan['new_output']).exists());return 19
  with mock.patch.object(M,'EXECUTION_BLOCKED',False),mock.patch.object(M,'HERE',self.here),mock.patch.object(sys,'argv',['launch','--repo',str(self.repo)]),mock.patch.object(M,'guard_candidate',return_value=self.proof),mock.patch.object(M.time,'time',return_value=self.now),mock.patch.object(M,'dispatch',side_effect=dispatch):
   self.assertEqual(M.main(),19)
   with self.assertRaisesRegex(ValueError,'AlreadyClaimed'):M.main()
 def test_expiry_during_readonly_checks_never_claims(self):
  path=self.repo/self.plan['cue_path'];path.write_text(json.dumps(self.cue))
  with mock.patch.object(M,'EXECUTION_BLOCKED',False),mock.patch.object(M,'HERE',self.here),mock.patch.object(sys,'argv',['launch','--repo',str(self.repo)]),mock.patch.object(M,'guard_candidate',return_value=self.proof),mock.patch.object(M.time,'time',side_effect=[self.now,self.now+200]),mock.patch.object(M,'dispatch')as d:
   with self.assertRaisesRegex(ValueError,'ExpiredBeforeClaim'):M.main()
   d.assert_not_called();self.assertFalse(path.with_name(path.stem+'.claimed.json').exists())
 def test_real_image54_exact792_archive_set_and_scripts(self):
  p=M.guard_candidate(HERE,ROOT,self.binding);self.assertEqual(len(p['runtime_sha256']),792);self.assertEqual(p['reserve_script_sha1'],self.binding['reserve_script_sha1'])
 def test_changed_runtime_map_and_added_extra_fail_closed(self):
  p=self.here/'tools/expected-runtime.json';v=json.loads(p.read_text());v['rust/extra.rs']='a'*64;p.write_text(json.dumps(v));
  with self.assertRaises(ValueError):M.guard_candidate(self.here,ROOT,self.binding)
if __name__=='__main__':unittest.main(verbosity=2)
