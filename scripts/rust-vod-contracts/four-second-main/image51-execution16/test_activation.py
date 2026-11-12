"""Pure execution-entry controls. execute/candidate execution are mocked.
Temporary copies of exact sealed inputs only; never create the real Root cue,
claim/output, import Django, run Docker or contact any Source/native endpoint.
"""
import copy,datetime,hashlib,importlib.util,json,shutil,sys,tempfile,time,types,unittest
from pathlib import Path
from unittest import mock
HERE=Path(__file__).resolve().parent
REPO=HERE.parents[2]
BASE=REPO/'artifacts/rust/m4-vod-four-second-main-bound-15'
REAL_MANIFEST=REPO/'artifacts/rust/candidate-build-observer-unlock-image51-1/build.json'
REAL_ARCHIVE=REAL_MANIFEST.parent/'source.tar'
SPEC=importlib.util.spec_from_file_location('activation16_test_entry',HERE/'tools/run.py');M=importlib.util.module_from_spec(SPEC);SPEC.loader.exec_module(M)
class Controls(unittest.TestCase):
 def setUp(self):
  self.tmp=tempfile.TemporaryDirectory();self.repo=Path(self.tmp.name);self.base=self.repo/'artifacts/rust/m4-vod-four-second-main-bound-15';self.execution=self.repo/'execution';shutil.copytree(BASE,self.base,ignore=shutil.ignore_patterns('__pycache__'));shutil.copytree(HERE,self.execution,ignore=shutil.ignore_patterns('__pycache__','test_activation.py','activation-controls*.log'));self.build=self.repo/'artifacts/rust/candidate-build-observer-unlock-image51-1';self.build.mkdir(parents=True);(self.build/'build.json').symlink_to(REAL_MANIFEST);(self.build/'source.tar').symlink_to(REAL_ARCHIVE);self.cuepath=self.repo/'artifacts/rust/m4-vod-four-second-image51-execution-cue-1.json';self.claim=self.cuepath.with_name(self.cuepath.stem+'.claimed.json');self.out=self.repo/'artifacts/rust/m4-vod-four-second-main-image51-1';self.now=time.time();issued=self.now-1;expires=self.now+100
  self.cue={'authorized':True,'action':'canonical51-physical-main-four-second-once','issued_epoch':issued,'expires_epoch':expires,'boot_id':Path('/proc/sys/kernel/random/boot_id').read_text().strip(),'reviewed_selection':'artifacts/rust/m4-vod-four-second-main-bound-15/selection.json','reviewed_selection_sha256':'40a8f027e4b64dddb59148e503b1331c3a96a2f284dcec4442159e393a5c569f','reviewed_policy_sha256':'c4440e6d23627cb218fba8208463c93495d2eced4bc1af4077aaf338e4c92472','reviewed_binding_sha256':'b7d2f3567bf503cbedf57e264549ae1c6cba18afab1f72c5bced2fcd8a6b189f','build_manifest':'artifacts/rust/candidate-build-observer-unlock-image51-1/build.json','build_manifest_sha256':'2dcf19e84324aa77d56a8df034729dd3d8a07ba7743d09d8f3fd7ebc9ca89957','candidate_image':'sha256:909d506f1c1deb29c60bd360da995eef9d5ff5c7ffa0ea3799bca11f7c93b5d0','candidate_tag':'dispatcharr-rust-app:observer-unlock-image51','source_tree_sha256':'fee70ac06a5a6aead2ba3b589d9939a85576e59798421473e80fb8056a7f1082','git_revision':'5afbb78ee8be031474b41278479fcdf623f2ddce','source_archive':'artifacts/rust/candidate-build-observer-unlock-image51-1/source.tar','source_archive_sha256':'4680d81f09fc1c4b956920cfd14a0d9b7e680f3e92d121a0fb271213a6a36d57','Root_input_count':772,'Root_reviewed_input_map_sha256':'8e685a88c94ef7a680bb2d8d56ed916411589b921f670b55cd50e2cb779d60ce','cases':['success','busy','clientloss'],'new_output':'artifacts/rust/m4-vod-four-second-main-image51-1','source_native_application_requests_authorized':True,'resource_sampling':False,'performance_acceptance':False,'full_M4_acceptance':False,'reviewed_activation_sha256':hashlib.sha256((self.execution/'activation.json').read_bytes()).hexdigest()};self.utc();self.proof={'image_id':self.cue['candidate_image'],'source_tree_sha256':self.cue['source_tree_sha256'],'git_revision':self.cue['git_revision']}
 def tearDown(self):self.tmp.cleanup()
 def utc(self):
  for n in['issued','expires']:
   value=self.cue[n+'_epoch']
   if type(value)in[int,float]and value==value and abs(value)<1e12:self.cue[n+'_utc']=datetime.datetime.fromtimestamp(value,datetime.timezone.utc).isoformat().replace('+00:00','Z')
 def write(self):self.cuepath.write_text(json.dumps(self.cue))
 def invoke(self,argv_change=None,times=None,dispatch=None):
  args=['run.py','--repo',str(self.repo),'--candidate-image',self.cue['candidate_tag'],'--build-manifest',str(self.build/'build.json'),'--source-archive',str(self.build/'source.tar'),'--output',str(self.out)]
  if argv_change:
   for key,value in argv_change.items():args[args.index(key)+1]=value
  with mock.patch.object(M,'HERE',self.execution/'tools'),mock.patch.object(M,'__file__',str(self.execution/'tools/run.py')),mock.patch.object(sys,'argv',args),mock.patch.object(M.time,'time',side_effect=times if times is not None else None,return_value=self.now),mock.patch.object(M,'load',return_value=types.SimpleNamespace(bound_candidate=lambda *a:self.proof)),mock.patch.object(M,'execute',dispatch or mock.Mock(return_value=19))as execute:
   result=M.main();return result,execute
 def rejected(self):
  with self.assertRaises((ValueError,FileNotFoundError)):self.invoke()
  self.assertFalse(self.claim.exists());self.assertFalse(self.out.exists())
 def test_missing_cue_never_claims_or_dispatches(self):self.rejected()
 def test_exact_valid_cue_claims_before_dispatch_and_cannot_replay(self):
  self.write()
  def dispatch(*args):self.assertTrue(self.claim.exists());self.assertFalse(self.out.exists());return 19
  result,_=self.invoke(dispatch=dispatch);self.assertEqual(result,19);self.assertTrue(json.loads(self.claim.read_text())['no_retry'])
  with self.assertRaisesRegex(ValueError,'AlreadyClaimed'):self.invoke()
 def test_expired_future_nonfinite_or_boolean_epoch_rejected(self):
  for changes in[{'issued_epoch':self.now-200,'expires_epoch':self.now-1},{'issued_epoch':self.now+1},{'issued_epoch':self.now-2000,'expires_epoch':self.now+1},{'issued_epoch':True},{'issued_epoch':float('nan')},{'expires_epoch':self.now+2000}]:
   original=copy.deepcopy(self.cue);self.cue.update(changes);self.utc();self.write();self.rejected();self.cue=original
 def test_UTC_text_and_epoch_must_agree(self):
  for value in['missingZ','2020-01-01T00:00:00Z',None]:
   self.cue['issued_utc']=value;self.write();self.rejected()
 def test_boot_action_scope_and_exact772_rejected(self):
  for key,value in[('boot_id','other'),('action','other'),('authorized',1),('resource_sampling',True),('performance_acceptance',True),('full_M4_acceptance',True),('cases',['success']),('Root_input_count',772.0),('new_output','other')]:
   old=self.cue[key];self.cue[key]=value;self.write();self.rejected();self.cue[key]=old
 def test_all_declared_hash_identity_bindings_rejected_if_changed(self):
  for key in['reviewed_selection_sha256','reviewed_policy_sha256','reviewed_binding_sha256','build_manifest_sha256','candidate_image','candidate_tag','source_tree_sha256','git_revision','source_archive_sha256','Root_reviewed_input_map_sha256']:
   old=self.cue[key];self.cue[key]='wrong';self.write();self.rejected();self.cue[key]=old
 def test_reviewed_bound_input_corruption_rejected(self):
  f=self.base/'tools/observation.py';f.write_text(f.read_text()+'\n# corrupt\n');self.write();self.rejected()
 def test_execution_input_corruption_rejected(self):
  f=self.execution/'tools/observation.py';f.write_text(f.read_text()+'\n# corrupt\n');self.write();self.rejected()
 def test_only_entry_activation_may_differ(self):
  f=self.execution/'tools/run.py';f.write_text(f.read_text()+'\n# unreviewed extra change\n');self.write();self.rejected()
 def test_activation_seal_corruption_rejected(self):
  self.write();(self.execution/'activation.json').write_text('{}');self.rejected()
 def test_existing_output_is_never_retried(self):
  self.out.mkdir();self.write()
  with self.assertRaisesRegex(ValueError,'FreshCanonical51Output'):self.invoke()
  self.assertFalse(self.claim.exists())
 def test_wrong_CLI_candidate_is_not_rebound(self):
  self.write()
  with self.assertRaisesRegex(ValueError,'ImageBinding'):self.invoke({'--candidate-image':'other'})
  self.assertFalse(self.claim.exists())
 def test_wrong_archive_bytes_rejected(self):
  f=self.build/'source.tar';f.unlink();f.write_bytes(b'wrong');self.write();self.rejected()
 def test_runtime_proof_mismatch_rejected_before_claim(self):
  self.proof['source_tree_sha256']='other';self.write();self.rejected()
 def test_expiry_during_input_checks_cannot_claim(self):
  self.write()
  with self.assertRaisesRegex(ValueError,'ExpiredBeforeClaim'):self.invoke(times=[self.now,self.cue['expires_epoch']+1])
  self.assertFalse(self.claim.exists())
if __name__=='__main__':unittest.main(verbosity=2)
