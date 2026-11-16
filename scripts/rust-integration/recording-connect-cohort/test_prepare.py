"""Pure admission/cleanup controls; no Docker, application or task execution."""
import hashlib,importlib.util,json,tempfile,unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
HERE=Path(__file__).resolve().parent
spec=importlib.util.spec_from_file_location('portable_prepare_controls',HERE/'prepare.py');tool=importlib.util.module_from_spec(spec);spec.loader.exec_module(tool)
class AdmissionControls(unittest.TestCase):
 def plan(self,root):
  repo=root/'repo';(repo/'scripts/rust-milestone-batches').mkdir(parents=True);coordinator=repo/'scripts/rust-milestone-batches/coordinator.py';coordinator.write_text('raise RuntimeError("must not execute")\n');out=root/'run';out.mkdir();build=root/'build.json';image='sha256:'+'a'*64
  tool.save(build,{'passed':True,'exit_code':0,'image_id':image,'git_revision':'b'*40,'source_tree_sha256':'c'*64})
  base={'candidate_build':str(build),'candidate_image_id':image,'candidate_git_revision':'b'*40,'candidate_source_tree_sha256':'c'*64,'fixture_output':str(out/'fixtures'),'inputs':[{'path':str(coordinator),'sha256':tool.sha(coordinator)},{'path':str(build),'sha256':tool.sha(build)}]}
  plan=out/'plan.json';tool.save(plan,{'repo':str(repo),'output':str(out),'base_binding':base,'Docker_host':None});return repo,out,plan,coordinator
 def test_changed_closure_rejected_before_import_or_uid(self):
  with tempfile.TemporaryDirectory()as tmp:
   repo,out,plan,coordinator=self.plan(Path(tmp));coordinator.write_text('changed\n')
   with patch.object(tool,'module')as imported,patch.object(tool,'uid_probe')as uid:
    with self.assertRaises(AssertionError):tool.run(SimpleNamespace(plan=plan))
    imported.assert_not_called();uid.assert_not_called()
 def test_changed_candidate_identity_rejected_before_import_or_uid(self):
  with tempfile.TemporaryDirectory()as tmp:
   repo,out,plan,coordinator=self.plan(Path(tmp));p=tool.load(plan);p['base_binding']['candidate_image_id']='sha256:'+'d'*64;tool.save(plan,p)
   with patch.object(tool,'module')as imported,patch.object(tool,'uid_probe')as uid:
    with self.assertRaises(AssertionError):tool.run(SimpleNamespace(plan=plan))
    imported.assert_not_called();uid.assert_not_called()
 def test_unknown_uid_cleanup_leaves_blocking_owned_marker(self):
  with tempfile.TemporaryDirectory()as tmp:
   repo,out,plan,coordinator=self.plan(Path(tmp))
   def failed_uid(*unused):
    tool.save(out/'uid/identity.json',{'owned_probe_name':'owned-unit-control','CID_file':str(out/'uid/probe.cid')});raise RuntimeError('controlled UID failure')
   with patch.object(tool,'module',return_value=object()),patch.object(tool,'uid_probe',side_effect=failed_uid):
    with self.assertRaises(RuntimeError):tool.run(SimpleNamespace(plan=plan))
   marker=tool.load(repo/'.dispatcharr-recording-cohort-cleanup-required.json');self.assertEqual(marker['stage'],'UID probe');self.assertEqual(marker['identity']['owned_probe_name'],'owned-unit-control');self.assertEqual(marker['cleanup'],{});self.assertFalse(tool.load(out/'report.json')['selected_pair_passed'])
 def test_failed_uid_with_proven_absence_does_not_fake_uncertain_cleanup(self):
  with tempfile.TemporaryDirectory()as tmp:
   repo,out,plan,coordinator=self.plan(Path(tmp))
   def failed_uid(*unused):tool.save(out/'uid/cleanup.json',{'exact_CID_absent':True});raise AssertionError('controlled hash mismatch')
   with patch.object(tool,'module',return_value=object()),patch.object(tool,'uid_probe',side_effect=failed_uid):
    with self.assertRaises(AssertionError):tool.run(SimpleNamespace(plan=plan))
   self.assertFalse((repo/'.dispatcharr-recording-cohort-cleanup-required.json').exists());self.assertFalse(tool.load(out/'report.json')['selected_pair_passed'])
if __name__=='__main__':unittest.main()
