"""Pure reconstruction/corruption controls; no Cargo/Docker/application execution."""
import copy,os,subprocess,tempfile,unittest
from pathlib import Path
from unittest import mock
import reconstruct as r

class Controls(unittest.TestCase):
 @classmethod
 def setUpClass(cls):
  cls.m=r.manifest();cls.patch=(r.HERE/'selected-source.patch').read_bytes();cls.repo=Path(os.environ['RECONSTRUCTION_REPO']).resolve(strict=True);cls.files={}
  for row in cls.m['selected_overlay']:
   if row['base_sha256']is not None:cls.files[row['path']]=subprocess.check_output(['git','-C',str(cls.repo),'cat-file','blob',r.BASE+':'+row['path']])
 def test_exact_base_derived_from_final767_and_three_ancestor_hashes(self):
  expected=r.base_expected(self.m);self.assertEqual(len(expected),766);self.assertNotIn('rust/crates/vod-http/src/generation_idle_observer.rs',expected)
  for row in self.m['selected_overlay']:
   if row['base_sha256']is not None:self.assertEqual(expected[row['path']],row['base_sha256'])
 def test_manifest_corruption_refuses_before_any_Git_or_output(self):
  with tempfile.TemporaryDirectory()as directory:
   root=Path(directory);(root/'reconstruction-manifest.json').write_text('{}')
   with mock.patch.object(r,'HERE',root),mock.patch.object(r.subprocess,'check_output')as git:
    with self.assertRaisesRegex(ValueError,'SealedManifestMismatch'):r.reconstruct(self.repo,root/'fresh')
    git.assert_not_called();self.assertFalse((root/'fresh').exists())
 def test_patch_corruption_refuses_before_any_Git_or_output(self):
  with tempfile.TemporaryDirectory()as directory:
   root=Path(directory);(root/'reconstruction-manifest.json').write_bytes((r.HERE/'reconstruction-manifest.json').read_bytes());(root/'selected-source.patch').write_bytes(self.patch+b'corrupt')
   with mock.patch.object(r,'HERE',root),mock.patch.object(r.subprocess,'check_output')as git:
    with self.assertRaisesRegex(ValueError,'SealedPatchMismatch'):r.reconstruct(self.repo,root/'fresh')
    git.assert_not_called();self.assertFalse((root/'fresh').exists())
 def test_exact_three_overlay_posthashes(self):
  final=r.overlay(self.files,self.patch,self.m['selected_overlay']);self.assertEqual(set(final),{x['path']for x in self.m['selected_overlay']})
  for row in self.m['selected_overlay']:self.assertEqual(r.sha(final[row['path']]),row['sha256'])
 def test_unselected_patch_path_and_truncation_rejected(self):
  bad=self.patch.replace(b'--- 208598ee/rust/crates/vod-http/src/generation_idle.rs',b'--- 208598ee/rust/unselected',1)
  for patch in[bad,self.patch[:-50],self.patch+self.patch]:
   with self.assertRaises(ValueError):r.overlay(self.files,patch,self.m['selected_overlay'])
 def test_old_context_corruption_rejected(self):
  files=dict(self.files);name=self.m['selected_overlay'][0]['path'];files[name]=b'corrupt\n'
  with self.assertRaises(ValueError):r.overlay(files,self.patch,self.m['selected_overlay'])
 def test_missing_extra_or_corrupt_base_final_and_coupled_files_rejected(self):
  expected={'owned':r.sha(b'correct')}
  for label in['Base766','Final767','CoupledFive']:
   for files in[{}, {'owned':b'incorrect'},{'owned':b'correct','extra':b'x'}]:
    with self.assertRaises(ValueError):r.verify_files(files,expected,label)
 def test_existing_output_rejected_without_Git_mutation(self):
  with tempfile.TemporaryDirectory()as directory,mock.patch.object(r.subprocess,'check_output')as git:
   with self.assertRaisesRegex(ValueError,'FreshNonGitOutputRequired'):r.reconstruct(self.repo,Path(directory))
   git.assert_not_called()
if __name__=='__main__':unittest.main(verbosity=2)
