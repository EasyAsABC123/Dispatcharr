"""Semantic corruption controls after resealing fixture bytes; no SDK execution."""
import hashlib,importlib.util,json,shutil,tempfile,unittest
from pathlib import Path
HERE=Path(__file__).parent;s=importlib.util.spec_from_file_location('sdk_evidence',HERE/'validate.py');m=importlib.util.module_from_spec(s);s.loader.exec_module(m)
class Controls(unittest.TestCase):
 def reject(self,name,change):
  with tempfile.TemporaryDirectory()as t:
   root=Path(t);shutil.copytree(HERE/'raw',root/'raw');shutil.copytree(HERE/'inputs',root/'inputs');shutil.copytree(HERE/'executed-tools',root/'executed-tools');shutil.copy2(HERE/'raw-manifest.json',root/'raw-manifest.json');p=root/'raw'/name;v=json.loads(p.read_text());change(v);p.write_text(json.dumps(v));manifest=json.loads((root/'raw-manifest.json').read_text());manifest['raw/'+name]={'bytes':p.stat().st_size,'sha256':hashlib.sha256(p.read_bytes()).hexdigest()};(root/'raw-manifest.json').write_text(json.dumps(manifest))
   with self.assertRaises((AssertionError,ValueError,KeyError)):m.validate(root)
 def test_positive(self):self.assertTrue(m.validate(HERE)['passed'])
 def test_wrong_ref(self):self.reject('verification.json',lambda v:v.update(revision='0'*40))
 def test_nonzero_actual_process(self):self.reject('actual-process-exit.json',lambda v:v.update(returncode=1))
 def test_false_success(self):self.reject('verification.json',lambda v:v.update(passed=False))
 def test_changed_Source(self):self.reject('all-source-after.json',lambda v:v.update({'rust/Cargo.toml':'0'*64}))
 def test_missing_copied_hash(self):self.reject('independent-image-copy-hashes.json',lambda v:v['files'].pop(next(iter(v['files']))))
 def test_wrong_copied_hash(self):self.reject('independent-image-copy-hashes.json',lambda v:v['files'].update({'rust/Cargo.toml':'0'*64}))
 def test_extra_missing_copied_file(self):self.reject('independent-image-copy-hashes.json',lambda v:v['missing'].append('rust/Cargo.toml'))
 def test_relabel_aux_false(self):self.reject('independent-image-copy-hashes.json',lambda v:v.update(passed=True))
 def test_wrong_network(self):self.reject('independent-image-configuration.json',lambda v:v.update(runtime_network_mode='bridge'))
 def test_host_Cargo_target(self):self.reject('independent-image-configuration.json',lambda v:v.update(Cargo_target='/workspace/rust/target'))
 def test_unclean_project(self):self.reject('independent-review.json',lambda v:v['actual_project_inventories']['containers'].append('leftover'))
 def test_wrong_cleanup_exit(self):self.reject('verification.json',lambda v:v.update(cleanup_exit_code=1))
if __name__=='__main__':unittest.main()
