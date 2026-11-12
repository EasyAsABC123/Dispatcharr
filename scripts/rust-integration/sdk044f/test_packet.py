"""Offline semantic corruption controls after resealing evidence bytes."""
import hashlib,importlib.util,json,shutil,tempfile,unittest
from pathlib import Path
HERE=Path(__file__).parent;spec=importlib.util.spec_from_file_location('sdk044f_evidence',HERE/'validate.py');m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
class Controls(unittest.TestCase):
 def reject(self,name,change,lines=False):
  with tempfile.TemporaryDirectory()as t:
   root=Path(t)
   for folder in ['raw','inputs','executed-tools']:shutil.copytree(HERE/folder,root/folder)
   shutil.copyfile(HERE/'raw-manifest.json',root/'raw-manifest.json');p=root/'raw'/name
   value=[json.loads(s)for s in p.read_text().splitlines()]if lines else json.loads(p.read_text());change(value);p.write_text(''.join(json.dumps(v)+'\n'for v in value)if lines else json.dumps(value));manifest=json.loads((root/'raw-manifest.json').read_text());manifest['raw/'+name]={'bytes':p.stat().st_size,'sha256':hashlib.sha256(p.read_bytes()).hexdigest()};(root/'raw-manifest.json').write_text(json.dumps(manifest))
   with self.assertRaises((ValueError,KeyError,AssertionError)):m.validate(root)
 def test_positive(self):self.assertTrue(m.validate(HERE)['passed'])
 def test_wrong_revision(self):self.reject('verification.json',lambda v:v.update(revision='0'*40))
 def test_host_nonzero(self):self.reject('actual-process-exit.json',lambda v:v.update(returncode=1))
 def test_host_not_waited(self):self.reject('actual-process-exit.json',lambda v:v.update(wait_completed=False))
 def test_false_success(self):self.reject('verification.json',lambda v:v.update(passed=False))
 def test_false_application_scope(self):self.reject('verification.json',lambda v:v.update(scope='application-parity'))
 def test_changed_source_after(self):self.reject('all-source-after.json',lambda v:v.update({'rust/Cargo.toml':'0'*64}))
 def test_missing_selected_input(self):self.reject('verifier-selected-inputs.json',lambda v:v.pop(next(iter(v))))
 def test_changed_copied_input(self):self.reject('independent-image-copy-hashes.json',lambda v:v['files'].update({'rust/Cargo.toml':'0'*64}))
 def test_missing_copied_input(self):self.reject('independent-image-copy-hashes.json',lambda v:v['files'].pop(next(iter(v['files']))))
 def test_relabel_auxiliary_positive(self):self.reject('independent-image-copy-hashes.json',lambda v:v.update(passed=False))
 def test_host_files_misclassified_copy(self):self.reject('SDK-copy-closure.json',lambda v:v.update({'docker/compose.rust.yml':'0'*64}))
 def test_host_control_wrong_hash(self):self.reject('host-only-controls.json',lambda v:v.update({'docker/compose.rust.yml':'0'*64}))
 def test_private_HEAD_wrong(self):self.reject('private-git-context-final.json',lambda v:v.update(HEAD='0'*40))
 def test_network_not_none(self):self.reject('independent-container-inspect.json',lambda v:v.update(network_mode='bridge'))
 def test_host_target_in_image(self):
  def f(v):v[0]['Config']['Env']=[x.replace('/tmp/dispatcharr-target','/workspace/rust/target')for x in v[0]['Config']['Env']]
  self.reject('actual-SDK-image-full-inspect.json',f)
 def test_wrong_image_identity(self):self.reject('verification.json',lambda v:v.update(image_ids=['0'*64]))
 def test_container_nonzero_die(self):
  def f(v):next(e for e in v if e.get('Action')=='die')['Actor']['Attributes']['exitCode']='1'
  self.reject('Docker-events.jsonl',f,True)
 def test_cleanup_exit_nonzero(self):self.reject('verification.json',lambda v:v.update(cleanup_exit_code=1))
 def test_leftover_project(self):self.reject('independent-cleanup-query-receipt.json',lambda v:v['queries'][0].update(stdout='leftover\n'))
 def test_host_PID_still_present(self):self.reject('independent-cleanup-query-receipt.json',lambda v:v.update(SDK_host_PID_proc_present=True))
 def test_root_negative_erased(self):self.reject('Root-review/Root-review.json',lambda v:v.update(cleanup_review_negative=''))
 def test_root_negative_relabels_retry(self):self.reject('Root-cleanup-negative-description.json',lambda v:v.update(SDK_workload_retry=True))
 def test_helper_artifact_not_application_scope(self):self.reject('ts-helper-comparison.json',lambda v:v.update(scope='application-performance'))
 def test_timeout_override(self):self.reject('plan.json',lambda v:v['command'].extend(['--timeout','1']))
 def test_dictionary_and_serialization_distinct(self):
  a=(HERE/'raw/rust-inputs792.json').read_bytes();b=(HERE/'raw/QA2-rust792-map.json').read_bytes();self.assertEqual(json.loads(a),json.loads(b));self.assertNotEqual(hashlib.sha256(a).hexdigest(),hashlib.sha256(b).hexdigest())
if __name__=='__main__':unittest.main()
