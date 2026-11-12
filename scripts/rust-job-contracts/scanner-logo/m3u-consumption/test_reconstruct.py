"""Offline exact788 reconstruction controls; no Cargo/Docker/network."""
import argparse,importlib.util,json,tempfile,unittest,subprocess
from pathlib import Path
p=argparse.ArgumentParser();p.add_argument('--repo',type=Path,required=True);a=p.parse_args();REPO=a.repo.resolve();HERE=Path(__file__).parent;spec=importlib.util.spec_from_file_location('_scanner_helper_reconstruction',HERE/'reconstruct.py');mod=importlib.util.module_from_spec(spec);spec.loader.exec_module(mod);ORIGINAL=json.loads((HERE/'source-overlays.json').read_text())
class Controls(unittest.TestCase):
 def attempt(self,change):
  with tempfile.TemporaryDirectory(prefix='rust-owned-logo-reconstruct-')as temporary:
   root=Path(temporary);m=json.loads(json.dumps(ORIGINAL));change(m);manifest=root/'manifest.json';manifest.write_text(json.dumps(m));
   with self.assertRaises((ValueError,subprocess.CalledProcessError)):mod.freeze(REPO,root/'source',manifest)
 def test_exact_success(self):
  with tempfile.TemporaryDirectory(prefix='rust-owned-logo-exact-')as temporary:self.assertEqual(len(mod.freeze(REPO,Path(temporary)/'source')['files']),788)
 def test_base_option_injection_rejected(self):self.attempt(lambda m:m.update(base='--help'))
 def test_base_changed(self):self.attempt(lambda m:m['base_file_map'].update({'rust/Cargo.toml':'0'*64}))
 def test_missing_overlay(self):self.attempt(lambda m:m['overlays'].pop(next(iter(m['overlays']))))
 def test_overlap_changed(self):self.attempt(lambda m:m['overlays']['rust/crates/job-transport/src/lib.rs'].update(before_sha256='0'*64))
 def test_blob_digest_changed(self):self.attempt(lambda m:m['overlays'][next(iter(m['overlays']))].update(sha256='0'*64))
 def test_invalid_blob_identifier(self):self.attempt(lambda m:m['overlays'][next(iter(m['overlays']))].update(git_blob='--help'))
 def test_final_map_changed(self):self.attempt(lambda m:m['expected_full_file_map'].update({'rust/Cargo.toml':'0'*64}))
 def test_unowned_overlay(self):
  def change(m):m['overlays']['rust/crates/app/src/main.rs']=m['overlays'].pop(next(iter(m['overlays'])))
  self.attempt(change)
 def test_unsafe_overlay(self):
  def change(m):m['overlays']['../escape.rs']=m['overlays'].pop(next(iter(m['overlays'])))
  self.attempt(change)
 def test_negative_payload_size_rejected(self):self.attempt(lambda m:m['overlays'][next(iter(m['overlays']))].update(bytes=-1))
 def test_named_corpus_size_mismatch_rejected(self):
  def change(m):
   item=m['overlays'][next(iter(m['overlays']))];item['bytes']+=1
  self.attempt(change)
 def test_existing_output_preserved(self):
  with tempfile.TemporaryDirectory(prefix='rust-owned-logo-existing-')as temporary:
   root=Path(temporary);sentinel=root/'sentinel';sentinel.write_text('preserve')
   with self.assertRaises(FileExistsError):mod.freeze(REPO,root)
   self.assertEqual(sentinel.read_text(),'preserve')
if __name__=='__main__':unittest.main(argv=['test_reconstruct'])
