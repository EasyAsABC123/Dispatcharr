"""Actual exact reconstruction and corruption controls; no Cargo/Docker calls."""
import importlib.util,json,tempfile,unittest
from pathlib import Path
spec=importlib.util.spec_from_file_location('_cleanup_expiry_frozen_builder',Path(__file__).with_name('reconstruct.py'));f=importlib.util.module_from_spec(spec);spec.loader.exec_module(f)
class Freeze(unittest.TestCase):
 def test_exact711(self):
  with tempfile.TemporaryDirectory()as d:self.assertEqual(len(f.freeze(Path.cwd(),Path(d)/'source')['files']),711)
 def test_preserve_existing(self):
  with tempfile.TemporaryDirectory()as d:
   marker=Path(d)/'keep';marker.write_text('keep')
   with self.assertRaises(FileExistsError):f.freeze(Path.cwd(),Path(d))
   self.assertEqual(marker.read_text(),'keep')
 def bad(self,mutate):
  record=json.loads(Path(__file__).with_name('source-overlays.json').read_text());mutate(record)
  with tempfile.TemporaryDirectory()as d:
   manifest=Path(d)/'bad.json';manifest.write_text(json.dumps(record))
   with self.assertRaises(ValueError):f.freeze(Path.cwd(),Path(d)/'source',manifest)
 def test_changed_base(self):self.bad(lambda r:r.update(base_rust_archive_sha256='0'*64))
 def test_missing_overlay(self):self.bad(lambda r:r['overlays'].pop(next(iter(r['overlays']))))
 def test_changed_blob_bytes(self):self.bad(lambda r:next(iter(r['overlays'].values())).update(sha256='0'*64))
 def test_changed_full_map(self):self.bad(lambda r:r['expected_full_file_map'].update({'rust/Cargo.lock':'0'*64}))
 def test_traversal(self):
  for p in ['../escape','/absolute','rust/crates/job-transport/../../app/evil.rs','rust\\evil.rs']:
   with self.assertRaises(ValueError):f.owned(p)
if __name__=='__main__':unittest.main(verbosity=2)
