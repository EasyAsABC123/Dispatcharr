"""Fresh public-only directory controls; no Source or Docker execution."""
import shutil,tempfile,unittest
from pathlib import Path
import validate_public
class Public(unittest.TestCase):
 def setUp(self):
  self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup);self.root=Path(self.temp.name)/'public';shutil.copytree(Path(__file__).parent,self.root,ignore=shutil.ignore_patterns('__pycache__'))
 def test_static_fresh_public_only(self):self.assertTrue(validate_public.validate(self.root)['passed'])
 def test_source_negative_cannot_disappear(self):
  (self.root/'verified-source-observer-setup-negative-1.json').unlink()
  with self.assertRaises(FileNotFoundError):validate_public.validate(self.root)
 def test_observer_positive_cannot_be_silently_changed(self):
  p=self.root/'verified-source-observer-contract-2.json';p.write_bytes(p.read_bytes()+b'\n')
  with self.assertRaisesRegex(ValueError,'PublicEvidenceHash'):validate_public.validate(self.root)
 def test_full_entry_cannot_be_activated_in_public_packet(self):
  p=self.root/'controller/run.py';p.write_text(p.read_text().replace('RESOURCE_EXECUTION_BLOCKED=True','RESOURCE_EXECUTION_BLOCKED=False'))
  with self.assertRaisesRegex(ValueError,'Controller36Bytes'):validate_public.validate(self.root)
 def test_missing_copied_runtime_fails_instead_of_private_fallback(self):
  (self.root/'controller/observer.py').unlink()
  with self.assertRaisesRegex(ValueError,'Controller36Bytes'):validate_public.validate(self.root)
if __name__=='__main__':unittest.main()
