"""Public-only attestation controls; private capture paths are never consulted."""
import json
from pathlib import Path
import shutil
import tempfile
import unittest
from verify_source_accounting import verify

ROOT=Path(__file__).resolve().parents[2]

class PublicAttestationControls(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.root=Path(self.tmp.name);self.path=self.root/'scripts/rust-playback-baseline/verified-source-accounting-1.json'
        self.path.parent.mkdir(parents=True);self.path.write_bytes((ROOT/self.path.relative_to(self.root)).read_bytes())
        for item in json.loads(self.path.read_text())['runtime_files']:
            target=self.root/item['path'];target.parent.mkdir(parents=True,exist_ok=True);shutil.copyfile(ROOT/item['path'],target)
    def mutate(self,fn):
        d=json.loads(self.path.read_text());fn(d);self.path.write_text(json.dumps(d))
    def test_public_only_no_private_artifacts_required(self):
        self.assertFalse((self.root/'artifacts').exists());verify(self.root)
    def test_runtime_tampering_rejected(self):
        (self.root/'scripts/rust-playback-baseline/run.py').write_text('tampered')
        with self.assertRaises(AssertionError):verify(self.root)
    def test_resource_acceptance_invention_rejected(self):
        self.mutate(lambda d:d.update(resource_acceptance=True))
        with self.assertRaises(AssertionError):verify(self.root)
    def test_missing_fault_case_rejected(self):
        self.mutate(lambda d:d['functional_cases'].pop())
        with self.assertRaises(AssertionError):verify(self.root)
    def test_hidden_reader_error_rejected(self):
        self.mutate(lambda d:d['many']['summary'].update(errors=1))
        with self.assertRaises(AssertionError):verify(self.root)

if __name__=='__main__':unittest.main()
