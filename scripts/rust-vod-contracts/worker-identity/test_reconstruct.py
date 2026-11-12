"""Public metadata/payload corruption controls; no private fallback or app work."""
import json
from pathlib import Path
import shutil
import tempfile
import unittest
from unittest.mock import patch
import reconstruct as r

class Controls(unittest.TestCase):
    def test_exact_payload(self):
        base, final, _, patches = r.prepare(r.HERE)
        self.assertEqual((len(base),len(final),len(patches)),(788,789,5))
    def test_bad_paths(self):
        for value in ['../rust/x','/rust/x','rust/../x','rust//x','rust/./x','scripts/x']:
            with self.subTest(value=value), self.assertRaises(ValueError):r.relative(value)
    def test_fresh_output_before_git(self):
        with tempfile.TemporaryDirectory() as directory, patch.object(r.subprocess,'check_output',side_effect=AssertionError('must not access Git')):
            with self.assertRaises(ValueError):r.reconstruct('/unused',directory)
    def test_manifest_and_payload_corruption(self):
        for name in ['runtime-selection.json','base-inputs.json','inputs.json','archive-binding.json','selected/rust/crates/vod-http/src/worker_identity.rs']:
            with self.subTest(name=name), tempfile.TemporaryDirectory() as directory:
                root=Path(directory)/'component';shutil.copytree(r.HERE,root)
                p=root/name;p.write_bytes(p.read_bytes()+b' ')
                with self.assertRaises(ValueError):r.prepare(root)
    def test_payload_symlink(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)/'component';shutil.copytree(r.HERE,root)
            p=root/'selected/rust/crates/vod-http/src/worker_identity.rs';data=p.read_bytes();q=root/'renamed';q.write_bytes(data);p.unlink();p.symlink_to(q)
            with self.assertRaises(ValueError):r.prepare(root)
    def test_no_fallback_on_missing_payload(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)/'component';shutil.copytree(r.HERE,root)
            (root/'selected/rust/crates/vod-http/src/worker_identity.rs').unlink()
            with self.assertRaises(ValueError):r.prepare(root)

if __name__=='__main__':unittest.main(verbosity=2)
