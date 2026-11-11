"""Reconstruction controls; no archive/build is read until test invocation."""
import importlib.util
import io
from pathlib import Path
import tarfile
import tempfile
import unittest
from unittest.mock import patch
spec=importlib.util.spec_from_file_location('_core8_reconstruction_tested',Path(__file__).with_name('reconstruct.py'))
r=importlib.util.module_from_spec(spec);spec.loader.exec_module(r)
class ReconstructionControls(unittest.TestCase):
    def test_immutable_source_exact(self):
        with tempfile.TemporaryDirectory() as directory:
            self.assertEqual(r.reconstruct(Path.cwd(),Path(directory)/'source')['benchmark_map'],r.BENCHMARK)
    def test_nonempty_preserved(self):
        with tempfile.TemporaryDirectory() as directory:
            marker=Path(directory)/'retain';marker.write_text('preserve')
            with self.assertRaises(ValueError):r.reconstruct(Path.cwd(),Path(directory))
            self.assertEqual(marker.read_text(),'preserve')
    def test_unsafe_archive(self):
        for name in ['../escape','/absolute','rust/../../escape']:
            raw=io.BytesIO()
            with tarfile.open(fileobj=raw,mode='w') as archive:archive.addfile(tarfile.TarInfo(name))
            with tempfile.TemporaryDirectory() as directory:
                with self.assertRaises(ValueError):r.extract(raw.getvalue(),Path(directory))
    def test_changed_production_map_refused(self):
        with tempfile.TemporaryDirectory() as directory,patch.object(r,'regular_map',return_value={}):
            with self.assertRaises(ValueError):r.reconstruct(Path.cwd(),Path(directory)/'source')
    def test_changed_overlay_refused(self):
        with tempfile.TemporaryDirectory() as directory,patch.object(r,'overlay_bytes',return_value=b'corrupted overlay'):
            with self.assertRaises(ValueError):r.reconstruct(Path.cwd(),Path(directory)/'source')
if __name__=='__main__':unittest.main()
