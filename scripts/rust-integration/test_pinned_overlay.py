"""Concrete immutable builder overlays must remain usable by fixture adapters."""
import importlib.util
from pathlib import Path
import sys
import tempfile
import unittest
sys.path.insert(0,str(Path(__file__).parent))
spec=importlib.util.spec_from_file_location('overlay_run',Path(__file__).with_name('run.py'));module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
class OverlayTests(unittest.TestCase):
    def test_template_and_already_bound_overlay_preserve_requested_image(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);source=root/'source.yml';target=root/'target.yml';pinned=root/'pinned.yml'
            source.write_text('services:\n  candidate:\n    image: dispatcharr-rust-candidate:milestone2\n')
            module.candidate_overlay(source,target,'dispatcharr-rust-candidate:image13')
            module.candidate_overlay(target,pinned,'dispatcharr-rust-candidate:image13')
            self.assertEqual(target.read_text(),pinned.read_text())
            with self.assertRaises(ValueError):module.candidate_overlay(target,pinned,'dispatcharr-rust-candidate:image14')
    def test_prefix_is_not_an_exact_image_match(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);source=root/'source.yml';source.write_text('image: dispatcharr-rust-candidate:image13-extra\n')
            with self.assertRaises(ValueError):module.candidate_overlay(source,root/'out.yml','dispatcharr-rust-candidate:image13')
if __name__=='__main__':unittest.main()
