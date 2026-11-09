import copy
import importlib.util
import json
from pathlib import Path
import shutil
import tempfile
import unittest

spec=importlib.util.spec_from_file_location('integration_runner',Path(__file__).with_name('run.py'))
runner=importlib.util.module_from_spec(spec)
spec.loader.exec_module(runner)

class IntegrationRunnerTests(unittest.TestCase):
    def test_keep_preserves_source_mount_and_records_cleanup_path(self):
        with tempfile.TemporaryDirectory() as output:
            report={'passed':True}
            with runner.fixture_source(report,True,Path(output)) as temporary:
                source=Path(temporary)/'source';source.mkdir()
                (source/'fixture').write_text('fixture')
            try:
                self.assertTrue(source.is_dir())
                self.assertTrue(report['resources_retained'])
                saved=json.loads((Path(output)/'integration.json').read_text())
                self.assertEqual(saved['retained_source_directory'],str(source))
            finally:
                shutil.rmtree(temporary)

    def test_failed_container_cleanup_keeps_source_without_keep_flag(self):
        with tempfile.TemporaryDirectory() as output:
            report={'passed':False,'cleanup_error':'TimeoutExpired'}
            with runner.fixture_source(report,False,Path(output)) as temporary:
                (Path(temporary)/'source').mkdir()
            try:
                self.assertTrue(Path(temporary).is_dir())
                self.assertTrue(report['resources_retained'])
            finally:
                shutil.rmtree(temporary)

    def test_successful_disposable_run_removes_source(self):
        with tempfile.TemporaryDirectory() as output:
            report={'passed':True}
            with runner.fixture_source(report,False,Path(output)) as temporary:
                (Path(temporary)/'source').mkdir()
            self.assertFalse(Path(temporary).exists())
            self.assertFalse(report['resources_retained'])

    def test_comparison_is_json_type_strict(self):
        self.assertNotEqual(runner.digest({'x':True}),runner.digest({'x':1}))
        self.assertNotEqual(runner.digest({'x':1}),runner.digest({'x':1.0}))

    def test_image_binding_rejects_stale_or_unbound_candidates(self):
        manifest={'passed':True,'image_id':'sha256:expected','source_tree_sha256':'sourcehash'}
        inspection={'Image':'sha256:expected','Config':{'Labels':{'org.opencontainers.image.source-tree-sha256':'sourcehash'}}}
        self.assertTrue(runner.validate_candidate_image(manifest,inspection)['manifest_verified'])
        stale=copy.deepcopy(inspection);stale['Image']='sha256:old'
        with self.assertRaises(ValueError):runner.validate_candidate_image(manifest,stale)
        unbound=copy.deepcopy(inspection);unbound['Config']['Labels']={}
        with self.assertRaises(ValueError):runner.validate_candidate_image(manifest,unbound)

    def test_overlay_preserves_config_and_rejects_yaml_injection(self):
        with tempfile.TemporaryDirectory() as temporary:
            source=Path(temporary)/'original.yml';destination=Path(temporary)/'candidate.yml'
            source.write_text('services:\n  candidate:\n    image: dispatcharr-rust-candidate:milestone2\n    read_only: true\n')
            runner.candidate_overlay(source,destination,'dispatcharr-rust-candidate:test-unique')
            self.assertIn('read_only: true',destination.read_text())
            self.assertIn('image: dispatcharr-rust-candidate:test-unique',destination.read_text())
            with self.assertRaises(ValueError):runner.candidate_overlay(source,destination,'bad\nservices: unsafe')

if __name__=='__main__':unittest.main()
