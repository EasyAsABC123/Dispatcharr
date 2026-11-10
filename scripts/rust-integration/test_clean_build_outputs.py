"""Isolated filesystem controls for generated-output cleanup."""
import contextlib
import importlib.util
import io
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location('cleanup', Path(__file__).with_name('clean_build_outputs.py'))
cleanup = importlib.util.module_from_spec(spec)
spec.loader.exec_module(cleanup)

class CleanupTests(unittest.TestCase):
    def test_completed_only_and_active_protection(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            artifacts = root / 'artifacts/rust'
            completed = artifacts / 'finished/target'
            active = artifacts / 'running/cargo-target'
            for target in (completed, active):
                (target / 'debug').mkdir(parents=True)
                (target / '.rustc_info.json').write_text('{}')
                (target / 'debug/test').write_bytes(b'ELF evidence')
            source = artifacts / 'finished/source.rs'
            source.write_text('preserved source')
            report = artifacts / 'finished/proof.json'
            report.write_text(json.dumps({'test_binary': str(completed / 'debug/test')}))
            output = artifacts / 'cleanup'
            with patch.object(cleanup, 'ROOT', root), patch.object(cleanup, 'ARTIFACTS', artifacts), patch.object(cleanup, 'process_paths', return_value=([], [active])), patch.object(cleanup, 'docker_mounts', return_value=[]), patch.object(cleanup, 'working_files', return_value={}), patch.object(cleanup.subprocess, 'check_output', return_value=''), patch.object(sys, 'argv', ['cleanup', '--output', str(output), '--execute']), contextlib.redirect_stdout(io.StringIO()):
                cleanup.main()
            self.assertFalse(completed.exists())
            self.assertTrue(active.exists())
            self.assertEqual(source.read_text(), 'preserved source')
            self.assertTrue(report.exists())
            manifest = json.loads((output / 'manifest.json').read_text())
            self.assertEqual(manifest['deleted_targets'], 1)
            saved = root / manifest['preserved_target_evidence'][0]['preserved_at'][0]
            self.assertEqual(saved.read_bytes(), b'ELF evidence')
            self.assertEqual(len((output / 'deletions.jsonl').read_text().splitlines()), 1)

    def test_source_or_symlink_is_not_output(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp)
            (path / 'debug').mkdir()
            (path / '.rustc_info.json').write_text('{}')
            self.assertTrue(cleanup.cargo_output(path))
            (path / 'Cargo.toml').write_text('[package]')
            self.assertFalse(cleanup.cargo_output(path))
            link = path / 'alias'
            link.symlink_to(path, target_is_directory=True)
            self.assertFalse(cleanup.cargo_output(link))

    def test_new_docker_mount_blocks_deletion_and_unavailable_inventory_aborts(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            artifacts = root / 'artifacts/rust'
            target = artifacts / 'finished/target'
            (target / 'debug').mkdir(parents=True)
            (target / '.rustc_info.json').write_text('{}')
            output = artifacts / 'cleanup'
            with patch.object(cleanup, 'ROOT', root), patch.object(cleanup, 'ARTIFACTS', artifacts), patch.object(cleanup, 'process_paths', return_value=([], [])), patch.object(cleanup, 'working_files', return_value={}), patch.object(cleanup.subprocess, 'check_output', return_value=''), patch.object(sys, 'argv', ['cleanup', '--output', str(output), '--execute']), contextlib.redirect_stdout(io.StringIO()):
                with patch.object(cleanup, 'docker_mounts', side_effect=[[], [target / 'debug/peer']]):
                    cleanup.main()
                self.assertTrue(target.exists())
                self.assertEqual(json.loads((output / 'manifest.json').read_text())['deleted_targets'], 0)
                with patch.object(sys, 'argv', ['cleanup', '--output', str(artifacts / 'unavailable'), '--execute']), patch.object(cleanup, 'docker_mounts', side_effect=OSError('Docker unavailable')):
                    with self.assertRaises(OSError):
                        cleanup.main()
                self.assertTrue(target.exists())

    def test_output_inside_target_is_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            artifacts = root / 'artifacts/rust'
            target = artifacts / 'finished/target'
            (target / 'debug').mkdir(parents=True)
            (target / '.rustc_info.json').write_text('{}')
            with patch.object(cleanup, 'ROOT', root), patch.object(cleanup, 'ARTIFACTS', artifacts), patch.object(cleanup, 'process_paths', return_value=([], [])), patch.object(cleanup, 'docker_mounts', return_value=[]), patch.object(cleanup, 'working_files', return_value={}), patch.object(sys, 'argv', ['cleanup', '--output', str(target / 'evidence'), '--execute']), contextlib.redirect_stderr(io.StringIO()):
                with self.assertRaises(SystemExit):
                    cleanup.main()
            self.assertTrue(target.exists())

    def test_cargo_target_argument_and_unknown_default_are_protected(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            proc = root / 'proc/123'
            proc.mkdir(parents=True)
            executable = root / 'toolchain/cargo'
            executable.parent.mkdir()
            executable.touch()
            (proc / 'exe').symlink_to(executable)
            (proc / 'cwd').symlink_to(root)
            (proc / 'comm').write_text('cargo')
            (proc / 'environ').write_bytes(b'')
            def path(value):
                return root / 'proc' if value == '/proc' else Path(value)
            with patch.object(cleanup, 'ROOT', root), patch.object(cleanup, 'ARTIFACTS', root / 'artifacts/rust'), patch.object(cleanup, 'Path', side_effect=path):
                (proc / 'cmdline').write_bytes(b'cargo\0test\0--target-dir\0custom-output\0')
                _, protected = cleanup.process_paths()
                self.assertIn(root / 'custom-output', protected)
                (proc / 'cmdline').write_bytes(b'cargo\0test\0')
                _, protected = cleanup.process_paths()
                self.assertIn(root / 'artifacts/rust', protected)
                self.assertIn(root / 'rust/target', protected)

    def test_docker_inventory_collects_only_live_bind_sources(self):
        with tempfile.TemporaryDirectory() as tmp:
            target = Path(tmp) / 'target'
            mounts = [{'Type': 'bind', 'Source': str(target)}, {'Type': 'volume', 'Source': '/docker/storage'}]
            with patch.object(cleanup.subprocess, 'check_output', side_effect=['container-id\n', json.dumps(mounts) + '\n']):
                self.assertEqual(cleanup.docker_mounts(), [target.resolve()])

if __name__ == '__main__':
    unittest.main()
