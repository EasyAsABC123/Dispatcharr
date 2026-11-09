"""Ensure contract trials execute captured bytes despite concurrent edits."""
import hashlib
import importlib.util
from pathlib import Path
import tempfile
import unittest
from vod_tool_snapshot import snapshot_sources,DIRECTORIES,COMPOSE


class ToolSnapshot(unittest.TestCase):
    def fixture(self,root):
        for directory in DIRECTORIES:(root/directory).mkdir(parents=True)
        for name in COMPOSE:
            path=root/name;path.parent.mkdir(exist_ok=True);path.write_text('services: {}\n')

    def test_copied_probe_is_executed_and_mode_survives_later_edit(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)/'root';root.mkdir();self.fixture(root)
            probe=root/'scripts/rust-timeshift-contracts/probe.py'
            probe.write_text('VALUE = 13\n');probe.chmod(0o755)
            ignored=probe.parent/'__pycache__';ignored.mkdir();(ignored/'cache.pyc').write_bytes(b'cache')
            snapshot=Path(directory)/'frozen';hashes=snapshot_sources(root,snapshot)
            captured=snapshot/probe.relative_to(root)
            probe.write_text('VALUE = 99\n')
            spec=importlib.util.spec_from_file_location('copied_probe_test',captured)
            module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
            self.assertEqual(module.VALUE,13)
            self.assertEqual(hashes[str(probe.relative_to(root))],hashlib.sha256(captured.read_bytes()).hexdigest())
            self.assertEqual(captured.stat().st_mode&0o777,0o755)
            self.assertFalse((snapshot/ignored.relative_to(root)/'cache.pyc').exists())

    def test_external_symlink_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)/'root';root.mkdir();self.fixture(root)
            outside=Path(directory)/'outside';outside.write_text('must not copy')
            (root/'scripts/rust-vod-contracts/leak.py').symlink_to(outside)
            with self.assertRaises(ValueError):snapshot_sources(root,Path(directory)/'frozen')

    def test_reusing_destination_cannot_silently_replace_evidence(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)/'root';root.mkdir();self.fixture(root)
            target=Path(directory)/'frozen';snapshot_sources(root,target)
            with self.assertRaises(FileExistsError):snapshot_sources(root,target)
