import hashlib
from pathlib import Path
import tempfile
import unittest
import tool_snapshot


class ToolSnapshotTests(unittest.TestCase):
    def fixture(self,root):
        for name in tool_snapshot.DIRECTORIES:
            path=root/name;path.mkdir(parents=True)
            (path/'sample.py').write_text('original fixture bytes')
            (path/'__pycache__').mkdir();(path/'__pycache__/sample.pyc').write_bytes(b'bytecode')
        for name in tool_snapshot.COMPOSE:
            path=root/name;path.parent.mkdir(parents=True,exist_ok=True);path.write_text('services: {}')

    def test_captured_bytes_and_hashes_survive_later_repository_edits(self):
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary)/'repo';root.mkdir();self.fixture(root)
            destination=Path(temporary)/'snapshot'
            hashes=tool_snapshot.snapshot_sources(root,destination)
            relative='scripts/rust-account-contracts/sample.py'
            (root/relative).write_text('later unrelated edit')
            self.assertEqual((destination/relative).read_text(),'original fixture bytes')
            self.assertEqual(hashes[relative],hashlib.sha256((destination/relative).read_bytes()).hexdigest())
            self.assertFalse(any('__pycache__' in path for path in hashes))
            self.assertEqual(len(hashes),7)

    def test_symlink_sources_are_rejected(self):
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary)/'repo';root.mkdir();self.fixture(root)
            (root/'scripts/rust-account-contracts/link.py').symlink_to('/etc/hostname')
            with self.assertRaises(ValueError):tool_snapshot.snapshot_sources(root,Path(temporary)/'snapshot')

if __name__=='__main__':unittest.main()
