"""Validate immutable grid replay archives before any extraction or Docker use."""
import importlib.util
import io
from pathlib import Path
import tarfile
import tempfile
import unittest

spec = importlib.util.spec_from_file_location('control_reads', Path(__file__).with_name('control_reads.py'))
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


class GridArchive(unittest.TestCase):
    def replay(self, entries):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            archive = root/'source.tar'
            with tarfile.open(archive, 'w') as target:
                for name, contents, symlink in entries:
                    member = tarfile.TarInfo(name)
                    member.size = len(contents)
                    if symlink:
                        member.type = tarfile.SYMTYPE
                        member.linkname = '/tmp/escape'
                    target.addfile(member, None if symlink else io.BytesIO(contents))
            output = root/'output'
            output.mkdir()
            frozen, hashes = module.freeze_current_rust(output, 'grid', archive)
            self.assertEqual((output/'grid-rust-source.tar').read_bytes(), archive.read_bytes())
            self.assertEqual((frozen/'rust/Cargo.toml').read_bytes(), b'manifest')
            self.assertEqual(set(hashes), {'rust/Cargo.toml', 'rust/Cargo.lock'})

    def test_valid(self):
        self.replay([('rust/Cargo.toml', b'manifest', False), ('rust/Cargo.lock', b'lock', False)])

    def test_reject_traversal(self):
        with self.assertRaises(ValueError):
            self.replay([('rust/../escape', b'x', False)])

    def test_reject_absolute(self):
        with self.assertRaises(ValueError):
            self.replay([('/tmp/escape', b'x', False)])

    def test_reject_symlink(self):
        with self.assertRaises(ValueError):
            self.replay([('rust/link', b'', True)])

    def test_reject_duplicate(self):
        with self.assertRaises(ValueError):
            self.replay([('rust/Cargo.toml', b'x', False), ('rust/Cargo.toml', b'y', False)])

    def test_reject_missing_lock(self):
        with self.assertRaises(ValueError):
            self.replay([('rust/Cargo.toml', b'manifest', False)])


if __name__ == '__main__':
    unittest.main()
