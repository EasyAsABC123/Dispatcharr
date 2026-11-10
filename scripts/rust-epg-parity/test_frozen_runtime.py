"""Runtime reuse must reject traversal, duplicate members and altered manifests."""
import io
import json
from pathlib import Path
import tarfile
import tempfile
import unittest
from frozen_runtime import create_manifest, load_manifest, read_archive


class RuntimeArchiveTests(unittest.TestCase):
    def archive(self, root, extra=()):
        path = root/'runtime.tar'
        with tarfile.open(path, 'w') as archive:
            for name, data in [('rust/Cargo.toml', b'fixture'), ('rust/Cargo.lock', b'lock'), *extra]:
                entry = tarfile.TarInfo(name)
                entry.size = len(data)
                archive.addfile(entry, io.BytesIO(data))
        return path

    def test_valid_archive_is_bound_to_both_digests(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            archive = self.archive(root)
            manifest = root/'runtime.json'
            create_manifest(archive, manifest)
            captured, _, returned, _ = load_manifest(manifest)
            self.assertEqual(returned, archive)
            self.assertEqual(captured['rust/Cargo.lock'], b'lock')

    def test_traversal_and_duplicate_paths_are_rejected(self):
        for name in ['../outside', 'rust/../outside', 'rust/./inside', 'rust/Cargo.toml']:
            with self.subTest(name=name), tempfile.TemporaryDirectory() as directory:
                archive = self.archive(Path(directory), [(name, b'bad')])
                with self.assertRaises(ValueError):
                    read_archive(archive)

    def test_changed_archive_is_rejected_before_reuse(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            archive = self.archive(root)
            manifest = root/'runtime.json'
            create_manifest(archive, manifest)
            with archive.open('ab') as target:
                target.write(b'altered')
            with self.assertRaises(ValueError):
                load_manifest(manifest)

    def test_changed_source_map_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            manifest = root/'runtime.json'
            value = create_manifest(self.archive(root), manifest)
            value['source_sha256']['rust/Cargo.lock'] = '0'*64
            manifest.write_text(json.dumps(value))
            with self.assertRaises(ValueError):
                load_manifest(manifest)


if __name__ == '__main__':
    unittest.main()
