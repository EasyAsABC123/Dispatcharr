"""Engine-free controls for grouped Programme Index package binding."""
from hashlib import sha256
from io import BytesIO
import json
from pathlib import Path
import subprocess
import tarfile
from tempfile import TemporaryDirectory
import unittest

import programme_index_owner as owner

REPO = Path(__file__).resolve().parents[3]


def digest(path):
    return sha256(path.read_bytes()).hexdigest()


class ProductionBinding(unittest.TestCase):
    def test_requires_git_ancestry_manifest_and_exact_archive_leaves(self):
        with TemporaryDirectory() as folder:
            root = Path(folder)
            archive = root / "source.tar"
            manifest = root / "build.json"
            image = "sha256:" + "a" * 64
            hashes = owner.PRODUCTION_LEAVES.copy()

            def write_package(changed=None, duplicate=False, platform="linux/amd64"):
                with tarfile.open(archive, "w") as stream:
                    for name in hashes:
                        body = subprocess.check_output(
                            ["/usr/bin/git", "-C", str(REPO), "show", owner.PRODUCTION_BASE + ":" + name])
                        if name == changed:
                            body += b"\n"
                        member = tarfile.TarInfo(name)
                        member.size = len(body)
                        stream.addfile(member, BytesIO(body))
                        if duplicate and name == next(iter(hashes)):
                            stream.addfile(member, BytesIO(body))
                manifest.write_text(json.dumps({
                    "git_revision": owner.PRODUCTION_BASE, "image_id": image, "platform": platform,
                    "source_sha256": hashes, "source_archive_sha256": digest(archive),
                }))
                return {"git": owner.PRODUCTION_BASE, "image_id": image, "build_manifest_file": str(manifest),
                        "build_manifest_sha256": digest(manifest), "source_archive_sha256": digest(archive)}

            exact = write_package()
            self.assertEqual(set(owner.production_binding(REPO, exact)), {str(manifest), str(archive)})
            with self.assertRaisesRegex(ValueError, "reviewed production revision"):
                owner.production_binding(REPO, exact | {"git": "0" * 40})
            with self.assertRaisesRegex(ValueError, "exact common final AMD64"):
                owner.production_binding(REPO, write_package(platform="linux/arm64"))
            with self.assertRaisesRegex(ValueError, "archived production leaf drifted"):
                owner.production_binding(REPO, write_package(changed=next(iter(hashes))))
            with self.assertRaisesRegex(ValueError, "unique regular production leaf"):
                owner.production_binding(REPO, write_package(duplicate=True))


if __name__ == "__main__":
    unittest.main()
