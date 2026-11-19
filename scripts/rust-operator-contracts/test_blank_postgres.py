"""Offline tests that also run from the documented minimal Git export."""
import io
import json
from pathlib import Path
import tarfile
from tempfile import TemporaryDirectory
import unittest

import run_blank_postgres as fixture


class BlankPostgres(unittest.TestCase):
    def test_fixture_reuses_storage_and_retains_only_runtime_secret_placeholders(self):
        self.assertTrue(fixture.BASE.is_file())
        images = [{"image": "sha256:" + part * 64} for part in ("a", "b")]
        overlay = fixture.composition(*images)
        services = overlay["services"]
        self.assertEqual(set(services), {"database", "cache", "before", "after"})
        self.assertTrue(overlay["networks"]["default"]["internal"])
        self.assertNotIn("image", services["database"])
        self.assertNotIn("image", services["cache"])
        self.assertNotIn("ports", services["before"])
        for role in ("before", "after"):
            self.assertNotIn("DATABASE_URL", services[role]["environment"])
            for name in fixture.CLIENT_NAMES:
                self.assertEqual(services[role]["environment"][name], "${FIXTURE_CLIENT_" + name + "?}")
            self.assertEqual(services[role]["environment"]["POSTGRES_PASSWORD"], "${OPERATOR_FIXTURE_POSTGRES_PASSWORD:?}")
        self.assertEqual(services["cache"]["environment"]["REDISCLI_AUTH"], "${OPERATOR_FIXTURE_REDIS_PASSWORD:?}")

    def test_only_client_values_vary_between_discrete_cases(self):
        url = object()
        cases = fixture.scenarios(url)
        self.assertEqual(len(cases), 9)
        for name, side, expected, changes in cases:
            with self.subTest(name=name):
                self.assertTrue(side in ("before", "after"))
                self.assertEqual(expected, not name.startswith("before-blank"))
                self.assertTrue(all(key.startswith("FIXTURE_CLIENT_") or key == "DATABASE_URL" for key in changes))
                if name == "after-explicit-wins":
                    self.assertIs(changes["DATABASE_URL"], url)
                    self.assertEqual(changes["FIXTURE_CLIENT_POSTGRES_PORT"], "1")
                    self.assertNotEqual(changes["FIXTURE_CLIENT_POSTGRES_USER"], "dispatch")
                    self.assertNotEqual(changes["FIXTURE_CLIENT_POSTGRES_DB"], "dispatcharr")
                else:
                    self.assertNotIn("DATABASE_URL", changes)
                    self.assertTrue(all(value == "" for value in changes.values()))
        self.assertEqual(len(cases[-2][3]), 3)

    def test_actual_archive_validator_and_revision_binding(self):
        with TemporaryDirectory(prefix="dispatcharr-exported-operator-test.") as directory:
            root = Path(directory)
            archive = root / "source.tar"
            manifest = root / "build.json"
            revision = "a" * 40
            fields = {
                "LICENSE": b"test license",
                "ATTRIBUTION.md": b"test Original authorship",
                "rust/Dockerfile.app": b"test container",
                "rust/Cargo.toml": b"test Cargo workspace",
                "frontend/package.json": b"{}",
                "frontend/package-lock.json": b"{}",
                fixture.CONFIG: b"test database configuration",
                fixture.SCHEMA: b"test schema",
            }
            with tarfile.open(archive, "w") as output:
                for name, data in fields.items():
                    item = tarfile.TarInfo(name)
                    item.size = len(data)
                    output.addfile(item, io.BytesIO(data))
            hashes = {name: fixture.digest(data) for name, data in fields.items()}
            tree = fixture.digest(json.dumps(hashes, sort_keys=True, separators=(",", ":")).encode())
            manifest.write_text(json.dumps({
                "format_version": 1, "passed": True, "git_revision": revision, "exit_code": 0,
                "source_input_mode": "committed-revision-snapshot", "runtime_packages_frozen": True,
                "image_id": "sha256:" + "b"*64, "source_tree_sha256": tree, "source_sha256": hashes,
                "source_archive": archive.name, "source_archive_sha256": fixture.digest(archive.read_bytes()),
            }))
            package, schema = fixture.package(manifest, revision, hashes[fixture.CONFIG])
            self.assertEqual(package["source_tree"], tree)
            self.assertEqual(schema, fields[fixture.SCHEMA])
            with self.assertRaises(ValueError):
                fixture.package(manifest, "c"*40, hashes[fixture.CONFIG])
            with self.assertRaises(ValueError):
                fixture.package(manifest, revision, "c"*64)
            with archive.open("ab") as writable:
                writable.write(b"corruption")
            with self.assertRaisesRegex(ValueError, "archive byte digest"):
                fixture.package(manifest, revision, hashes[fixture.CONFIG])


if __name__ == "__main__":
    unittest.main()
