"""Offline authority, cold rebinding and refusal controls for the ordinary scheduler."""
import json
import os
from pathlib import Path
import shutil
import subprocess
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

import scheduler_install_owner as owner

TEMPLATE = os.environ.get("M6_REPLAY_SCHEDULER")
ARTIFACTS = os.environ.get("M6_REPLAY_ARTIFACTS")


@unittest.skipUnless(TEMPLATE and ARTIFACTS, "set scheduler stage and artifact root for replay")
class SchedulerInstall(unittest.TestCase):
    def setUp(self):
        self.stage = Path(TEMPLATE).resolve()
        self.source = Path(ARTIFACTS).resolve() / owner.ORIGINAL_REL
        self.seal = (self.stage / "workload-seal.sha256").read_text().strip()
        binding = owner.read(self.stage / "package-binding.json")
        manifest = next(path for path in binding["pins"] if path.endswith("/package/build.json"))
        self.candidate = {"git": binding["candidate"], "image_id": binding["image"],
                          "build_manifest_file": manifest, "build_manifest_sha256": binding["manifest"],
                          "source_archive_sha256": binding["archive"]}

    def replay(self, candidate=None):
        return owner.evaluate_install(self.stage, self.seal, candidate or self.candidate, self.source)

    def test_replays_actual_installation_without_granting_tick_credit(self):
        proof = self.replay()
        self.assertEqual(proof["scope"], owner.NODE)
        self.assertEqual(proof["known_default_count"], 6)
        self.assertEqual(proof["reinstalled_default"], "fetch-channel-statuses")
        self.assertEqual(proof["preserved_default_count"], 5)
        self.assertEqual(len(proof["ordered_sql_sha256"]), 3)
        self.assertNotIn("semantic_assertions_passed", proof)

    def test_refuses_other_candidate_and_changed_sql_or_inventories(self):
        with self.assertRaisesRegex(ValueError, "common ordinary package"):
            self.replay(dict(self.candidate, git="0" * 40))
        original = owner.read
        for leaf, inject, error in (
            ("scheduler-scope-install.json", lambda d: d["native_ready"]["scan-files"].update(count=1),
             "preserves other disabled rows"),
            ("independent-cleanup.json", lambda d: d["inventories"]["volume"].append("unexpected"),
             "inventory did not retire"),
        ):
            def read(path):
                data = original(path)
                if path.name == leaf:
                    inject(data)
                return data

            with self.subTest(leaf=leaf), patch.object(owner, "read", read):
                with self.assertRaisesRegex(ValueError, error):
                    self.replay()

    def test_fresh_stage_binds_without_receipts_and_preflight_uses_zero_engine_calls(self):
        with TemporaryDirectory() as folder:
            repository = Path(folder)
            stage = repository / "stage"
            output = repository / "future-coordinator"
            coordinator = repository / "scripts/rust-milestone-batches/coordinator.py"
            coordinator.parent.mkdir(parents=True)
            coordinator.write_text("# offline binding sentinel\n")
            source = repository / "artifacts/rust" / owner.ORIGINAL_REL
            source.parent.mkdir(parents=True)
            shutil.copyfile(self.source, source)
            context = owner.prepare(self.stage, self.candidate, stage)
            spec = owner.dispatch_spec(stage, context["seal"], self.candidate, repository, "ordinary", output)
            self.assertIn("run", spec["argv"])
            self.assertFalse(output.exists())
            for args in (["--preflight"], ["--run"]):
                process = subprocess.run(["/usr/bin/python3", "-B", str(stage / "run.py"), *args],
                                         capture_output=True, timeout=40)
                if args == ["--preflight"]:
                    self.assertEqual(process.returncode, 0, process.stderr)
                    self.assertEqual(json.loads(process.stdout)["engine_calls"], 0)
                else:
                    self.assertNotEqual(process.returncode, 0)
                    self.assertIn(b"reviewed coordinator lease required", process.stderr)
            self.assertFalse((stage / "actual-native-1").exists())
            self.assertFalse((stage / "owner-1").exists())
