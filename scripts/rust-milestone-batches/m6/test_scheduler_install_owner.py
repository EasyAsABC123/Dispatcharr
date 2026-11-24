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
        self.original_tick = Path(ARTIFACTS).resolve() / owner.tick.ORIGINAL_LANE
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

    def test_replay_uses_actual_original_and_broker_but_refuses_old_timing_for_credit(self):
        args = (self.stage, self.seal, self.candidate, self.source, self.original_tick)
        proof = owner.evaluate_tick(*args, require_arms=False)
        self.assertEqual(proof["cadence_seconds"], 3600)
        self.assertEqual(len(proof["native_terminal_sha256"]), 3)
        self.assertNotEqual(proof["native_broker_sha256"], proof["original_broker_sha256"])
        self.assertEqual(proof["due_arm_sha256"], [])
        with self.assertRaisesRegex(ValueError, "expected exact bounded receipt"):
            owner.evaluate_tick(*args, require_arms=True)

    def test_due_windows_reject_publication_before_arm_and_after_budget(self):
        arms = [{"prior_count": 0, "started": 10.0, "finished": 11.0},
                {"prior_count": 1, "started": 25.0, "finished": 26.0}]
        owner.tick.armed_windows(arms, [12.0, 13.0, 30.0])
        for times in ([9.0, 13.0, 30.0], [12.0, 13.0, 92.0]):
            with self.assertRaisesRegex(ValueError, "outside its owned due-arm window"):
                owner.tick.armed_windows(arms, times)

    def test_arming_instrument_preserves_original_sql_and_binds_command(self):
        class FakeFixture:
            def sql(self, script, secret=False):
                self.called = script, secret
                return True

        with TemporaryDirectory() as folder:
            root = Path(folder)
            fixture = owner.tick.instrument(FakeFixture)()
            fixture.output = root
            receipt = {"stdout": str(root / "command-0010.stdout"),
                       "argv_sha256": "a" * 64, "stdout_sha256": "b" * 64}
            fixture.cli = [receipt]
            script = ("root.total_run_count==0\n" + owner.tick.ARM +
                      "\nroot.save(update_fields=['enabled','last_run_at'])\nPeriodicTasks.update_changed()")
            self.assertIs(fixture.sql(script, secret=True), True)
            self.assertEqual(fixture.called, (script, True))
            witness = owner.read(root / "scheduler-tick-arm-0.json")
            self.assertEqual(witness["command"], "command-0010.json")
            self.assertEqual(witness["argv_sha256"], receipt["argv_sha256"])

    def test_tick_refuses_other_installation_context(self):
        with TemporaryDirectory() as folder:
            output = Path(folder)
            scoped = self.replay()
            directory = owner.common.attempt_directory(output, owner.NODE)
            directory.mkdir()
            scoped.update(semantic_assertions_passed=True, full_required_workflow=True)
            phase = {"actual_exit": 0, "failure": None, "remaining_group_members": [],
                     "output_bytes_discarded": 0, "process_group_settled": True,
                     "stdout_EOF": True, "stderr_EOF": True}
            result = dict.fromkeys(("execution", "cleanup", "evaluation"), phase)
            result.update(id=owner.NODE, status="passed", cleanup_verified=True, qualification=scoped)
            attempt = {"node": {"id": owner.NODE}, "candidate": self.candidate,
                       "binding": {"argv": ["python", "-I", "-B", "owner", str(self.stage)]}}
            (directory / "attempt.json").write_text(json.dumps(attempt))
            (directory / "result.json").write_text(json.dumps(result))
            (directory / "evaluation.stdout").write_text(json.dumps(scoped))
            proof = owner.evaluate_tick(self.stage, self.seal, self.candidate, self.source,
                                        self.original_tick, require_arms=False)
            self.assertEqual(owner.same_installer(output, self.stage, proof),
                             owner.digest(directory / "result.json"))
            for wrong in (dict(proof, stage_seal="0" * 64), dict(proof, cleanup_sha256="0" * 64)):
                with self.assertRaisesRegex(ValueError, "same owned passed installer"):
                    owner.same_installer(output, self.stage, wrong)

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
            original = repository / "artifacts/rust" / owner.tick.ORIGINAL_LANE
            original.parent.mkdir(parents=True)
            shutil.copyfile(self.original_tick, original)
            original_execution = "Original-execution.json"
            shutil.copyfile(self.original_tick.parents[1] / original_execution,
                            original.parents[1] / original_execution)
            context = owner.prepare(self.stage, self.candidate, stage)
            spec = owner.dispatch_spec(stage, context["seal"], self.candidate, repository, "ordinary", output)
            self.assertIn("run", spec["argv"])
            tick_spec = owner.dispatch_spec(stage, context["seal"], self.candidate, repository,
                                            "ordinary", output, node=owner.tick.NODE)
            self.assertIn("tick-reuse", tick_spec["argv"])
            self.assertIn("tick-evaluate", tick_spec["evaluator_argv"])
            self.assertNotIn("run", tick_spec["argv"])
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
