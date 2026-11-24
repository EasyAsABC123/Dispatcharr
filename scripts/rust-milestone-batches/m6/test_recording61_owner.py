"""Offline replay and rejection controls for the retained physical observations."""
from copy import deepcopy
import fcntl
import json
from importlib.util import module_from_spec, spec_from_file_location
import os
import subprocess
import sys
from types import SimpleNamespace
from tempfile import TemporaryDirectory
from pathlib import Path
import unittest
from unittest.mock import patch

import prepare_recording61 as prepare
import recording61_owner as owner
import retained_scheduler_semantics as semantics

ARTIFACTS = os.environ.get("M6_REPLAY_ARTIFACTS")
STAGE = os.environ.get("M6_REPLAY_RECORDING61")
VERSION = Path(__file__).resolve().parents[2] / (
    "rust-job-contracts/m6-missing-source/epoch2/m6-missing-domains/Source-extension.json"
)


class PhysicalLock(unittest.TestCase):
    def test_parent_holds_exclusive_lock_on_the_checkout_filesystem(self):
        # Keep this on the real checkout filesystem. A system temporary
        # directory might use a different lock-device namespace.
        child = """import os, sys
from pathlib import Path
import recording61_owner as owner
raise SystemExit(0 if owner.parent_holds_lock(Path('/proc') / str(os.getppid()), Path(sys.argv[1])) else 1)
"""
        here = Path(__file__).resolve().parent
        with TemporaryDirectory(dir=here) as directory:
            lock = Path(directory) / "owned.lock"

            def has_parent_lock():
                run = subprocess.run(
                    [sys.executable, "-B", "-c", child, str(lock)], cwd=here,
                    check=False, capture_output=True, timeout=10,
                )
                self.assertEqual(run.stderr, b"")
                return run.returncode == 0

            with lock.open("xb") as handle:
                self.assertFalse(has_parent_lock())
                fcntl.flock(handle, fcntl.LOCK_SH)
                self.assertFalse(has_parent_lock())
                fcntl.flock(handle, fcntl.LOCK_EX)
                self.assertTrue(has_parent_lock())
                lock.rename(lock.with_name("held-inode"))
                lock.touch()
                self.assertFalse(has_parent_lock())


class RetainedVersion(unittest.TestCase):
    def test_raw_terminal_and_distinct_notification_effects(self):
        self.assertEqual(owner.digest(VERSION), semantics.VERSION_SHA)
        document = owner.read(VERSION)
        self.assertEqual(len(semantics.versions(document)), 4)
        cases = document["report"]["cases"]
        cases[2]["after"]["tables"][semantics.NOTIFICATIONS][0]["id"] += 1
        with self.assertRaisesRegex(ValueError, "replaced the existing"):
            semantics.versions(document)

    def test_backend_receipt_identity_mismatch(self):
        document = owner.read(VERSION)
        first, second, *_ = document["report"]["cases"]
        first["actual"]["backend"] = second["actual"]["backend"]
        with self.assertRaisesRegex(ValueError, "raw backend identity"):
            semantics.versions(document)


@unittest.skipUnless(ARTIFACTS and STAGE, "set replay artifact and recording61 stage paths")
class PhysicalReplay(unittest.TestCase):
    def setUp(self):
        artifacts = Path(ARTIFACTS).resolve()
        self.stage = Path(STAGE).resolve()
        self.seal = (self.stage / "workload-seal.sha256").read_text().strip()
        binding = owner.read(self.stage / "package-binding.json")
        self.candidate = {
            "git": binding["candidate"], "image_id": binding["image"],
            "build_manifest_sha256": binding["manifest"], "source_archive_sha256": binding["archive"],
            "build_manifest_file": next(path for path in binding["pins"] if path.endswith("/package/build.json")),
        }
        self.version_pair = artifacts / "m6-239ba8-core-version200-pair-2"
        self.calendar = artifacts / "m6-source-native-calendar-vectors-final-1/source-scheduler-calendar-private.json"

    def replay(self, candidate=None):
        return owner.verify(self.stage, self.seal, candidate or self.candidate)

    def test_native_recording_retained_source_and_distinct_decoder_gates(self):
        result = self.replay()
        self.assertTrue(result["exact_owned_absence"])
        self.assertEqual(result["retained_original_reduction_sha256"], owner.ORIGINAL_REDUCTION)
        self.assertEqual(result["scope_results"]["declared_decode"]["actual_exit"], 0)
        self.assertEqual(result["scope_results"]["strict_diagnostic"]["actual_exit"], 183)

    def test_materialized_owner_preflight_and_absent_current_receipts(self):
        with TemporaryDirectory() as folder:
            stage = Path(folder) / "stage"
            binding = prepare.prepare(self.stage, stage, self.candidate)
            process = subprocess.run([sys.executable, "-B", str(stage / "run.py"), "preflight"],
                                     check=True, capture_output=True, timeout=30)
            self.assertIn(b'"engine_calls": 0', process.stdout)
            direct = subprocess.run([sys.executable, "-B", str(stage / "run.py"), "run"],
                                    capture_output=True, timeout=30)
            self.assertNotEqual(direct.returncode, 0)
            self.assertIn(b"reviewed coordinator lease required", direct.stderr)
            self.assertFalse((stage / "actual-1").exists())
            with self.assertRaises(FileNotFoundError):
                owner.verify(stage, binding["seal"], self.candidate)

    def test_binding_preflight_accepts_no_execution_receipts_or_output(self):
        script = Path(owner.__file__).parents[1] / "coordinator.py"
        spec = spec_from_file_location("recording61_test_coordinator", script)
        coordinator = module_from_spec(spec)
        spec.loader.exec_module(coordinator)
        nodes = [row for row in coordinator.inventory() if row["id"] in (owner.ONEOFF, owner.NODE)]
        node = next(row for row in nodes if row["id"] == owner.NODE)
        predecessor = next(row for row in nodes if row["id"] == owner.ONEOFF)
        with TemporaryDirectory() as folder:
            root = Path(folder) / "coordinator"
            stage = Path(folder) / "stage"
            context = prepare.prepare(self.stage, stage, self.candidate)
            binding = owner.dispatch_spec(stage, context["seal"], self.candidate, script.parents[2],
                                          node["entrypoint"], root)
            prior = owner.dispatch_spec(stage, context["seal"], self.candidate, script.parents[2],
                                        predecessor["entrypoint"], root, node=owner.ONEOFF)
            self.assertIn("reuse", binding["argv"])
            self.assertIn("run", prior["argv"])
            document = {"candidate": self.candidate, "nodes": {owner.NODE: binding, owner.ONEOFF: prior},
                        "owner_bindings": {"m6": {"recording61": context}}}
            self.assertEqual(coordinator.owner_readiness(nodes, document, root), {})
            self.assertFalse(root.exists())
            unpaired = deepcopy(document)
            del unpaired["nodes"][owner.ONEOFF]
            with self.assertRaisesRegex(ValueError, "same owned one-off predecessor"):
                coordinator.owner_readiness(nodes, unpaired, root)
            self.assertFalse((stage / "owner-1").exists())
            root.mkdir()
            self.assertEqual(coordinator.owner_readiness(nodes, document, root), {})
            bad = deepcopy(document)
            bad["nodes"][owner.NODE]["argv"] = ["/usr/bin/true"]
            with self.assertRaisesRegex(ValueError, "exact checker"):
                coordinator.owner_readiness(nodes, bad, root)

    def test_direct_launch_fences_before_any_fixture_or_engine_call(self):
        with TemporaryDirectory() as folder:
            repository = Path(folder)
            coordinator = repository / "scripts/rust-milestone-batches/coordinator.py"
            coordinator.parent.mkdir(parents=True)
            coordinator.write_text(
                "import fcntl,json,os,subprocess,sys\n"
                "from pathlib import Path\n"
                "lock = Path(__file__).with_name('.dispatcher.lock').open('rb')\n"
                "if sys.argv[-1] == 'own': fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)\n"
                "binding = json.loads(Path(sys.argv[2]).read_text())\n"
                "raise SystemExit(subprocess.run(binding['argv'], env={}).returncode)\n")
            stage = repository / "stage"
            context = prepare.prepare(self.stage, stage, self.candidate)
            output = repository / "output"
            binding = owner.dispatch_spec(stage, context["seal"], self.candidate, repository,
                                          "fixture", output, node=owner.ONEOFF)
            binding_file = repository / "binding.json"
            binding_file.write_text(json.dumps(binding))
            directory = owner.attempt_directory(output, owner.ONEOFF)
            directory.mkdir(parents=True)
            attempt = {"node": {"id": owner.ONEOFF}, "candidate": self.candidate, "binding": binding, "attempt": 1}
            (directory / "attempt.json").write_text(json.dumps(attempt))
            for name in ("execution.stdout", "execution.stderr"):
                (directory / name).touch()
            lock_path = coordinator.with_name(".dispatcher.lock")
            lock_path.touch()
            command = [sys.executable, "-B", str(coordinator), "--bindings",
                       str(binding_file), "--output", str(output)]

            def blocked(mode, message):
                result = subprocess.run([*command, mode], cwd=repository, capture_output=True, timeout=10)
                self.assertNotEqual(result.returncode, 0)
                self.assertIn(message, result.stderr)
                self.assertFalse((stage / "owner-1").exists())

            blocked("none", b"actual held coordinator lease required")
            with lock_path.open("rb") as outsider:
                fcntl.flock(outsider, fcntl.LOCK_EX)
                blocked("other", b"dispatcher lease is held by a different process")
            blocked("own", b"exact isolated recording61 daemon and ordinary feature settings required")
            attempt["attempt"] = 2
            (directory / "attempt.json").write_text(json.dumps(attempt))
            blocked("own", b"actual exact recording61 coordinator attempt required")

    def test_borrowed_fixture_records_literal_failure_and_cleanup(self):
        with TemporaryDirectory() as folder:
            stage = Path(folder)
            lock = stage / ".dispatcher.lock"
            phases = []

            def fixture_main(argv, coordinator_lease):
                phase, = argv
                phases.append(phase)
                if phase == "run":
                    with coordinator_lease(lock):
                        return 17
                return 0

            driver = SimpleNamespace(main=fixture_main)
            with patch.object(owner, "parent_holds_lock", return_value=True) as guard:
                self.assertEqual(owner.run_campaign(stage, driver, lock), 17)
                guard.assert_called_once()
            self.assertEqual(phases, ["preflight", "run", "cleanup"])
            ledger = stage / "owner-1"
            self.assertEqual((ledger / "campaign.actual-exit").read_bytes(), b"17\n")
            self.assertEqual((ledger / "run.actual-exit").read_bytes(), b"17\n")
            self.assertEqual((ledger / "cleanup.actual-exit").read_bytes(), b"0\n")
            self.assertFalse((ledger / "proof.actual-exit").exists())

    def test_beat_publication_replays_real_wire_sql_broker_and_db2(self):
        proof = owner.verify_publication(self.stage, self.seal, self.candidate)
        self.assertEqual(proof["signed_create_status"], 201)
        self.assertNotEqual(proof["schedule_name"], proof["single_clocked_root"]["task_id"])
        self.assertEqual(proof["authoritative_result"]["terminal"], "SUCCESS")
        self.assertNotIn("declared_decode", proof)

    def test_media_reuse_requires_passed_same_stage_and_redis_result(self):
        publication = owner.verify_publication(self.stage, self.seal, self.candidate)
        recording = self.replay()["scope_results"]
        with TemporaryDirectory() as folder:
            output = Path(folder)
            with self.assertRaisesRegex(ValueError, "expected exact bounded receipt"):
                owner.prior_publication(output, self.stage, self.seal, self.candidate, recording)
            directory = owner.attempt_directory(output, owner.ONEOFF)
            directory.mkdir()
            attempt = {"node": {"id": owner.ONEOFF}, "candidate": self.candidate,
                       "binding": {"argv": ["python", "-I", "-B", "owner", str(self.stage)]}}
            (directory / "attempt.json").write_text(json.dumps(attempt))
            publication.update(full_required_workflow=True, semantic_assertions_passed=True)
            phase = {"actual_exit": 0, "failure": None, "remaining_group_members": [],
                     "output_bytes_discarded": 0, "process_group_settled": True,
                     "stdout_EOF": True, "stderr_EOF": True}
            result = dict.fromkeys(("execution", "cleanup", "evaluation"), phase)
            result.update(id=owner.ONEOFF, status="passed", cleanup_verified=True, qualification=publication)
            (directory / "evaluation.stdout").write_text(json.dumps(publication))
            (directory / "result.json").write_text(json.dumps(result))
            self.assertEqual(owner.prior_publication(output, self.stage, self.seal, self.candidate, recording),
                             owner.digest(directory / "result.json"))
            recording["result"]["private_result_sha256"] = "0" * 64
            with self.assertRaisesRegex(ValueError, "same owned passed one-off predecessor"):
                owner.prior_publication(output, self.stage, self.seal, self.candidate, recording)

    def test_publication_rejects_bad_create_and_mismatched_broker(self):
        original_read = owner.read
        raw = self.stage / "actual-1/Native/business/scenario.json"
        for name, mutation, error in (
            ("booking", lambda data: data["requests"][0].update(sha256="0" * 64), "actual signed create"),
            ("clock", lambda data: data["clocked_task"].update(one_off=False), "saved schedule"),
            ("broker", lambda data: data.update(task_id=data["route_task_name"]), "broker UUID"),
        ):
            def changed_read(path):
                result = original_read(path)
                if path == raw:
                    mutation(result)
                return result
            with self.subTest(name=name), patch.object(owner, "read", changed_read):
                with self.assertRaisesRegex(ValueError, error):
                    owner.verify_publication(self.stage, self.seal, self.candidate)

    def test_historical_receipt_refused_for_different_package(self):
        candidate = dict(self.candidate, git="a2ec7dbb943ff1b93eb235a028517fc2dd623d23")
        with self.assertRaisesRegex(ValueError, "common candidate"):
            self.replay(candidate)

    def test_missing_decoder_eof_and_dirty_retirement_fail_independently(self):
        for leaf, mutate, reason in (
            ("Native/business/declared-whole-EOF.json", lambda d: d.update(stdout_EOF=False), "decode receipt"),
            ("independent-cleanup.json", lambda d: d.update(exact_cleanup=False), "scoped retirement"),
            ("Native-execution.json", lambda d: d.update(actual_exit=False), "arm did not settle"),
        ):
            path = self.stage / "actual-1" / leaf
            original_read = owner.read

            def changed_read(observed):
                result = original_read(observed)
                if observed == path:
                    mutate(result)
                return result

            with self.subTest(leaf=leaf), patch.object(owner, "read", changed_read):
                with self.assertRaisesRegex(ValueError, reason):
                    self.replay()

    def test_retained_scheduled_version_pair_keeps_historical_revision(self):
        result = semantics.scheduled_version_pair(self.version_pair)
        self.assertEqual(result["historical_native_revision"], "239ba8ccce931137e7421b62ce1a8e55f0bd5c39")
        self.assertNotEqual(result["historical_native_revision"], self.candidate["git"])
        self.assertEqual(result["case_labels"], ["no-update", "update", "repeat", "malformed"])

    def test_actual_controlled_beat_and_no_duplicate_fold(self):
        self.assertEqual(owner.digest(self.calendar), semantics.CALENDAR_SHA)
        document = owner.read(self.calendar)
        self.assertEqual(semantics.calendar(document)["controlled_original_beat_first_publication_offset_seconds"],
                         1800)
        no_publication = deepcopy(document)
        no_publication["beat_dst"]["envelopes"].clear()
        with self.assertRaises(ValueError):
            semantics.calendar(no_publication)
        second_fold = next(row for row in document["vectors"]
                           if row["mode"] == "legacy-fixed-offset" and row["input"]["name"] == "ny_fall_second")
        second_fold["actual"]["due"] = True
        with self.assertRaisesRegex(ValueError, "boundary observations"):
            semantics.calendar(document)
