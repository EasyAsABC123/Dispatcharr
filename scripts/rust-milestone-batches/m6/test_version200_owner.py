"""Focused offline replay of the exact observed worker, SQL and HTTP receipts."""
import json
import os
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

import version200_owner as owner

REFERENCES = os.environ.get("M6_REPLAY_ARTIFACTS")
CANDIDATE = os.environ.get("M6_VERSION_COLD_CANDIDATE")


@unittest.skipUnless(REFERENCES, "set M6_REPLAY_ARTIFACTS to the retained Version authority")
class VersionOwner(unittest.TestCase):
    def setUp(self):
        self.repository = Path(REFERENCES).resolve().parents[1]
        self.output = Path(REFERENCES).resolve() / "m6-239ba8-core-version200-pair-2/actual-version200-2"
        receipt = owner.read(self.output / "receipt.json")
        self.historical = {"git": receipt["candidate"], "image_id": receipt["image"],
                           "build_manifest_sha256": receipt["manifest_sha256"]}

    def replay(self):
        return owner.evaluate(self.output, self.repository, self.historical, historical=True)

    def test_old_native_receipts_replay_all_cases_but_cannot_satisfy_ordinary_release(self):
        proof = self.replay()
        self.assertEqual(tuple(row["case"] for row in proof["cases"]), owner.LABELS)
        self.assertEqual(len({row["task_id"] for row in proof["cases"]}), 4)
        self.assertNotIn("semantic_assertions_passed", proof)
        with self.assertRaisesRegex(ValueError, "fresh ordinary AIO"):
            owner.evaluate(self.output, self.repository, self.historical)
        wrong = dict(self.historical, git="0" * 40)
        with self.assertRaisesRegex(ValueError, "common candidate"):
            owner.evaluate(self.output, self.repository, wrong, historical=True)

    def test_http_cases_fail_if_loopback_origin_does_not_settle(self):
        read = owner.read

        def changed(path):
            result = read(path)
            if path.name == "origin-stop.json":
                result["actual_requests"] = 3
            return result

        with patch.object(owner, "read", changed):
            with self.assertRaisesRegex(ValueError, "four settled Version origin"):
                self.replay()

    def test_event_and_sql_receipt_mutations_fail_independently(self):
        read = owner.read
        root = self.output / "native"
        probe = self.replay()
        terminal = probe["cases"][1]["terminal_sql_sha256"]
        commands = owner.read(root / "command-executions.json")
        terminal_path = next(Path(row["stdout"]) for row in commands
                             if Path(row["stdout"]).stat().st_size
                             and owner.digest(Path(row["stdout"])) == terminal)
        for receipt, mutate in (
            (root / "events-stopped.json", lambda row: row["events"].pop()),
            (terminal_path, lambda row: row["notice_sequence"].update(last_value=999)),
        ):
            def changed(path):
                value = read(path)
                if path == receipt:
                    mutate(value)
                return value

            with self.subTest(receipt=receipt.name), patch.object(owner, "read", changed):
                with self.assertRaises(AssertionError):
                    self.replay()

    @unittest.skipUnless(CANDIDATE, "set exact ordinary package candidate JSON path")
    def test_ordinary_current_composition_preflights_without_engine_or_outputs(self):
        candidate = json.loads(Path(CANDIDATE).read_text())
        self.assertEqual(owner.preflight(self.repository, candidate)["engine_calls"], 0)
        with TemporaryDirectory() as folder:
            root = Path(folder)
            spec = owner.dispatch_spec(root / "version", candidate, self.repository, "ordinary", root / "matrix")
            self.assertIn("run", spec["argv"])
            self.assertIn("evaluate", spec["evaluator_argv"])
            self.assertFalse((root / "version").exists())
            self.assertFalse((root / "matrix").exists())
