"""Replay completed local XMLTV receipts against the scoped release projection."""

from copy import deepcopy
import os
from pathlib import Path
import unittest

import second_import_owner as owner

REPLAY = os.environ.get("M6_REPLAY_LOCAL_REFRESH")


@unittest.skipUnless(REPLAY, "set the reviewed paired local refresh receipt path")
class PositiveLocalRefresh(unittest.TestCase):
    def setUp(self):
        self.reports = []
        for node in (owner.SOURCE, owner.NATIVE):
            directory = Path(REPLAY).resolve() / node
            self.reports.append((
                owner.read(directory / "source-refresh-workflow-private.json"),
                owner.read(directory / "integration.json"),
            ))

    def test_retained_pair_projects_the_same_signed_local_refresh(self):
        source, native = self.reports
        owner.typed_branch(source[0], False)
        owner.typed_branch(native[0], True)
        self.assertEqual(owner.positive_local_refresh(*source), owner.positive_local_refresh(*native))
        projection = owner.positive_local_refresh(*native)
        self.assertEqual(projection["http"]["status"], 202)
        self.assertEqual(projection["realtime_events"], 11)
        self.assertEqual(set(projection["typed_terminals"]), set(owner.TASKS))
        self.assertIn(owner.LOCAL, owner.CELLS)

    def test_wrong_owned_file_or_result_expiry_fails(self):
        real, integration = self.reports[1]
        for key, value, message in (
            ("file_mode", "0o600", "exact ordinary readable"),
            ("ttl", False, "first local import kept actual result expiry"),
        ):
            with self.subTest(key=key):
                wrong = deepcopy(real)
                if key == "file_mode":
                    wrong["imports"][0][key] = value
                else:
                    ttl = next(row for row in wrong["checks"] if row["name"] == f"resultTTL1-{owner.TASKS[0]}")
                    ttl["passed"] = value
                with self.assertRaisesRegex(ValueError, message):
                    owner.positive_local_refresh(wrong, integration)

    def test_existing_source_worker_lineage_is_required(self):
        original, integration = self.reports[0]
        wrong = deepcopy(original)
        root = next(worker for worker in wrong["path_context"]["workers"] if worker["task"] == owner.TASKS[0])
        root["parent_id"] = root["id"]
        with self.assertRaisesRegex(ValueError, "actual Original signed local Refresh root"):
            owner.positive_local_refresh(wrong, integration)
