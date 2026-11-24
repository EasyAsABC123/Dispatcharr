"""Offline custody and semantic rejection controls for the SD owner."""
from copy import deepcopy
from pathlib import Path
from tempfile import TemporaryDirectory
from uuid import uuid4
import json
import os
import unittest

import sd_guide_evidence as evidence
import sd_guide_owner as owner

REPO = Path(__file__).resolve().parents[3]
ROOT = os.environ.get("M6_SD_REFERENCE_ROOT")
TEMPLATE = os.environ.get("M6_SD_PACKAGE_TEMPLATE")
CANDIDATE = os.environ.get("M6_SD_COLD_CANDIDATE")


class SchedulesDirect(unittest.TestCase):
    def test_exact_cases_cannot_promote_cold_restart_or_drop_a_case(self):
        original = evidence.read(REPO / evidence.SOURCE)
        candidate = deepcopy(original)
        child = str(uuid4())
        candidate["cases"][-1]["actual"]["child"] = child
        self.assertEqual(evidence.cases(candidate, original), child)
        bad = deepcopy(candidate)
        bad["unrun_required"] = []
        with self.assertRaisesRegex(ValueError, "unresolved scope"):
            evidence.cases(bad, original)
        bad = deepcopy(candidate)
        bad["cases"][1]["actual"]["unchanged_Programme_rows"] = False
        with self.assertRaisesRegex(ValueError, "differs from retained Original"):
            evidence.cases(bad, original)

    @unittest.skipUnless(ROOT and TEMPLATE and CANDIDATE, "exact retained package inputs required")
    def test_cold_owner_and_historical_raw_reservation_without_engine(self):
        repo = Path(ROOT)
        original = evidence.read(repo / evidence.SOURCE)
        historical = repo / "artifacts/rust/normal-graph-batch39-functional-1/fixtures/Native-e41bbb6c"
        candidate = evidence.read(historical / evidence.CASE / "report.json")
        child = evidence.cases(candidate, original)
        self.assertTrue(evidence.retry(historical, child))
        with self.assertRaisesRegex(ValueError, "ETA and due result differ"):
            evidence.retry(historical, str(uuid4()))
        with TemporaryDirectory() as folder:
            root = Path(folder)
            stage = root / "stage"
            package = json.loads(Path(CANDIDATE).read_bytes())
            cold = owner.prepare(repo, Path(TEMPLATE), package, stage)
            self.assertEqual(cold["cold_preflight"]["engine_calls"], 0)
            binding = owner.dispatch_spec(repo, stage, cold["seal"], package, "sd", root / "out")
            self.assertIn("run", binding["argv"])
            self.assertFalse((stage / "actual-1").exists())
            with self.assertRaisesRegex(ValueError, "exact common final AMD64"):
                owner.dispatch_spec(repo, stage, cold["seal"], package | {"git": "0" * 40}, "sd", root / "out")
