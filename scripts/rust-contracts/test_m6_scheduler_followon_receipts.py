"""Exercise dependency scheduling and reject unproved receipt reuse."""
from hashlib import sha256
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch


def load(path):
    spec = spec_from_file_location(path.stem, path)
    module = module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


HERE = Path(__file__).resolve().parent
coordinator = load(HERE.parent / "rust-milestone-batches/coordinator.py")
adapter = load(HERE / "m6_scheduler_followon_receipts.py")
STATS = "m6/stats-live-ghost"


class SchedulerFollowonReceipts(unittest.TestCase):
    def setUp(self):
        self.nodes = [row for row in coordinator.inventory() if row["id"] in {*adapter.ORDER, STATS}]
        self.candidate = {"git": adapter.APPLICATION, "build_manifest_sha256": adapter.MANIFEST}
        self.authorities = {}
        predecessor = {}
        for key in adapter.ORDER:
            checksum = sha256(key.encode()).hexdigest()
            self.authorities[key] = {
                "id": key, "status": adapter.STATUS, "candidate": self.candidate,
                "receipt_sha256": checksum, "dependencies": predecessor, "cleanup_verified": True,
                "satisfies_dependency": True, "counts_as_new_execution": False,
            }
            predecessor = {key: checksum}

    def test_current_receipts_unlock_stats_without_dispatching_their_fixtures(self):
        calls = []

        def worker(node, *_):
            calls.append(node["id"])
            return {"id": node["id"], "status": "passed", "cleanup_verified": True}

        binding = {"argv": ["demo"], "cleanup_argv": ["demo"], "evaluator_argv": ["demo"],
                   "seconds": 1, "cleanup_seconds": 1, "output_bytes": 1, "input_sha256": {}}
        with TemporaryDirectory() as directory, patch.object(coordinator, "validate", return_value=None):
            output = Path(directory) / "report"
            result = coordinator.schedule(self.nodes, {STATS: binding}, output, self.candidate, worker,
                                          authorities=self.authorities)
        self.assertEqual(calls, [STATS])
        self.assertTrue(result["full_batch_passed"])
        self.assertEqual(result["results"][adapter.RECURRING]["status"], adapter.STATUS)
        self.assertFalse(result["results"][adapter.BACKEND]["counts_as_new_execution"])

    def test_missing_tick_and_foreign_candidate_are_refused(self):
        absent = {key: row for key, row in self.authorities.items() if key != adapter.TICK}
        with self.assertRaisesRegex(ValueError, "exact four current"):
            adapter.validate_chain(self.nodes, self.candidate, absent)
        foreign = self.authorities | {adapter.TICK: self.authorities[adapter.TICK] | {"candidate": {"git": "older"}}}
        with self.assertRaisesRegex(ValueError, "unverified current scheduler receipt chain"):
            adapter.validate_chain(self.nodes, self.candidate, foreign)

    def test_backend_cannot_omit_or_change_recurring_predecessor(self):
        bad_graph = [row | {"depends_on": []} if row["id"] == adapter.BACKEND else row for row in self.nodes]
        with self.assertRaisesRegex(ValueError, "exact canonical dependency graph"):
            adapter.validate_chain(bad_graph, self.candidate, self.authorities)
        wrong = self.authorities | {adapter.BACKEND: self.authorities[adapter.BACKEND] | {"dependencies": {}}}
        with self.assertRaisesRegex(ValueError, "unverified current scheduler receipt chain"):
            adapter.validate_chain(self.nodes, self.candidate, wrong)

    def test_receipt_intake_rejects_double_dispatch_and_missing_physical_outputs(self):
        with self.assertRaisesRegex(ValueError, "cannot also be dispatched"):
            adapter.resolve({}, self.nodes, self.candidate, {adapter.RECURRING: {}})
        with TemporaryDirectory() as directory, patch.object(adapter, "expected_capsule", return_value=Path(directory)), \
                patch.object(adapter, "check_capsule"), patch.object(adapter, "replay") as replay:
            with self.assertRaisesRegex(ValueError, "actual-exit"):
                adapter.resolve({}, self.nodes, self.candidate, {})
            replay.assert_not_called()

    def test_report_distinguishes_current_receipts_from_retained_source(self):
        report = coordinator.binding_coverage(self.nodes, {}, self.authorities)
        self.assertEqual(report["verified_current_receipts"], sorted(adapter.ORDER))
        self.assertEqual(report["retained_source_authority"], [])
        self.assertEqual(report["unbound_rows"], [STATS])


if __name__ == "__main__":
    unittest.main()
