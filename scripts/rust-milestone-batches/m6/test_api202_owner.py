"""Focused offline coordinator checks for API202 route selection and promotion."""

import importlib.util
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch


HERE = Path(__file__).resolve().parent


def load(path, name):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


owner = load(HERE / "api202_owner.py", "test_api202_route")
coordinator = load(HERE.parent / "coordinator.py", "test_api202_coordinator")


class RouteTests(unittest.TestCase):
    def setUp(self):
        self.node = {"id": owner.NODE, "owner": "m6", "entrypoint": owner.ENTRY,
                     "depends_on": [], "resources": ["m6/ingestion-api"]}
        self.candidate = {"git": "a" * 40}
        self.context = {"stage": "/external/fixture"}
        self.bound = {"api202_binding": owner.KIND, "argv": ["ordinary"]}
        self.output = HERE / "synthetic-never-written"

    def check(self, bound, *, context=True):
        dispatch = SimpleNamespace(dispatch_spec=lambda *args: self.bound)

        def modules(path, _name):
            if path.name == "batch.py":
                return SimpleNamespace(load=lambda: ({}, {}, None))
            if path.name == "api202_owner.py":
                self.assertEqual(path, HERE / "api202_owner.py")
                return dispatch
            if path.name == "entrypoint.py":
                return SimpleNamespace(export=lambda: {"nodes": [{
                    "global_id": owner.NODE, "runner": ["probe", "<ROOT_COMMON_CONTEXT>", "<ROOT_NODE_RECEIPT>"],
                }]})
            raise AssertionError(path.name)

        binding = {"candidate": self.candidate, "nodes": {owner.NODE: bound}}
        if context:
            binding["owner_bindings"] = {"m6": {"api202_publication": self.context}}
        with patch.object(coordinator, "module", side_effect=modules):
            return coordinator.owner_readiness([self.node], binding, self.output)

    def test_exact_tracked_route_and_legacy_probe_are_both_available(self):
        self.assertEqual(self.check(self.bound), {})
        historical = {"m6_context": "context", "m6_receipt": "receipt",
                      "argv": ["probe", "context", "receipt"]}
        self.assertEqual(self.check(historical, context=False), {})
        self.assertIn(owner.NODE, coordinator.PARTIAL_PROBES)

    def test_coverage_distinguishes_admitted_route_and_partial_diagnostic(self):
        for bound, full in (
            (self.bound, True),
            ({"argv": ["historical"]}, False),
            ({"api202_binding": "another-route"}, False),
        ):
            with self.subTest(bound=bound):
                coverage = coordinator.binding_coverage([self.node], {owner.NODE: bound}, {})
                self.assertEqual(coverage["execution_bindings"], [owner.NODE] if full else [])
                self.assertEqual(coverage["partial_probes"], [] if full else [owner.NODE])
                self.assertEqual(coverage["unbound_rows"], [])

    def test_reject_changed_route_and_missing_owner_context(self):
        with self.assertRaises(ValueError):
            self.check(self.bound | {"argv": ["other"]})
        with self.assertRaises(ValueError):
            self.check(self.bound, context=False)

    def result(self, *, exact=True, attempt_digest=True, cleanup=True):
        with tempfile.TemporaryDirectory() as root:
            output = Path(root) / "attempt"
            bound = {"argv": ["run"], "cleanup_argv": ["cleanup"],
                     "evaluator_argv": ["evaluate", "@EXECUTION@"], "seconds": 1, "cleanup_seconds": 1}
            if exact:
                bound["api202_binding"] = owner.KIND

            def command(argv, _bound, directory, mode, _seconds):
                path = directory / (mode + ".stdout")
                value = {"exact_owned_absence": cleanup}
                if mode == "evaluation":
                    self.assertEqual(argv[1], str(directory / "execution.json"))
                    value = {"semantic_assertions_passed": True, "full_required_workflow": True,
                             "candidate": self.candidate, "entrypoint": owner.ENTRY,
                             "api202_binding": owner.KIND, "attempt_sha256": (
                                 owner.digest(directory / "attempt.json") if attempt_digest else "old")}
                path.write_text(json.dumps(value))
                return {"actual_exit": 0, "failure": None, "stdout": str(path),
                        "process_group_settled": True, "stdout_EOF": True, "stderr_EOF": True}

            with patch.object(coordinator, "command", side_effect=command):
                return coordinator.execute(self.node, bound, output, self.candidate)["status"]

    def test_only_exact_bound_fresh_evaluator_and_cleanup_can_promote(self):
        self.assertEqual(self.result(), "passed")
        self.assertEqual(self.result(exact=False), "failed")
        self.assertEqual(self.result(attempt_digest=False), "failed")
        self.assertEqual(self.result(cleanup=False), "failed")

    def test_runtime_rejects_different_dispatcher_bytes(self):
        with patch.object(owner, "dispatch_spec", return_value={"input_sha256": {
                str(HERE.parent / "coordinator.py"): "new"}}):
            with self.assertRaisesRegex(ValueError, "same bound dispatcher bytes"):
                owner.route(self.context, self.candidate, HERE.parents[2], self.output, "old", "run", None)


if __name__ == "__main__":
    unittest.main()
