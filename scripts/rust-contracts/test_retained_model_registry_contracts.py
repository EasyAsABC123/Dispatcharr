"""Guard exact Django registration assignments and their Original-only scope."""
from collections import Counter
import copy
import recording_hls_source_contracts as plain_hls
from test_final_residual_wave import restore_profile_group_projection
import hashlib
import json
from pathlib import Path
import unittest

import retained_model_registry_contracts as subject

ROOT = Path(__file__).resolve().parents[2]
PRIOR_HTTP = "4f3de925c242621b09d1aa456f3ddef46cd709faaeecb94b2afe2222533f346b"
PRIOR_OPENAPI = "3fe98d17ddd79988eb01b59dc5837eee9b076a5b6620715f206ac32ee2a335fe"
PRIOR_TYPED = "0d170d8f5abfe3abefdab8313a1cd21ca8306f4386886ad3061a9ed57f5a5678"
PRIOR_HTTP_MAP = "d94858a8c2725725cfd5adafa19f84079212c6bb6e572bc39461a3b44492f902"


def digest(value):
    return hashlib.sha256((json.dumps(value, indent=2, ensure_ascii=False) + "\n").encode()).hexdigest()


class ModelRegistration(unittest.TestCase):
    def test_register_reverses_to_exact_prior_http_openapi_and_typed_rows(self):
        binding = json.loads((ROOT / subject.INPUT).read_bytes())
        subject.validate(ROOT, binding)
        register = json.loads((ROOT / "docs/rust-contract-register.json").read_bytes())
        plain_hls.restore_before(ROOT, register["runtime_routes"], register["executable_Source_mapping"])
        restore_profile_group_projection(ROOT, register["runtime_routes"], register["executable_Source_mapping"])
        typed = register["non_HTTP_candidates"]
        import retained_task_registry_contracts as task_registry
        import retained_management_registration_contracts as command_registry
        command_registry.restore_before(ROOT, typed)
        command_input = register["executable_Source_mapping"]["inputs"].pop()
        self.assertEqual((command_input["kind"], command_input["path"]), (command_registry.KIND, command_registry.INPUT))
        task_registry.restore_before(ROOT, typed)
        task_input = register["executable_Source_mapping"]["inputs"].pop()
        self.assertEqual((task_input["kind"], task_input["path"]), (task_registry.KIND, task_registry.INPUT))
        added = [row for row in typed if row["state"] == subject.STATE]
        self.assertEqual((len(typed), len(added)), (250, 50))
        self.assertEqual(sum(row["result"] is None for row in typed), 23)
        self.assertEqual(sum(row["fully_verified"] is True for row in typed), 0)
        self.assertEqual(sum("executable_Source_contract" in row for row in typed), 227)
        self.assertEqual(register["counts"]["fully_verified_route_operations"], 0)
        self.assertEqual(register["executable_Source_mapping"]["operations"], 797)
        self.assertEqual([row["executable_Source_contract"] for row in added], binding["non_HTTP_contracts"])
        self.assertEqual(digest(register["runtime_routes"]), PRIOR_HTTP)
        self.assertEqual(digest(register["openapi_operations"]), PRIOR_OPENAPI)
        last = register["executable_Source_mapping"]["inputs"].pop()
        self.assertEqual((last["kind"], last["path"]), (subject.KIND, subject.INPUT))
        self.assertEqual(digest(register["executable_Source_mapping"]), PRIOR_HTTP_MAP)
        subject.restore_before(ROOT, typed)
        self.assertEqual(digest(typed), PRIOR_TYPED)

    def test_exact_observed_classes_and_narrow_scope(self):
        binding = subject.derive(ROOT)
        rows = binding["non_HTTP_contracts"]
        self.assertEqual(Counter(row["observation"]["kind"] for row in rows), subject.REGISTRY_KINDS)
        self.assertEqual(sum(row["observation"].get("field_count", 0) for row in rows), 424)
        observations = json.loads(subject.exact_bytes(ROOT, subject.OBSERVATIONS))["observations"]
        for row, raw in zip(rows, observations, strict=True):
            summary = row["observation"]
            self.assertEqual(row["observation_sha256"], subject.source_contracts.digest(raw))
            if summary["kind"] == "django-registered-model":
                self.assertEqual(set(summary), {"kind", "label", "table", "primary_key", "field_count"})
                self.assertEqual(summary["field_count"], len(raw["fields"]))
            else:
                self.assertEqual(set(summary), {"kind", "bound_models", "declared_methods"})
        self.assertEqual(len({row["source"]["path"] for row in rows}), 10)
        self.assertEqual(binding["fixtures"][subject.FIXTURE]["database_connections"], 0)
        self.assertTrue(all(not row["fully_verified"] and not any(
            row["result"][key] for key in ("Native_coverage_increased", "model_data_parity_verified",
                                           "runtime_behavior_verified")) for row in rows))

    def test_reject_source_coordinates_swaps_and_invented_parity(self):
        declarations, observed, receipt = [json.loads(subject.exact_bytes(ROOT, path)) for path in
                                           (subject.DECLARATIONS, subject.OBSERVATIONS, subject.EXECUTION)]
        changes = {
            "class line": lambda d, o, r: d[0].update(line=1),
            "source bytes identity": lambda d, o, r: d[0].update(source_sha256="0" * 64),
            "ordered row identity": lambda d, o, r: o["observations"].reverse(),
            "database connections": lambda d, o, r: o.update(database_connections=1),
            "cleanup": lambda d, o, r: r.update(cleanup=False),
        }
        for name, change in changes.items():
            with self.subTest(name=name):
                d, o, r = copy.deepcopy((declarations, observed, receipt))
                change(d, o, r)
                with self.assertRaises(AssertionError):
                    subject.materialize(d, o, r)
        binding = subject.derive(ROOT)
        binding["non_HTTP_contracts"][0]["result"]["runtime_behavior_verified"] = True
        with self.assertRaises(AssertionError):
            subject.validate(ROOT, binding)


if __name__ == "__main__":
    unittest.main()
