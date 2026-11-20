"""Verify the three Original registrations and preserve previous assignments."""
import copy
import json
import unittest

import retained_channel_task_effects
import retained_management_registration_contracts as subject
from test_retained_task_registry_contracts import ROOT, PRIOR_HTTP, PRIOR_OPENAPI, digest

PRIOR_TYPED = "b22c87250962db01bd42a2a2406f3a9b142c187335ce441c86c1c77c63b08d8e"
PRIOR_HTTP_MAP = "2ad4a0e05308f39b5140895c9a450a6ce5399b535489880b8fd25bc0091dccdb"


class ManagementRegistration(unittest.TestCase):
    def test_exact_prior_assignments_and_final_partial_counts(self):
        stored = json.loads((ROOT / subject.INPUT).read_bytes())
        subject.validate(ROOT, stored)
        document = json.loads((ROOT / "docs/rust-contract-register.json").read_bytes())
        typed = document["non_HTTP_candidates"]
        self.assertEqual(len(typed), 250)
        self.assertTrue(all("executable_Source_contract" in row and not row["fully_verified"] for row in typed))
        self.assertEqual(document["executable_Source_non_HTTP_mapping"]["candidates"], 250)
        self.assertEqual(document["executable_Source_mapping"]["operations"], 797)
        self.assertEqual(document["counts"]["fully_verified_route_operations"], 0)
        self.assertEqual(digest(document["runtime_routes"]), PRIOR_HTTP)
        self.assertEqual(digest(document["openapi_operations"]), PRIOR_OPENAPI)
        changed = {row["candidate_id"]: row["executable_Source_contract"] for row in typed if row["state"] == subject.STATE}
        self.assertEqual(changed, {row["candidate_id"]: row for row in stored["non_HTTP_contracts"]})
        retained_channel_task_effects.restore_before(ROOT, typed)
        subject.restore_before(ROOT, typed)
        self.assertEqual(digest(typed), PRIOR_TYPED)
        reference = document["executable_Source_mapping"]["inputs"].pop()
        self.assertEqual((reference["kind"], reference["path"]), (subject.KIND, subject.INPUT))
        self.assertEqual(digest(document["executable_Source_mapping"]), PRIOR_HTTP_MAP)

    def test_exact_source_registration_owners_and_compact_rows(self):
        rows = subject.derive(ROOT)["non_HTTP_contracts"]
        raw = json.loads(subject.exact_bytes(ROOT, subject.OBSERVATIONS))["observations"]
        self.assertEqual(
            [(row["observation"]["command"], row["observation"]["owner"], row["source"]["line"]) for row in rows],
            [("proxy", "apps.proxy", 7), ("dropdb", "core", 8), ("kill_processes", "core", 6)],
        )
        for row, observed in zip(rows, raw, strict=True):
            self.assertEqual(set(row["observation"]), {"command", "owner"})
            self.assertEqual(row["observation_sha256"], subject.source_contracts.digest(observed))
            self.assertEqual(row["original_file_sha256"], observed["source_sha256"])
            self.assertEqual(row["result"], {
                "state": subject.STATE,
                "fully_verified": False,
                "Native_coverage_increased": False,
                "management_execution_verified": False,
            })

    def test_reject_registration_identity_and_no_effect_drift(self):
        inputs = [json.loads(subject.exact_bytes(ROOT, path)) for path in (
            subject.DECLARATIONS, subject.OBSERVATIONS, subject.EXECUTION,
        )]
        changes = {
            "wrong app owner": lambda d, o, e: o["observations"][0].update(owner="core"),
            "wrong coordinate": lambda d, o, e: d[1].update(line=9),
            "wrong order": lambda d, o, e: o["observations"].reverse(),
            "module import": lambda d, o, e: o.update(command_modules_loaded=True),
            "command dispatch": lambda d, o, e: o.update(management_commands_started=True),
            "external effect": lambda d, o, e: o.update(external_effects_executed=True),
            "failed cleanup": lambda d, o, e: e.update(cleanup=False),
        }
        for name, change in changes.items():
            with self.subTest(name=name):
                changed = copy.deepcopy(inputs)
                change(*changed)
                with self.assertRaises(AssertionError):
                    subject.materialize(*changed)


if __name__ == "__main__":
    unittest.main()
