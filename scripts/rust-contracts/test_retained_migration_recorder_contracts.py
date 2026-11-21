"""Portable migration evidence, projection and scope controls."""
from collections import Counter
import copy
import recording_hls_source_contracts as plain_hls
from test_final_residual_wave import restore_profile_group_projection
import hashlib
import json
from pathlib import Path
import unittest

import retained_migration_recorder_contracts as subject

ROOT = Path(__file__).resolve().parents[2]
PRIOR_HTTP = "4f3de925c242621b09d1aa456f3ddef46cd709faaeecb94b2afe2222533f346b"
PRIOR_TYPED = "0e6978431c8a59d8f58ff671157cc11fee8ff37e8a8af7849fd4c4188ab7ba0a"


def digest(value):
    return hashlib.sha256((json.dumps(value, indent=2, ensure_ascii=False) + "\n").encode()).hexdigest()


class MigrationRecorder(unittest.TestCase):
    def test_current_register_keeps_exact_prior_http_and_typed_rows(self):
        published = json.loads((ROOT / subject.INPUT).read_bytes())
        subject.validate(ROOT, published)
        register = json.loads((ROOT / "docs/rust-contract-register.json").read_bytes())
        plain_hls.restore_before(ROOT, register["runtime_routes"], register["executable_Source_mapping"])
        restore_profile_group_projection(ROOT, register["runtime_routes"], register["executable_Source_mapping"])
        typed = register["non_HTTP_candidates"]
        import retained_model_registry_contracts as model_registry
        import retained_task_registry_contracts as task_registry
        import retained_management_registration_contracts as command_registry
        command_registry.restore_before(ROOT, typed)
        task_registry.restore_before(ROOT, typed)
        model_registry.restore_before(ROOT, typed)
        selected = [entry["executable_Source_contract"] for entry in typed if entry["source"]["kind"] == "migration"]
        self.assertEqual(len(typed), 250)
        self.assertEqual(len(selected), 145)
        self.assertEqual(sum("executable_Source_contract" in row for row in typed), 177)
        self.assertEqual(register["executable_Source_mapping"]["operations"], 797)
        self.assertEqual(selected, published["non_HTTP_contracts"])
        self.assertEqual(digest(register["runtime_routes"]), PRIOR_HTTP)
        self.assertEqual(Counter(row["Original_SQLite"]["state"] for row in selected), {
            "applied_before_failure": 120, "observed_failed": 1, "not_reached": 24,
        })
        self.assertTrue(all(row["Original_PostgreSQL17"] == "recorded_applied_on_fresh_database" for row in selected))
        self.assertTrue(all(row["fully_verified"] is row["result"]["datastore_parity_passed"]
                            is row["result"]["Native_coverage_increased"] is False for row in selected))
        subject.restore_before(ROOT, typed)
        self.assertEqual(digest(typed), PRIOR_TYPED)

    def test_reject_recorder_identity_wrong_backend_partition_and_invented_parity(self):
        observed = subject.checked(ROOT, subject.OBSERVATIONS)
        pg = subject.checked(ROOT, subject.PG)
        changes = {
            "wrong original": lambda o, p: o.update(Original_git="0" * 40),
            "wrong label": lambda o, p: o["app_labels"]["apps/channels"].update(label="channels"),
            "wrong source coordinate": lambda o, p: o["declarations"][0]["source"].update(line=1),
            "missing PostgreSQL key": lambda o, p: p["migrations"].pop(),
            "duplicate migration": lambda o, p: o["declarations"].__setitem__(0, o["declarations"][1]),
            "SQLite promoted": lambda o, p: o["sqlite"].update(CLI_exit=0),
            "wrong SQLite failure": lambda o, p: o["sqlite"].update(first_failed_migration="dispatcharr_channels.0039_add_radio_fields"),
            "SQLite cleanup false": lambda o, p: o["sqlite"].update(final_labelled_CID_absence=False),
        }
        for name, change in changes.items():
            with self.subTest(name=name):
                projection, manifest = copy.deepcopy(observed), copy.deepcopy(pg)
                change(projection, manifest)
                with self.assertRaises(AssertionError):
                    subject.materialize(projection, manifest)
        modified = subject.derive(ROOT)
        modified["non_HTTP_contracts"][0]["result"]["Native_coverage_increased"] = True
        with self.assertRaises(AssertionError):
            subject.validate(ROOT, modified)


if __name__ == "__main__":
    unittest.main()
