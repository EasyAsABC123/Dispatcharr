"""Test the 20-cell metadata assignment and reject execution or Source drift."""
import copy
import hashlib
import json
from pathlib import Path
import unittest

import retained_task_registry_contracts as subject

ROOT = Path(__file__).resolve().parents[2]
PRIOR_HTTP = "4f3de925c242621b09d1aa456f3ddef46cd709faaeecb94b2afe2222533f346b"
PRIOR_OPENAPI = "3fe98d17ddd79988eb01b59dc5837eee9b076a5b6620715f206ac32ee2a335fe"
PRIOR_TYPED = "79a134d68f000b2d6735fec4f0fe1f341d2a7097e7042f356b8537f0366baccb"
PRIOR_HTTP_MAP = "ddb086aaa7c2a7ee63d51789f0f8696a182d7bb265f6c8892bd88ed2c770d20c"


def digest(value):
    return hashlib.sha256((json.dumps(value,indent=2,ensure_ascii=False)+"\n").encode()).hexdigest()


class TaskRegistration(unittest.TestCase):
    def test_exact_prior_register_and_remaining_management(self):
        stored = json.loads((ROOT/subject.INPUT).read_bytes())
        subject.validate(ROOT,stored)
        doc = json.loads((ROOT/"docs/rust-contract-register.json").read_bytes())
        typed = doc["non_HTTP_candidates"]
        import retained_management_registration_contracts as command_registry
        command_registry.restore_before(ROOT, typed)
        command_input = doc["executable_Source_mapping"]["inputs"].pop()
        self.assertEqual((command_input["kind"], command_input["path"]), (command_registry.KIND, command_registry.INPUT))
        self.assertEqual(len(typed),250)
        self.assertEqual([row["source"]["kind"] for row in typed if row["result"] is None],["management_command"]*3)
        self.assertEqual(sum("executable_Source_contract" in row for row in typed),247)
        self.assertEqual(doc["executable_Source_mapping"]["operations"],797)
        self.assertEqual(doc["counts"]["fully_verified_route_operations"],0)
        self.assertTrue(all(row["fully_verified"] is False for row in typed))
        self.assertEqual(digest(doc["runtime_routes"]),PRIOR_HTTP)
        self.assertEqual(digest(doc["openapi_operations"]),PRIOR_OPENAPI)
        ref = doc["executable_Source_mapping"]["inputs"].pop()
        self.assertEqual((ref["kind"],ref["path"]),(subject.KIND,subject.INPUT))
        self.assertEqual(digest(doc["executable_Source_mapping"]),PRIOR_HTTP_MAP)
        changed = {row["candidate_id"]:row["executable_Source_contract"] for row in typed if row["state"]==subject.STATE}
        self.assertEqual(changed,{row["candidate_id"]:row for row in stored["non_HTTP_contracts"]})
        subject.restore_before(ROOT,typed)
        self.assertEqual(digest(typed),PRIOR_TYPED)

    def test_20_exact_compact_projections_and_no_execution(self):
        rows = subject.derive(ROOT)["non_HTTP_contracts"]
        raw = json.loads(subject.exact_bytes(ROOT,subject.OBSERVATIONS))["observations"]
        self.assertEqual(len(rows),20)
        self.assertEqual(len({row["source"]["path"] for row in rows}),6)
        for bound, observed in zip(rows,raw,strict=True):
            self.assertEqual(bound["observation_sha256"],subject.source_contracts.digest(observed))
            self.assertEqual(set(bound["observation"]),{"task_name","bound","parameter_count","time_limit","soft_time_limit"})
            self.assertEqual(bound["observation"]["parameter_count"],len(observed["parameters"]))
            self.assertFalse(bound["result"]["task_execution_verified"] or bound["result"]["Native_coverage_increased"])
        alias = next(row for row in rows if row["source"]["name"]=="fetch_sd_guide_for_epg")
        self.assertEqual(alias["source"]["path"],"apps/epg/sd_tasks.py")
        self.assertEqual(alias["observation"]["task_name"],"apps.epg.tasks.fetch_sd_guide_for_epg")

    def test_reject_unrelated_identity_effect_and_registration_edits(self):
        inputs = [json.loads(subject.exact_bytes(ROOT,path)) for path in (subject.DECLARATIONS,subject.OBSERVATIONS,subject.EXECUTION)]
        changes = {
            "source line": lambda d,o,e: d[0].update(line=1),
            "wrong metadata": lambda d,o,e: o["observations"][0].update(bound=True),
            "row swap": lambda d,o,e: o["observations"].reverse(),
            "external effects": lambda d,o,e: o.update(external_effects_executed=True),
            "management invocation": lambda d,o,e: o.update(management_commands_started=True),
            "task execution": lambda d,o,e: e.update(task_execution_accepted=True),
            "cleanup": lambda d,o,e: e.update(cleanup=False),
        }
        for name, change in changes.items():
            with self.subTest(name=name):
                copied=copy.deepcopy(inputs)
                change(*copied)
                with self.assertRaises(AssertionError):
                    subject.materialize(*copied)


if __name__=="__main__":
    unittest.main()
