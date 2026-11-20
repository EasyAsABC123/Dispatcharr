"""Check the portable typed Original worker and WebSocket route projection."""
import copy
import json
from pathlib import Path
import unittest

import retained_worker_wire_contracts as retained

ROOT = Path(__file__).resolve().parents[2]


class OriginalWorkerWire(unittest.TestCase):
    def test_canonical_wire_and_exact_worker_roots_remain_partial(self):
        value = json.loads((ROOT / retained.BINDING).read_bytes())
        retained.validate(ROOT, value)
        contracts = value["non_HTTP_contracts"]
        self.assertEqual([c["source"]["kind"] for c in contracts], ["task_candidate"]*3 + ["websocket_route"])
        self.assertEqual([len(c["observations"]) for c in contracts], [1, 1, 6, 55])
        self.assertFalse(any(c["fully_verified"] for c in contracts))
        self.assertEqual(contracts[-1]["source"]["path"], "dispatcharr/routing.py")
        self.assertEqual(value["fixtures"][retained.WIRE_FIXTURE]["exceptional_source_members_left_before_compose_down"], 4)
        self.assertFalse(value["fixtures"][retained.WORKER_FIXTURE]["default_scheduler_execution"])

    def test_reject_claims_beyond_the_retained_source_scope(self):
        baseline = json.loads((ROOT / retained.BINDING).read_bytes())
        for corrupt in (
            lambda r: r["non_HTTP_contracts"][0]["observations"][0].update(backend_result=False),
            lambda r: r["non_HTTP_contracts"][1]["observations"][0].update(stale_notification_row_deleted=False),
            lambda r: r["non_HTTP_contracts"][2]["source"].update(line=99),
            lambda r: r["fixtures"][retained.WIRE_FIXTURE].update(exceptional_source_member_cleanup_claimed=True),
        ):
            value = copy.deepcopy(baseline)
            corrupt(value)
            with self.assertRaises(AssertionError):
                retained.validate(ROOT, value)


if __name__ == "__main__":
    unittest.main()
