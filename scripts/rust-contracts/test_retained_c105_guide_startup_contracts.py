"""Preserve unrelated Source cases and reject task capture promotions."""
import copy
import json
from pathlib import Path
import unittest

import retained_c105_guide_startup_contracts as subject
import source_contracts
from typed_registry_test_fixture import historical_typed_registry

ROOT=Path(__file__).resolve().parents[2]


class C105GuideStartup(unittest.TestCase):
    def test_exact_three_typed_original_tasks(self):
        packet=json.loads((ROOT/subject.INPUT).read_bytes())
        subject.validate(ROOT,packet)
        self.assertEqual(packet['actual_Original_worker_observations'],3)
        after=json.loads((ROOT/'docs/rust-contract-register.json').read_bytes())['non_HTTP_candidates']
        prior=copy.deepcopy(after)
        subject.restore_pre_c105_startup(prior)
        self.assertEqual({old['candidate_id'] for old,new in zip(prior,after,strict=True) if old!=new},
                         {row[0] for row in subject.IDENTITIES})
        with historical_typed_registry(ROOT, 21) as fixture:
            mapping = source_contracts.apply_non_HTTP(fixture, prior)
        self.assertEqual(mapping['candidates'], 21)
        self.assertEqual(mapping['fully_verified_added'], 0)
        self.assertEqual(prior,after)

    def test_rejects_per_task_pid_terminal_and_network_promotion(self):
        packet=subject.derive(ROOT)
        for mutation in (
            lambda value: value['fixtures'][subject.FIXTURE].update(per_task_worker_prerun_postrun_witness=True),
            lambda value: value['non_HTTP_contracts'][1]['observations'][0].update(backend_result=None),
            lambda value: value['non_HTTP_contracts'][2]['result'].update(fully_verified=True),
        ):
            updated=copy.deepcopy(packet)
            mutation(updated)
            with self.assertRaises(AssertionError):
                subject.validate(ROOT,updated)
