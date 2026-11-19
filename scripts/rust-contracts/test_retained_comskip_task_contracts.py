"""Verify retained Comskip task scope and fail closed when its binding changes."""
import copy
import json
from pathlib import Path
import unittest

import retained_comskip_task_contracts as subject
import source_contracts


ROOT = Path(__file__).resolve().parents[2]
REGISTER = ROOT/'docs/rust-contract-register.json'


class RetainedComskipControls(unittest.TestCase):
    def test_exact_task_registration_and_idempotence(self):
        value = json.loads((ROOT/subject.BINDING_PATH).read_bytes())
        subject.validate(ROOT, value)
        self.assertEqual(value['contracts'], [])
        [contract] = value['non_HTTP_contracts']
        self.assertEqual(contract['candidate_id'], subject.IDENTIFIER)
        self.assertEqual(contract['result']['actual_worker_observations'], 6)
        self.assertFalse(contract['fully_verified'])
        expected = json.loads(REGISTER.read_bytes())['non_HTTP_candidates']
        earlier = copy.deepcopy(expected)
        subject.restore_pre_comskip_task(ROOT, earlier)
        changed = [old['candidate_id'] for old, new in zip(earlier, expected, strict=True) if old != new]
        self.assertEqual(changed, [subject.IDENTIFIER])
        self.assertEqual(source_contracts.apply_non_HTTP(ROOT, earlier)['candidates'], 16)
        self.assertEqual(earlier, expected)

    def test_rejects_reclassifying_failed_controller_or_repeat_effect(self):
        original = subject.derive(ROOT)
        for name, modify in (
            ('controller exit', lambda value: next(iter(value['fixtures'].values()))['literal_controller_receipts']['1'].update(actual_exit=0)),
            ('repeat event count', lambda value: value['non_HTTP_contracts'][0]['observations'][1].update(Comskip_websocket_events=1)),
        ):
            with self.subTest(name=name):
                value = copy.deepcopy(original)
                modify(value)
                with self.assertRaises(AssertionError):
                    subject.validate(ROOT, value)
