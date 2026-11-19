"""Protect the exact two scoped Original worker assignments."""
import copy
import json
from pathlib import Path
import unittest

import retained_clocked_recording_contracts as subject
import source_contracts

ROOT = Path(__file__).resolve().parents[2]


class RetainedClockedRecordingControls(unittest.TestCase):
    def test_assignments_preserve_the_remaining_non_http_rows(self):
        packet = json.loads((ROOT/subject.BINDING_PATH).read_bytes())
        subject.validate(ROOT, packet)
        self.assertEqual(packet['contracts'], [])
        expected = json.loads((ROOT/'docs/rust-contract-register.json').read_bytes())['non_HTTP_candidates']
        prior = copy.deepcopy(expected)
        subject.restore_pre_clocked_recording(ROOT, prior)
        self.assertEqual({old['candidate_id'] for old, new in zip(prior, expected, strict=True) if old != new},
                         {row[0] for row in subject.IDENTITIES})
        self.assertEqual(source_contracts.apply_non_HTTP(ROOT, prior)['candidates'], 18)
        self.assertEqual(prior, expected)

    def test_rejects_promoting_unconsumed_tasks_or_rewriting_backend(self):
        canonical = subject.derive(ROOT)
        for mutation in (
            lambda value: value.update(actual_Original_worker_observations=4),
            lambda value: value['non_HTTP_contracts'][1]['observations'][0].update(backend_result='ok'),
        ):
            corrupt = copy.deepcopy(canonical)
            mutation(corrupt)
            with self.assertRaises(AssertionError):
                subject.validate(ROOT, corrupt)
