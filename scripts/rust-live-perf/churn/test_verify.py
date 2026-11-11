"""Corruption controls using the complete published actual recording."""
import copy
import importlib.util
import json
from pathlib import Path
import unittest

HERE = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location('churn_numeric', HERE / 'verify.py')
checker = importlib.util.module_from_spec(spec)
spec.loader.exec_module(checker)


class ChurnEvidence(unittest.TestCase):
    def setUp(self):
        self.data = json.loads((HERE / 'projection.json').read_text())

    def test_complete(self):
        self.assertEqual(checker.verify(self.data)['readers'], 66)

    def test_actual_record_corruptions(self):
        mutations = [
            lambda d: d['report']['phases'].pop(),
            lambda d: d['report']['phases'][0]['runtime'].update(id='replacement'),
            lambda d: d['report']['phases'][0].update(source_bound_image='wrong'),
            lambda d: d['report']['phases'][0]['cleanup_state'].update(provider_slots=1),
            lambda d: d['report']['phases'][0]['cleanup_state'].update(clients=1),
            lambda d: d['report']['phases'][0]['authenticated_identity'].update(all_owned_user_id=False),
            lambda d: d['report']['phases'][0]['viewers'][0].update(unexpected_eof=True),
            lambda d: d['report']['phases'][0]['viewers'][0].update(decoder_frames=0),
            lambda d: d['report'].update(performance_verified=True),
            lambda d: d['parent_review']['remaining_owned_resources']['m2-live-churn-image47-1']['containers'].append('leftover'),
        ]
        for index, mutate in enumerate(mutations):
            with self.subTest(corruption=index):
                changed = copy.deepcopy(self.data)
                mutate(changed)
                with self.assertRaises(ValueError):
                    checker.verify(changed)


if __name__ == '__main__':
    unittest.main(verbosity=2)
