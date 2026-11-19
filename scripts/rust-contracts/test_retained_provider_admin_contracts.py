import copy
from pathlib import Path
import unittest

import retained_provider_admin_contracts as subject

ROOT = Path(__file__).resolve().parents[2]


class ProviderAdmin(unittest.TestCase):
    def test_retained_original_declared_methods_and_scope(self):
        packet = subject.load(ROOT)
        subject.validate(ROOT, packet)
        selected = {row['capture_case']: row for row in packet['contracts']}
        self.assertEqual({scope: len(row['cases']) for scope, row in selected.items()},
                         {'profile': 4, 'timezone': 6, 'useragent': 3})
        self.assertIsNone(selected['timezone']['method'])
        self.assertEqual(selected['timezone']['observed_methods'], ['GET'])
        self.assertEqual({case['role'] for case in selected['timezone']['cases']},
                         {'admin', 'standard', 'viewer', 'inactive', 'anonymous'})
        self.assertEqual(packet['fixtures'][subject.fixture_id('profile')]['actual_controller_exit'], 1)
        self.assertTrue(all(not row['fully_verified'] and
                            not any(spec['complete'] for spec in row['source_role_specs'].values())
                            for row in selected.values()))

    def test_reject_alias_role_and_failed_outer_run_promotion(self):
        packet = subject.load(ROOT)
        changes = (
            lambda value: value['contracts'][0]['cases'][0].update(path_template='/api/m3u/accounts/1/profiles/2.json'),
            lambda value: value['contracts'][0]['cases'][0].update(method='PATCH'),
            lambda value: value['contracts'][0]['cases'][0].update(role='viewer'),
            lambda value: value['fixtures'][subject.fixture_id('profile')].update(actual_controller_exit=0),
        )
        for change in changes:
            altered = copy.deepcopy(packet)
            change(altered)
            with self.assertRaises(AssertionError):
                subject.validate(ROOT, altered)
