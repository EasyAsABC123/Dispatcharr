"""Exact retained VOD Source assignments and split receipt boundaries."""
import copy
from pathlib import Path
import unittest

import retained_exact_vod_contracts as subject

ROOT = Path(__file__).resolve().parents[2]


class VODSplit(unittest.TestCase):
    def test_original_roles_and_real_native_retry_mismatches(self):
        packet = subject.load(ROOT)
        subject.validate(ROOT,packet)
        fixture = packet['fixtures'][subject.FIXTURE]
        self.assertEqual((fixture['actual_initial_paired_launcher_exit'],
                          fixture['actual_native_retry_launcher_exit']), (1,0))
        self.assertEqual(fixture['candidate98_scoped_primary_status_mismatches'],5)
        self.assertFalse(fixture['Native_parity_claimed'])
        self.assertEqual([row['resolver_ordinal'] for row in packet['contracts']],[399,402,413,414])
        self.assertEqual(sum(len(row['cases']) for row in packet['contracts']),6)

    def test_reject_status_role_alias_and_phase_promotion(self):
        original = subject.load(ROOT)
        for label in ('wrong_role','missing_profile','missing_literal_extension','rewritten_exit','retry_marked_parity'):
            with self.subTest(label=label):
                candidate = copy.deepcopy(original)
                if label=='wrong_role':
                    candidate['contracts'][1]['cases'][1]['role']='admin'
                elif label=='missing_profile':
                    candidate['contracts'][0]['cases'][0]['path_template']='/proxy/vod/movie/{movie_uuid}/{private_session}'
                elif label=='missing_literal_extension':
                    candidate['contracts'][2]['cases'][0]['path_template']='/movie/{private_xc_username}/{private_xc_password}/{movie_pk}'
                elif label=='rewritten_exit':
                    candidate['fixtures'][subject.FIXTURE]['actual_initial_paired_launcher_exit']=0
                else:
                    candidate['fixtures'][subject.FIXTURE]['Native_parity_claimed']=True
                with self.assertRaises(AssertionError):
                    subject.validate(ROOT,candidate)
