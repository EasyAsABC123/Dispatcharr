"""Scoped retained management business and known-failure boundary tests."""
import copy
from pathlib import Path
import unittest

import retained_canonical_management_contracts as subject

ROOT = Path(__file__).resolve().parents[2]


class CanonicalManagement(unittest.TestCase):
    def test_exact_roles_and_original_program_failure(self):
        packet = subject.load(ROOT)
        subject.validate(ROOT,packet)
        self.assertEqual(len(packet['contracts']),16)
        self.assertEqual(sum(len(row['cases']) for row in packet['contracts']),50)
        known, = [row for row in packet['contracts'] if row['result']['known_Source_failure']]
        self.assertEqual((known['batch_case'],known['result']['actual_business_status']),('program_create_defect',500))
        self.assertIs(known['result']['Source_business_success'],False)
        self.assertIs(known['owned_state']['committed_create_row'],False)
        manager, = [row for row in packet['contracts'] if row['batch_case']=='recurring_patch']
        self.assertEqual(manager['observed_roles'],['anonymous','standard','dvr_manager'])

    def test_alias_role_and_program_success_promotions_are_rejected(self):
        packet = subject.load(ROOT)
        for label in ('formatted_route','unobserved_admin','normalize_program_error','committed_program'):
            with self.subTest(label=label):
                invalid = copy.deepcopy(packet)
                by_name = {row['batch_case']:row for row in invalid['contracts']}
                if label=='formatted_route':
                    by_name['logo_list']['cases'][0]['literal_path']='/api/channels/logos.json'
                elif label=='unobserved_admin':
                    by_name['recurring_patch']['source_role_specs']['admin']={'complete':True}
                elif label=='normalize_program_error':
                    by_name['program_create_defect']['result']['actual_business_status']=201
                else:
                    by_name['program_create_defect']['owned_state']['committed_create_row']=True
                with self.assertRaises(AssertionError):
                    subject.validate(ROOT,invalid)
