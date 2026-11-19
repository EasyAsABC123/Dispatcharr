"""Check the exact scope of retained EPG and VOD logo Primary bindings."""
import copy
from pathlib import Path
import unittest

import retained_epg_vod_logo_contracts as subject
from source_contracts import checked

ROOT = Path(__file__).resolve().parents[2]


class EpgAndVodLogo(unittest.TestCase):
    def test_exact_methods_roles_query_and_original_failures_remain_partial(self):
        packet = subject.load(ROOT)
        subject.validate(ROOT, packet)
        self.assertEqual(len(packet['contracts']), 27)
        self.assertEqual(sum(len(row['cases']) for row in packet['contracts']), 108)
        self.assertEqual(sum(row['result']['known_Original_legacy_error_cells'] for row in packet['contracts']), 5)
        self.assertTrue(all(row['method'] is not None and row['fully_verified'] is False
                            and all(role['complete'] is False for role in row['source_role_specs'].values())
                            for row in packet['contracts']))

    def test_literal_poster_programme_error_query_and_lifecycle_are_required(self):
        receipt = checked(ROOT, {'path': subject.RECEIPT, 'sha256': subject.RECEIPT_SHA})
        for kind, name, faults in (
            ('epg', 'poster_json', ({'actual_status': 200}, {'actual_body_sha256': '0'*64}, {'role': 'viewer'})),
            ('epg', 'programs_create', ({'actual_status': 201}, {'actual_body_sha256': '0'*64})),
        ):
            operation = next(row for row in receipt['cohorts'][kind]['observations'] if row['name'] == name)
            scope = next(row for row in subject.scopes(kind) if row[0] == name)
            for changed in faults:
                candidate = copy.deepcopy(operation)
                candidate['cases'][-1].update(changed)
                with self.assertRaises(AssertionError):
                    subject.check_operation(kind, candidate, scope)
        vod = next(row for row in receipt['cohorts']['vod']['observations'] if row['name'] == 'json_list')
        scope = next(row for row in subject.scopes('vod') if row[0] == 'json_list')
        with self.assertRaises(AssertionError):
            subject.check_operation('vod', {**vod, 'actual_path': '/api/vod/vodlogos.json'}, scope)
        with self.assertRaises(AssertionError):
            subject.check_cohort('vod', {**receipt['cohorts']['vod'], 'physical_cleanup': False})
