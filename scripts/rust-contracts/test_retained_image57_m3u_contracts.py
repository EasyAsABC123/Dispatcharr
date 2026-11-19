import copy
from pathlib import Path
import unittest

import retained_image57_m3u_contracts as subject

ROOT = Path(__file__).resolve().parents[2]


class Image57M3U(unittest.TestCase):
    def test_exact_original_roles_and_stored_state_controls(self):
        packet = subject.load(ROOT)
        subject.validate(ROOT, packet)
        rows = packet['contracts']
        self.assertEqual(len(rows), 8)
        self.assertEqual(sum(len(row['cases']) for row in rows), 37)
        self.assertEqual({case['role'] for row in rows for case in row['cases']},
                         {'admin', 'standard', 'viewer'})
        self.assertEqual({row['method'] for row in rows}, {'POST', 'DELETE', 'PATCH', 'PUT'})
        self.assertTrue(all(not row['fully_verified'] and
                            not any(spec['complete'] for spec in row['source_role_specs'].values())
                            for row in rows))

    def test_alias_role_or_unscoped_response_payload_rejected(self):
        packet = subject.load(ROOT)
        alterations = ({'path_template': '/api/m3u/server-groups.json'},
                       {'role': 'admin'}, {'response_body': '[discarded fixture value]'})
        for alteration in alterations:
            with self.subTest(fields=tuple(alteration)):
                altered = copy.deepcopy(packet)
                altered['contracts'][0]['cases'][1].update(alteration)
                with self.assertRaises(AssertionError):
                    subject.validate(ROOT, altered)
