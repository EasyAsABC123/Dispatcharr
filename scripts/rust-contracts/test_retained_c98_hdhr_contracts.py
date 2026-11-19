import copy
from pathlib import Path
import unittest

import retained_c98_hdhr_contracts as subject

ROOT = Path(__file__).resolve().parents[2]


class C98HDHRAndReact(unittest.TestCase):
    def test_exact_route_methods_roles_and_literal_format(self):
        packet = subject.load(ROOT)
        subject.validate(ROOT, packet)
        rows = {row['resolver_ordinal']: row for row in packet['contracts']}
        self.assertEqual({ordinal:len(row['cases']) for ordinal,row in rows.items()},
                         {177:1,179:2,180:1,364:1,590:6})
        self.assertEqual(rows[180]['cases'][0]['path_template'], '/api/hdhr/devices/{owned_pk}.json')
        self.assertIsNone(rows[364]['method'])
        self.assertIsNone(rows[590]['method'])
        self.assertEqual(rows[590]['observed_methods'], ['GET','HEAD'])
        self.assertEqual(rows[590]['observed_roles'], [subject.INVALID])
        self.assertTrue(all(not row['fully_verified'] and
                            not any(role['complete'] for role in row['source_role_specs'].values())
                            for row in rows.values()))

    def test_reject_role_alias_and_scope_promotions(self):
        packet = subject.load(ROOT)
        for index,change in ((0,{'role':'admin'}),
                              (1,{'path_template':'/hdhr/devices/{owned_pk}/'}),
                              (2,{'path_template':'/api/hdhr/devices/{owned_pk}/'}),
                              (4,{'method':'POST'})):
            changed = copy.deepcopy(packet)
            changed['contracts'][index]['cases'][0].update(change)
            with self.assertRaises(AssertionError):
                subject.validate(ROOT, changed)
