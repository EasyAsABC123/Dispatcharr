import copy
from pathlib import Path
import unittest

import retained_hdhr_literal_contracts as subject
from source_contracts import checked

ROOT=Path(__file__).resolve().parents[2]


class LiteralHDHomeRun(unittest.TestCase):
    def test_all_physical_cells_and_unresolved_discovery_declarations(self):
        packet=subject.load(ROOT)
        subject.validate(ROOT,packet)
        self.assertEqual(sum(len(row['cases']) for row in packet['contracts']),57)
        unresolved=[row for row in packet['contracts'] if row['method'] is None]
        self.assertEqual({row['resolver_ordinal'] for row in unresolved},{365,367,370})
        self.assertTrue(all(row['source_action'] is None and row['observed_methods']==['GET'] for row in unresolved))

    def test_role_path_sql_and_native_promotions_fail(self):
        receipt=checked(ROOT,{'path':subject.RECEIPT,'sha256':subject.RECEIPT_SHA})
        row=copy.deepcopy(receipt['rows'][0])
        scope=subject.scopes()[0]
        for alteration in (lambda r:r['cases'][0].update(role='standard'),
                           lambda r:r.update(actual_path='/api/hdhr/devices/2.json'),
                           lambda r:r['proof'].update(selected_sql_state_matches=False)):
            changed=copy.deepcopy(row)
            alteration(changed)
            with self.assertRaises(AssertionError):
                subject.observed(changed,scope)
        promoted=copy.deepcopy(receipt)
        promoted['Native_verified']=True
        with self.assertRaises(AssertionError):
            subject.qualification(promoted)
