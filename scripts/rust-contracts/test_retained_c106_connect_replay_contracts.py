"""Check the exact Connect roster and the narrow legacy response expectation."""
import copy
from pathlib import Path
import unittest

import retained_c106_connect_replay_contracts as subject
from source_contracts import checked

ROOT=Path(__file__).resolve().parents[2]


class ConnectReplay(unittest.TestCase):
    def test_all_twenty_bindings_preserve_partial_roles(self):
        packet=subject.load(ROOT)
        subject.validate(ROOT,packet)
        self.assertEqual(len(packet['contracts']),20)
        self.assertEqual(sum(len(row['cases']) for row in packet['contracts']),120)
        self.assertEqual(sum(row['result']['known_legacy_error_cells'] for row in packet['contracts']),2)
        self.assertTrue(all(row['fully_verified'] is False and not any(
            role['complete'] for role in row['source_role_specs'].values()) for row in packet['contracts']))

    def test_legacy_mime_role_body_and_status_cannot_be_generalized(self):
        receipt=checked(ROOT,{'path':subject.RECEIPT,'sha256':subject.RECEIPT_SHA})
        original=next(row for row in receipt['operations'] if row['resolver_ordinal']==337)
        declaration=next(row for row in subject.DECLARATIONS if row[0]=='nested-subscriptions')
        for update in ({'mime':'application/json'},{'actual_status':200},
                       {'raw_response_sha256':'0'*64},{'authentication_challenge_present':True}):
            changed=copy.deepcopy(original)
            changed['role_cases'][0]['Source'].update(update)
            with self.assertRaises(AssertionError):
                subject.observed(changed,declaration,'json')
        changed=copy.deepcopy(original)
        changed['role_cases'][2]['Source'].update(actual_status=500)
        with self.assertRaises(AssertionError):
            subject.observed(changed,declaration,'json')
