"""Check explicit Source scopes before the register consumes their bindings."""
import copy
from pathlib import Path
import unittest

import retained_logo_m3u_contracts as subject
from source_contracts import checked

ROOT=Path(__file__).resolve().parents[2]


class LogoAndM3U(unittest.TestCase):
    def test_both_actual_scopes_keep_roles_methods_and_legacy_error_partial(self):
        packet=subject.load(ROOT)
        subject.validate(ROOT,packet)
        self.assertEqual(len(packet['contracts']),27)
        self.assertEqual(sum(len(row['cases']) for row in packet['contracts']),108)
        self.assertEqual(sum(row['result']['known_Original_legacy_error_cells'] for row in packet['contracts']),1)
        unresolved=[row for row in packet['contracts'] if row['method'] is None]
        self.assertEqual({(row['resolver_ordinal'],tuple(row['observed_methods'])) for row in unresolved},
                         {(31,('DELETE',)),(32,('POST',))})
        self.assertTrue(all(row['source_action'] is None for row in unresolved))
        self.assertTrue(all(row['fully_verified'] is False and not any(
            role['complete'] for role in row['source_role_specs'].values()) for row in packet['contracts']))

    def test_exact_upload_error_alias_roles_and_owned_cleanup_cannot_be_promoted(self):
        receipt=checked(ROOT,{'path':subject.RECEIPT,'sha256':subject.RECEIPT_SHA})
        logo=next(row for row in receipt['cohorts']['logo']['observations'] if row['name']=='upload_json')
        scope=next(row for row in subject.scopes('logo') if row[0]=='upload_json')
        for changed in ({'actual_status':201},{'actual_body_sha256':'0'*64},{'role':'standard'}):
            observation=copy.deepcopy(logo)
            observation['cases'][-1].update(changed)
            with self.assertRaises(AssertionError):
                subject.check_operation('logo',observation,scope)
        m3u=next(row for row in receipt['cohorts']['m3u']['observations'] if row['name']=='profiles_retrieve_json')
        scope=next(row for row in subject.scopes('m3u') if row[0]=='profiles_retrieve_json')
        with self.assertRaises(AssertionError):
            subject.check_operation('m3u',{**m3u,'actual_path':m3u['actual_path'].removesuffix('.json')+'/'},scope)
        with self.assertRaises(AssertionError):
            subject.check_cohort('m3u',{**receipt['cohorts']['m3u'],'physical_cleanup':False})
