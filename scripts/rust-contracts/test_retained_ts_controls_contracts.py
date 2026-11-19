"""Exact Original TS controls receipt and scope guards."""
import copy
from pathlib import Path
import unittest

import retained_ts_controls_contracts as subject

ROOT = Path(__file__).resolve().parents[2]


class TsControls(unittest.TestCase):
    def test_positive_and_two_failed_pre_compose_attempts(self):
        packet = subject.load(ROOT)
        subject.validate(ROOT,packet)
        self.assertEqual([c['resolver_ordinal'] for c in packet['contracts']],[388,390])
        self.assertEqual([c['result']['Source_case_observations'] for c in packet['contracts']],[5,2])
        for case in packet['contracts']:
            self.assertIsNone(case['method'])
            self.assertFalse(case['fully_verified'])
        self.assertEqual(packet['fixtures'][subject.FIXTURE]['prior_pre_Compose_failures'],2)

    def test_method_role_path_and_scope_cannot_be_promoted(self):
        actual = subject.load(ROOT)
        for key,value in [('method','GET'),('route','status'),('status',204),('role','anonymous')]:
            with self.subTest(key=key):
                altered = copy.deepcopy(actual)
                target = altered['contracts'][1]['cases'][-1]
                target['path' if key=='route' else key] = ('/proxy/ts/status/'+target['path'].rsplit('/',1)[1]
                                                       if key=='route' else value)
                with self.assertRaises(AssertionError):
                    subject.validate(ROOT,altered)
        for key,value in [('actual_source_controller_exit',1),('native_execution_claimed',True),
                          ('prior_pre_Compose_failures',0)]:
            with self.subTest(key=key):
                altered = copy.deepcopy(actual)
                altered['fixtures'][subject.FIXTURE][key] = value
                with self.assertRaises(AssertionError):
                    subject.validate(ROOT,altered)
