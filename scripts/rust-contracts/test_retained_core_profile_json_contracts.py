import copy
from pathlib import Path
import unittest

import retained_core_profile_json_contracts as subject

ROOT=Path(__file__).resolve().parents[2]


class CoreProfileJSON(unittest.TestCase):
    def test_original_controls_keep_all_thirteen_literal_declarations_partial(self):
        packet=subject.load(ROOT)
        subject.validate(ROOT,packet)
        self.assertEqual(sum(len(row['cases']) for row in packet['contracts']),78)

    def test_reject_canonical_twin_and_promoted_native_or_role_claim(self):
        packet=subject.load(ROOT)
        for mutation in (lambda value: value.update(Native_coverage_increased=True),
                         lambda value: value['contracts'][0]['cases'][0].update(path_template='/api/core/useragents/'),
                         lambda value: value['contracts'][0]['source_role_specs']['viewer'].update(complete=True)):
            changed=copy.deepcopy(packet)
            mutation(changed)
            with self.assertRaises(AssertionError):
                subject.validate(ROOT,changed)
