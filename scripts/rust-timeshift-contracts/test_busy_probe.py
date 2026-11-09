import copy
import json
from pathlib import Path
import unittest
from busy_probe import compare


class BusyComparison(unittest.TestCase):
    def test_wire_quota_old_generation_and_established_grace_are_required(self):
        source=json.loads((Path(__file__).parent/'python-busy-seek-goldens.json').read_text())
        # Exercise the comparator's explicit controlled-lifetime requirement.
        source['controlled_established_body_min_seconds']=2.5
        self.assertTrue(compare(source,copy.deepcopy(source))['captured_busy_controls_match'])
        for field in ['payload','tail_quota','generation','grace','lifetime','missing_case']:
            candidate=copy.deepcopy(source)
            if field=='payload':candidate['events'][0]['captured_sha256']='wrong'
            if field=='tail_quota':candidate['events'][2]['during_replacement']['profile_connections']=2
            if field=='generation':candidate['events'][0]['old_response_after_replacement_bytes']=2_000_001
            if field=='grace':candidate['events'][1]['after_grace']['api_exists']=True
            if field=='lifetime':candidate.pop('controlled_established_body_min_seconds')
            if field=='missing_case':candidate['events'].pop()
            self.assertFalse(compare(source,candidate)['captured_busy_controls_match'],field)


if __name__=='__main__':
    unittest.main()
