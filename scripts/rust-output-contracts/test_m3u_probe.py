"""Evidence gates must reject export-byte, cache, event and length regressions."""
import copy
import json
from pathlib import Path
import unittest
from m3u_probe import EXACT_DENIAL_CASES, compare


class M3uAcceptance(unittest.TestCase):
    def setUp(self):self.source=json.loads((Path(__file__).parent/'python-m3u-goldens.json').read_text())
    def test_exact_captured_source_matches(self):self.assertTrue(compare(self.source,copy.deepcopy(self.source))['captured_m3u_http_matches'])
    def test_extra_or_missing_case_cannot_pass(self):
        for delta in ['extra','missing']:
            candidate=copy.deepcopy(self.source)
            if delta=='extra':candidate['events'].append(copy.deepcopy(candidate['events'][0]))
            else:candidate['events'].pop()
            self.assertFalse(compare(self.source,candidate)['captured_m3u_http_matches'])
    def test_export_hash_and_cache_staleness_and_audit_are_compared(self):
        for mutation in ['bytes','cache','audit']:
            candidate=copy.deepcopy(self.source)
            if mutation=='bytes':candidate['events'][0]['sha256']='changed'
            if mutation=='cache':candidate['two_second_cache']['expired_body_changed']=False
            if mutation=='audit':candidate['system_event_counts']['m3u_download']-=1
            self.assertFalse(compare(self.source,candidate)['captured_m3u_http_matches'])
    def test_known_json_denials_require_exact_body_and_content_length(self):
        for case in EXACT_DENIAL_CASES:
            with self.subTest(case=case):
                candidate = copy.deepcopy(self.source)
                event = next(value for value in candidate['events'] if value['case'] == case)
                event['bytes'] -= 1
                event['headers']['content-length'] = str(event['bytes'])
                event['sha256'] = 'different JSON whitespace'
                self.assertFalse(compare(self.source,candidate)['captured_m3u_http_matches'])
                fixture = copy.deepcopy(self.source)
                event = next(value for value in fixture['events'] if value['case'] == case)
                event['headers']['content-length'] = str(event['bytes'] + 1)
                self.assertFalse(compare(fixture,fixture)['captured_m3u_http_matches'])
