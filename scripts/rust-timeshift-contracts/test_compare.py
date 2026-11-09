import copy
import json
from pathlib import Path
import unittest
from compare import compare_media,compare_sessions
class Comparison(unittest.TestCase):
    def test_media_does_not_normalize_payload_quota_or_provider_fallback(self):
        source=json.loads((Path(__file__).parent/'python-media-goldens.json').read_text())
        candidate=copy.deepcopy(source)
        candidate['events'][0]['elapsed_seconds']=999
        candidate['events'][0]['after_body']['api_ttl']=580
        self.assertTrue(compare_media(source,candidate)['captured_media_matches'])
        for field in ['payload','quota','format','ttl','handshake_ttl']:
            modified=copy.deepcopy(source)
            if field=='payload':modified['events'][0]['sha256']='wrong'
            if field=='quota':modified['events'][0]['after_grace']['profile_connections']=1
            if field=='format':modified['events'][0]['origin']['requests'][0]['shape']='query'
            if field=='ttl':modified['events'][0]['after_body']['api_ttl']=9999
            if field=='handshake_ttl':modified['events'][0]['after_body']['api_ttl']=60
            self.assertFalse(compare_media(source,modified)['captured_media_matches'],field)
    def test_session_methods_are_set_but_auth_failure_status_and_body_are_exact(self):
        source=json.loads((Path(__file__).parent/'python-session-goldens.json').read_text())
        candidate=copy.deepcopy(source)
        for event in candidate['events']:
            if event.get('allow'):event['allow']=', '.join(reversed(event['allow'].split(', ')))
        self.assertTrue(compare_sessions(source,candidate)['captured_wire_matches'])
        candidate['events'][0]['status']=200
        self.assertFalse(compare_sessions(source,candidate)['captured_wire_matches'])
if __name__=='__main__':unittest.main()
