import unittest
import copy
import hashlib
import json
from pathlib import Path
from urllib.parse import parse_qsl,urlsplit
from direct_probe import normalized_location,compare


class DirectEvidence(unittest.TestCase):
    def test_json_representation_may_differ_but_auth_cache_redirect_and_bytes_must_match(self):
        source=json.loads((Path(__file__).parent/'python-direct-auth-goldens.json').read_text())
        candidate=copy.deepcopy(source)
        for event in candidate['events']:
            if event['headers'].get('content-type')=='application/json':
                event['body']=json.dumps(json.loads(event['body']),sort_keys=True,separators=(',',':'))
                encoded=event['body'].encode();event['bytes']=len(encoded);event['sha256']=hashlib.sha256(encoded).hexdigest()
                event['headers']['content-length']=str(len(encoded))
        for event in candidate['events']:
            if 'allow' in event['headers']:event['headers']['allow']=', '.join(reversed(event['headers']['allow'].split(', ')))
        self.assertTrue(compare(source,candidate)['captured_direct_auth_matches'])
        for field in ['json_shape','json_length','capability','cache','timestamp','media','method']:
            modified=copy.deepcopy(source)
            by_name={event['case']:event for event in modified['events']}
            if field=='json_shape':by_name['global-network-denial']['body']='{"detail":"Forbidden"}'
            if field=='json_length':by_name['missing-auth']['headers']['content-length']='0'
            if field=='capability':by_name['mint-is-not-api-capability']['status']=200
            if field=='cache':by_name['mint-proxy-header']['headers']['cache-control']='public, max-age=21600'
            if field=='timestamp':by_name['bare-redirect']['headers']['location']=by_name['bare-redirect']['headers']['location'].replace('T11:00:00Z',':11-00')
            if field=='media':by_name['authenticated-client-session']['sha256']='wrong'
            if field=='method':by_name['missing-auth']['headers']['allow']='GET, POST, OPTIONS'
            self.assertFalse(compare(source,modified)['captured_direct_auth_matches'],field)
    def test_tokens_sessions_and_provider_credentials_are_redacted_without_reordering(self):
        value=normalized_location('/proxy/catchup/channel?foo=one&token=private.jwt.value&foo=two&session_id=opaque')
        self.assertNotIn('private.jwt.value',value)
        self.assertNotIn('opaque',value)
        self.assertEqual(parse_qsl(urlsplit(value).query),[
            ('foo','one'),('token','<access-token>'),('foo','two'),('session_id','<session>')])
        value=normalized_location('http://origin:8080/timeshift/providername/providersecret/35/2026-11-09T11:00:00Z/fixture.ts')
        self.assertNotIn('providername',value)
        self.assertNotIn('providersecret',value)
        self.assertIn('/35/2026-11-09T11:00:00Z/fixture.ts',value)


if __name__=='__main__':unittest.main()
