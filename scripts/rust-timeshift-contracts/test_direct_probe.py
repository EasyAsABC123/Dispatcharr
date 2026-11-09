import unittest
from urllib.parse import parse_qsl,urlsplit
from direct_probe import normalized_location


class DirectEvidence(unittest.TestCase):
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
