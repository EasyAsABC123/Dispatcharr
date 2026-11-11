"""Queued quiet-window tests; run only after the load trial finishes."""
import base64
import hashlib
import hmac
import json
import time
import unittest
from unittest.mock import patch
import startup_probes


def signed(kind,identity='7',jti=None,*,iat=None,exp=None):
    def encode(value):
        return base64.urlsafe_b64encode(json.dumps(value,separators=(',',':')).encode()).decode().rstrip('=')
    now=int(time.time()) if iat is None else iat
    body=encode({'alg':'HS256','typ':'JWT'})+'.'+encode({'token_type':kind,'user_id':identity,
            'jti':jti or 'fixture-'+kind,'iat':now,'exp':exp if exp is not None else now+(1800 if kind=='access' else 86400)})
    return body+'.'+base64.urlsafe_b64encode(hmac.new(b'playback-fixture-only',body.encode(),hashlib.sha256).digest()).decode().rstrip('=')


class StartupTests(unittest.TestCase):
    def setUp(self):
        clock=patch('startup_probes.time.time',return_value=int(time.time()))
        clock.start()
        self.addCleanup(clock.stop)

    def test_normalization_requires_signature_claims_and_distinct_token_ids(self):
        pair={'access':signed('access'),'refresh':signed('refresh')}
        original=dict(pair)
        normalized=startup_probes.token_pair(pair,7)
        self.assertEqual(normalized,{'access':'<verified-fixture-access>','refresh':'<verified-fixture-refresh>'})
        self.assertEqual(pair,original)
        for invalid in [{'access':signed('access','8'),'refresh':signed('refresh')},
                        {'access':signed('access',7),'refresh':signed('refresh')},
                        {'access':signed('access',jti='same'),'refresh':signed('refresh',jti='same')},
                        {'access':pair['access']+'invalid','refresh':pair['refresh']}]:
            with self.assertRaises(ValueError):startup_probes.token_pair(invalid,7)

    def test_pair_timing_is_anchored_to_refresh_including_second_boundary(self):
        now=int(time.time())-2
        with patch('startup_probes.time.time',return_value=now+2):
            pair={'access':signed('access',iat=now+1,exp=now+1800),
                  'refresh':signed('refresh',iat=now,exp=now+86400)}
            self.assertEqual(startup_probes.token_pair(pair,7)['access'],'<verified-fixture-access>')
            invalid=[
                # Old independent lifetime relation passes, actual pair anchor fails.
                {'access':signed('access',iat=now+1,exp=now+1801),'refresh':pair['refresh']},
                {'access':pair['access'],'refresh':signed('refresh',iat=now,exp=now+86399)},
                {'access':signed('access',iat=now-1,exp=now+1800),'refresh':pair['refresh']},
                {'access':signed('access',iat=True,exp=now+1800),'refresh':pair['refresh']},
                {'access':signed('access',iat=now+1,exp=str(now+1800)),'refresh':pair['refresh']},
                {'access':signed('access',iat=now-40,exp=now+1760),
                 'refresh':signed('refresh',iat=now-40,exp=now-40+86400)},
            ]
            for value in invalid:
                with self.subTest(value_number=invalid.index(value)):
                    with self.assertRaises(ValueError):startup_probes.token_pair(value,7)
        # Exact lifetime relationships alone cannot authorize already-expired claims.
        with patch('startup_probes.time.time',return_value=now+1801):
            with self.assertRaises(ValueError):startup_probes.token_pair(pair,7)

    @patch('startup_probes.time.sleep', return_value=None)
    def test_probe_capture_does_not_emit_credentials_or_exception_messages(self, pause):
        secret='fixture-sensitive-message'
        def broken(*args,**kwargs):raise RuntimeError(secret)
        accounts={role:{'access':secret,'username':'rust-probe-'+role,'password':secret,'id':index+1}
                  for index,role in enumerate(['admin','standard','viewer','inactive'])}
        rows=[]
        startup_probes.run(broken,'http://127.0.0.1:1','http://127.0.0.1:2',accounts,
                           lambda name,passed,details:rows.append((name,passed,details)))
        self.assertGreater(len(rows),20)
        self.assertTrue(all(not row[1] for row in rows))
        self.assertNotIn(secret,json.dumps(rows))
        self.assertTrue(all(row[2]=={'error_type':'RuntimeError'} for row in rows))
        self.assertEqual([call.args for call in pause.call_args_list],[(31,),(31,)])

    @patch('startup_probes.time.sleep', return_value=None)
    def test_bootstrap_posts_require_a_visible_existing_fixture_administrator(self, pause):
        calls=[];rows=[]
        def denied(url,**kwargs):
            calls.append((url,kwargs.get('method')))
            return 403,{'detail':'Permission denied'}
        accounts={role:{'access':'fixture-only','username':'rust-probe-'+role,'password':'fixture-only','id':index+1}
                  for index,role in enumerate(['admin','standard','viewer','inactive'])}
        startup_probes.run(denied,'http://127.0.0.1:1','http://127.0.0.1:2',accounts,
                           lambda name,passed,details:rows.append((name,passed,details)))
        self.assertFalse(any('initialize-superuser' in url and method=='POST' for url,method in calls))
        self.assertIn(('startup:configured-status-fixture',False,{'error_type':'RuntimeError'}),rows)


if __name__=='__main__':unittest.main()
