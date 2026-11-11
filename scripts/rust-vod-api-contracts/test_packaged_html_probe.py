"""Pure comparison controls; synthetic records establish no backend parity."""
import copy
import datetime
import hashlib
from email.utils import format_datetime
import unittest
from packaged_html_probe import compare, ALPHABET


def fixture():
    now = datetime.datetime.now(datetime.timezone.utc).replace(microsecond=0)
    expiry = format_datetime(now + datetime.timedelta(seconds=31_449_600), usegmt=True)
    def row(secret, mask):
        token = mask * 32 + ALPHABET[(ALPHABET.index(secret) + ALPHABET.index(mask)) % 62] * 32
        body = '<html>safe payload "csrfToken": "' + token + '"</html>'
        cookie = 'csrftoken=' + secret * 32 + '; expires=' + expiry + '; Max-Age=31449600; Path=/; SameSite=Lax'
        return {'spec':'accept_HTML','path':'/api/vod/all/','accept':'text/html','role':'standard','method':'GET','repeat':0,'cookie_strategy':'fresh-no-cookie','status':200,'body_text':body,'body_bytes':len(body.encode()),'body_sha256':hashlib.sha256(body.encode()).hexdigest(),'request_cookie':'','content_type':'text/html; charset=utf-8','observed_utc':now.isoformat(),'raw_response_headers':[['Content-Type','text/html; charset=utf-8'],['Content-Length',str(len(body.encode()))],['Vary','Accept, origin, Cookie'],['Allow','GET, HEAD, OPTIONS'],['Set-Cookie',cookie]]}
    source = {'passed':True,'target':'Original','records':[row('a','c') for _ in range(56)],'fingerprints_before':{'owned':'same'},'fingerprints_after':{'owned':'same'}}
    native = copy.deepcopy(source);native['target']='candidate';native['records']=[row('b','d') for _ in range(56)]
    return source,native


class Controls(unittest.TestCase):
    def test_independent_valid_masks_and_cookies_preserve_deterministic_bytes(self):
        source,native=fixture();self.assertNotEqual(source['records'][0]['body_sha256'],native['records'][0]['body_sha256']);result=compare(source,native);self.assertTrue(result['passed']);self.assertEqual(result['compared'],56)
    def test_non_CSRF_body_change_is_not_normalized(self):
        source,native=fixture();row=native['records'][0];row['body_text']=row['body_text'].replace('safe','evil');row['body_sha256']=hashlib.sha256(row['body_text'].encode()).hexdigest();self.assertFalse(compare(source,native)['passed'])
    def test_wrong_mask_secret_and_extra_slot_are_rejected(self):
        source,native=fixture();row=native['records'][0];row['body_text']=row['body_text'].replace('e'*32,'f'*32);row['body_sha256']=hashlib.sha256(row['body_text'].encode()).hexdigest()
        with self.assertRaises(ValueError):compare(source,native)
        source,native=fixture();row=native['records'][0];row['body_text']+='"csrfToken": "';row['body_bytes']=len(row['body_text'].encode());row['body_sha256']=hashlib.sha256(row['body_text'].encode()).hexdigest()
        with self.assertRaises(ValueError):compare(source,native)
    def test_expiry_metadata_and_intrinsic_headers_are_not_ignored(self):
        source,native=fixture();native['records'][0]['observed_utc']=(datetime.datetime.now(datetime.timezone.utc)+datetime.timedelta(days=1)).isoformat()
        with self.assertRaises(ValueError):compare(source,native)
        source,native=fixture();native['records'][0]['raw_response_headers'][2][1]='Cookie';self.assertFalse(compare(source,native)['passed'])
        source,native=fixture();native['fingerprints_after']['owned']='changed'
        with self.assertRaises(AssertionError):compare(source,native)


if __name__=='__main__':unittest.main()
