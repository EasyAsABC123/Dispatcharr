"""Draft exact guide network replay; no programme/order/time normalization.
Original Dispatcharr authors and contributors retain all application credit.
"""
import argparse
import datetime
import hashlib
import json
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import HTTPRedirectHandler, Request, build_opener

class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, *args):
        return None

def capture(base, fixture, packaged=False):
    anchor = datetime.datetime.fromisoformat(fixture['anchor'])
    hour = datetime.datetime.fromisoformat(fixture['utc_hour'])
    def guard():
        now = datetime.datetime.now(datetime.timezone.utc)
        if not 0 <= (now-anchor).total_seconds() < 300 or now.replace(minute=0,second=0,microsecond=0) != hour:
            raise RuntimeError('Expired or UTC-hour-crossing guide wire fixture')
        return now.isoformat()
    opener = build_opener(NoRedirect())
    cases = []
    differences = []
    selected = ['Content-Length', 'Allow', 'WWW-Authenticate']
    if packaged:
        selected += ['Vary', 'X-Content-Type-Options', 'X-Frame-Options',
                     'Referrer-Policy', 'Cross-Origin-Opener-Policy']
    for index, case in enumerate(fixture['cases']):
        before = guard()
        headers = {key.removeprefix('HTTP_').replace('_','-'): value
                   for key,value in case['headers'].items()}
        request = Request(base+case['path'], headers=headers, method=case['method'])
        try:
            response = opener.open(request, timeout=15)
        except HTTPError as error:
            response = error
        with response:
            body = response.read(8*1024*1024+1)
            if len(body)>8*1024*1024:
                raise RuntimeError('Owned guide response exceeds fixture budget')
            actual = {'status':response.status,'body_bytes':len(body),
                'body_sha256':hashlib.sha256(body).hexdigest(),
                'content_type':response.getheader('Content-Type'),
                'selected_headers':{name:response.getheader(name) for name in selected}}
        expected = {key:case['expected'][key] for key in ['status','body_bytes','body_sha256','content_type']}
        expected['selected_headers']={name:case['expected']['selected_headers'][name] for name in selected}
        if actual != expected:
            differences.append({'index':index,'method':case['method'],'role':case['role'],
                'path':case['path'],'expected':expected,'actual':actual})
        cases.append({'index':index,'method':case['method'],'role':case['role'],
            'before':before,'after':guard(),**actual})
    return {'scope':'Packaged network guide replay' if packaged else 'Independent native-network guide replay; app middleware headers excluded',
        'passed':len(cases)==43 and not differences,'cases':cases,'differences':differences,
        'selected_header_names':selected,'normalization':None,
        'application_parity_verified':False,'performance_verified':False}

if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--base',required=True)
    parser.add_argument('--fixture',type=Path,required=True)
    parser.add_argument('--packaged',action='store_true')
    args=parser.parse_args()
    result=capture(args.base,json.loads(args.fixture.read_text()),args.packaged)
    print(json.dumps(result,indent=2))
    raise SystemExit(0 if result['passed'] else 1)
