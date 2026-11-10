"""Replay actual original current-programme wire against a packaged candidate.
Original Dispatcharr authors/contributors retain full application credit.
"""
import datetime
import hashlib
import json
from urllib.request import Request, build_opener, HTTPRedirectHandler
from urllib.error import HTTPError
class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self,*args):return None

def capture(base,fixture):
    anchor=datetime.datetime.fromisoformat(fixture['anchor'])
    if not 0 <= (datetime.datetime.now(datetime.timezone.utc)-anchor).total_seconds() < 300:
        raise RuntimeError('Real-clock fixture exceeded its five-minute replay window')
    opener=build_opener(NoRedirect());cases=[];differences=[]
    for index,case in enumerate(fixture['cases']):
        body=case['raw_body'] if case['raw_body'] is not None else json.dumps(case['body'],ensure_ascii=False)
        headers={name.removeprefix('HTTP_').replace('_','-'):value for name,value in case['headers'].items()}
        headers['Content-Type']=case['content_type']
        request=Request(base+case['path'],data=body.encode(),headers=headers,method=case['method'])
        try:response=opener.open(request,timeout=15)
        except HTTPError as error:response=error
        with response:
            data=response.read(8*1024*1024+1)
            if len(data)>8*1024*1024:raise RuntimeError('Owned response unexpectedly large')
            actual={'status':response.status,'body_bytes':len(data),'body_sha256':hashlib.sha256(data).hexdigest(),
                'content_type':response.getheader('Content-Type'),'content_length':response.getheader('Content-Length')}
        # Content length is a representation length on HEAD; compare it too.
        # Other cases require exact entity bytes/type/status and their actual
        # emitted length, without timestamp, row or error-body normalization.
        if actual!=case['expected']:differences.append({'index':index,'method':case['method'],'role':case['role'],'expected':case['expected'],'actual':actual})
        cases.append({'index':index,'method':case['method'],'role':case['role'],**actual})
    return {'scope':'Packaged real-clock current-programme HTTP wire replay','passed':len(cases)==64 and not differences,
        'cases':cases,'differences':differences,'performance_verified':False,'application_parity_verified':False}
