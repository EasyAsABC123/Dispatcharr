"""Bounded packaged range/progress witness; original authors retain all credit."""
import hashlib,time
from urllib.request import Request,urlopen

def capture(base,source,inspect):
    events=[]
    for case in source['cases']:
        path='/proxy/vod/movie/a4d78cd1-bf9f-4a63-a746-f40566825cf9/'+case['session_id']
        with urlopen(Request(base+path,headers={'Range':case['range'],'User-Agent':'fixture-progress-edge'}),timeout=15) as response:
            body=response.read(1_000_001)
            if len(body)>1_000_000:raise RuntimeError('Progress capture exceeds1MB')
            state=inspect(case['session_id'])
            events.append({'case':case['case'],'status':response.status,'bytes':len(body),'sha256':hashlib.sha256(body).hexdigest(),'bytes_sent':state['bytes_sent'],'metadata_present_at_eof':state['exists']})
        deadline=time.monotonic()+4
        while inspect(case['session_id'])['exists']:
            if time.monotonic()>deadline:raise RuntimeError('Progress fixture didnotcleanup')
            time.sleep(.05)
    differences=[]
    for expected,actual in zip(source['cases'],events):
        for key in ['case','status','bytes','sha256','bytes_sent']:
            if expected[key]!=actual[key]:differences.append({'case':expected['case'],'field':key,'source':expected[key],'candidate':actual[key]})
        if not actual['metadata_present_at_eof']:differences.append({'case':expected['case'],'field':'metadata unavailable at EOFsample'})
    return {'format_version':1,'scope':'Packaged actualrangebody progress boundary','capture_complete':len(events)==3,'events':events,'source_progress_matches':not differences,'differences':differences,'application_parity_verified':False,'performance_verified':False}
