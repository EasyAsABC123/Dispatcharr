"""Original Dispatcharr idle-reopen characterization; no capability URLs saved."""
import hashlib
import json
import time
from pathlib import Path
from urllib.request import Request, urlopen
from urllib.error import HTTPError


def fetch(base, path, *, body=None, headers=None):
    request = Request(base+path, headers=headers or {},
                      data=json.dumps(body).encode() if body is not None else None)
    try:
        response = urlopen(request, timeout=20)
    except HTTPError as error:
        response = error
    with response:
        payload = response.read(2_000_001)
        if len(payload) > 2_000_000:
            raise ValueError('idle probe exceeds bounded capture')
        return response.status, {k.lower():v for k,v in response.headers.items()
            if k.lower() in ('content-type','content-length','content-range','accept-ranges')}, payload


def capture(base, origin, seed, output, observe, prefix="timeshift-idle"):
    status,_,_ = fetch(origin,'/archive-control',body={'mode':'all'},headers={'Content-Type':'application/json'})
    if status != 200:
        raise RuntimeError('idle probe origin reset failed')
    status,_,body = fetch(base,'/api/catchup/sessions/',body={
        'channel_uuid':seed['channel_uuid'],'start':'2026-11-09T11:00:00Z','duration':30},
        headers={'Content-Type':'application/json','Authorization':'Bearer '+seed['users']['owner']['token']})
    if status != 201:
        raise RuntimeError('idle probe capability mint failed')
    capability = json.loads(body)
    events = []
    for name,byte_range in [('startup-probe','bytes=0-255'),
                            ('established-reopen','bytes=0-1879999'),
                            ('seek-reopen','bytes=1880000-1880255')]:
        began=time.monotonic()
        status,headers,body=fetch(base,capability['playback_url'],headers={
            'Range':byte_range,'User-Agent':'fixture-idle-same-capability'})
        event={'case':name,'range':byte_range,'status':status,'headers':headers,
               'bytes':len(body),'sha256':hashlib.sha256(body).hexdigest(),
               'elapsed_seconds':round(time.monotonic()-began,3),
               'state':observe(capability['session_id'],True)}
        if name=='established-reopen' and status==206:
            path=Path(output)/(prefix+'-established.ts')
            path.write_bytes(body)
            event['media_file']=path.name
        events.append(event)
    # Preserve the source outcome, including startup reconnect grace suppression.
    time.sleep(9)
    after_grace=observe(capability['session_id'],True)
    _,_,origin_body=fetch(origin,'/archive-stats')
    return {'format_version':1,'scope':'original same-capability idle Range reconnect only',
            'capture_complete':len(events)==3,'events':events,'after_grace':after_grace,
            'origin':json.loads(origin_body),'application_parity_verified':False,
            'performance_verified':False}


def compare(source, candidate):
    """Compare bytes and lifecycle; retain timestamp relationships, not wall time."""
    differences=[]
    expected={event['case']:event for event in source['events']}
    actual={event['case']:event for event in candidate['events']}
    for case in sorted(expected.keys()|actual.keys()):
        if case not in expected or case not in actual:
            differences.append({'case':case,'field':'present'})
            continue
        left,right=expected[case],actual[case]
        for key in ['range','status','headers','bytes','sha256']:
            if left.get(key)!=right.get(key):differences.append({'case':case,'field':key})
        for key in ['api_exists','pool_exists','pool_busy','profile_connections','stats_client_count','pool_fields','cached_final_url_present']:
            if left['state'].get(key)!=right['state'].get(key):differences.append({'case':case,'field':'state.'+key})
        for field in ['total_bytes','state']:
            if [v.get(field) for v in left['state']['metadata']]!=[v.get(field) for v in right['state']['metadata']]:differences.append({'case':case,'field':'metadata.'+field})
        left_base=[v.get('playback_base_secs') for v in left['state']['clients']]
        right_base=[v.get('playback_base_secs') for v in right['state']['clients']]
        if len(left_base)!=len(right_base) or any((a is None)!=(b is None) or a is not None and abs(float(a)-float(b))>1e-9 for a,b in zip(left_base,right_base)):
            differences.append({'case':case,'field':'playback_base_secs'})
    for backend,label in [(source,'source'),(candidate,'candidate')]:
        events=backend['events']
        if len(events)!=3 or any(len(e['state']['clients'])!=1 or len(e['state']['metadata'])!=1 for e in events):
            differences.append({'case':label,'field':'three-single-client-observations'})
            continue
        if len({e['state']['clients'][0].get('connected_at') for e in events})!=1 or len({e['state']['metadata'][0].get('init_time') for e in events})!=1:
            differences.append({'case':label,'field':'preserved-start-times'})
        anchors=[float(e['state']['clients'][0].get('position_anchor_at',0)) for e in events]
        if not all(a<b for a,b in zip(anchors,anchors[1:])):differences.append({'case':label,'field':'reanchored-seeks'})
        for phase in [e['state'] for e in events]+[backend['after_grace']]:
            if not 570<=phase['api_ttl']<=600 or not 35<=phase['pool_ttl']<=60:
                differences.append({'case':label,'field':'bounded-sliding-ttl'})
    for key in ['api_exists','pool_exists','pool_busy','profile_connections','stats_client_count','pool_fields','cached_final_url_present']:
        if source['after_grace'].get(key)!=candidate['after_grace'].get(key):differences.append({'case':'after-grace','field':key})
    if source['origin']!=candidate['origin']:differences.append({'case':'origin','field':'exact-fixture-and-range-order'})
    return {'captured_idle_reopen_matches':not differences,'differences':differences,
            'normalizations':['wall-clock timestamps compared by preservation/reanchor relationships','numeric seek base tolerance 1e-9 seconds','bounded sliding TTLs account for observer boot'],
            'application_parity_verified':False,'performance_verified':False}
