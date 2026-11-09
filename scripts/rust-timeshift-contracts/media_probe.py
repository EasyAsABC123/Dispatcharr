"""Bounded native archive media characterization; never persist capabilities."""
import hashlib
import json
import time
import urllib.error
import urllib.request
from pathlib import Path


def request(base, path, *, method='GET', body=None, headers=None, limit=2_000_000):
    req = urllib.request.Request(base+path, method=method, headers=headers or {},
        data=json.dumps(body).encode() if body is not None else None)
    try:
        response = urllib.request.urlopen(req, timeout=20)
    except urllib.error.HTTPError as error:
        response = error
    with response:
        payload = response.read(limit+1)
        if len(payload)>limit:
            raise ValueError('archive probe exceeds bounded capture')
        return response.status, dict(response.headers), payload


def capture(base, origin, seed, output, observe):
    output = Path(output)
    events = []
    for mode in ['all','query-only','html-first','forbidden','not-found','server-error']:
        status,_,_ = request(origin,'/archive-control',method='POST',body={'mode':mode},headers={'Content-Type':'application/json'})
        if status != 200:
            raise RuntimeError('fixture control failed')
        status,_,payload = request(base,'/api/catchup/sessions/',method='POST',
            body={'channel_uuid':seed['channel_uuid'],'start':'2026-11-09T11:00:00Z','duration':30},
            headers={'Content-Type':'application/json','Authorization':'Bearer '+seed['users']['owner']['token']})
        if status != 201:
            raise RuntimeError('archive API session mint failed')
        session = json.loads(payload)
        began = time.monotonic()
        status,headers,payload = request(base,session['playback_url'],headers={'Range':'bytes=0-1879999'})
        elapsed = time.monotonic()-began
        event = {'case':mode,'status':status,'bytes':len(payload),'sha256':hashlib.sha256(payload).hexdigest(),
            'headers':{key.lower():value for key,value in headers.items() if key.lower() in ('content-type','content-length','content-range','accept-ranges','cache-control','pragma','x-content-type-options')},
            'elapsed_seconds':round(elapsed,3)}
        if status in (200,206):
            path = output/('timeshift-'+mode+'.ts')
            path.write_bytes(payload)
            event['media_file'] = path.name
        else:
            event['error_body'] = payload.decode('utf-8','replace')[:200]
        event['after_body'] = observe(session['session_id'])
        # Observe source cleanup without forcing or hiding its outcome.
        deadline = time.monotonic()+11
        while True:
            state = observe(session['session_id'])
            if not state['api_exists'] or time.monotonic()>deadline:
                break
            time.sleep(.5)
        event['after_grace'] = state
        _,_,stats = request(origin,'/archive-stats')
        event['origin'] = json.loads(stats)
        events.append(event)
    return {'format_version':1,'scope':'original native catch-up media/provider fallback and lifecycle characterization',
        'capture_complete':len(events)==6,'application_parity_verified':False,'performance_verified':False,'events':events}
