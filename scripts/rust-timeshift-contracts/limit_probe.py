"""Actual JWT/API capability positive-user-limit refusal/retry wire capture."""
import hashlib
import json
from pathlib import Path
import time
import urllib.error
import urllib.request


def fetch(base,path,headers=None,body=None):
    request=urllib.request.Request(base+path,headers=headers or {},data=json.dumps(body).encode() if body is not None else None)
    try:response=urllib.request.urlopen(request,timeout=20)
    except urllib.error.HTTPError as error:response=error
    with response:
        payload=response.read(2_000_001)
        if len(payload)>2_000_000:raise ValueError('native limit capture exceeded bounded body')
        return response.status,{key.lower():value for key,value in response.headers.items() if key.lower() in ('content-type','content-length','content-range','accept-ranges','x-content-type-options')},payload


def capture(base,origin,seed,output,observe,control,label):
    output=Path(output);events=[];sid=None
    config=control('begin',label)
    try:
        reset=urllib.request.Request(origin+'/archive-control',method='POST',headers={'Content-Type':'application/json'},data=json.dumps({'mode':'all'}).encode())
        with urllib.request.urlopen(reset,timeout=5) as response:
            if response.status!=200:raise RuntimeError('native limit origin reset failed')
        status,_,body=fetch(base,'/api/catchup/sessions/',headers={'Authorization':'Bearer '+seed['users']['owner']['token'],'Content-Type':'application/json'},body={'channel_uuid':seed['channel_uuid'],'start':'2026-11-09T11:00:00Z','duration':30})
        if status!=201:raise RuntimeError('positive native limit capability mint failed')
        session=json.loads(body);sid=session['session_id'];path=session['playback_url']
        for name in ['existing-live-refusal','same-capability-retry']:
            if name=='same-capability-retry':control('clear-live',label)
            status,headers,body=fetch(base,path,headers={'Range':'bytes=0-1879999','User-Agent':'fixture-positive-user-limit'})
            event={'case':name,'status':status,'headers':headers,'bytes':len(body),'sha256':hashlib.sha256(body).hexdigest(),'state':observe(sid)}
            if status==206:
                file=output/('limit-'+label+'.ts');file.write_bytes(body);event['media_file']=file.name
            else:event['error_body']=body.decode('utf-8','replace')[:200]
            events.append(event)
        deadline=time.monotonic()+12
        while True:
            state=observe(sid)
            if not state['api_exists'] or time.monotonic()>deadline:break
            time.sleep(.5)
        expected=(events[0]['status']==403 and events[0].get('error_body')=='Stream limit exceeded' and events[0]['state']['profile_connections']==0 and events[0]['state']['api_exists'] and 570<=events[0]['state']['api_ttl']<=600 and not events[0]['state']['pool_exists'] and events[1]['status']==206 and events[1]['bytes']==1_880_000 and events[1]['sha256']=='5eb05561584b30d6ef1c1f27a26d27bf2572e5580e29dff45ec9bd2b6c19f8a0' and not state['api_exists'] and not state['pool_exists'] and state['profile_connections']==0 and state['stats_client_count']==0)
        return {'format_version':1,'scope':'actual JWT-issued capability positive-cap refusal and retry against source accounting fixture','capture_complete':len(events)==2,'expected_control_behaviors_passed':expected,'configuration':config,'events':events,'after_grace':state,'application_parity_verified':False,'performance_verified':False}
    finally:control('restore',label)
