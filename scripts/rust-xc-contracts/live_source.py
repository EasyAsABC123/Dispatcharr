#!/usr/bin/env python3
"""Actual original XC live-entry auth/method/lookup wire contracts.

Full application credit remains with original Dispatcharr authors/contributors.
No credentials or full credential-bearing request paths are persisted.
"""
import hashlib
import http.client
import json
from pathlib import Path
from urllib.parse import quote
import source


def exercise(base, control):
    seed=control.call({'op':'seed'});rows=[]
    def request(name,role='standard',channel='999999.ts',prefix=True,method='GET',headers=None,wrong=False):
        user=seed['users'][role]
        target=('/live/' if prefix else '/')+quote(user['username'],safe='')+'/'+quote('invalid' if wrong else user['password'],safe='')+'/'+quote(channel,safe='')
        conn=http.client.HTTPConnection(base.removeprefix('http://'),timeout=10)
        conn.request(method,target,headers={'Host':'xc.fixture.invalid:8088',**(headers or {})})
        response=conn.getresponse();body=response.read(128*1024);assert len(body)<128*1024
        row={'case':name,'method':method,'role':role,'channel_input':channel,'path_shape':'/live/<user>/<pass>/<id>' if prefix else '/<user>/<pass>/<id>','status':response.status,'headers':{key.lower():value for key,value in response.getheaders() if key.lower() in ['content-type','allow','www-authenticate','retry-after','cache-control']},'body_bytes':len(body)}
        if 'allow' in row['headers']:row['headers']['allow']=', '.join(sorted(part.strip() for part in row['headers']['allow'].split(',')))
        if body:
            try:row['json']=json.loads(body)
            except (ValueError,UnicodeError):row['body_sha256']=hashlib.sha256(body).hexdigest()
        conn.close();rows.append(row);return row
    for role in seed['users']:
        request(role+' missing channel',role)
    for channel in ['invalid.ts','999999','999999.TS','999999.mp4','999999.MP4','999999.m3u8','999999.unknown','999999.1.ts','-1.ts','+999999.ts',' 999999.ts','٩٩٩٩٩٩.ts',str(1<<80)+'.ts']:
        request('numeric/suffix '+channel,channel=channel)
    for prefix in [True,False]:
        request('short='+str(not prefix),prefix=prefix)
        request('wrong credentials short='+str(not prefix),prefix=prefix,wrong=True)
    for method in ['HEAD','OPTIONS','POST','PUT','DELETE']:
        request('method '+method,method=method)
        request('method invalid Bearer '+method,method=method,headers={'Authorization':'Bearer invalid'})
    request('GET invalid Bearer before XC',headers={'Authorization':'Bearer invalid'},wrong=True)
    request('GET invalid API key before XC',headers={'X-API-Key':'invalid'},wrong=True)
    control.call({'op':'network','values':{'XC_API':'192.0.2.0/24'}});request('XC_API deny');control.call({'op':'network','values':{'XC_API':'0.0.0.0/0,::/0'}})
    control.call({'op':'network','values':{'STREAMS':'192.0.2.0/24'}});request('STREAMS not applied before XC lookup');control.call({'op':'network','values':{'STREAMS':'0.0.0.0/0,::/0'}})
    return rows,control.call({'op':'side_effects'})


if __name__=='__main__':
    source.exercise=exercise
    source.EXTRA_TOOLS=[Path(__file__).resolve()]
    source.SOURCE_SCOPE='Actual original XC live-entry method/auth/lookup contracts; no native or positive playback acceptance'
    raise SystemExit(source.main())
