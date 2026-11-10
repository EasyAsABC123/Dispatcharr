"""Original network VOD ordinary reads versus a manifest-bound native app.
All application credit remains with original Dispatcharr authors/contributors.
No performance, fullVOD, provider-refresh or browser acceptance is inferred.
"""
import concurrent.futures,hashlib,http.client,ipaddress,json,threading,time
from urllib.parse import urlsplit

def probe(base,source,source_burst):
    parsed=urlsplit(base)
    if parsed.scheme!='http' or not ipaddress.ip_address(parsed.hostname).is_loopback or not parsed.port:raise RuntimeError('Owned loopback candidate endpoint required')
    def request(case):
        headers={k.removeprefix('HTTP_').replace('_','-'):v for k,v in case['headers'].items()}
        headers['Content-Type']='application/json';method=case['method']
        body=None if method in ('GET','HEAD')else json.dumps(case.get('body')).encode()
        connection=http.client.HTTPConnection(parsed.hostname,parsed.port,timeout=10)
        try:
            connection.request(method,case['path'],body=body,headers=headers);response=connection.getresponse();data=response.read(8*1024*1024+1)
            if len(data)>8*1024*1024:raise RuntimeError('Ordinary response exceeds owned8MiB capture')
            return {'status':response.status,'body_bytes':len(data),'body_sha256':hashlib.sha256(data).hexdigest(),'content_type':response.getheader('Content-Type'),'selected_headers':{k:response.getheader(k)for k in ['Content-Length','Allow','Vary','WWW-Authenticate','X-Content-Type-Options','X-Frame-Options','Referrer-Policy','Cross-Origin-Opener-Policy']}}
        finally:connection.close()
    records=[];differences=[];outside=0
    for index,case in enumerate(source['cases']):
        bare=case['path'].split('?',1)[0];parts=bare.strip('/').split('/')
        if len(parts)not in (3,4)or parts[:2]!=['api','vod']or parts[2]not in ['movies','series','episodes']or not bare.endswith('/'):
            outside+=1;continue
        actual=request(case);expected=case['expected'];record={'index':index,'path':case['path'],'role':case['role'],'method':case['method'],'actual':actual};records.append(record)
        if actual!={key:expected[key]for key in actual}and len(differences)<20:differences.append({'case':record,'expected':expected})
    standard=next(c for c in source['cases']if c['path']=='/api/vod/movies/'and c['method']=='GET'and c['role']=='standard')
    barrier=threading.Barrier(11);epoch=time.monotonic()
    def simultaneous(index):
        barrier.wait(timeout=5);began=time.monotonic()-epoch;actual=request(standard);return {'index':index,'started_seconds':began,**actual}
    with concurrent.futures.ThreadPoolExecutor(max_workers=10)as executor:
        futures=[executor.submit(simultaneous,index)for index in range(10)];barrier.wait(timeout=5);burst=[future.result(timeout=15)for future in futures]
    burst_pass=all(row['status']==200 and row['body_sha256']==source_burst['records'][0]['body_sha256']for row in burst)
    return {'passed':not differences and len(records)>350 and burst_pass,'scope':__doc__,'compared':len(records),'outside_component_not_pass':outside,'records':records,'differences':differences,'source_simultaneous10':source_burst,'candidate_simultaneous10':burst,'burst_pass':burst_pass,'candidate_start_spread_seconds':max(row['started_seconds']for row in burst)-min(row['started_seconds']for row in burst),'performance_verified':False,'browser_verified':False}
