"""Actual packaged static HTTP vs pinned Source nginx capture; no asset mocks.
Original Dispatcharr/nginx/DRF and bundled asset authors retain all credit.
"""
import hashlib
import http.client
import json
import sys
from pathlib import Path

HEADERS = ['content-type', 'content-length', 'last-modified', 'etag', 'content-range',
           'accept-ranges', 'vary', 'cache-control', 'content-encoding', 'allow',
           'x-content-type-options', 'x-frame-options', 'referrer-policy', 'cross-origin-opener-policy']


def observe():
    source=json.loads(Path(__file__).with_name('static-nginx-source-1.json').read_text())
    records=[];differences=[]
    for index,expected in enumerate(source['records']):
        c=http.client.HTTPConnection('candidate',9192,timeout=10)
        try:
            c.request(expected['method'],expected['path'],headers={'Accept-Encoding':'identity',**expected['request_headers']})
            response=c.getresponse();body=response.read(2*1024*1024+1);assert len(body)<=2*1024*1024
            headers={k.lower():v for k,v in response.getheaders()};old={k.lower():v for k,v in expected['headers']}
            same=(response.status==expected['status']and len(body)==expected['body_bytes']and hashlib.sha256(body).hexdigest()==expected['body_sha256']and all(headers.get(k)==old.get(k)for k in HEADERS))
            record={'index':index,'method':expected['method'],'path':expected['path'],'status':response.status,'same':same,'body_sha256':hashlib.sha256(body).hexdigest()};records.append(record)
            if not same:differences.append({'case':record,'Source_status':expected['status'],'native_body_bytes':len(body),'Source_body_bytes':expected['body_bytes'],'headers':{k:{'Source':old.get(k),'native':headers.get(k)}for k in HEADERS if headers.get(k)!=old.get(k)}})
        finally:c.close()
    return {'passed':not differences,'compared':len(records),'assets':source['assets'],'records':records,'differences':differences,'source_nginx_version':source['nginx_version'],'scope':'Captured27 assets/68 identity-encoding GET/HEAD/conditional/range/error observations; selected14 intrinsic headers and exactbody bytes; no general static grammar/gzip/proxy or full BrowsableAPI acceptance','performance_verified':False}


def probe(command,compose,evidence,binding):
    completed=command(compose+['exec','-T','baseline','/dispatcharrpy/bin/python','/vod-api-probe/static_packaged_probe.py','--observe'],timeout=180)
    report=json.loads(completed.stdout.splitlines()[-1]);report['candidate_binding']=binding
    (evidence/'packaged-static.json').write_text(json.dumps(report,indent=2)+'\n')
    return report


if __name__=='__main__':
    assert sys.argv[1:]==['--observe']
    print(json.dumps(observe()))
