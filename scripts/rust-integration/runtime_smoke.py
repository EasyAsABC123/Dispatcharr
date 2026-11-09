#!/usr/bin/env python3
"""Verify packaged React, method guards and bounded SIGTERM on a retained fixture.

Full application credit belongs to the original Dispatcharr authors. This probe
intentionally stops only the candidate in the explicitly selected retained
integration project; it does not remove its containers or fixture volumes.
"""
import argparse
import hashlib
import http.client
import json
import os
from pathlib import Path
import socket
import subprocess
import time
import urllib.error
import urllib.parse
import urllib.request


def request(base, path, method='GET', headers=None):
    req=urllib.request.Request(base+path,method=method,headers=headers or {})
    try:
        response=urllib.request.urlopen(req,timeout=15)
    except urllib.error.HTTPError as error:
        response=error
    with response:
        return response.status,dict(response.headers),response.read()


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--integration-evidence',type=Path,required=True)
    parser.add_argument('--output',type=Path,required=True)
    args=parser.parse_args()
    parent=args.integration_evidence.resolve()
    source=json.loads((parent/'integration.json').read_text())
    if not source.get('resources_retained'):
        parser.error('requires an explicitly retained disposable integration fixture')
    output=args.output.resolve();output.mkdir(parents=True,exist_ok=False)
    environment=dict(os.environ,BASELINE_SOURCE_DIR=source['retained_source_directory'],
                     BASELINE_EVIDENCE_DIR=str(parent),FIXTURE_SECONDS='180')
    compose=['docker','compose','-p',source['project'],'-f',str(parent/'tool-source/docker/compose.rust-playback.yml'),'-f',str(parent/'candidate-overlay.yml')]
    def command(*arguments,timeout=30):
        return subprocess.check_output(list(arguments),env=environment,text=True,timeout=timeout,stderr=subprocess.PIPE).strip()
    def compose_command(*arguments,timeout=30):
        return command(*compose,*arguments,timeout=timeout)
    base='http://'+compose_command('port','candidate','9192')
    origin='http://'+compose_command('port','origin','8080')
    container=compose_command('ps','-q','candidate')
    report={'scope':'packaged-React-and-bounded-process-shutdown','passed':False,'checks':[],
            'project':source['project'],'candidate_image_binding':source['candidate_image_binding'],
            'integration_report_sha256':hashlib.sha256((parent/'integration.json').read_bytes()).hexdigest(),
            'probe_sha256':hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
            'application_performance_verified':False}
    def record(name,passed,details=None):
        report['checks'].append({'name':name,'passed':bool(passed),'details':details})
    held=None;partial=None
    try:
        status,headers,index=request(base,'/')
        record('bundled-react-index',status==200 and b'<div id="root"' in index)
        served_hash=hashlib.sha256(index).hexdigest()
        bundled_hash=compose_command('exec','-T','candidate','sha256sum','/opt/dispatcharr-frontend/index.html').split()[0]
        record('served-index-matches-image',served_hash==bundled_hash,{'sha256':served_hash})
        status,_,navigation=request(base,'/channels')
        record('react-navigation-fallback',status==200 and navigation==index)
        import re
        assets=re.findall(rb'(?:src|href)="(/assets/[^"?]+)',index)
        record('production-assets-present',bool(assets))
        for asset in assets[:3]:
            status,_,body=request(base,asset.decode())
            record('production-asset-served',status==200 and len(body)>0,{'bytes':len(body)})
        status,headers,body=request(base,'/api/unmigrated-runtime-probe/')
        record('unimplemented-api-is-json404',status==404 and 'application/json' in headers.get('content-type','') and json.loads(body)=={'detail':'Not found.'})
        before=json.loads(request(origin,'/stats')[2])['connections']
        for path,authorization,expected in [('/proxy/ts/stream/not-a-channel',None,405),('/proxy/hls/not-a-token/index.m3u8',None,405),('/proxy/ts/status',None,401),('/proxy/ts/stream/not-a-channel','Bearer invalid',401)]:
            status,headers,body=request(base,path,'HEAD',{'Authorization':authorization} if authorization else {})
            record('head-auth-before-media:'+path,status==expected and not body,{'status':status})
        after=json.loads(request(origin,'/stats')[2])['connections']
        record('head-opens-no-origin',after==before)
        seed=json.loads((parent/'seed.json').read_text())
        parsed=urllib.parse.urlsplit(base)
        held=http.client.HTTPConnection(parsed.hostname,parsed.port,timeout=65)
        held.request('GET',seed['path'])
        response=held.getresponse()
        first=response.read(188)
        record('held-native-response-is-ts',response.status==200 and len(first)==188 and first[0]==0x47)
        partial=socket.create_connection((parsed.hostname,parsed.port),timeout=5)
        partial.sendall(b'GET / HTTP/1.1\r\nHost: fixture\r\n')
        time.sleep(.2)
        started=time.monotonic()
        command('docker','kill','--signal','TERM',container)
        exit_code=command('docker','wait',container,timeout=15)
        elapsed=time.monotonic()-started
        record('sigterm-with-unread-body-and-partial-headers',exit_code=='0' and elapsed<14,{'elapsed_seconds':elapsed,'exit_code':exit_code})
        channel=seed['channel_uuid']
        script="local p='live:channel:'..ARGV[1];return {redis.call('EXISTS',p..':owner'),redis.call('SCARD',p..':clients'),redis.call('EXISTS',p..':metadata'),redis.call('EXISTS',p..':input:buffer:index')}"
        values=compose_command('exec','-T','baseline','redis-cli','--no-auth-warning','-a','playback-fixture-only','--raw','EVAL',script,'0',channel).splitlines()
        record('native-owner-clients-metadata-input-drained',values==['0','0','0','0'],{'remaining_counts':values})
        report['passed']=all(check['passed'] for check in report['checks'])
    except (OSError,ValueError,RuntimeError,subprocess.SubprocessError) as error:
        report['failure']=type(error).__name__+': '+str(error)
    finally:
        if held:held.close()
        if partial:partial.close()
        (output/'runtime.json').write_text(json.dumps(report,indent=2)+'\n')
    print(json.dumps({'passed':report['passed'],'evidence':str(output)}))
    return 0 if report['passed'] else 1


if __name__=='__main__':
    raise SystemExit(main())
