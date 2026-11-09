#!/usr/bin/env python3
"""Original-Python/Rust notification cache proof in an isolated Redis container.

Original Dispatcharr authors retain all application credit. No external Redis,
credentials, database or production notification rows are accessed.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import time
import tempfile
import uuid
from fixture_ports import write_port_overlay
ROOT=Path(__file__).resolve().parents[2]
IMAGE='ghcr.io/dispatcharr/dispatcharr@sha256:34f9011514edd31ef0ff111b5f256d2e37614fcb08a6ee1ed9eccec9251a856f'
def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--output',type=Path,required=True);args=parser.parse_args();output=args.output.resolve();output.mkdir(parents=True,exist_ok=True)
    if any(output.iterdir()):parser.error('Output must be empty')
    frozen_source=tempfile.TemporaryDirectory(prefix='dispatcharr-notification-cache-source-')
    frozen=Path(frozen_source.name)
    captured={str(path.relative_to(ROOT)):path.read_bytes() for path in (ROOT/'rust').rglob('*') if path.is_file() and not set(path.relative_to(ROOT/'rust').parts)&{'target','.git','__pycache__'}}
    for relative,content in captured.items():
        target=frozen/relative;target.parent.mkdir(parents=True,exist_ok=True);target.write_bytes(content)
    prefix='rust-probe-notification-cache-'+uuid.uuid4().hex[:12];name=prefix;report={'passed':False,'scope':'isolated original Django/Rust notification cache interoperability','source_image':IMAGE,'application_performance_verified':False}
    def docker(*args,body=None):return subprocess.check_output(['docker',*args],input=body,text=True,stderr=subprocess.PIPE,timeout=90).strip()
    configure="from django.conf import settings;settings.configure(CACHES={'default':{'BACKEND':'django_redis.cache.RedisCache','LOCATION':'redis://127.0.0.1:6379/0','OPTIONS':{'CLIENT_CLASS':'django_redis.client.DefaultClient'}}});from django.core.cache import cache;"
    try:
        ports=write_port_overlay(output/'fixture-ports.yml',{'redis':[6379]});port=ports['services']['redis'][0]['host']
        docker('run','--rm','-d','--name',name,'-p','127.0.0.1:'+str(port)+':6379','--entrypoint','redis-server',IMAGE,'--save','','--appendonly','no','--bind','0.0.0.0','--protected-mode','no')
        for _ in range(100):
            try:
                if docker('exec',name,'redis-cli','ping')=='PONG':break
            except subprocess.CalledProcessError:pass
            time.sleep(.1)
        else:raise RuntimeError('Redis readiness failed')
        seed=configure+"import json;prefix="+repr(prefix)+";cache.set(prefix+':python-false',False,300);cache.set(prefix+':python-dict',{'M3U_EPG':'::/0','nested':[True,None,1.5]},300);print(json.dumps({'seeded':True}))"
        report['python_seeded']=json.loads(docker('exec',name,'/dispatcharrpy/bin/python','-c',seed))['seeded']
        env=dict(os.environ,CARGO_TARGET_DIR=str(ROOT/'rust/target'),REDIS_TEST_URL='redis://127.0.0.1:'+str(port),NOTIFICATION_CACHE_TEST_PREFIX=prefix)
        command=['cargo','test','--manifest-path',str(frozen/'rust/Cargo.toml'),'-p','dispatcharr-store','--locked','--offline','--lib','notifications::tests::redis_cache_interoperates','--','--ignored']
        with (output/'rust-tests.log').open('w') as log:result=subprocess.run(command,env=env,stdout=log,stderr=subprocess.STDOUT,timeout=180)
        report['rust_exit_code']=result.returncode
        if result.returncode:raise RuntimeError('Rust cache gate failed')
        verify=configure+"import json;prefix="+repr(prefix)+";assert cache.get(prefix+':rust-true') is True;assert cache.get(prefix+':rust-false') is False;assert cache.get(prefix+':rust-dict')=={'M3U_EPG':'::/0','nested':[True,None,1.5]};assert all(0<cache.ttl(prefix+':'+suffix)<=300 for suffix in ['rust-true','rust-false','rust-dict']);print(json.dumps({'decoded_rust_values':True,'ttl_bounded':True}))"
        report.update(json.loads(docker('exec',name,'/dispatcharrpy/bin/python','-c',verify)));report['passed']=True
    except (OSError,RuntimeError,ValueError,subprocess.SubprocessError) as error:report['error_type']=type(error).__name__
    finally:
        try:docker('rm','-f',name);report['container_removed']=True
        except subprocess.CalledProcessError:report['container_removed']=False;report['passed']=False
        report['source_snapshot_manifest_sha256']=hashlib.sha256(json.dumps({name:hashlib.sha256(content).hexdigest() for name,content in captured.items()},sort_keys=True,separators=(',',':')).encode()).hexdigest()
        report['source_snapshot_files']=len(captured)
        report['source_scope']='exact captured Rust workspace bytes compiled from temporary immutable copy'
        frozen_source.cleanup()
        (output/'cache-contracts.json').write_text(json.dumps(report,indent=2)+'\n')
    print(json.dumps({'passed':report['passed'],'evidence':str(output)}));return 0 if report['passed'] else 1
if __name__=='__main__':raise SystemExit(main())
