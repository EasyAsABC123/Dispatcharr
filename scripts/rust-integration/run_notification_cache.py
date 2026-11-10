#!/usr/bin/env python3
"""Original-Python/Rust notification cache proof in an isolated Redis container.

Original Dispatcharr authors retain all application credit. No external Redis,
production credentials, database or notification rows are accessed.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import time
import shutil
import uuid
from fixture_ports import write_port_overlay
ROOT=Path(__file__).resolve().parents[2]
IMAGE='ghcr.io/dispatcharr/dispatcharr@sha256:34f9011514edd31ef0ff111b5f256d2e37614fcb08a6ee1ed9eccec9251a856f'
def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--output',type=Path,required=True);parser.add_argument('--frozen-rust-source',type=Path);parser.add_argument('--physical',action='store_true');args=parser.parse_args();output=args.output.resolve();output.mkdir(parents=True,exist_ok=True)
    if any(output.iterdir()):parser.error('Output must be empty')
    frozen=output/'runtime';source=(args.frozen_rust_source or (ROOT/'rust')).resolve()
    assert source==ROOT/'rust' or (source.is_relative_to(ROOT/'artifacts/rust') and (source/'Cargo.lock').is_file()),'Retained Rust source required'
    captured={}
    for directory,names,files in os.walk(source):
        names[:]=[name for name in names if name not in ['target','.git','__pycache__']]
        for filename in files:
            path=Path(directory)/filename
            if path.is_symlink():raise RuntimeError('Regular compiler inputs required')
            captured[str(Path('rust')/path.relative_to(source))]=path.read_bytes()
    tools=output/'tools/scripts/rust-integration';tools.mkdir(parents=True)
    for path in [Path(__file__).resolve(),ROOT/'scripts/rust-integration/fixture_ports.py']:shutil.copyfile(path,tools/path.name)
    for relative,content in captured.items():
        target=frozen/relative;target.parent.mkdir(parents=True,exist_ok=True);target.write_bytes(content)
    prefix='rust-probe-notification-cache-'+uuid.uuid4().hex[:12];name=prefix;report={'passed':False,'scope':'isolated original Django/Rust notification cache interoperability','source_image':IMAGE,'application_performance_verified':False}
    def docker(*args,body=None):return subprocess.check_output(['docker',*args],input=body,text=True,stderr=subprocess.PIPE,timeout=90).strip()
    configure="from django.conf import settings;settings.configure(CACHES={'default':{'BACKEND':'django_redis.cache.RedisCache','LOCATION':'redis://:playback-fixture-only@127.0.0.1:6379/0','OPTIONS':{'CLIENT_CLASS':'django_redis.client.DefaultClient'}}});from django.core.cache import cache;"
    try:
        ports=write_port_overlay(output/'fixture-ports.yml',{'redis':[6379]});port=ports['services']['redis'][0]['host']
        docker('run','--rm','-d','--name',name,'-p','127.0.0.1:'+str(port)+':6379','--entrypoint','redis-server',IMAGE,'--save','','--appendonly','no','--bind','0.0.0.0','--protected-mode','no','--requirepass','playback-fixture-only')
        for _ in range(100):
            try:
                if docker('exec',name,'redis-cli','--no-auth-warning','-a','playback-fixture-only','ping')=='PONG':break
            except subprocess.CalledProcessError:pass
            time.sleep(.1)
        else:raise RuntimeError('Redis readiness failed')
        seed=configure+"import json;prefix="+repr(prefix)+";cache.set(prefix+':python-false',False,300);cache.set(prefix+':python-dict',{'M3U_EPG':'::/0','nested':[True,None,1.5]},300);print(json.dumps({'seeded':True}))"
        report['python_seeded']=json.loads(docker('exec',name,'/dispatcharrpy/bin/python','-c',seed))['seeded']
        env=dict(os.environ,CARGO_TARGET_DIR=str(output/'target'),REDIS_TEST_URL='redis://:playback-fixture-only@127.0.0.1:'+str(port)+'/0',NOTIFICATION_CACHE_TEST_PREFIX=prefix,NOTIFICATION_SCOPE_FIXTURE='playback-fixture-only')
        command=['cargo','test','--manifest-path',str(frozen/'rust/Cargo.toml'),'-p','dispatcharr-store','--locked','--offline','--lib','notifications::tests::'if args.physical else'notifications::tests::redis_cache_interoperates','--','--ignored','--test-threads=1']
        with (output/'rust-tests.log').open('w') as log:result=subprocess.run(command,env=env,stdout=log,stderr=subprocess.STDOUT,timeout=180)
        report['rust_exit_code']=result.returncode
        if result.returncode:raise RuntimeError('Rust cache gate failed')
        if args.physical:
            assert '6 passed; 0 failed' in (output/'rust-tests.log').read_text(),'All six cache gates required'
            report['physical_scope_tests']=5
        verify=configure+"import json;prefix="+repr(prefix)+";assert cache.get(prefix+':rust-true') is True;assert cache.get(prefix+':rust-false') is False;assert cache.get(prefix+':rust-dict')=={'M3U_EPG':'::/0','nested':[True,None,1.5]};assert cache.get('coresettings:group:network_access')=={'M3U_EPG':'::/0'};assert all(0<cache.ttl(prefix+':'+suffix)<=300 for suffix in ['rust-true','rust-false','rust-dict']);print(json.dumps({'decoded_rust_values':True,'ttl_bounded':True}))"
        report.update(json.loads(docker('exec',name,'/dispatcharrpy/bin/python','-c',verify)));report['passed']=True
    except (OSError,RuntimeError,ValueError,subprocess.SubprocessError) as error:report['error_type']=type(error).__name__
    finally:
        try:docker('rm','-f',name);report['container_removed']=True
        except subprocess.CalledProcessError:report['container_removed']=False;report['passed']=False
        report['source_snapshot_manifest_sha256']=hashlib.sha256(json.dumps({name:hashlib.sha256(content).hexdigest() for name,content in captured.items()},sort_keys=True,separators=(',',':')).encode()).hexdigest()
        report['source_snapshot_files']=len(captured)
        report['source_scope']='exact captured Rust workspace retained under runtime; compiled as isolated component'
        report['source_input']=str(source)
        (output/'cache-contracts.json').write_text(json.dumps(report,indent=2)+'\n')
    print(json.dumps({'passed':report['passed'],'evidence':str(output)}));return 0 if report['passed'] else 1
if __name__=='__main__':raise SystemExit(main())
