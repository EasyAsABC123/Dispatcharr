#!/usr/bin/env python3
"""Source-bound native fMP4 HTTP/FFmpeg tests with a disposable pinned Redis."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import time
import tempfile
import uuid
ROOT=Path(__file__).resolve().parents[2]
IMAGE='ghcr.io/dispatcharr/dispatcharr@sha256:34f9011514edd31ef0ff111b5f256d2e37614fcb08a6ee1ed9eccec9251a856f'
def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--output',type=Path,required=True);parser.add_argument('--fixture',type=Path,action='append',required=True);args=parser.parse_args()
    args.output.mkdir(parents=True,exist_ok=True)
    if any(args.output.iterdir()):parser.error('output must be empty')
    container_name='dispatcharr-fmp4-'+uuid.uuid4().hex[:12]
    paths=[Path(__file__).resolve()]
    paths.extend(path for path in (ROOT/'rust').rglob('*') if path.is_file() and not set(path.relative_to(ROOT/'rust').parts)&{'target','.git','__pycache__'})
    inputs={str(path.relative_to(ROOT)):path.read_bytes() for path in sorted(paths)}
    temporary=tempfile.TemporaryDirectory(prefix='dispatcharr-fmp4-source-')
    frozen=Path(temporary.name)
    for name,content in inputs.items():
        path=frozen/name;path.parent.mkdir(parents=True,exist_ok=True);path.write_bytes(content)
    report={'scope':'native-fmp4-HTTP-remux-and-cleanup','passed':False,'application_performance_verified':False,'milestone_completed':False,'image':IMAGE,'container':container_name,'source_commit':subprocess.check_output(['git','rev-parse','HEAD'],cwd=ROOT,text=True).strip(),'source_sha256':{name:hashlib.sha256(content).hexdigest() for name,content in inputs.items()},'source_scope':'immutable captured Rust workspace and test runner','trials':[]}
    status=1
    def docker(*args):return subprocess.check_output(['docker',*args],text=True,stderr=subprocess.PIPE,timeout=90).strip()
    try:
        report['ffmpeg_version']=subprocess.check_output(['ffmpeg','-version'],text=True).splitlines()[0]
        docker('run','-d','--rm','--name',container_name,'-p','127.0.0.1::6379','--entrypoint','redis-server',IMAGE,'--save','','--appendonly','no','--bind','0.0.0.0','--protected-mode','no')
        address=docker('port',container_name,'6379').splitlines()[0]
        deadline=time.monotonic()+20
        while docker('exec',container_name,'redis-cli','ping')!='PONG':
            if time.monotonic()>deadline:raise RuntimeError('Redis readiness deadline')
            time.sleep(.1)
        for index,fixture in enumerate(args.fixture):
            command=['cargo','test','--manifest-path',str(frozen/'rust/Cargo.toml'),'-p','dispatcharr-fmp4-http','--locked','--offline','--test','http','--','--ignored','--nocapture']
            environment=dict(os.environ,CARGO_TARGET_DIR=str(ROOT/'rust/target'),REDIS_TEST_URL='redis://'+address,FMP4_TEST_FIXTURE=str(fixture.resolve()))
            with (args.output/f'tests-{index}.log').open('w') as log:result=subprocess.run(command,cwd=ROOT,env=environment,stdout=log,stderr=subprocess.STDOUT,timeout=120)
            report['trials'].append({'fixture':str(fixture),'fixture_sha256':hashlib.sha256(fixture.read_bytes()).hexdigest(),'command':command,'exit_code':result.returncode})
            if result.returncode:raise RuntimeError('Remux/HTTP test failure')
        report['passed']=True;status=0
    except (OSError,RuntimeError,subprocess.SubprocessError) as error:report['failure']=str(error)
    except KeyboardInterrupt:report['failure']='interrupted';status=130
    finally:
        try:docker('rm','-f',container_name);report['cleanup']='owned Redis container removed'
        except (OSError,subprocess.SubprocessError):report['cleanup_failed']=True;report['passed']=False;status=1
        report['artifact_sha256']={p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted(args.output.iterdir())if p.is_file()}
        temporary.cleanup()
        (args.output/'evidence.json').write_text(json.dumps(report,indent=2)+'\n')
    print(json.dumps({'passed':report['passed'],'output':str(args.output),'failure':report.get('failure')}));return status
if __name__=='__main__':raise SystemExit(main())
