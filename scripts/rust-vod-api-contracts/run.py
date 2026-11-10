#!/usr/bin/env python3
"""Guarded original VOD REST oracle and optional native SQL component runner.
Native SQL comparison does not establish native HTTP or performance parity.
All application credit belongs to original Dispatcharr authors/contributors.
Each observation mode receives a fresh owned disposable Source Compose project.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import time
import uuid

ROOT=Path(__file__).resolve().parents[2]
sys.path[:0]=[str(ROOT/'scripts/rust-integration')]
from shared_ts import BASELINE,IMAGE,fixture_source
from fixture_ports import write_port_overlay
from run import candidate_overlay,validate_candidate_image


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output',type=Path,required=True)
    parser.add_argument('--mode',choices=['factory','http','both'],default='both')
    parser.add_argument('--native-sql',action='store_true',help='Compare ordinary read component on actual Source database; not native HTTP/app acceptance')
    parser.add_argument('--frozen-rust-source',type=Path,help='Reuse an already captured coherent Rust tree for a harness-only correction')
    parser.add_argument('--native-http',action='store_true',help='Run independent native TCP HTTP peer against original network corpus')
    parser.add_argument('--http-wire',action='store_true',help='Add original network method/detail/negotiation/routing characterization')
    parser.add_argument('--build-manifest','--candidate-build-manifest',dest='build_manifest',type=Path,help='Successful immutable app manifest for packaged replay; skips native peer compilation')
    parser.add_argument('--candidate-image',help='Must equal the successful manifest tag')
    args=parser.parse_args()
    manifest=json.loads(args.build_manifest.read_text())if args.build_manifest else None
    if args.candidate_image and manifest is None:parser.error('--candidate-image requires --build-manifest')
    if manifest is not None:
        if not manifest.get('passed')or(args.candidate_image and args.candidate_image!=manifest['tag']):parser.error('Successful matching candidate manifest required')
        if args.native_sql or args.native_http:parser.error('Packaged replay must not compile a host native peer')
        if args.mode!='http':parser.error('Packaged replay requires --mode http')
        args.http_wire=True
    if args.native_http:args.native_sql=True;args.http_wire=True
    if args.http_wire and args.mode!='http':parser.error('--http-wire requires --mode http')
    out=args.output.resolve();out.mkdir(parents=True,exist_ok=False)
    report={'scope':'Original VOD REST metadata-only capture; separate Factory and network fixtures',
        'passed':False,'baseline_commit':BASELINE,'baseline_image':IMAGE,
        'native_verified':False,'provider_refresh_verified':False,'performance_verified':False,'modes':[]}
    modes=['factory','http']if args.mode=='both'else[args.mode]
    try:
        for mode in modes:
            evidence=out/mode;evidence.mkdir()
            files=[Path(__file__).resolve(),Path(__file__).with_name('source.py'),*([Path(__file__).with_name('packaged_probe.py'),Path(__file__).with_name('effects_source.py'),ROOT/'docker/compose.rust-candidate.yml',ROOT/'scripts/rust-integration/run.py']if manifest is not None else[]),*([Path(__file__).with_name('burst_source.py')]if args.http_wire else[]),
                ROOT/'docker/compose.rust-playback.yml',ROOT/'scripts/rust-integration/shared_ts.py',
                ROOT/'scripts/rust-integration/fixture_ports.py',ROOT/'scripts/rust-baseline/dependencies.py',
                *([ROOT/'scripts/rust-vod-api-contracts/source-vod-rest-1.json',ROOT/'scripts/rust-vod-api-contracts/source-default-queries-1.json',ROOT/'scripts/rust-vod-api-contracts/schedule_source.py',ROOT/'scripts/rust-vod-api-contracts/category_fault_source.py']if args.native_sql else []),
                *[ROOT/'scripts/rust-playback-baseline'/name for name in
                  ['seed.py','origin.py','range_http.py','start.sh','supervisor.py','uwsgi.ini']]]
            tools=evidence/'tools';hashes={}
            for path in files:
                target=tools/path.relative_to(ROOT);target.parent.mkdir(parents=True,exist_ok=True)
                data=path.read_bytes();target.write_bytes(data);hashes[str(path.relative_to(ROOT))]=hashlib.sha256(data).hexdigest()
            if args.native_sql:
                rust=(args.frozen_rust_source or(ROOT/'rust')).resolve()
                if not(rust/'Cargo.toml').is_file():raise RuntimeError('Rust source root missingCargo.toml')
                for directory,names,filenames in os.walk(rust):
                    names[:]=[name for name in names if name not in ['target','.git','__pycache__']]
                    for filename in filenames:
                        path=Path(directory)/filename;target=tools/'rust'/path.relative_to(rust)
                        target.parent.mkdir(parents=True,exist_ok=True);shutil.copyfile(path,target)
                rust_hashes={str(path.relative_to(tools/'rust')):hashlib.sha256(path.read_bytes()).hexdigest()for path in(tools/'rust').rglob('*')if path.is_file()}
                result_manifest=hashlib.sha256(json.dumps(rust_hashes,sort_keys=True,separators=(',',':')).encode()).hexdigest()
                (evidence/'frozen-source-ready.json').write_text(json.dumps({'rust_manifest_sha256':result_manifest}))
            project='dispatcharr-vod-api-'+mode+'-'+uuid.uuid4().hex[:10]
            result={'scope':'Original '+mode+' VOD REST source capture','passed':False,
                'project':project,'tool_sha256':hashes,'observation':mode,'commands':[]}
            if args.native_sql:
                result['native_rust_manifest_sha256']=result_manifest
                result['native_rust_source_input']=str(rust)
            with fixture_source(result,evidence,'source-run.json')as temporary:
                source=Path(temporary)/'source';source.mkdir()
                archive=subprocess.check_output(['git','archive',BASELINE],cwd=ROOT)
                result['baseline_archive_sha256']=hashlib.sha256(archive).hexdigest()
                subprocess.run(['tar','-x','--no-same-owner','-C',str(source)],input=archive,check=True)
                (source/'media').mkdir(exist_ok=True)
                environment=dict(os.environ,BASELINE_SOURCE_DIR=str(source),BASELINE_EVIDENCE_DIR=str(evidence),
                    FIXTURE_SECONDS='900',DISPATCHARR_ENV='aio')
                ports=evidence/'ports.yml';result['ports']=write_port_overlay(ports,{'baseline':[5656,5432,6379]if args.native_sql else[5656],'origin':[8080],**({'candidate':[9192]}if manifest is not None else{})})
                overlay=evidence/'oracle.yml';overlay.write_text(json.dumps({'services':{'baseline':{'volumes':[
                    str(tools/'scripts/rust-vod-api-contracts')+':/vod-api-probe:ro']}}}))
                compose=['docker','compose','-p',project,'-f',str(tools/'docker/compose.rust-playback.yml'),'-f',str(overlay),'-f',str(ports)]
                if manifest is not None:
                    candidate=candidate_overlay(tools/'docker/compose.rust-candidate.yml',evidence/'candidate.yml',manifest['tag']);compose+=['-f',str(candidate),'-f',str(ports)]
                    result['build_manifest_sha256']=hashlib.sha256(args.build_manifest.read_bytes()).hexdigest();shutil.copyfile(args.build_manifest,evidence/'build-manifest.json')
                def command(argv,timeout=180):
                    result['commands'].append(argv)
                    completed=subprocess.run(argv,cwd=ROOT,env=environment,text=True,capture_output=True,timeout=timeout)
                    if completed.returncode:
                        (evidence/'failed-command.log').write_text(completed.stdout+'\n'+completed.stderr)
                        completed.check_returncode()
                    return completed
                try:
                    command(compose+['up','-d','--no-build','baseline','origin'])
                    # Use a Docker exec health probe: no host public-interface
                    # assumptions or retry of observed API workloads.
                    deadline=time.monotonic()+180
                    while True:
                        ready=subprocess.run(compose+['exec','-T','baseline','/dispatcharrpy/bin/python','-c',
                            "from urllib.request import urlopen;assert urlopen('http://127.0.0.1:5656/api/core/version/',timeout=2).status==200"],
                            cwd=ROOT,env=environment,text=True,capture_output=True,timeout=10)
                        if ready.returncode==0:break
                        if time.monotonic()>=deadline:raise RuntimeError('Original fixture readiness deadline')
                        time.sleep(.2)
                    captured=command(compose+['exec','-T','-e','VOD_API_DISPOSABLE_FIXTURE=playback-fixture-only',
                        '-e','VOD_API_OBSERVATION='+mode,*(['-e','VOD_API_HTTP_WIRE=1']if args.http_wire else[]),'baseline','/dispatcharrpy/bin/python','/vod-api-probe/source.py'],timeout=300)
                    (evidence/'source-worker.log').write_text(captured.stderr)
                    corpus=json.loads(captured.stdout.splitlines()[-1]);data=json.dumps(corpus,indent=2,ensure_ascii=False)+'\n'
                    (evidence/'source-corpus.json').write_text(data)
                    result.update(passed=corpus['capture_complete'],cases=len(corpus['cases']),
                        corpus_sha256=hashlib.sha256(data.encode()).hexdigest(),provider_trap_attempts=len(corpus['provider_trap_attempts']),
                        provider_refresh_rows_unchanged=corpus['provider_refresh_rows_unchanged'])
                    assert result['passed'],'Incomplete Source metadata capture or unexpected provider refresh'
                    if args.http_wire:
                        burst=command(compose+['exec','-T','-e','VOD_API_DISPOSABLE_FIXTURE=playback-fixture-only','baseline','/dispatcharrpy/bin/python','/vod-api-probe/burst_source.py'],timeout=30)
                        observed=json.loads(burst.stdout.splitlines()[-1]);assert observed['passed'];(evidence/'source-burst.json').write_text(json.dumps(observed,indent=2)+'\n')
                    if manifest is not None:
                        command(compose+['up','-d','--no-build','candidate'])
                        identifier=command(compose+['ps','-q','candidate']).stdout.strip()
                        inspected=json.loads(command(['docker','inspect',identifier]).stdout)[0]
                        result['candidate_binding']=validate_candidate_image(manifest,inspected)
                        candidate_base='http://'+command(compose+['port','candidate','9192']).stdout.strip()
                        import urllib.request
                        deadline=time.monotonic()+90
                        while True:
                            try:
                                with urllib.request.urlopen(candidate_base+'/api/core/version/',timeout=2)as response:assert response.status==200
                                break
                            except (OSError,AssertionError):
                                if time.monotonic()>=deadline:raise RuntimeError('Packaged candidate readiness deadline')
                                time.sleep(.1)
                        sys.path.insert(0,str(tools/'scripts/rust-vod-api-contracts'))
                        from packaged_probe import probe
                        packaged=probe(candidate_base,corpus,observed)
                        (evidence/'packaged-http.json').write_text(json.dumps(packaged,indent=2)+'\n')
                        result['packaged_ordinary_http_passed']=packaged['passed'];assert packaged['passed'],'Packaged ordinary HTTP mismatch'
                        effects=command(compose+['exec','-T','-e','VOD_API_DISPOSABLE_FIXTURE=playback-fixture-only','baseline','/dispatcharrpy/bin/python','/vod-api-probe/effects_source.py'],timeout=30)
                        after=json.loads(effects.stdout.splitlines()[-1]);before={key:corpus['final_effects'][key]for key in after}
                        (evidence/'packaged-effects.json').write_text(json.dumps({'passed':after==before,'before':before,'after':after},indent=2)+'\n');assert after==before,'Packaged reads changed owned metadata/provider refresh rows'
                        shutdown_at=time.monotonic();command(compose+['stop','--timeout','15','candidate'],timeout=25)
                        stopped=json.loads(command(['docker','inspect',identifier]).stdout)[0]['State']
                        result['candidate_shutdown']={'elapsed_seconds':time.monotonic()-shutdown_at,'exit_code':stopped['ExitCode'],'running':stopped['Running']}
                        assert not stopped['Running']and stopped['ExitCode']==0,'Packaged candidate did not stop gracefully'
                        result['application_image_verified']=True
                    if args.native_sql:
                        scheduled=command(compose+['exec','-T','-e','VOD_API_DISPOSABLE_FIXTURE=playback-fixture-only','baseline','/dispatcharrpy/bin/python','/vod-api-probe/schedule_source.py'],timeout=30)
                        (evidence/'source-schedule.json').write_text(json.dumps(json.loads(scheduled.stdout.splitlines()[-1]),indent=2)+'\n')
                        faulted=command(compose+['exec','-T','-e','VOD_API_DISPOSABLE_FIXTURE=playback-fixture-only','baseline','/dispatcharrpy/bin/python','/vod-api-probe/category_fault_source.py'],timeout=30)
                        (evidence/'source-category-fault.json').write_text(json.dumps(json.loads(faulted.stdout.splitlines()[-1]),indent=2)+'\n')
                        pgport=command(compose+['port','baseline','5432']).stdout.strip().rsplit(':',1)[1]
                        redisport=command(compose+['port','baseline','6379']).stdout.strip().rsplit(':',1)[1]
                        native_environment=dict(environment,VOD_TEST_NATIVE_HTTP='1'if args.native_http else'0',VOD_SOURCE_BURST=str(evidence/'source-burst.json'),REDIS_TEST_URL='redis://:playback-fixture-only@127.0.0.1:'+redisport,VOD_TEST_CACHED_AUTH='1',DATABASE_URL='postgresql://dispatch:playback-fixture-only@127.0.0.1:'+pgport+'/dispatcharr',
                            VOD_API_NATIVE_FIXTURE='playback-fixture-only',VOD_SOURCE_CORPUS=str(evidence/'source-corpus.json'),VOD_NATIVE_EVIDENCE=str(evidence),VOD_SOURCE_DEFAULT_SQL=str(tools/'scripts/rust-vod-api-contracts/source-default-queries-1.json'),VOD_SOURCE_SCHEDULE=str(evidence/'source-schedule.json'),VOD_SOURCE_CATEGORY_FAULT=str(evidence/'source-category-fault.json'))
                        target=Path(os.environ.get('VOD_CARGO_TARGET_DIR',str(ROOT/'artifacts/rust/vod-rest-cargo-target')))
                        argv=['cargo','test','--manifest-path',str(tools/'rust/Cargo.toml'),'--target-dir',str(target),'--locked','--offline','-p','dispatcharr-store','--lib',
                            'vod_api_draft::vod_api_native_tests','--',*(['--skip','http_']if not args.native_http else[]),'--ignored','--nocapture','--test-threads=1']
                        result['native_command']=argv
                        with(evidence/'native-sql-tests.log').open('w')as log:
                            native=subprocess.run(argv,cwd=ROOT,env=native_environment,text=True,stdout=log,stderr=subprocess.STDOUT,timeout=240)
                        native.check_returncode()
                        result['native_ordinary_component']=json.loads((evidence/'ordinary-sql.json').read_text())
                        assert result['native_ordinary_component']['passed'],'Native ordinary component mismatch'
                        if args.native_http:
                            result['native_http_component']=json.loads((evidence/'ordinary-http.json').read_text());assert result['native_http_component']['passed']
                        result['native_scope']='Only original ordinary read cases vs nativeSQL component; outside/unsupported families not counted as passes'

                except Exception as error:
                    result.update(failure=type(error).__name__,failure_message=str(error));result['passed']=False
                finally:
                    try:command(compose+['down','--volumes','--remove-orphans','--timeout','5'],timeout=60);result['cleanup']='owned Compose project removed'
                    except Exception:result['cleanup_error']=True;result['passed']=False
            report['modes'].append({'mode':mode,'passed':result['passed'],'cases':result.get('cases'),
                'evidence':str(evidence/'source-run.json'),'cleanup':result.get('cleanup')})
            if not result['passed']:break
        report['passed']=len(report['modes'])==len(modes)and all(row['passed']for row in report['modes'])
    finally:
        (out/'source-run.json').write_text(json.dumps(report,indent=2)+'\n')
    print(json.dumps({'passed':report['passed'],'modes':report['modes'],'output':str(out)}))
    return 0 if report['passed']else 1

if __name__=='__main__':raise SystemExit(main())
