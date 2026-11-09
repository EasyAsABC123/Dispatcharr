#!/usr/bin/env python3
"""Actual original logo bytes/CRUD/uploads versus native HTTP in the same disposable network.

Only disposable original PostgreSQL/Redis fixture services are used. Original Dispatcharr authors
retain application credit. No application-image or performance claim is made.
"""
import argparse
import hashlib
import importlib.util
import json
import io
import tarfile
import os
from pathlib import Path
import subprocess
import tempfile
import time
import urllib.request
import uuid
from fixture_ports import write_port_overlay
from tool_snapshot import snapshot_sources
from run import BASELINE,fixture_source,candidate_overlay,validate_candidate_image
ROOT=Path(__file__).resolve().parents[2]
def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--output',type=Path,required=True);parser.add_argument('--candidate-build-manifest',type=Path);parser.add_argument('--case-prefix',default='');args=parser.parse_args();output=args.output.resolve();output.mkdir(parents=True,exist_ok=True)
    if any(output.iterdir()):parser.error('Output must be empty')
    tools=output/'tool-source';tool_hashes=snapshot_sources(ROOT,tools);project='dispatcharr-logos-'+uuid.uuid4().hex[:10]
    manifest=json.loads(args.candidate_build_manifest.read_text())if args.candidate_build_manifest else None
    if manifest is not None and not manifest.get('passed'):parser.error('Successful candidate build manifest required')
    report={'passed':False,'scope':'actual original logo bytes/CRUD/uploads versus native HTTP in original container network and disposable PostgreSQL','project':project,'performance_verified':False,'application_image_verified':False,'tool_source_sha256':tool_hashes}
    status=1
    with fixture_source(report,False,output) as directory:
        temporary=Path(directory);source=temporary/'source';source.mkdir();source.chmod(0o755)
        env=dict(os.environ,BASELINE_SOURCE_DIR=str(source),BASELINE_EVIDENCE_DIR=str(output),DISPATCHARR_ENV='aio')
        archive=subprocess.check_output(['git','archive',BASELINE],cwd=ROOT);subprocess.run(['tar','-x','--same-permissions','--no-same-owner','-C',str(source)],input=archive,check=True);(source/'media').mkdir(exist_ok=True)
        overlay=output/'logo-fixture.yml';overlay.write_text('services:\n  baseline:\n    ports: ["127.0.0.1::5432", "127.0.0.1::6379"]\n  origin:\n    entrypoint: [/dispatcharrpy/bin/python, /ip-tools/logo_origin.py]\n    volumes:\n      - '+str(tools/'scripts/rust-integration')+':/ip-tools:ro\n')
        ports=output/'fixture-ports.yml';report['fixture_ports']=write_port_overlay(ports,{'baseline':[5656,5432,6379],'origin':[8080],**({'candidate':[9192]}if manifest is not None else{})})
        compose=['docker','compose','-p',project,'-f',str(tools/'docker/compose.rust-playback.yml'),'-f',str(overlay),'-f',str(ports)]
        if manifest is not None:
            shared=temporary/'shared-logos';outside=temporary/'shared-outside';shared.mkdir();outside.mkdir();shared.chmod(0o777);outside.chmod(0o777)
            shared_overlay=output/'shared-logo-files.yml';shared_overlay.write_text('services:\n  baseline:\n    volumes:\n      - '+str(shared)+':/data/logos\n      - '+str(outside)+':/tmp/rust-probe-logo-fixture\n  candidate:\n    volumes:\n      - '+str(shared)+':/data/logos\n      - '+str(outside)+':/tmp/rust-probe-logo-fixture\n')
            candidate=candidate_overlay(tools/'docker/compose.rust-candidate.yml',output/'candidate-overlay.yml',manifest['tag'])
            compose+=['-f',str(candidate),'-f',str(shared_overlay),'-f',str(ports)]
            report['build_manifest_sha256']=hashlib.sha256(args.candidate_build_manifest.read_bytes()).hexdigest()
        def command(arguments,timeout=180,body=None):return subprocess.check_output(arguments,env=env,input=body,text=True,stderr=subprocess.PIPE,timeout=timeout)
        try:
            command(compose+['up','-d','baseline'])
            base='http://'+command(compose+['port','baseline','5656']).strip();deadline=time.monotonic()+180
            while True:
                try:
                    with urllib.request.urlopen(base+'/api/core/version/',timeout=2) as response:
                        if response.status==200:break
                except OSError:
                    if time.monotonic()>deadline:raise RuntimeError('Source readiness failed')
                    time.sleep(.2)
            marker_helper=(tools/'scripts/rust-integration/logo_marker_witness.py').read_text()
            helper_prefix='import types,sys\n_helper=types.ModuleType("logo_marker_witness")\nexec('+repr(marker_helper)+',_helper.__dict__)\nsys.modules["logo_marker_witness"]=_helper\n'
            fixture_tool=helper_prefix+'import os\nos.environ["LOGO_CASE_PREFIX"]='+repr(args.case_prefix)+'\n'+(tools/'scripts/rust-integration/logo_fixture.py').read_text()
            if manifest is not None:fixture_tool=fixture_tool.replace('/tmp/rust-probe-logo-outside.png','/tmp/rust-probe-logo-fixture/rust-probe-logo-outside.png')
            completed=subprocess.run(compose+['exec','-T','baseline','/dispatcharrpy/bin/python','-'],env=env,input=fixture_tool,text=True,capture_output=True,timeout=120)
            (output/'source-worker.log').write_text(completed.stderr);completed.check_returncode();golden=json.loads(completed.stdout.splitlines()[-1]);fixture=output/'logo-fixture.json';fixture.write_text(json.dumps(golden,indent=2)+'\n')
            if manifest is not None:
                command(compose+['up','-d','--no-build','candidate'])
                container=command(compose+['ps','-q','candidate']).strip();inspection=json.loads(command(['docker','inspect',container]))[0];report['candidate_image_binding']=validate_candidate_image(manifest,inspection)
                candidate_url='http://'+command(compose+['port','candidate','9192']).strip();deadline=time.monotonic()+90
                while True:
                    try:
                        with urllib.request.urlopen(candidate_url+'/health/ready',timeout=2)as response:
                            if response.status==200:break
                    except OSError:
                        if time.monotonic()>deadline:raise RuntimeError('Candidate readiness failed')
                        time.sleep(.2)
                probe=helper_prefix+(tools/'scripts/rust-integration/logo_packaged_probe.py').read_text()
                completed=subprocess.run(compose+['exec','-T','baseline','/dispatcharrpy/bin/python','-'],env=env,input=probe,text=True,capture_output=True,timeout=180)
                (output/'packaged-probe.log').write_text(completed.stderr);completed.check_returncode();actual=json.loads(completed.stdout.splitlines()[-1]);(output/'packaged-checks.json').write_text(json.dumps(actual,indent=2)+'\n');report.update(passed=actual['passed'],application_image_verified=True,source_cases=actual['source_cases'],source_http_responses=actual['source_http_responses'],packaged_checks_sha256=hashlib.sha256((output/'packaged-checks.json').read_bytes()).hexdigest());status=0 if actual['passed']else 1
            else:
                captured={str(path.relative_to(ROOT)):path.read_bytes() for path in (ROOT/'rust').rglob('*') if path.is_file() and not set(path.relative_to(ROOT/'rust').parts)&{'target','.git','__pycache__'}}
                rust_hashes={name:hashlib.sha256(data).hexdigest() for name,data in captured.items()}
                with tarfile.open(output/'rust-source.tar','w') as archive:
                    for relative,data in sorted(captured.items()):
                        info=tarfile.TarInfo(relative);info.size=len(data);info.mode=0o644;info.mtime=0;archive.addfile(info,io.BytesIO(data))
                report['rust_source_sha256']=rust_hashes
                report['rust_source_archive_sha256']=hashlib.sha256((output/'rust-source.tar').read_bytes()).hexdigest()
                frozen=temporary/'rust-source'
                for relative,data in captured.items():target=frozen/relative;target.parent.mkdir(parents=True,exist_ok=True);target.write_bytes(data)
                test_env=dict(env,CARGO_TARGET_DIR=str(ROOT/'rust/target'))
                built=subprocess.run(['cargo','test','--manifest-path',str(frozen/'rust/Cargo.toml'),'-p','dispatcharr-store','--test','logo_sql','--no-run','--message-format=json','--locked','--offline'],env=test_env,capture_output=True,text=True,timeout=600)
                (output/'rust-build.log').write_text(built.stderr)
                if built.returncode:raise RuntimeError('Native fixture binary build failed')
                products=[json.loads(line)for line in built.stdout.splitlines()if line.startswith('{')]
                executable=next(Path(item['executable'])for item in products if item.get('executable')and item.get('target',{}).get('name')=='logo_sql')
                import shutil
                binary=output/'native-logo-probe';shutil.copy2(executable,binary);binary.chmod(0o755)
                report['native_test_executable_sha256']=hashlib.sha256(binary.read_bytes()).hexdigest()
                test_command=compose+['exec','-T','-e','DATABASE_TEST_URL=postgresql://dispatch:playback-fixture-only@127.0.0.1:5432/dispatcharr','-e','REDIS_TEST_URL=redis://:playback-fixture-only@127.0.0.1:6379/0','-e','LOGO_DISPOSABLE_FIXTURE=playback-fixture-only','-e','LOGO_FIXTURE=/evidence/logo-fixture.json','baseline','/evidence/native-logo-probe','--ignored','--nocapture']
                with (output/'rust-tests.log').open('w') as log:check=subprocess.run(test_command,env=env,stdout=log,stderr=subprocess.STDOUT,timeout=600)
                report.update(passed=check.returncode==0,rust_exit_code=check.returncode,source_cases=len(golden['cases']),source_throttle_scenarios=len(golden.get('throttle_scenarios',[])),source_throttle_requests=sum(len(case['steps']) for case in golden.get('throttle_scenarios',[])),source_proxy_scenarios=len(golden.get('proxy_scenarios',[])),source_proxy_requests=sum(len(case['steps']) for case in golden.get('proxy_scenarios',[])),fixture_sha256=hashlib.sha256(fixture.read_bytes()).hexdigest(),rust_snapshot_manifest_sha256=hashlib.sha256(json.dumps({name:hashlib.sha256(data).hexdigest() for name,data in captured.items()},sort_keys=True,separators=(',',':')).encode()).hexdigest(),no_external_provider_requests=True)
                status=check.returncode
        except (OSError,ValueError,RuntimeError,subprocess.SubprocessError) as error:report['error_type']=type(error).__name__;status=1
        finally:
            try:command(compose+['down','--volumes','--remove-orphans']);report['cleanup']='project removed'
            except subprocess.SubprocessError:report['cleanup_error']=True;report['passed']=False;status=1
    print(json.dumps({'passed':report['passed'],'evidence':str(output)}));return status
if __name__=='__main__':raise SystemExit(main())
