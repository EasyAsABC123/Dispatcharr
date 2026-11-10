#!/usr/bin/env python3
"""Actual original file-to-guide versus bounded native import service proof.

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
import sys
sys.path.insert(0,str(Path(__file__).resolve().parents[2]/'scripts/rust-integration'))
from fixture_ports import write_port_overlay
from tool_snapshot import snapshot_sources
from run import BASELINE,fixture_source
ROOT=Path(__file__).resolve().parents[2]
def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--output',type=Path,required=True);parser.add_argument('--native-test-manifest',type=Path);args=parser.parse_args();output=args.output.resolve();output.mkdir(parents=True,exist_ok=True)
    if any(output.iterdir()):parser.error('Output must be empty')
    tools=output/'tool-source';tool_hashes=snapshot_sources(ROOT,tools)
    for item in (ROOT/'scripts/rust-epg-parity').glob('*.py'):
        target=tools/'scripts/rust-epg-parity'/item.name;target.parent.mkdir(parents=True,exist_ok=True);target.write_bytes(item.read_bytes());tool_hashes[str(item.relative_to(ROOT))]=hashlib.sha256(target.read_bytes()).hexdigest()
    project='dispatcharr-epg-import-'+uuid.uuid4().hex[:10]
    report={'passed':False,'scope':'actual original parse_programs_for_source versus bounded native file-to-PG service on migrated schema','project':project,'performance_verified':False,'application_image_verified':False,'tool_source_sha256':tool_hashes}
    status=1
    with fixture_source(report,False,output) as directory:
        temporary=Path(directory);source=temporary/'source';source.mkdir();source.chmod(0o755)
        env=dict(os.environ,BASELINE_SOURCE_DIR=str(source),BASELINE_EVIDENCE_DIR=str(output),DISPATCHARR_ENV='aio')
        archive=subprocess.check_output(['git','archive',BASELINE],cwd=ROOT);subprocess.run(['tar','-x','--same-permissions','--no-same-owner','-C',str(source)],input=archive,check=True);(source/'media').mkdir(exist_ok=True)
        overlay=output/'epg-import-fixture.yml';overlay.write_text('services:\n  baseline:\n    ports: ["127.0.0.1::5432", "127.0.0.1::6379"]\n  origin:\n    entrypoint: [/dispatcharrpy/bin/python, /ip-tools/ip_origin.py]\n    volumes:\n      - '+str(tools/'scripts/rust-integration')+':/ip-tools:ro\n')
        ports=output/'fixture-ports.yml';report['fixture_ports']=write_port_overlay(ports,{'baseline':[5656,5432,6379],'origin':[8080]})
        compose=['docker','compose','-p',project,'-f',str(tools/'docker/compose.rust-playback.yml'),'-f',str(overlay),'-f',str(ports)]
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
            fixture_tool=(tools/'scripts/rust-epg-parity/import_source.py').read_text()
            completed=subprocess.run(compose+['exec','-T','baseline','/dispatcharrpy/bin/python','-'],env=env,input=fixture_tool,text=True,capture_output=True,timeout=120)
            (output/'source-worker.log').write_text(completed.stderr);completed.check_returncode();golden=json.loads(completed.stdout.splitlines()[-1]);fixture=output/'epg-import-fixture.json';fixture.write_text(json.dumps(golden,indent=2)+'\n')
            if args.native_test_manifest:
                from importlib.util import spec_from_file_location,module_from_spec
                component_spec=spec_from_file_location('frozen_epg_component',tools/'scripts/rust-epg-parity/component_container.py')
                component=module_from_spec(component_spec);component_spec.loader.exec_module(component)
                code,provenance=component.execute(compose,env,output,args.native_test_manifest.resolve(),'source_sql','epg-import-fixture.json')
                check=type('ContainerResult',(),{'returncode':code})()
                report['native_container']=provenance
                report['rust_source_sha256']=provenance['source_sha256']
                report['rust_source_archive_sha256']=provenance['source_archive_sha256']
                rust_hashes=provenance['source_sha256'];captured=None
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
                pg=command(compose+['port','baseline','5432']).strip();redis=command(compose+['port','baseline','6379']).strip()
                test_env=dict(env,DATABASE_TEST_URL='postgresql://dispatch:playback-fixture-only@'+pg+'/dispatcharr',REDIS_TEST_URL='redis://:playback-fixture-only@'+redis+'/0',EPG_IMPORT_DISPOSABLE_FIXTURE='playback-fixture-only',EPG_IMPORT_FIXTURE=str(fixture),CARGO_TARGET_DIR=str(output/'cargo-target'))
                with (output/'rust-tests.log').open('w') as log:check=subprocess.run(['cargo','test','--manifest-path',str(frozen/'rust/Cargo.toml'),'-p','dispatcharr-ingest-epg','--test','source_sql','--locked','--offline','--','--ignored','--nocapture'],env=test_env,stdout=log,stderr=subprocess.STDOUT,timeout=600)
            report.update(passed=check.returncode==0,rust_exit_code=check.returncode,source_cases=len(golden['cases']),source_throttle_scenarios=len(golden.get('throttle_scenarios',[])),source_throttle_requests=sum(len(case['steps']) for case in golden.get('throttle_scenarios',[])),source_proxy_scenarios=len(golden.get('proxy_scenarios',[])),source_proxy_requests=sum(len(case['steps']) for case in golden.get('proxy_scenarios',[])),fixture_sha256=hashlib.sha256(fixture.read_bytes()).hexdigest(),rust_snapshot_manifest_sha256=hashlib.sha256(json.dumps(rust_hashes,sort_keys=True,separators=(',',':')).encode()).hexdigest(),no_external_provider_requests=True)
            status=check.returncode
        except (OSError,ValueError,RuntimeError,subprocess.SubprocessError) as error:report['error_type']=type(error).__name__;status=1
        finally:
            try:command(compose+['down','--volumes','--remove-orphans','--timeout','15']);report['cleanup']='project removed'
            except subprocess.SubprocessError:report['cleanup_error']=True;report['passed']=False;status=1
    print(json.dumps({'passed':report['passed'],'evidence':str(output)}));return status
if __name__=='__main__':raise SystemExit(main())
