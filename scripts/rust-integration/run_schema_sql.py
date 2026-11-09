#!/usr/bin/env python3
"""Actual original native fresh schema/default initialization versus original migrations.

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
from run import BASELINE,fixture_source
ROOT=Path(__file__).resolve().parents[2]
def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--output',type=Path,required=True);args=parser.parse_args();output=args.output.resolve();output.mkdir(parents=True,exist_ok=True)
    if any(output.iterdir()):parser.error('Output must be empty')
    tools=output/'tool-source';tool_hashes=snapshot_sources(ROOT,tools);project='dispatcharr-schema-'+uuid.uuid4().hex[:10]
    report={'passed':False,'scope':'native fresh schema and defaults versus original migrated reference','project':project,'performance_verified':False,'application_image_verified':False,'tool_source_sha256':tool_hashes}
    status=1
    with fixture_source(report,False,output) as directory:
        temporary=Path(directory);source=temporary/'source';source.mkdir();source.chmod(0o755)
        env=dict(os.environ,BASELINE_SOURCE_DIR=str(source),BASELINE_EVIDENCE_DIR=str(output),DISPATCHARR_ENV='aio')
        archive=subprocess.check_output(['git','archive',BASELINE],cwd=ROOT);subprocess.run(['tar','-x','--same-permissions','--no-same-owner','-C',str(source)],input=archive,check=True);(source/'media').mkdir(exist_ok=True)
        overlay=output/'bootstrap-fixture.yml';overlay.write_text('services:\n  baseline:\n    entrypoint: [bash, /schema-tools/capture_clean_schema.sh]\n    environment: {SCHEMA_REFERENCE_SERVE: \"1\"}\n    volumes: [\"'+str(tools/'scripts/rust-integration')+':/schema-tools:ro\"]\n    ports: ["127.0.0.1::5432", "127.0.0.1::6379"]\n  origin:\n    entrypoint: [/dispatcharrpy/bin/python, /ip-tools/ip_origin.py]\n    volumes:\n      - '+str(tools/'scripts/rust-integration')+':/ip-tools:ro\n')
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
            for name in ['dispatcharr_native','dispatcharr_sentinel','dispatcharr_race','dispatcharr_failure']:
                command(compose+['exec','-T','baseline','/usr/lib/postgresql/17/bin/createdb','-h','/var/run/postgresql','-U','dispatch','--template=template0',name])
            fixture=output/'defaults.json'
            golden=json.loads(fixture.read_text())
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
            test_env=dict(env,DATABASE_TEST_URL='postgresql://dispatch:playback-fixture-only@'+pg+'/dispatcharr_native',REDIS_TEST_URL='redis://:playback-fixture-only@'+redis+'/0',SCHEMA_DISPOSABLE_FIXTURE='playback-fixture-only',DATABASE_REFERENCE_URL='postgresql://dispatch:playback-fixture-only@'+pg+'/dispatcharr',DATABASE_SENTINEL_URL='postgresql://dispatch:playback-fixture-only@'+pg+'/dispatcharr_sentinel',DATABASE_RACE_URL='postgresql://dispatch:playback-fixture-only@'+pg+'/dispatcharr_race',DATABASE_FAILURE_URL='postgresql://dispatch:playback-fixture-only@'+pg+'/dispatcharr_failure',CARGO_TARGET_DIR=str(ROOT/'rust/target'))
            with (output/'rust-tests.log').open('w') as log:check=subprocess.run(['cargo','test','--manifest-path',str(frozen/'rust/Cargo.toml'),'-p','dispatcharr-store','--test','schema_sql','--locked','--offline','--','--ignored','--nocapture'],env=test_env,stdout=log,stderr=subprocess.STDOUT,timeout=600)
            report.update(passed=check.returncode==0,rust_exit_code=check.returncode,source_cases=len(golden['tables']),source_default_rows=sum(golden['tables'].values()),source_migrations=len(golden['migrations']),source_throttle_scenarios=len(golden.get('throttle_scenarios',[])),source_throttle_requests=sum(len(case['steps']) for case in golden.get('throttle_scenarios',[])),source_proxy_scenarios=len(golden.get('proxy_scenarios',[])),source_proxy_requests=sum(len(case['steps']) for case in golden.get('proxy_scenarios',[])),fixture_sha256=hashlib.sha256(fixture.read_bytes()).hexdigest(),rust_snapshot_manifest_sha256=hashlib.sha256(json.dumps({name:hashlib.sha256(data).hexdigest() for name,data in captured.items()},sort_keys=True,separators=(',',':')).encode()).hexdigest(),no_external_provider_requests=True)
            if check.returncode==0:
                with (output/'rollback-tests.log').open('w')as log:rollback=subprocess.run(['cargo','test','--manifest-path',str(frozen/'rust/Cargo.toml'),'-p','dispatcharr-store','--lib','schema::tests::default_failure_after_all_ddl_rolls_back_and_retry_initializes','--locked','--offline','--','--ignored','--nocapture'],env=test_env,stdout=log,stderr=subprocess.STDOUT,timeout=600)
                report['rollback_test_exit_code']=rollback.returncode
                report['passed']=report['passed'] and rollback.returncode==0
                native_ddl=command(compose+['exec','-T','baseline','/usr/lib/postgresql/17/bin/pg_dump','-h','/var/run/postgresql','-U','dispatch','-d','dispatcharr_native','--schema-only','--no-owner','--no-acl'])
                (output/'native-schema.raw.sql').write_text(native_ddl)
                def ddl_body(raw):return '\n'.join(line for line in raw.splitlines() if not line.startswith('\\') and not line.startswith('SET ') and not line.startswith("SELECT pg_catalog.set_config('search_path'"))+'\n'
                expected_ddl=ddl_body((output/'schema.raw.sql').read_text());actual_ddl=ddl_body(native_ddl)
                report['schema_ddl_equal']=expected_ddl==actual_ddl
                report['source_schema_ddl_sha256']=hashlib.sha256(expected_ddl.encode()).hexdigest()
                report['native_schema_ddl_sha256']=hashlib.sha256(actual_ddl.encode()).hexdigest()
                report['passed']=report['passed'] and report['schema_ddl_equal']
            status=0 if report['passed'] else 1
        except (OSError,ValueError,RuntimeError,subprocess.SubprocessError) as error:report['error_type']=type(error).__name__;status=1
        finally:
            try:command(compose+['down','--volumes','--remove-orphans']);report['cleanup']='project removed'
            except subprocess.SubprocessError:report['cleanup_error']=True;report['passed']=False;status=1
    print(json.dumps({'passed':report['passed'],'evidence':str(output)}));return status
if __name__=='__main__':raise SystemExit(main())
