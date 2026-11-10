#!/usr/bin/env python3
"""Capture original guide import producer/status effects in disposable Compose infrastructure.

This source-only gate is not native acceptance or a performance result.
Original Dispatcharr authors and contributors retain application credit.
"""
import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import io
import tarfile
import subprocess
import time
import urllib.request
import uuid

ROOT = Path(__file__).resolve().parents[2]


def load(path, name):
    spec = importlib.util.spec_from_file_location(name, path)
    value = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(value)
    return value


def main():
    import sys
    sys.path.insert(0, str(ROOT/'scripts/rust-integration'))
    from fixture_ports import write_port_overlay
    from tool_snapshot import snapshot_sources
    integration = load(ROOT/'scripts/rust-integration/run.py', 'epg_read_integration')
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)

    parser.add_argument('--native',action='store_true',help='Compare frozen native channel helper against original rows/progress')
    parser.add_argument('--native-test-manifest',type=Path)
    args = parser.parse_args()
    if args.native_test_manifest:args.native=True
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    tools = output/'tool-source'
    hashes = snapshot_sources(ROOT, tools)
    for relative in ['scripts/rust-epg-parity/run_channels.py',
                     'scripts/rust-epg-parity/channels_source.py',
                     'scripts/rust-epg-parity/component_container.py',
                     'scripts/rust-epg-contracts/source_reads.py',
                     'scripts/rust-epg-contracts/window_source.py',
                     'scripts/rust-epg-contracts/query_order_source.py',
                     'scripts/rust-epg-parity/extract_source.py']:
        source_tool = ROOT/relative
        target = tools/relative
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(source_tool.read_bytes())
        hashes[relative] = hashlib.sha256(target.read_bytes()).hexdigest()
    project = 'dispatcharr-epg-channels-'+uuid.uuid4().hex[:10]
    report = {'scope':'original-channel-guide-helper-ordering', 'passed':False,
              'native_verified':False, 'performance_verified':False,
              'project':project, 'tool_source_sha256':hashes}
    status = 1
    if args.native and not args.native_test_manifest:
        captured={str(path.relative_to(ROOT)):path.read_bytes() for path in (ROOT/'rust').rglob('*') if path.is_file() and not set(path.relative_to(ROOT/'rust').parts)&{'target','.git','__pycache__'}}
        rust_hashes={name:hashlib.sha256(data).hexdigest() for name,data in captured.items()}
        with tarfile.open(output/'rust-source.tar','w') as archive:
            for relative,data_bytes in sorted(captured.items()):
                info=tarfile.TarInfo(relative);info.size=len(data_bytes);info.mode=0o644;info.mtime=0;archive.addfile(info,io.BytesIO(data_bytes))
        report['rust_source_sha256']=rust_hashes
        report['rust_source_archive_sha256']=hashlib.sha256((output/'rust-source.tar').read_bytes()).hexdigest()
    with integration.fixture_source(report, False, output) as directory:
        source = Path(directory)/'source'
        source.mkdir(mode=0o755)
        archive = subprocess.check_output(['git','archive',integration.BASELINE], cwd=ROOT)
        subprocess.run(['tar','-x','--same-permissions','--no-same-owner','-C',str(source)],
                       input=archive, check=True)
        (source/'media').mkdir(exist_ok=True)
        env = dict(os.environ, BASELINE_SOURCE_DIR=str(source),
                   BASELINE_EVIDENCE_DIR=str(output), DISPATCHARR_ENV='aio')
        ports = output/'fixture-ports.yml'
        report['fixture_ports'] = write_port_overlay(ports, {'baseline':[5656,5432,6379], 'origin':[8080]})
        origin_overlay = output/'origin.yml'
        origin_overlay.write_text('services:\n  origin:\n    entrypoint: [/dispatcharrpy/bin/python, /ip-tools/ip_origin.py]\n    volumes:\n      - '+str(tools/'scripts/rust-integration')+':/ip-tools:ro\n')
        compose = ['docker','compose','-p',project,'-f',str(tools/'docker/compose.rust-playback.yml'),
                   '-f',str(ports),'-f',str(origin_overlay)]

        def command(arguments, timeout=180, body=None):
            return subprocess.run(arguments, env=env, input=body, text=True,
                                  capture_output=True, timeout=timeout, check=True)

        try:
            command(compose+['up','-d','baseline'])
            base = 'http://'+command(compose+['port','baseline','5656']).stdout.strip()
            deadline = time.monotonic()+180
            while True:
                try:
                    with urllib.request.urlopen(base+'/api/core/version/', timeout=2) as response:
                        if response.status == 200:
                            break
                except OSError:
                    if time.monotonic() >= deadline:
                        raise RuntimeError('Source readiness deadline')
                    time.sleep(.2)
            result = command(compose+['exec','-T','baseline','/dispatcharrpy/bin/python','-'],
                             timeout=120, body=(tools/'scripts/rust-epg-parity/channels_source.py').read_text())
            (output/'source-worker.log').write_text(result.stderr)
            fixture = json.loads(result.stdout.splitlines()[-1])
            fixture_path = output/'channels-source.json'
            fixture_path.write_text(json.dumps(fixture, indent=2)+'\n')
            report.update(passed=True, source_cases=len(fixture['cases']),
                          fixture_sha256=hashlib.sha256(fixture_path.read_bytes()).hexdigest())
            if args.native:
                if args.native_test_manifest:
                    spec=importlib.util.spec_from_file_location('channel_component',tools/'scripts/rust-epg-parity/component_container.py')
                    component=importlib.util.module_from_spec(spec);spec.loader.exec_module(component)
                    code,provenance=component.execute(compose,env,output,args.native_test_manifest.resolve(),'channels_sql','channels-source.json')
                    report['native_container']=provenance
                    report['rust_source_sha256']=provenance['source_sha256']
                    report['rust_source_archive_sha256']=provenance['source_archive_sha256']
                    if code!=0:raise RuntimeError('Native channel component failed')
                else:
                    frozen=Path(directory)/'rust-source'
                    for relative,data_bytes in captured.items():
                        target=frozen/relative;target.parent.mkdir(parents=True,exist_ok=True);target.write_bytes(data_bytes)
                    pg=command(compose+['port','baseline','5432']).stdout.strip()
                    redis=command(compose+['port','baseline','6379']).stdout.strip()
                    native_env=dict(env,DATABASE_TEST_URL='postgresql://dispatch:playback-fixture-only@'+pg+'/dispatcharr',REDIS_TEST_URL='redis://:playback-fixture-only@'+redis+'/0',EPG_CHANNEL_DISPOSABLE_FIXTURE='playback-fixture-only',EPG_CHANNEL_FIXTURE=str(fixture_path),CARGO_TARGET_DIR=str(output/'cargo-target'))
                    result=subprocess.run(['cargo','test','--manifest-path',str(frozen/'rust/Cargo.toml'),'-p','dispatcharr-ingest-epg','--test','channels_sql','--locked','--offline','--','--ignored','--nocapture','--test-threads=1'],env=native_env,text=True,capture_output=True,timeout=600)
                    (output/'native-channels.log').write_text(result.stdout+'\n'+result.stderr)
                    result.check_returncode()
                report.update(native_verified=True,native_source_cases=21,native_physical_scenarios=2,application_jobs_verified=False,websocket_transport_verified=False,auto_logo_enabled_variants_verified=True)
            status = 0
        except subprocess.CalledProcessError as error:
            (output/'failed-command.log').write_text((error.stdout or '')+'\n'+(error.stderr or ''))
            report['passed'] = False
            report['error_type'] = type(error).__name__
        except (OSError, ValueError, RuntimeError, subprocess.SubprocessError) as error:
            report['passed'] = False
            report['error_type'] = type(error).__name__
        finally:
            try:
                command(compose+['down','--volumes','--remove-orphans'])
                report['cleanup'] = 'project removed'
            except subprocess.SubprocessError:
                report.update(passed=False, cleanup_error=True)
                status = 1
    (output/'result.json').write_text(json.dumps(report, indent=2)+'\n')
    print(json.dumps({'passed':report['passed'],'evidence':str(output), 'native_verified':report['native_verified']}))
    return status


if __name__ == '__main__':
    raise SystemExit(main())
