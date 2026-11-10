#!/usr/bin/env python3
"""Capture original EPG read behavior in disposable Compose infrastructure.

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

    parser.add_argument('--native-order',action='store_true',help='Run frozen native Store ordering regression')
    args = parser.parse_args()
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    tools = output/'tool-source'
    hashes = snapshot_sources(ROOT, tools)
    for relative in ['scripts/rust-epg-contracts/run_query_order.py',
                     'scripts/rust-epg-contracts/source_reads.py',
                     'scripts/rust-epg-contracts/window_source.py',
                     'scripts/rust-epg-contracts/query_order_source.py',
                     'scripts/rust-epg-parity/extract_source.py']:
        source_tool = ROOT/relative
        target = tools/relative
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(source_tool.read_bytes())
        hashes[relative] = hashlib.sha256(target.read_bytes()).hexdigest()
    project = 'dispatcharr-epg-reads-'+uuid.uuid4().hex[:10]
    report = {'scope':'same-database-original-native-EPG-query-order-diagnosis', 'passed':False,
              'native_verified':False, 'performance_verified':False,
              'project':project, 'tool_source_sha256':hashes}
    status = 1
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
        report['fixture_ports'] = write_port_overlay(ports, {'baseline':[5656,5432,6379]})
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
                             timeout=120, body=(tools/'scripts/rust-epg-contracts/source_reads.py').read_text())
            (output/'source-worker.log').write_text(result.stderr)
            fixture = json.loads(result.stdout.splitlines()[-1])
            fixture_path = output/'source-reads.json'
            fixture_path.write_text(json.dumps(fixture, indent=2)+'\n')
            report.update(passed=True, source_cases=len(fixture['cases']),
                          fixture_sha256=hashlib.sha256(fixture_path.read_bytes()).hexdigest())
            import re
            provider = (ROOT/'rust/crates/store/src/provider_reads.rs').read_bytes()
            (output/'provider_reads.rs').write_bytes(provider)
            native = re.search(r'Self::EpgSources=>"([^"\n]+)"',provider.decode()).group(1)
            prepared_native = native
            native = native.replace('$1::bigint','NULL::bigint').replace('$2::bigint','NULL::bigint').replace('$1','NULL').replace('$2','NULL')
            queries = {'source':fixture['collection_ordering']['sources']['sql'], 'native_literal':native, 'native_prepared':prepared_native}
            injected = 'QUERY_ORDER_INPUT = '+repr(queries)+'\n'
            diagnosis = command(compose+['exec','-T','baseline','/dispatcharrpy/bin/python','-'],
                                timeout=120, body=injected+(tools/'scripts/rust-epg-contracts/query_order_source.py').read_text())
            (output/'query-worker.log').write_text(diagnosis.stderr)
            data = json.loads(diagnosis.stdout.splitlines()[-1])
            (output/'query-plans.json').write_text(json.dumps(data,indent=2)+'\n')
            report['query_ids'] = {key:value['ids'] for key,value in data['queries'].items()}
            report['provider_sha256'] = hashlib.sha256(provider).hexdigest()
            if args.native_order:
                frozen = output/'frozen-source'
                source_hashes = {}
                with tarfile.open(output/'rust-source.tar','w') as rust_archive:
                    for base, dirs, names in os.walk(ROOT/'rust'):
                        dirs[:] = sorted(d for d in dirs if d not in {'target','.git','__pycache__'})
                        for name in sorted(names):
                            original = Path(base)/name
                            relative = original.relative_to(ROOT)
                            data_bytes = original.read_bytes()
                            destination = frozen/relative
                            destination.parent.mkdir(parents=True, exist_ok=True)
                            destination.write_bytes(data_bytes)
                            source_hashes[str(relative)] = hashlib.sha256(data_bytes).hexdigest()
                            entry = tarfile.TarInfo(str(relative))
                            entry.size = len(data_bytes)
                            entry.mode = 0o644
                            entry.mtime = 0
                            rust_archive.addfile(entry,io.BytesIO(data_bytes))
                assert source_hashes['rust/crates/store/src/provider_reads.rs'] == report['provider_sha256']
                report['rust_source_sha256'] = source_hashes
                report['rust_source_archive_sha256'] = hashlib.sha256((output/'rust-source.tar').read_bytes()).hexdigest()
                postgres = command(compose+['port','baseline','5432']).stdout.strip()
                native_env = dict(env, DATABASE_TEST_URL='postgresql://dispatch:playback-fixture-only@'+postgres+'/dispatcharr',
                                  EPG_READ_DISPOSABLE_FIXTURE='playback-fixture-only', EPG_READ_FIXTURE=str(fixture_path),
                                  CARGO_TARGET_DIR=str(output/'cargo-target'))
                result = subprocess.run(['cargo','test','--manifest-path',str(frozen/'rust/Cargo.toml'),
                                         '-p','dispatcharr-store','--test','epg_order_sql','--locked','--offline',
                                         '--','--ignored','--nocapture'],env=native_env,capture_output=True,
                                        text=True,timeout=600)
                (output/'native-order.log').write_text(result.stdout+'\n'+result.stderr)
                result.check_returncode()
                report['native_verified'] = True
                report['native_collection_order_verified'] = True
                report['native_order_cases'] = 144
                assert all(value['ids'] == data['queries']['source']['ids'] for value in data['queries'].values())
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
