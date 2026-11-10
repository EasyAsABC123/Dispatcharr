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


def freeze_current_rust(output, prefix='current'):
    frozen = output/(prefix+'-frozen-source')
    hashes = {}
    with tarfile.open(output/(prefix+'-rust-source.tar'), 'w') as archive:
        for base, directories, names in os.walk(ROOT/'rust'):
            directories[:] = sorted(d for d in directories if d not in {'target','.git','__pycache__'})
            for name in sorted(names):
                path = Path(base)/name
                relative = path.relative_to(ROOT)
                data = path.read_bytes()
                destination = frozen/relative
                destination.parent.mkdir(parents=True, exist_ok=True)
                destination.write_bytes(data)
                hashes[str(relative)] = hashlib.sha256(data).hexdigest()
                entry=tarfile.TarInfo(str(relative));entry.size=len(data);entry.mode=0o644;entry.mtime=0
                archive.addfile(entry,io.BytesIO(data))
    return frozen, hashes


def main():
    import sys
    sys.path.insert(0, str(ROOT/'scripts/rust-integration'))
    from fixture_ports import write_port_overlay
    from tool_snapshot import snapshot_sources
    integration = load(ROOT/'scripts/rust-integration/run.py', 'epg_read_integration')
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--native', action='store_true', help='Also run frozen native SQL row-projection test')
    parser.add_argument('--native-http', action='store_true', help='Also run original programme JSON/HEAD HTTP and capacity ownership comparisons')
    parser.add_argument('--candidate-image', help='Optional immutable packaged app programme read gate')
    parser.add_argument('--build-manifest', type=Path)
    parser.add_argument('--guide-source', action='store_true', help='Capture original current-programme and streamed guide-grid contracts on owned rows')
    parser.add_argument('--native-current', action='store_true', help='Run frozen current-programme parser/DB selection against original guide capture')
    parser.add_argument('--candidate-current', action='store_true', help='Compare actual Source/native real-clock current-programme wire instead of programme list reads')
    parser.add_argument('--native-grid', action='store_true', help='Run original/native grid metadata and stored-row component comparisons')
    parser.add_argument('--native-grid-http', action='store_true', help='Compare original Factory grid captures to independent native HTTP, plus physical resource gates')
    parser.add_argument('--grid-negotiation-source', action='store_true', help='Capture additional original-only guide Accept/format/method grammar separately from accepted40cases')
    parser.add_argument('--native-grid-wire', action='store_true', help='Draft actual original/native-network guide replay with real clocks and strict expiry/hour guards')
    parser.add_argument('--candidate-grid-wire', action='store_true', help='Compare actual unpatched original/native packaged grid network replies')
    args = parser.parse_args()
    if args.candidate_grid_wire:
        if not args.candidate_image:parser.error('candidate-grid-wire requires candidate-image and build-manifest')
        args.guide_source=True
    if args.native_grid_wire:
        args.guide_source = True
    if args.grid_negotiation_source:
        args.guide_source = True
    if args.native_grid_http:
        args.guide_source = True
    if args.native_grid:
        args.guide_source = True
    if args.native_current:
        args.guide_source = True
    if args.candidate_current:
        if not args.candidate_image:parser.error('candidate-current requires candidate-image and build-manifest')
        args.guide_source = True
    if bool(args.candidate_image) != bool(args.build_manifest):
        parser.error('candidate-image and build-manifest must be supplied together')
    if args.native_http:
        args.native = True
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    tools = output/'tool-source'
    hashes = snapshot_sources(ROOT, tools)
    for relative in ['scripts/rust-epg-contracts/control_reads.py',
                     'scripts/rust-epg-contracts/source_reads.py',
                     'scripts/rust-epg-contracts/window_source.py',
                     'scripts/rust-epg-contracts/probe_programs.py',
                     'scripts/rust-epg-contracts/guide_source.py',
                     'scripts/rust-epg-contracts/grid_wire_source.py',
                     'scripts/rust-epg-contracts/probe_grid.py',
                     'scripts/rust-epg-contracts/current_wire_source.py',
                     'scripts/rust-epg-contracts/probe_current.py',
                     'scripts/rust-epg-parity/extract_source.py']:
        source_tool = ROOT/relative
        target = tools/relative
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(source_tool.read_bytes())
        hashes[relative] = hashlib.sha256(target.read_bytes()).hexdigest()
    project = 'dispatcharr-epg-reads-'+uuid.uuid4().hex[:10]
    report = {'scope':'original-stored-EPG-read-oracle', 'passed':False,
              'native_verified':False, 'performance_verified':False,
              'project':project, 'tool_source_sha256':hashes}
    grid_snapshot = freeze_current_rust(output, 'grid') if args.native_grid or args.native_grid_http or args.native_grid_wire else None
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
        compose = ['docker','compose','-p',project,'-f',str(tools/'docker/compose.rust-playback.yml'),
                   '-f',str(ports)]
        if args.candidate_image:
            candidate_overlay = output/'candidate.yml'
            candidate_overlay.write_text(json.dumps({'services':{'candidate':{
                'image':args.candidate_image,
                'environment':{'DISPATCHARR_TRUSTED_PROXIES':'127.0.0.0/8,172.16.0.0/12,10.0.0.0/8'},
                'cpus':2,'mem_limit':'512m','pids_limit':128}}}))
            compose += ['-f',str(tools/'docker/compose.rust-candidate.yml'),'-f',str(candidate_overlay)]

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
            report['original_helper_oracles'] = {}
            for relative,name in [('scripts/rust-epg-contracts/window_source.py','window'),
                                  ('scripts/rust-epg-parity/extract_source.py','extraction')]:
                oracle = command(compose+['exec','-T','baseline','/dispatcharrpy/bin/python','-'],
                                 timeout=120,body=(tools/relative).read_text())
                (output/(name+'-source-worker.log')).write_text(oracle.stderr)
                data = json.loads(oracle.stdout.splitlines()[-1])
                oracle_path = output/(name+'-source.json')
                oracle_path.write_text(json.dumps(data,indent=2)+'\n')
                report['original_helper_oracles'][name] = {
                    'sha256':hashlib.sha256(oracle_path.read_bytes()).hexdigest(),
                    'cases':sum(len(v) for k,v in data.items() if k.endswith('cases')),
                    'native_verified':False}
            result = command(compose+['exec','-T','baseline','/dispatcharrpy/bin/python','-'],
                             timeout=120, body=(tools/'scripts/rust-epg-contracts/source_reads.py').read_text())
            (output/'source-worker.log').write_text(result.stderr)
            fixture = json.loads(result.stdout.splitlines()[-1])
            fixture_path = output/'source-reads.json'
            fixture_path.write_text(json.dumps(fixture, indent=2)+'\n')
            report.update(passed=True, source_cases=len(fixture['cases']),
                          fixture_sha256=hashlib.sha256(fixture_path.read_bytes()).hexdigest())
            if args.guide_source:
                guide_options = ['-e','GUIDE_NEGOTIATION_CAPTURE=1'] if args.grid_negotiation_source else []
                guide = command(compose+['exec','-T']+guide_options+['baseline','/dispatcharrpy/bin/python','-'],
                                timeout=180,body=(tools/'scripts/rust-epg-contracts/guide_source.py').read_text())
                (output/'guide-source-worker.log').write_text(guide.stderr)
                guide_data=json.loads(guide.stdout.splitlines()[-1])
                (output/'guide-source.json').write_text(json.dumps(guide_data,indent=2)+'\n')
                report['original_guide_http_cases']=len(guide_data['cases'])
                report['original_grid_exploratory_negotiation_cases']=len(guide_data.get('negotiation_cases', []))
                report['native_guide_http_verified']=False
                if args.native_grid:
                    frozen, grid_hashes = grid_snapshot
                    report['grid_rust_source_sha256'] = grid_hashes
                    postgres = command(compose+['port','baseline','5432']).stdout.strip()
                    grid_env = dict(env, DATABASE_TEST_URL='postgresql://dispatch:playback-fixture-only@'+postgres+'/dispatcharr',
                        EPG_READ_DISPOSABLE_FIXTURE='playback-fixture-only', GUIDE_READ_FIXTURE=str(output/'guide-source.json'),
                        CARGO_TARGET_DIR=os.environ.get('EPG_GRID_TARGET_DIR',str(output/'grid-cargo-target')))
                    native = subprocess.run(['cargo','test','--manifest-path',str(frozen/'rust/Cargo.toml'),
                        '-p','dispatcharr-store','--test','epg_grid_sql','--locked','--offline',
                        '--','--ignored','--nocapture','--test-threads=1'],env=grid_env,capture_output=True,text=True,timeout=600)
                    (output/'native-grid.log').write_text(native.stdout+'\n'+native.stderr)
                    native.check_returncode()
                    entity = subprocess.run(['cargo','test','--manifest-path',str(frozen/'rust/Cargo.toml'),
                        '-p','dispatcharr-output-http','--test','epg_grid_sql','--locked','--offline',
                        '--','--ignored','--nocapture','--test-threads=1'],env=grid_env,capture_output=True,text=True,timeout=600)
                    (output/'native-grid-entity.log').write_text(entity.stdout+'\n'+entity.stderr)
                    entity.check_returncode()
                    report['native_grid_complete_entity_verified'] = True
                    report['native_grid_metadata_stored_verified'] = True
                    report['native_grid_http_verified'] = False
                if args.native_grid_http:
                    frozen, grid_hashes = grid_snapshot
                    report['grid_rust_source_sha256'] = grid_hashes
                    postgres = command(compose+['port','baseline','5432']).stdout.strip()
                    redis = command(compose+['port','baseline','6379']).stdout.strip()
                    grid_env = dict(env, DATABASE_TEST_URL='postgresql://dispatch:playback-fixture-only@'+postgres+'/dispatcharr',
                        REDIS_TEST_URL='redis://:playback-fixture-only@'+redis+'/0',
                        EPG_READ_DISPOSABLE_FIXTURE='playback-fixture-only', GUIDE_READ_FIXTURE=str(output/'guide-source.json'),
                        CARGO_TARGET_DIR=os.environ.get('EPG_GRID_TARGET_DIR',str(output/'grid-cargo-target')))
                    with (output/'native-grid-http.log').open('w') as log:
                        native = subprocess.run(['cargo','test','--manifest-path',str(frozen/'rust/Cargo.toml'),
                            '-p','dispatcharr-output-http','--test','epg_grid_http','--locked','--offline',
                            '--','--ignored','--nocapture','--test-threads=1'],env=grid_env,
                            stdout=log,stderr=subprocess.STDOUT,text=True,timeout=900)
                    native.check_returncode()
                    report['native_grid_http_verified'] = True
                    report['grid_http_scope'] = 'Pinned original APIRequestFactory GET entities/errors compared with independent native HTTP; actual original HTTP HEAD separately observed; not packaged app acceptance'
                if args.native_grid_wire:
                    frozen, grid_hashes = grid_snapshot
                    report['grid_rust_source_sha256'] = grid_hashes
                    postgres = command(compose+['port','baseline','5432']).stdout.strip()
                    redis = command(compose+['port','baseline','6379']).stdout.strip()
                    grid_env = dict(env, DATABASE_TEST_URL='postgresql://dispatch:playback-fixture-only@'+postgres+'/dispatcharr',
                        REDIS_TEST_URL='redis://:playback-fixture-only@'+redis+'/0',
                        EPG_READ_DISPOSABLE_FIXTURE='playback-fixture-only',
                        GUIDE_WIRE_FIXTURE=str(output/'guide-wire-source.json'),
                        GRID_WIRE_DRIVER=str(tools/'scripts/rust-epg-contracts/probe_grid.py'),
                        CARGO_TARGET_DIR=os.environ.get('EPG_GRID_TARGET_DIR',str(output/'grid-cargo-target')))
                    test_command=['cargo','test','--manifest-path',str(frozen/'rust/Cargo.toml'),
                        '-p','dispatcharr-output-http','--test','epg_grid_wire','--locked','--offline']
                    # Compile BEFORE capturing the real-clock five-minute fixture.
                    with (output/'native-grid-wire-build.log').open('w') as log:
                        compiled=subprocess.run(test_command+['--no-run'],env=grid_env,
                            stdout=log,stderr=subprocess.STDOUT,text=True,timeout=900)
                    compiled.check_returncode()
                    wire=command(compose+['exec','-T','baseline','/dispatcharrpy/bin/python','-'],
                        timeout=120,body=(tools/'scripts/rust-epg-contracts/grid_wire_source.py').read_text())
                    (output/'guide-wire-source-worker.log').write_text(wire.stderr)
                    wire_data=json.loads(wire.stdout.splitlines()[-1])
                    (output/'guide-wire-source.json').write_text(json.dumps(wire_data,indent=2)+'\n')
                    with (output/'native-grid-wire.log').open('w') as log:
                        native=subprocess.run(test_command+['--','--ignored','--nocapture','--test-threads=1'],
                            env=grid_env,stdout=log,stderr=subprocess.STDOUT,text=True,timeout=180)
                    native.check_returncode()
                    report['native_grid_real_clock_network_verified']=True
                    report['grid_real_clock_network_cases']=len(wire_data['cases'])
                    report['grid_network_scope']='Actual original and independent native HTTP; real clocks; exact bodies; no programme/order/time normalization; app middleware excluded'
                if args.native_current:
                    frozen, hashes = freeze_current_rust(output)
                    report['current_rust_source_sha256']=hashes
                    postgres=command(compose+['port','baseline','5432']).stdout.strip()
                    native_env=dict(env,DATABASE_TEST_URL='postgresql://dispatch:playback-fixture-only@'+postgres+'/dispatcharr',
                        EPG_READ_DISPOSABLE_FIXTURE='playback-fixture-only',GUIDE_READ_FIXTURE=str(output/'guide-source.json'),
                        CARGO_TARGET_DIR=str(output/'current-cargo-target'))
                    native=subprocess.run(['cargo','test','--manifest-path',str(frozen/'rust/Cargo.toml'),
                        '-p','dispatcharr-store','--test','epg_current_sql','--locked','--offline',
                        '--','--ignored','--nocapture','--test-threads=1'],env=native_env,
                        capture_output=True,text=True,timeout=600)
                    (output/'native-current.log').write_text(native.stdout+'\n'+native.stderr)
                    native.check_returncode()
                    report['native_current_db_selection_verified']=True
            if args.native:
                frozen = output/'frozen-source'
                source_hashes = {}
                with tarfile.open(output/'rust-source.tar','w') as rust_archive:
                    for base, dirs, names in os.walk(ROOT/'rust'):
                        dirs[:] = sorted(d for d in dirs if d not in {'target','.git','__pycache__'})
                        for name in sorted(names):
                            original = Path(base)/name
                            relative = original.relative_to(ROOT)
                            data = original.read_bytes()
                            destination = frozen/relative
                            destination.parent.mkdir(parents=True, exist_ok=True)
                            destination.write_bytes(data)
                            source_hashes[str(relative)] = hashlib.sha256(data).hexdigest()
                            entry = tarfile.TarInfo(str(relative))
                            entry.size = len(data)
                            entry.mode = 0o644
                            entry.mtime = 0
                            rust_archive.addfile(entry,io.BytesIO(data))
                report['rust_source_sha256'] = source_hashes
                report['rust_source_archive_sha256'] = hashlib.sha256((output/'rust-source.tar').read_bytes()).hexdigest()
                postgres = command(compose+['port','baseline','5432']).stdout.strip()
                native_env = dict(env, DATABASE_TEST_URL='postgresql://dispatch:playback-fixture-only@'+postgres+'/dispatcharr',
                                  EPG_READ_DISPOSABLE_FIXTURE='playback-fixture-only', EPG_READ_FIXTURE=str(fixture_path),
                                  CARGO_TARGET_DIR=str(output/'cargo-target'))
                result = subprocess.run(['cargo','test','--manifest-path',str(frozen/'rust/Cargo.toml'),
                                         '-p','dispatcharr-store','--test','epg_reads_sql','--locked','--offline',
                                         '--','--ignored','--nocapture'],env=native_env,capture_output=True,
                                        text=True,timeout=600)
                (output/'native-projection.log').write_text(result.stdout+'\n'+result.stderr)
                result.check_returncode()
                report['native_verified'] = True
                report['native_collection_order_verified'] = True
                report['native_order_differences'] = [json.loads(line.split(' ',1)[1]) for line in result.stdout.splitlines() if line.startswith('EPG_ROW_ORDER_DIFFERENCE ')]
                report['native_scope'] = 'Actual-schema row fields and collection order compared without normalization. HTTP/UI transport, permissions, routes, renderers and body admission remain separate gates.'
                if args.native_http:
                    http = subprocess.run(['cargo','test','--manifest-path',str(frozen/'rust/Cargo.toml'),
                                           '-p','dispatcharr-store','--test','epg_program_http_sql','--locked','--offline',
                                           '--','--ignored','--nocapture','--test-threads=1'],env=native_env,
                                          capture_output=True,text=True,timeout=600)
                    (output/'native-program-http.log').write_text(http.stdout+'\n'+http.stderr)
                    http.check_returncode()
                    report['native_program_json_head_http_verified'] = True
                    report['native_program_browsable_html_verified'] = False
                    report['native_program_packaged_image_verified'] = False
                build = subprocess.run(['cargo','build','--manifest-path',str(frozen/'rust/Cargo.toml'),
                                        '-p','dispatcharr-ingest-core','--example','xmltv_format_parity',
                                        '--locked','--offline'],env=native_env,capture_output=True,
                                       text=True,timeout=600)
                (output/'native-format-build.log').write_text(build.stdout+'\n'+build.stderr)
                build.check_returncode()
                format_cases = json.loads((output/'extraction-source.json').read_text())['format_cases']
                replay = subprocess.run([str(output/'cargo-target/debug/examples/xmltv_format_parity')],
                                        input=''.join(json.dumps(case)+'\n' for case in format_cases),
                                        capture_output=True,text=True,timeout=30,check=True)
                rows = [json.loads(line) for line in replay.stdout.splitlines()]
                differences = [{'index':index,'expected':case['expected'],'actual':actual}
                               for index,(case,actual) in enumerate(zip(format_cases,rows))
                               if case['expected'] != actual]
                format_report = {'passed':len(rows)==len(format_cases) and not differences,
                                 'source_cases':len(format_cases),'native_cases':len(rows),
                                 'differences':differences,'source_mime_hint_supplied':True,
                                 'host_mime_lookup_verified':False,'file_extraction_verified':False}
                (output/'native-format.json').write_text(json.dumps(format_report,indent=2)+'\n')
                if not format_report['passed']:
                    raise RuntimeError('Native XMLTV format component mismatches')
                report['native_format_verified'] = True
            if args.candidate_image:
                if args.candidate_grid_wire:
                    wire=command(compose+['exec','-T','baseline','/dispatcharrpy/bin/python','-'],timeout=180,
                        body=(tools/'scripts/rust-epg-contracts/grid_wire_source.py').read_text())
                    (output/'guide-wire-source-worker.log').write_text(wire.stderr)
                    grid_fixture=json.loads(wire.stdout.splitlines()[-1])
                    (output/'guide-wire-source.json').write_text(json.dumps(grid_fixture,indent=2)+'\n')
                if args.candidate_current:
                    wire=command(compose+['exec','-T','baseline','/dispatcharrpy/bin/python','-'],timeout=180,
                                 body=(tools/'scripts/rust-epg-contracts/current_wire_source.py').read_text())
                    (output/'current-wire-source-worker.log').write_text(wire.stderr)
                    current_fixture=json.loads(wire.stdout.splitlines()[-1])
                    (output/'current-wire-source.json').write_text(json.dumps(current_fixture,indent=2)+'\n')
                command(compose+['up','-d','--no-build','candidate'])
                candidate_id=command(compose+['ps','-q','candidate']).stdout.strip()
                inspected=json.loads(command(['docker','inspect',candidate_id]).stdout)[0]
                adapter=load(tools/'scripts/rust-playback-baseline/run.py','epg_candidate_provenance')
                manifest_bytes=args.build_manifest.read_bytes()
                source_hash=adapter.validate_build_manifest(json.loads(manifest_bytes),inspected['Image'],
                    inspected['Config']['Labels'].get('org.opencontainers.image.source-tree-sha256'))
                report['candidate_provenance']={'image_id':inspected['Image'],'source_tree_sha256':source_hash,
                    'build_manifest_sha256':hashlib.sha256(manifest_bytes).hexdigest()}
                candidate='http://'+command(compose+['port','candidate','9192']).stdout.strip()
                deadline=time.monotonic()+30
                while True:
                    try:
                        with urllib.request.urlopen(candidate+'/health/ready',timeout=2) as ready:
                            if ready.status==200:break
                    except OSError:
                        if time.monotonic()>deadline:raise RuntimeError('Packaged guide API readiness deadline')
                        time.sleep(.1)
                probe=load(tools/'scripts/rust-epg-contracts'/('probe_grid.py' if args.candidate_grid_wire else 'probe_current.py' if args.candidate_current else 'probe_programs.py'),'packaged_programme_probe')
                packaged=probe.capture(candidate,grid_fixture,packaged=True) if args.candidate_grid_wire else probe.capture(candidate,current_fixture if args.candidate_current else fixture)
                (output/('packaged-grid.json' if args.candidate_grid_wire else 'packaged-current.json' if args.candidate_current else 'packaged-programmes.json')).write_text(json.dumps(packaged,indent=2)+'\n')
                if not packaged['passed']:raise RuntimeError('Packaged programme response differences')
                report['native_grid_packaged_image_verified' if args.candidate_grid_wire else 'native_current_packaged_image_verified' if args.candidate_current else 'native_program_packaged_image_verified']=True
                command(compose+['stop','--timeout','12','candidate'])
                stopped=json.loads(command(['docker','inspect',candidate_id]).stdout)[0]
                report['candidate_shutdown']={k:stopped['State'][k] for k in ['ExitCode','OOMKilled']}
                if report['candidate_shutdown']!={'ExitCode':0,'OOMKilled':False}:
                    raise RuntimeError('Packaged candidate did not shut down cleanly')
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
