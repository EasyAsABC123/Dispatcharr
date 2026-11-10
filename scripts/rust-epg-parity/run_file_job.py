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
import shutil
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
    parser.add_argument('--native',action='store_true',help='Frozen native ordinary-local-file job effects; compressed/remote and child execution remain open')

    parser.add_argument('--native-test-manifest',type=Path)
    parser.add_argument('--current-order-native',action='store_true',help='Store ordered programme-first/full-metadata pipeline using Source order fixture; no app hook')
    parser.add_argument('--current-bridge',action='store_true',help='Real Store to real CurrentLookup with one-connection pool, Source outcomes and physical ownership')
    parser.add_argument('--current-lookup',action='store_true',help='Native component-only fresh-index/file lookup and physical guards; no app or durable queue')
    parser.add_argument('--index-service',action='store_true',help='Native retained index SQL/task/file-lease component; separate from pure scanner')
    parser.add_argument('--source-helper',choices=['file_job_source.py','auto_logos_source.py','programme_index_source.py','current_lookup_source.py','current_order_source.py','grid_concurrency_source.py'],default='file_job_source.py')
    parser.add_argument('--grid-concurrency-native',action='store_true',help='Actual Store grid metadata relation-lock comparison; no HTTP or performance acceptance')
    args = parser.parse_args()
    if args.grid_concurrency_native:
        if args.source_helper!='grid_concurrency_source.py' or args.native or args.native_test_manifest or args.current_order_native or args.current_bridge or args.current_lookup or args.index_service:parser.error('Grid native gate requires its Source helper and no other native modes')
        args.native=True
    if args.source_helper=='grid_concurrency_source.py' and not args.grid_concurrency_native and (args.native or args.native_test_manifest or args.current_order_native or args.current_bridge or args.current_lookup or args.index_service):parser.error('Grid concurrency oracle is Source-only')
    if args.current_bridge:args.current_lookup=True
    if args.current_order_native:
        if args.source_helper!='current_order_source.py' or args.native_test_manifest or args.current_lookup or args.index_service:parser.error('Store current-order gate requires its Source helper and host component tests')
        args.native=True
    if args.current_lookup:
        if args.source_helper!='current_lookup_source.py' or args.native_test_manifest:parser.error('Current lookup requires its Source helper; Docker test packaging is not implemented')
        args.native=True
    if args.source_helper=='current_order_source.py' and not args.current_order_native and (args.native or args.native_test_manifest or args.index_service):parser.error('Ordering oracle is Source-only')
    if args.source_helper=='current_lookup_source.py' and args.native and not args.current_lookup:parser.error('Use --current-lookup for the native component')
    if args.native_test_manifest:args.native=True
    if args.index_service:
        if args.source_helper!='programme_index_source.py':parser.error('Index service requires programme_index_source.py')
        args.native=True
    if args.native_test_manifest and args.source_helper=='programme_index_source.py':parser.error('Index component Docker packaging is not implemented yet')
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    tools = output/'tool-source'
    hashes = snapshot_sources(ROOT, tools)
    for relative in ['scripts/rust-epg-parity/run_file_job.py',
                     'scripts/rust-epg-parity/file_job_source.py',
                     'scripts/rust-epg-parity/auto_logos_source.py',
                     'scripts/rust-epg-parity/programme_index_source.py',
                     'scripts/rust-epg-parity/current_lookup_source.py',
                     'scripts/rust-epg-parity/current_order_source.py',
                     'scripts/rust-epg-parity/grid_concurrency_source.py',
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
    project = 'dispatcharr-epg-file-job-'+uuid.uuid4().hex[:10]
    report = {'scope':('original-grid-concurrency' if args.source_helper=='grid_concurrency_source.py' else 'original-current-programme-metadata-order' if args.source_helper=='current_order_source.py' else 'original-current-file-lookup-preconditions' if args.source_helper=='current_lookup_source.py' else 'original-automatic-EPG-logo-helper' if args.source_helper=='auto_logos_source.py' else 'original-programme-index' if args.source_helper=='programme_index_source.py' else 'original-file-refresh-task-ordering'), 'passed':False,
              'native_verified':False, 'performance_verified':False,
              'project':project, 'tool_source_sha256':hashes,'source_helper':args.source_helper}
    status = 1
    if args.native and not args.native_test_manifest:
        captured={}
        # Prune build trees before traversal, rather than stat every target
        # artifact while other agents hold a coherent source capture window.
        for base,directories,names in os.walk(ROOT/'rust'):
            directories[:]=sorted(name for name in directories if name not in {'target','.git','__pycache__'})
            for name in sorted(names):
                path=Path(base)/name
                if path.is_symlink() or not path.is_file():raise ValueError('Rust snapshot requires regular repository files')
                captured[str(path.relative_to(ROOT))]=path.read_bytes()
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
                             timeout=120, body=(tools/'scripts/rust-epg-parity'/args.source_helper).read_text())
            (output/'source-worker.log').write_text(result.stderr)
            fixture = json.loads(result.stdout.splitlines()[-1])
            fixture_path = output/('grid-concurrency-source.json' if args.source_helper=='grid_concurrency_source.py' else 'current-order-source.json' if args.source_helper=='current_order_source.py' else 'current-lookup-source.json' if args.source_helper=='current_lookup_source.py' else 'auto-logos-source.json' if args.source_helper=='auto_logos_source.py' else 'programme-index-source.json' if args.source_helper=='programme_index_source.py' else 'file-job-source.json')
            fixture_path.write_text(json.dumps(fixture, indent=2)+'\n')
            report.update(passed=True, source_cases=len(fixture['cases']),
                          fixture_sha256=hashlib.sha256(fixture_path.read_bytes()).hexdigest())
            if args.source_helper=='current_order_source.py':report['source_mutation_schedules']=len(fixture['mutation_schedules'])
            if args.grid_concurrency_native:
                frozen=output/'frozen-source'
                for relative,data_bytes in captured.items():
                    target=frozen/relative;target.parent.mkdir(parents=True,exist_ok=True);target.write_bytes(data_bytes)
                pg=command(compose+['port','baseline','5432']).stdout.strip()
                native_env=dict(env,DATABASE_TEST_URL='postgresql://dispatch:playback-fixture-only@'+pg+'/dispatcharr',GRID_CONCURRENCY_DISPOSABLE_FIXTURE='playback-fixture-only',GRID_CONCURRENCY_FIXTURE=str(fixture_path),CARGO_TARGET_DIR=str(output/'cargo-target'))
                result=subprocess.run(['cargo','test','--manifest-path',str(frozen/'rust/Cargo.toml'),'-p','dispatcharr-store','--test','grid_concurrency_sql','--locked','--offline','--','--ignored','--nocapture','--test-threads=1'],env=native_env,text=True,capture_output=True,timeout=600)
                (output/'native-grid-concurrency.log').write_text(result.stdout+'\n'+result.stderr)
                result.check_returncode()
                report.update(native_verified=True,native_metadata_relation_lock_verified=True,application_stream_schedule_verified=False)
            if args.native and not args.grid_concurrency_native:
                if args.native_test_manifest:
                    spec=importlib.util.spec_from_file_location('file_job_component',tools/'scripts/rust-epg-parity/component_container.py')
                    component=importlib.util.module_from_spec(spec);spec.loader.exec_module(component)
                    code,provenance=component.execute(compose,env,output,args.native_test_manifest.resolve(),('auto_logos_sql' if args.source_helper=='auto_logos_source.py' else ('index_service_sql' if args.index_service else 'programme_index_sql') if args.source_helper=='programme_index_source.py' else 'file_job_sql'),fixture_path.name)
                    report['native_container']=provenance
                    report['rust_source_sha256']=provenance['source_sha256']
                    report['rust_source_archive_sha256']=provenance['source_archive_sha256']
                    if code!=0:raise RuntimeError('Native file-job component failed')
                else:
                    frozen=output/'frozen-source'
                    for relative,data_bytes in captured.items():
                        target=frozen/relative;target.parent.mkdir(parents=True,exist_ok=True);target.write_bytes(data_bytes)
                    pg=command(compose+['port','baseline','5432']).stdout.strip()
                    redis=command(compose+['port','baseline','6379']).stdout.strip()
                    native_env=dict(env,DATABASE_TEST_URL='postgresql://dispatch:playback-fixture-only@'+pg+'/dispatcharr',REDIS_TEST_URL='redis://:playback-fixture-only@'+redis+'/0',EPG_FILE_JOB_DISPOSABLE_FIXTURE='playback-fixture-only',EPG_FILE_JOB_FIXTURE=str(fixture_path),EPG_LOGO_DISPOSABLE_FIXTURE='playback-fixture-only',EPG_LOGO_FIXTURE=str(fixture_path),EPG_INDEX_DISPOSABLE_FIXTURE='playback-fixture-only',EPG_INDEX_FIXTURE=str(fixture_path),EPG_CURRENT_DISPOSABLE_FIXTURE='playback-fixture-only',EPG_CURRENT_FIXTURE=str(fixture_path),CURRENT_ORDER_DISPOSABLE_FIXTURE='playback-fixture-only',CURRENT_ORDER_FIXTURE=str(fixture_path),CARGO_TARGET_DIR=str(output/'cargo-target'))
                    result=subprocess.run(['cargo','test','--manifest-path',str(frozen/'rust/Cargo.toml'),'-p',('dispatcharr-store' if args.current_order_native else 'dispatcharr-ingest-epg'),'--test',('epg_current_lookup_sql' if args.current_order_native else 'current_bridge_sql' if args.current_bridge else 'current_lookup_sql' if args.current_lookup else 'auto_logos_sql' if args.source_helper=='auto_logos_source.py' else ('index_service_sql' if args.index_service else 'programme_index_sql') if args.source_helper=='programme_index_source.py' else 'file_job_sql'),'--locked','--offline','--','--ignored','--nocapture','--test-threads=1'],env=native_env,text=True,capture_output=True,timeout=600)
                    (output/'native-file-job.log').write_text(result.stdout+'\n'+result.stderr)
                    result.check_returncode()
                report.update(native_verified=True,native_source_cases=len(fixture['cases']),native_physical_scenarios=2 if args.source_helper=='auto_logos_source.py' else 3,source_only_cases=[],application_dispatch_verified=False,child_task_execution_verified=False,websocket_transport_verified=False)
                if args.source_helper=='programme_index_source.py':report.update(native_source_cases=21,native_physical_scenarios=0,native_pure_guards=4,source_only_cases=['missing-file','file-held','defer-exhausted'],index_task_sql_lease_verified=False)
                if args.index_service:report.update(native_source_cases=24,native_physical_scenarios=3,native_pure_guards=0,source_only_cases=[],index_task_sql_lease_verified=True,lookup_verified=False)
                if args.current_lookup:report.update(native_source_cases=17,native_malformed_shape_http500_cases=6,native_unsupported_shape_cases=0,native_physical_scenarios=2,source_only_cases=['missing-epg','no-source'],lookup_sql_component_verified=True,durable_dispatch_verified=False,application_wired=False)
                if args.current_bridge:report.update(native_source_cases=19,native_malformed_shape_http500_cases=6,native_unsupported_shape_cases=0,native_physical_scenarios=4,source_only_cases=[],real_store_lookup_bridge_verified=True,native_standalone_regression_tests=3,one_connection_pool=True)
                if args.current_order_native:report.update(native_source_cases=9,source_observed_mutation_schedules=len(fixture['mutation_schedules']),native_physical_scenarios=4,native_deferred_serialization_cases=2,source_only_cases=[],ordered_store_pipeline_component_verified=True,application_wired=False,durable_dispatch_verified=False)
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
                if args.current_lookup and report['passed']:
                    roots=set()
                    for case in fixture['cases']:
                        for key in ['file_path','extracted_file_path','cache_file_path']:
                            if not case.get(key):continue
                            path=Path(case[key])
                            if not path.is_absolute() or len(path.parts)<3 or path.parts[1]!='tmp' or not path.parts[2].startswith('owned-current-lookup-'):raise ValueError('Native current fixture path outside owned temp root')
                            roots.add(Path(*path.parts[:3]))
                    for root in roots:
                        if root.is_symlink():raise ValueError('Native current fixture root is a symlink')
                        if root.exists():shutil.rmtree(root)
                    report['owned_native_file_roots_removed']=len(roots)
            except (subprocess.SubprocessError,OSError,ValueError):
                report.update(passed=False, cleanup_error=True)
                status = 1
    (output/'result.json').write_text(json.dumps(report, indent=2)+'\n')
    print(json.dumps({'passed':report['passed'],'evidence':str(output), 'native_verified':report['native_verified']}))
    return status


if __name__ == '__main__':
    raise SystemExit(main())
