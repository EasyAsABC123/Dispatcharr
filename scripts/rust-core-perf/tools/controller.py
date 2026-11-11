#!/usr/bin/env python3
"""Private M1 active-core functional controller. No resource sampling action yet.
Original Dispatcharr authors retain full application/algorithm credit/licenses.
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
import traceback
import importlib.util

HERE = Path(__file__).resolve().parent
_spec=importlib.util.spec_from_file_location('dispatcharr_m1_core_private_loader',HERE/'m1_imports.py')
LOCAL=importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(LOCAL)
ROOT = Path.cwd().resolve()
SUPPORT = HERE.parent / 'support'
# Verify the complete bundled closure before importing executable Python.
_manifest=json.loads((HERE.parent/'support-manifest.json').read_text())
_actual={str(p.relative_to(SUPPORT)):hashlib.sha256(p.read_bytes()).hexdigest() for p in SUPPORT.rglob('*') if p.is_file() and '__pycache__' not in p.parts}
if _actual != _manifest: raise ValueError('Bundled support closure mismatch')
sys.path.insert(0, str(SUPPORT / 'scripts/rust-integration'))
from run import BASELINE, fixture_source
from fixture_ports import write_port_overlay
CoreRuntime=LOCAL.load('runtime').CoreRuntime
validate_plan=LOCAL.load('policy').validate_plan
validate_work=LOCAL.load('policy').validate_work


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', required=True, type=Path)
    parser.add_argument('--binary', required=True, type=Path)
    parser.add_argument('--action', choices=['functional', 'experiment'], default='functional')
    parser.add_argument('--source-quiet', type=Path)
    parser.add_argument('--measurement-quiet', type=Path)
    parser.add_argument('--functional-proof', type=Path)
    args = parser.parse_args()
    out = args.output.resolve()
    out.mkdir(parents=True, exist_ok=True)
    if any(out.iterdir()):
        parser.error('New empty output required')
    plan = json.loads((HERE.parent / 'protocol.json').read_text())
    validate_plan(plan)
    tools = out / 'core-tools'
    shutil.copytree(HERE, tools, ignore=shutil.ignore_patterns('__pycache__'))
    plan_path = out / 'protocol.json'
    plan_path.write_text(json.dumps(plan, indent=2) + '\n')
    binary = out / 'native-core-peer'
    shutil.copy2(args.binary.resolve(), binary)
    binary.chmod(0o755)
    if args.action == 'experiment':
        if args.source_quiet is None or args.measurement_quiet is None or args.functional_proof is None:
            parser.error('Source/measurement quiet cue paths and exact prior functional proof required')
        proof = json.loads(args.functional_proof.read_text())
        if (proof.get('passed') is not True or proof.get('cleanup') != 'project removed'
                or proof.get('default_database_unchanged') is not True
                or proof.get('resource_samples_collected') is not False
                or proof.get('native_binary_sha256') != sha(binary)
                or proof.get('held_controls', {}).get('passed') is not True
                or {(v.get('backend'),v.get('viewers')) for v in proof.get('phases', [])} != {(b,v)for b in ['source','native']for v in [1,10,100]}):
            raise ValueError('Exact release ELF functional proof required before qualification')
        authorize=LOCAL.load('metrics').authorize
        authorize(args.source_quiet, 'm1-core-source-qualification', plan, sha(binary))
    nonce = uuid.uuid4().hex[:10]
    database = 'rust_probe_backup_jobs_' + nonce
    project = 'dispatcharr-m1-core-functional-' + nonce
    report = {'passed': False, 'scope': 'Separate active core container/common stopped-web PGRedis infrastructure; release native ELF and actual Source buffer functional correctness only, no resource samples or qualification/measurement',
              'project': project, 'copied_database': database, 'performance_verified': False,
              'resource_samples_collected': False, 'native_binary_sha256': sha(binary),
              'protocol_sha256': sha(plan_path), 'tools_sha256': {str(p.relative_to(tools)): sha(p) for p in tools.rglob('*') if p.is_file()},
              'phases': []}
    critical = ['docker/compose.rust-playback.yml',
                *['scripts/rust-playback-baseline/'+name for name in ['start.sh','supervisor.py','uwsgi.ini','seed.py','origin.py']],
                *['scripts/rust-integration/'+name for name in ['run.py','backup_fixture.py','fixture_ports.py']],
                *['scripts/rust-guide-perf/'+name for name in ['observer.py','cgroup.py','docker_stats.py']],
                'scripts/rust-perf/perf.py']
    report['support_sha256']={name:sha(SUPPORT/name)for name in critical}
    if args.action == 'experiment':
        report['functional_proof_sha256']=sha(args.functional_proof)
    with fixture_source(report, False, out) as temp:
        source = Path(temp) / 'source'
        source.mkdir()
        archive = subprocess.check_output(['git', 'archive', BASELINE], cwd=ROOT)
        subprocess.run(['tar', '-x', '--same-permissions', '--no-same-owner', '-C', str(source)], input=archive, check=True)
        source_map={str(p.relative_to(source)):sha(p) for p in source.rglob('*') if p.is_file()}
        expected_source=json.loads((HERE.parent/'original-source-map.json').read_text())
        if source_map != expected_source: raise ValueError('Pinned Original full source mismatch')
        report['original_full_source_map_sha256']=hashlib.sha256(json.dumps(source_map,sort_keys=True,separators=(',',':')).encode()).hexdigest()
        report['complete_support_map_sha256']=hashlib.sha256(json.dumps(_manifest,sort_keys=True,separators=(',',':')).encode()).hexdigest()
        (source / 'media').mkdir(exist_ok=True)
        report['original_archive_sha256'] = hashlib.sha256(archive).hexdigest()
        assert report['original_archive_sha256'] == 'f2323ac705be2af5d1c264f26b1d297cb2bede53874e6bceccee4bf6603fa7ff'
        env = dict(os.environ, BASELINE_SOURCE_DIR=str(source), BASELINE_EVIDENCE_DIR=str(out), DISPATCHARR_ENV='aio')
        ports = out / 'fixture-ports.yml'
        report['ports'] = write_port_overlay(ports, {'baseline': [5656], 'origin': [8080]})
        overlay = out / 'fixture.yml'
        overlay.write_text(json.dumps({'services': {'baseline': {'volumes': [str(SUPPORT / 'scripts/rust-integration') + ':/backup-tools:ro'], 'cpus': 2.0, 'mem_limit': '512m'}}, 'networks': {'default': {'internal': True}}}, indent=2) + '\n')
        compose = ['docker', 'compose', '-p', project, '-f', str(SUPPORT / 'docker/compose.rust-playback.yml'), '-f', str(overlay), '-f', str(ports)]

        def command(args, body=None, timeout=180):
            return subprocess.check_output(compose + args, env=env, input=body, text=True, stderr=subprocess.PIPE, timeout=timeout)

        def manifest():
            raw = command(['exec', '-T', '-e', 'POSTGRES_DB=dispatcharr', '-e', 'BACKUP_DISPOSABLE_FIXTURE=playback-fixture-only', 'baseline', '/dispatcharrpy/bin/python', '/backup-tools/backup_fixture.py'], json.dumps({'action': 'baseline_manifest'}))
            return json.loads(raw.splitlines()[-1])

        before = None
        runtime = None
        try:
            command(['up', '-d', 'baseline'])
            deadline = time.monotonic() + 180
            while True:
                try:
                    command(['exec', '-T', 'baseline', '/dispatcharrpy/bin/python', '-c', "import urllib.request; assert urllib.request.urlopen('http://127.0.0.1:5656/api/core/version/',timeout=2).status==200"], timeout=5)
                    break
                except subprocess.SubprocessError:
                    if time.monotonic() > deadline:
                        raise TimeoutError('Original infrastructure readiness')
                    time.sleep(.2)
            before = manifest()
            if args.action == 'experiment':
                lifecycle=LOCAL.load('metrics').lifecycle
                ident = command(['ps', '-q', 'baseline']).strip()
                lifecycle({'infrastructure': {'id': ident, 'limits': {'nano_cpus': 2_000_000_000, 'memory_bytes': 512*1024*1024}}}, SUPPORT, out/'infrastructure-setup-lifecycle.json')
            command(['exec', '-T', '-e', 'PGPASSWORD=playback-fixture-only', 'baseline', 'psql', '-h', '127.0.0.1', '-U', 'dispatch', '-d', 'postgres', '-v', 'ON_ERROR_STOP=1', '-c', 'CREATE DATABASE ' + database])
            command(['exec', '-T', '-e', 'POSTGRES_DB=' + database, '-e', 'BACKUP_DISPOSABLE_FIXTURE=playback-fixture-only', 'baseline', '/dispatcharrpy/bin/python', '/backup-tools/backup_fixture.py'], json.dumps({'action': 'seed'}))
            runtime = CoreRuntime(compose, env, out, database, source, tools, plan)
            report['web_stop'] = runtime.stop_web_and_origin()
            ldd = command(['exec', '-T', 'baseline', 'ldd', '/evidence/native-core-peer'])
            (out / 'native-loader.log').write_text(ldd)
            if 'not found' in ldd:
                raise ValueError('Native release ELF loader closure incomplete')
            functional = dict(plan, warmup_seconds=1, measure_seconds=10)
            if args.action == 'functional':
                held = runtime.start('native', 1, 250, functional, 'held-controls', held=True)
                held_result = runtime.finish(held)
                if held_result.get('passed') is not True or not all(v is True for v in held_result.values()):
                    raise ValueError('Held-frame/final-owner controls failed')
                report['held_controls'] = held_result
                for viewers in plan['viewers']:
                    for backend in ['source', 'native']:
                        state = runtime.start(backend, viewers, 250, functional, backend + '-' + str(viewers))
                        value = runtime.finish(state)
                        work = validate_work(value, backend, viewers, 250, 1, 10, require_history=(backend == 'native'))
                        report['phases'].append({'backend': backend, 'viewers': viewers, 'work': work,
                                                 'selected': state['selected'], 'output': Path(state['output']).name})
                        (out / 'progress.json').write_text(json.dumps(report['phases'], indent=2) + '\n')
            else:
                experiment=LOCAL.load('orchestrator').experiment
                report['scope'] = 'Whole active core plus common stopped-web PGRedis, exact Source-only common pacing and separately cued paired matrix; no HTTP/application acceptance'
                report['resource_sampling_attempted'] = True
                report['experiment'] = experiment(runtime, plan, SUPPORT, sha(binary), args.source_quiet,
                    args.measurement_quiet, {'original_archive_sha256': report['original_archive_sha256'],
                        'protocol_sha256': report['protocol_sha256'], 'native_binary_sha256': report['native_binary_sha256'],
                        'tools_sha256': report['tools_sha256'], 'support_sha256': report['support_sha256'],
                        'functional_proof_sha256': report['functional_proof_sha256']})
                report['performance_verified'] = report['experiment']['performance_verified']
                report['resource_samples_collected'] = True
                lifecycle({'infrastructure': runtime.inspected('baseline')}, SUPPORT, out/'infrastructure-completed-lifecycle.json')
            report['default_database_unchanged'] = manifest() == before
            if not report['default_database_unchanged']:
                raise ValueError('Default fixture database changed')
            report['passed'] = True
        except (Exception, KeyboardInterrupt) as error:
            report['error_type'] = type(error).__name__
            report['manual_stop'] = isinstance(error, KeyboardInterrupt)
            (out/'controller-failure.log').write_text(traceback.format_exc()[-65536:])
            if isinstance(error, subprocess.CalledProcessError):
                (out / 'failure-private.log').write_text(str(error.stderr or '')[:65536])
        finally:
            if runtime is not None:
                runtime.release_all()
            if args.action == 'experiment':
                try:
                    lifecycle=LOCAL.load('metrics').lifecycle
                    selected = {}
                    # Include exited containers too: State.OOMKilled must not
                    # disappear merely because exec/cgroup reads are unavailable.
                    for label, service in [('infrastructure', 'baseline'), ('core', 'core')]:
                        lookup = compose + (['-f', str(out/'core-runtime.yml')] if (out/'core-runtime.yml').exists() else [])
                        ident = subprocess.check_output(lookup+['ps','-a','-q',service],env=env,text=True,stderr=subprocess.PIPE,timeout=5).strip()
                        if ident:
                            selected[label]={'id':ident,'limits':{'nano_cpus':2_000_000_000,'memory_bytes':512*1024*1024}}
                    if selected:
                        lifecycle(selected, SUPPORT, out/'final-lifecycle.json')
                except (Exception, KeyboardInterrupt) as error:
                    report.update(passed=False, lifecycle_audit_failed=type(error).__name__)
            if not report['passed']:
                try:
                    core = subprocess.check_output(compose + ['-f', str(out / 'core-runtime.yml'), 'ps', '-q', 'core'], env=env, text=True, stderr=subprocess.PIPE, timeout=5).strip()
                    if core:
                        logs = subprocess.check_output(['docker', 'logs', '--tail', '200', core], env=env, stderr=subprocess.STDOUT, timeout=10)
                        (out / 'active-core-failure.log').write_bytes(logs[-4*1024*1024:])
                except subprocess.SubprocessError:
                    pass
            if before is not None:
                try:
                    report['default_database_unchanged'] = manifest() == before
                    if not report['default_database_unchanged']:
                        report['passed'] = False
                except subprocess.SubprocessError:
                    report.update(passed=False, database_audit_failed=True)
            try:
                # Include runtime overlay so its sole core service is removed.
                cleanup = compose + ['-f', str(out / 'core-runtime.yml')] if (out / 'core-runtime.yml').exists() else compose
                subprocess.check_output(cleanup + ['down', '--volumes', '--remove-orphans'], env=env, stderr=subprocess.PIPE, timeout=45)
                report['cleanup'] = 'project removed'
            except subprocess.SubprocessError:
                report.update(passed=False, cleanup_failed=True)
    sampling_status=LOCAL.load('reporting').sampling_status
    report.update(sampling_status(out))
    if not report['passed']:
        report['performance_verified'] = False
    (out / 'functional.json').write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps({'passed': report['passed'], 'output': str(out)}))
    return 0 if report['passed'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
