"""BLOCKED Native-only routed image55 Main worker-header fixture. No application work on import."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import time
import uuid

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
BASE = '1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430'
ARCHIVE = 'f2323ac705be2af5d1c264f26b1d297cb2bede53874e6bceccee4bf6603fa7ff'
SUPPORT = 'f0bfc2f200dcfd1f64ae32a9855ea250731ce67f'
HARD_BLOCKED = True


def save(path, value):
    Path(path).write_text(json.dumps(value, indent=2, allow_nan=False) + '\n')


def validate_binding(repo):
    from candidate_inputs import bound_candidate
    value = json.loads((ROOT / 'candidate-binding.json').read_text())
    for key, digest in [('manifest', 'manifest_sha256'), ('archive', 'archive_sha256')]:
        if hashlib.sha256((repo / value[key]).read_bytes()).hexdigest() != value[digest]:
            raise ValueError('ExactCandidateInput:' + key)
    return bound_candidate(repo / value['manifest'], value['tag'], HERE / 'expected-runtime.json', repo / value['archive'])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--repo', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    # No flag/environment escape, claim, output creation, archive, or Docker before review.
    if HARD_BLOCKED:
        raise RuntimeError('WORKER_HEADER_MAIN55_BLOCKED: Root review, exact activation and fresh once cue required')
    raise RuntimeError('NO_ACTIVATION_PROTOCOL_IMPLEMENTED: candidate binding alone is not permission')


def execute_reviewed_fixture(repo, out):
    """Adapter implementation only; no public entry invokes it in this sealed draft.

    A separately reviewed activation must verify the entire seal, fresh cue, boot,
    nonce and exclusive fsynced claim before this method may be dispatched.
    """
    import bounded_process as bounded
    import fixture_guards as guards
    from identity import kernel
    from origin_coverage import validate as validate_origin
    repo = Path(repo).resolve(); out = Path(out).resolve()
    proof = validate_binding(repo)
    from source_reference import validate_frozen_reference
    source_reference = validate_frozen_reference(ROOT)
    out.mkdir(parents=True, exist_ok=False)
    shutil.copytree(ROOT, out / 'controller', ignore=shutil.ignore_patterns('__pycache__'))
    original = out / 'original-source'; original.mkdir()
    # Explicit immutable revisions and real repository cwd, never nested archive HEAD.
    raw = subprocess.check_output(['git', '-C', str(repo), 'archive', BASE])
    if hashlib.sha256(raw).hexdigest() != ARCHIVE: raise ValueError('OriginalFullArchive')
    subprocess.run(['tar', '-x', '--no-same-owner', '-C', str(original)], input=raw, check=True)
    (original / 'media').mkdir(exist_ok=True)
    support = out / 'support'; support.mkdir()
    raw = subprocess.check_output(['git', '-C', str(repo), 'archive', SUPPORT, 'scripts', 'docker'])
    subprocess.run(['tar', '-x', '--no-same-owner', '-C', str(support)], input=raw, check=True)
    save(out / 'support-binding.json', {'revision': SUPPORT, 'archive_sha256': hashlib.sha256(raw).hexdigest()})
    import importlib.util
    spec = importlib.util.spec_from_file_location('header_ports', support / 'scripts/rust-integration/fixture_ports.py')
    ports = importlib.util.module_from_spec(spec); spec.loader.exec_module(ports)
    nonce = uuid.uuid4().hex; project = 'dispatcharr-worker-header-' + uuid.uuid4().hex[:10]
    database = 'rust_pending_' + uuid.uuid4().hex[:10]
    template, source_db, native_db = (database + suffix for suffix in ('_template', '_source', '_native'))
    overlay = out / 'fixture.json'
    toolroot = out / 'controller'
    save(overlay, {'services': {
        'baseline': {'volumes': [str(toolroot / 'tools') + ':/pending-tools:ro', str(toolroot) + ':/header-root:ro'], 'environment': {'PENDING_MAIN_FIXTURE': 'playback-fixture-only', 'PENDING_MAIN_NONCE': nonce, 'HOSTNAME': 'owned-environment-spoof'}},
        'origin': {'entrypoint': ['/bin/sh', '-c', 'PYTHONPATH=/pending-tools exec /dispatcharrpy/bin/python /pending-tools/launch_origin.py'], 'volumes': [str(toolroot / 'tools') + ':/pending-tools:ro', str(out) + ':/evidence'], 'environment': {'PENDING_MAIN_FIXTURE': 'playback-fixture-only', 'NORMAL_FIXTURE_NONCE': nonce, 'VOD_FIXTURE_BYTES_PER_SECOND': '32768'}},
        'candidate': {'image': proof['image_id'], 'environment': {'DATABASE_URL': 'postgresql://dispatch:playback-fixture-only@baseline:5432/' + native_db, 'REDIS_URL': 'redis://:playback-fixture-only@baseline:6391/0', 'HOSTNAME': 'owned-environment-spoof'}}}})
    portfile = out / 'ports.yml'; ports.write_port_overlay(portfile, {'baseline': [5656], 'origin': [8080], 'candidate': [9192]})
    compose = ['docker', 'compose', '-p', project, '-f', str(support / 'docker/compose.rust-playback.yml'), '-f', str(support / 'scripts/rust-vod-contracts/compose.yml'), '-f', str(support / 'docker/compose.rust-candidate.yml'), '-f', str(overlay), '-f', str(portfile)]
    env = dict(os.environ, BASELINE_SOURCE_DIR=str(original), BASELINE_EVIDENCE_DIR=str(out))
    jobs = {}; command_index = 0
    report = {'passed': False, 'project': project, 'candidate': proof, 'fixture_nonce': nonce, 'Source_business_methods_replaced': False, 'Source_VOD_requests_rerun': False, 'retained_Source6_offline_reference': source_reference, 'full_M4_or_header_parity': False, 'resource_sampler': False}
    default_before = None
    def stop_owned():
        subprocess.run(compose + ['kill'], env=env, stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=15, check=True)
    def command(argv, timeout=180):
        nonlocal command_index
        command_index += 1
        return bounded.Collected(argv, env, 8388608, stop_owned, stderr_path=out / ('command-' + str(command_index) + '.stderr')).wait(timeout)
    def run(*args, timeout=180): return command(compose + list(args), timeout)
    def helper(db, action, port=6379):
        return json.loads(run('exec', '-T', '-e', 'POSTGRES_DB=' + db, '-e', 'REDIS_PORT=' + str(port), 'baseline', '/dispatcharrpy/bin/python', '/pending-tools/header_db.py', action).splitlines()[-1])
    def pg(sql):
        return run('exec', '-T', '-e', 'PGPASSWORD=playback-fixture-only', 'baseline', 'psql', '-h', '127.0.0.1', '-U', 'dispatch', '-d', 'postgres', '-v', 'ON_ERROR_STOP=1', '-At', '-c', sql)
    def ready(url, limit=120):
        end = time.monotonic() + limit
        while time.monotonic() < end:
            try:
                run('exec', '-T', 'baseline', '/dispatcharrpy/bin/python', '-c', 'import urllib.request;assert urllib.request.urlopen(' + repr(url) + ',timeout=2).status==200', timeout=5)
                return
            except bounded.CollectionFailure: time.sleep(.1)
        raise TimeoutError('BoundedOwnedReadiness')
    def child(role, argv, db='dispatcharr', port=6379):
        args = compose + ['exec', '-T', '-e', 'POSTGRES_DB=' + db, '-e', 'REDIS_PORT=' + str(port), 'baseline', '/dispatcharrpy/bin/python', '/pending-tools/owned_process.py', 'start', role] + argv
        jobs[role] = bounded.Collected(args, env, 8388608, stop_owned, stdout_path=out / (role + '.stdout'), stderr_path=out / (role + '.stderr'))
    try:
        run('up', '-d', '--no-build', 'baseline'); ready('http://127.0.0.1:5656/api/core/version/')
        baseline_cid=run('ps','-q','baseline').decode().strip()
        baseline_inspect=json.loads(command(['docker','inspect',baseline_cid]))[0]
        if baseline_inspect['Config']['Env'].count('HOSTNAME=owned-environment-spoof') != 1: raise ValueError('SourceEnvironmentSpoofConfigured')
        report['baseline_HOSTNAME_canary_configured']=True
        run('exec', '-T', 'baseline', '/dispatcharrpy/bin/python', '/verification/supervisor.py', 'control', 'stop')
        run('up', '-d', '--no-build', 'origin'); ready('http://origin:8080/health')
        report['origin_preflight'] = json.loads(run('exec', '-T', 'baseline', '/dispatcharrpy/bin/python', '/pending-tools/origin_guard.py').splitlines()[-1])
        if report['origin_preflight']['passed'] is not True: raise ValueError('ActualOriginPreflight')
        default_before = helper('dispatcharr', 'default-audit'); report['default_before'] = default_before
        pg('CREATE DATABASE ' + template); helper(template, 'migrate'); helper(template, 'seed')
        before = helper(template, 'fingerprint'); report['template_fingerprint'] = before
        for db in (source_db, native_db): pg('CREATE DATABASE ' + db + ' TEMPLATE ' + template)
        report['clone_binding'] = guards.clones(before, helper(source_db, 'fingerprint'), helper(native_db, 'fingerprint'), [template, source_db, native_db])
        for role, port in [('native-redis', 6391)]:
            child(role, ['redis-server', '--bind', '0.0.0.0', '--port', str(port), '--requirepass', 'playback-fixture-only', '--save', '', '--appendonly', 'no', '--dir', '/tmp'])
            end = time.monotonic() + 10
            while time.monotonic() < end:
                try:
                    if not run('exec', '-T', 'baseline', 'redis-cli', '-p', str(port), '-a', 'playback-fixture-only', 'PING').strip().endswith(b'PONG'): raise ValueError('RedisPING')
                    break
                except bounded.CollectionFailure: time.sleep(.1)
            else: raise TimeoutError('OwnedRedisReady')
        for mode, db, port, base in [('native', native_db, 6391, 'http://candidate:9192')]:
            if mode == 'native':
                run('up', '-d', '--no-build', 'candidate'); cid = run('ps', '-q', 'candidate').decode().strip()
                inspection = json.loads(command(['docker', 'inspect', cid]))[0]
                if inspection['Image'] != proof['image_id'] or inspection['Config']['Env'].count('HOSTNAME=owned-environment-spoof') != 1: raise ValueError('ActualMainImageAndCanary')
                chunks = [run('exec', '-T', 'candidate', 'cat', path) for path in ['/proc/sys/kernel/hostname', '/proc/1/stat', '/proc/1/status', '/proc/1/cgroup']]
                witness = kernel(*chunks, 1, native=True)
            else:
                ready(base + '/api/core/version/')
                witness = json.loads(run('exec', '-T', 'baseline', '/dispatcharrpy/bin/python', '/pending-tools/kernel_witness.py').splitlines()[-1])
            ready(base + '/api/core/version/')
            save(out / ('kernel-' + mode + '.json'), witness)
            actual_before = helper(db, 'fingerprint', port)
            if actual_before['dataset_sha256'] != before['dataset_sha256']: raise ValueError('IndependentUnmutatedCloneBeforePhase')
            run('exec', '-T', '-e', 'POSTGRES_DB=' + db, '-e', 'REDIS_PORT=' + str(port), '-e', 'HEADER_MODE=' + mode, '-e', 'HEADER_BASE=' + base, 'baseline', '/dispatcharrpy/bin/python', '/pending-tools/phase.py', timeout=100)
            report[mode] = json.loads((out / ('worker-header-' + mode + '-private.json')).read_text())
            if report[mode]['qualification']['worker_header_branch_scope_qualified'] is not True: raise ValueError('ActualFiniteHeaderBranches')
        report['passed'] = True
    except Exception as error:
        report['error'] = type(error).__name__ + ':' + str(error)[:512]
    finally:
        def cleanup(action, label):
            try: return action()
            except Exception as error:
                report['passed'] = False; report.setdefault('cleanup_errors', []).append(label + ':' + type(error).__name__)
        cleanup(lambda: run('stop', '--timeout', '15', 'candidate', timeout=30), 'CandidateStop')
        for role, job in jobs.items():
            cleanup(lambda role=role: run('exec', '-T', 'baseline', '/dispatcharrpy/bin/python', '/pending-tools/owned_process.py', 'stop', role), 'OwnedPIDStop:' + role)
            def settle(job=job):
                try: job.wait(15)
                except bounded.CollectionFailure:
                    if job.invalid is not None or job.process.returncode not in (0, 143, -15) or job.process.poll() is None or any(t.is_alive() for t in job.threads): raise
            cleanup(settle, 'OwnedExecJoin:' + role)
        if default_before is not None:
            after = cleanup(lambda: helper('dispatcharr', 'default-audit'), 'DefaultAudit')
            report['default_after'] = after; report['default_named_tables_sequences_unchanged'] = after == default_before
            if after != default_before: report['passed'] = False
        cleanup(lambda: run('stop', '--timeout', '15', 'origin', timeout=30), 'OriginStop')
        def origin_terminal():
            terminal = json.loads((out / 'header-origin-terminal.json').read_text())
            if terminal.get('server_thread_settled') is not True: raise ValueError('OwnedOriginThreadJoin')
            report['origin_terminal'] = terminal; report['origin_terminal_validation'] = validate_origin(terminal, nonce, terminal=True)
        cleanup(origin_terminal, 'OriginPhysicalTerminal')
        cleanup(lambda: run('down', '--volumes', '--remove-orphans', '--timeout', '10', timeout=120), 'ComposeDown')
        report['remaining'] = {}
        for kind, args in {'containers': ['ps', '-aq'], 'networks': ['network', 'ls', '-q'], 'volumes': ['volume', 'ls', '-q']}.items():
            result = cleanup(lambda args=args: command(['docker'] + args + ['--filter', 'label=com.docker.compose.project=' + project]), 'IndependentInventory:' + kind)
            report['remaining'][kind] = result.decode().strip() if result is not None else 'UNVERIFIED'
        if any(report['remaining'].values()): report['passed'] = False
        save(out / 'summary-private.json', report)
    return 0 if report['passed'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
