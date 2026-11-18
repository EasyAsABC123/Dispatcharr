"""Execute one prepared no-network UID resolver control only when Root schedules it.
Original Dispatcharr and Django/DRF authors retain all application/framework credit.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import time
import uuid

H = Path(__file__).resolve().parent
R = H.parents[2]
load = lambda p: json.loads(Path(p).read_bytes())
sha = lambda p: hashlib.sha256(Path(p).read_bytes()).hexdigest()
parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--output', type=Path, required=True)
parser.add_argument('--execute', action='store_true', required=True,
                    help='Root schedules this only after its functional fixture is released')
args = parser.parse_args()
args.output.mkdir(parents=True, exist_ok=False, mode=0o700)
plan = load(H / 'plan.json')
closure = load(H / 'closure.json')
for name, expected in closure['files'].items():
    assert sha(H / name) == expected, 'prepared resolver control input drift'
inputs = load(H / 'input.json')
for reference in [inputs['runtime_discovery'], *inputs['retained_case_authorities']]:
    assert sha(R / reference['path']) == reference['sha256'], 'retained Source/discovery authority drift'
source = Path(plan['Original_source_root'])
expected = load(H / 'source-files.json')
assert {str(p.relative_to(source)): sha(p) for p in source.rglob('*') if p.is_file()} == expected
name = 'dispatcharr-pure-resolver-' + uuid.uuid4().hex[:10]
cidfile = args.output / 'probe.cid'
commands = []
env = dict(os.environ, DOCKER_HOST='unix:///tmp/dispatcharr-docker.sock',
           DOCKER_CONFIG=str(R / 'artifacts/rust/tooling/docker-config'))
docker = str(R / 'artifacts/rust/tooling/docker/docker')


def call(argv, timeout=30, check=True):
    began = time.monotonic()
    completed = subprocess.run([docker, *argv], env=env, capture_output=True,
                               text=True, timeout=timeout)
    commands.append({'argv': [docker, *argv], 'actual_exit': completed.returncode,
                     'stdout': completed.stdout, 'stderr': completed.stderr,
                     'seconds': time.monotonic() - began})
    (args.output / 'commands.json').write_text(json.dumps(commands, indent=2) + '\n')
    if check and completed.returncode:
        raise RuntimeError('OwnedPureResolverCommandFailed')
    return completed


report = {'passed': False, 'state': 'ATTEMPTED_NOT_QUALIFIED',
          'Source_HTTP_business_calls': 0, 'Source_requests_replayed': 0,
          'Native_acceptance': False, 'full_milestone_accepted': False,
          'performance_claims': False, 'image_id': plan['Source_image_id'],
          'prepared_closure_sha256': sha(H / 'closure.json')}
try:
    assert not call(['container', 'ls', '--all', '--filter', 'name=^/' + name + '$', '--format', '{{.ID}}']).stdout.strip()
    argv = ['run', '--pull=never', '--rm', '--network', 'none', '--read-only',
            '--cap-drop', 'ALL', '--security-opt', 'no-new-privileges',
            '--user', '65532:65532', '--pids-limit', '32', '--cpus', '1',
            '--memory', '256m', '--memory-swap', '256m',
            '--tmpfs', '/tmp:rw,nosuid,nodev,size=64m,mode=1777',
            '--name', name, '--cidfile', str(cidfile),
            '--mount', 'type=bind,src=' + str(source) + ',dst=/app,readonly',
            '--mount', 'type=bind,src=' + str(H) + ',dst=/resolver,readonly',
            '--env', 'DISPATCHARR_ENV=aio', '--env', 'PYTHONDONTWRITEBYTECODE=1',
            '--entrypoint', '/dispatcharrpy/bin/python', plan['Source_image_id'],
            '-B', '/resolver/qualify_resolver.py']
    completed = call(argv, timeout=90, check=False)
    # Preserve any startup logger output, but only use the final structured line.
    report['control_actual_exit'] = completed.returncode
    lines = completed.stdout.splitlines()
    if not lines:
        report['control_structured_output_absent'] = True
        report['control_stderr_sha256'] = hashlib.sha256(completed.stderr.encode()).hexdigest()
        raise RuntimeError('PureResolverStructuredOutputAbsentSeeActualStderr')
    observed = json.loads(lines[-1])
    if not observed.get('passed'):
        report['control_failure'] = observed
    assert completed.returncode == 0 and observed['passed'] and observed['actual_UID'] == 65532
    assert observed['all592_resolver_identities_order_actions_exact']
    counts = observed['counts']
    assert all(value == 0 for kind, value in counts.items() if kind != 'network_attempts')
    assert observed['Source_HTTP_business_calls'] == 0
    assert counts['network_attempts'] == observed['qualified_blocked_import_probe_count']
    assert counts['network_attempts'] in [0, 1]
    assert len(observed['denied_operations']) == counts['network_attempts']
    assert observed['network_initialization_or_runtime_parity_claimed'] is False
    report['actual_guard_counts'] = counts
    report['qualified_blocked_import_probe_count'] = observed['qualified_blocked_import_probe_count']
    report['network_scope'] = observed['network_scope']
    assert len(observed['observations']) == 3
    (args.output / 'resolver-observations.json').write_text(json.dumps(observed, indent=2) + '\n')
    report.update(passed=True, state='PURE_RESOLVER_SOURCE12_THREE_PATHS_QUALIFIED_PENDING_ROOT_TERMINAL',
                  actual_exit=completed.returncode, Source_paths=3,
                  actual_source_files=1211, resolver_routes=592,
                  observation_sha256=sha(args.output / 'resolver-observations.json'))
except Exception as error:
    report['error_type'] = type(error).__name__
finally:
    if cidfile.exists():
        cid = cidfile.read_text().strip()
        assert len(cid) == 64 and all(c in '0123456789abcdef' for c in cid)
        present = bool(call(['container', 'ls', '--all', '--no-trunc', '--filter', 'id=' + cid, '--format', '{{.ID}}']).stdout.strip())
        if present:
            call(['container', 'rm', '--force', cid])
        absent = not call(['container', 'ls', '--all', '--no-trunc', '--filter', 'id=' + cid, '--format', '{{.ID}}']).stdout.strip()
        report.update(exact_CID=cid, exact_CID_absent=absent)
        report['passed'] = report['passed'] and absent
    else:
        report.update(passed=False, exact_CID_absent=None)
    (args.output / 'report.json').write_text(json.dumps(report, indent=2) + '\n')
print(json.dumps(report))
raise SystemExit(0 if report['passed'] else 1)
