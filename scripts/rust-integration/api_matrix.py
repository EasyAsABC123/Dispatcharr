"""Read-only React API resource characterization inside an owned Compose fixture."""
import hashlib
import importlib.util
import json
import math
import os
import platform
from pathlib import Path
import time
import urllib.error
import urllib.request

PATHS = ['/api/channels/channels/?page=1&page_size=200',
         '/api/core/settings/', '/api/core/useragents/']


def load(path, name):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def run(*, compose, command, endpoint, token, output, tools, manifest,
        repetitions=3, warmup=15, duration=60):
    if (repetitions not in [1, 2, 3] or not 0 <= warmup <= 60
            or not 10 <= duration <= 300 or not all(math.isfinite(v) for v in [warmup, duration])):
        raise ValueError('Invalid API matrix limits')
    if not manifest or not manifest.get('passed'):
        raise ValueError('API matrix requires an immutable successful candidate build manifest')
    api = load(tools/'scripts/rust-integration/api_load.py', 'captured_api_load')
    perf = load(tools/'scripts/rust-perf/perf.py', 'captured_resource_observer')
    output = Path(output)
    output.mkdir(exist_ok=False)
    environment = {'host': platform.node(), 'system': platform.platform(),
                   'logical_cpus': os.cpu_count(),
                   'docker': command(['docker', 'version', '--format', '{{.Server.Version}}']).strip()}
    workload = {'scope': 'read-only-React-API-subset', 'paths': PATHS, 'users': 10,
                'period': .25, 'warmup': warmup, 'duration': duration,
                'timeout': 3, 'sample_interval': 1}
    report = {'scope': workload['scope'], 'passed': False, 'workload': workload,
              'measurement_quality': ('repeated-isolated-characterization'
                  if repetitions == 3 and warmup >= 15 and duration >= 60 else 'load-smoke'),
              'application_performance_verified': False, 'milestone_complete': False,
              'trials': [], 'comparisons': [], 'source_runtime': 'unchanged pinned original uWSGI',
              'candidate_image': manifest['image_id'],
              'notes': ['Read-only channel/settings/user-agent endpoints used by unchanged React initialization.',
                        'Synthetic paced10-user repeated initialization reads, not measured browser behavior.',
                        'Whole measured service group includes backend, PostgreSQL, Redis and idle origin.',
                        'Both backends restart web and clear the owned isolated Redis before every trial.',
                        'JSON values must match the original response on every measured request.',
                        'Tokens and response bodies are not written to performance reports.']}
    expected = None
    try:
        for trial in range(1, repetitions+1):
            pair = {}
            for backend in (['original', 'native'] if trial % 2 else ['native', 'original']):
                reset = {'candidate_stop': command(compose+['stop', '--timeout', '15', 'candidate'])}
                reset['source_web_stop'] = json.loads(command(compose+['exec', '-T', 'baseline',
                    '/dispatcharrpy/bin/python', '/verification/supervisor.py', 'control', 'stop']))
                reset['redis_clear'] = command(compose+['exec', '-T', 'baseline', 'redis-cli', 'FLUSHDB']).strip()
                if int(command(compose+['exec', '-T', 'baseline', 'redis-cli', 'dbsize'])) != 0:
                    raise RuntimeError('Owned Redis reset was incomplete')
                if backend == 'original':
                    reset['source_web_start'] = json.loads(command(compose+['exec', '-T', 'baseline',
                        '/dispatcharrpy/bin/python', '/verification/supervisor.py', 'control', 'start']))
                    base = endpoint('baseline', 5656)
                    services = ['baseline', 'origin']
                    revision = '1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430'
                else:
                    command(compose+['up', '-d', '--no-deps', '--no-build', '--force-recreate', 'candidate'])
                    base = endpoint('candidate', 9192)
                    services = ['baseline', 'origin', 'candidate']
                    container = command(compose+['ps', '-q', 'candidate']).strip()
                    inspection = json.loads(command(['docker', 'inspect', container]))[0]
                    revision = inspection['Image']
                    if (revision != manifest['image_id'] or inspection['Config']['Labels'].get(
                            'org.opencontainers.image.source-tree-sha256') != manifest['source_tree_sha256']):
                        raise RuntimeError('Candidate image/source identity changed')
                deadline = time.monotonic()+60
                while True:
                    try:
                        if backend == 'native':
                            with urllib.request.urlopen(base+'/health/ready', timeout=3) as response:
                                if json.load(response).get('status') != 'ready':
                                    raise RuntimeError('Candidate not ready')
                        else:
                            api.request(base, PATHS[0], token, 3)
                        break
                    except (OSError, ValueError, RuntimeError):
                        if time.monotonic() >= deadline:
                            raise TimeoutError('API restart readiness exceeded60seconds')
                        time.sleep(.25)
                if expected is None:
                    if backend != 'original':
                        raise RuntimeError('Original reference must precede the first native measurement')
                    expected = {path: api.request(base, path, token, 3)[0] for path in PATHS}
                    report['original_json_sha256'] = {path: hashlib.sha256(json.dumps(value,
                        sort_keys=True, separators=(',', ':'), allow_nan=False).encode()).hexdigest()
                        for path, value in expected.items()}
                ids = []
                for service in services:
                    found = command(compose+['ps', '--status', 'running', '-q', service]).splitlines()
                    if not found:
                        raise RuntimeError('Measured service is not running: '+service)
                    ids.extend(found)
                parameters = dict(users=workload['users'], period=workload['period'], timeout=workload['timeout'])
                if warmup:
                    warmed = api.phase(base, PATHS, token, expected, duration=warmup, **parameters)
                    if warmed['summary']['errors'] or warmed['summary']['users_without_success']:
                        raise RuntimeError('API warmup had semantic/delivery errors')
                measurement = api.phase(base, PATHS, token, expected, duration=duration,
                    sample=lambda: perf.resources(ids), sample_interval=1, **parameters)
                data = {'workload': workload, 'environment': environment, 'revision': revision,
                        'backend': backend, 'trial': trial, 'reset': reset,
                        'services': services, 'measurement': measurement}
                filename = f'{backend}-trial-{trial}.json'
                (output/filename).write_text(json.dumps(data, indent=2)+'\n')
                pair[backend] = data
                report['trials'].append({'backend': backend, 'trial': trial, 'file': filename,
                    'sha256': hashlib.sha256((output/filename).read_bytes()).hexdigest()})
                print(f'API {backend} trial{trial}: '+json.dumps(measurement['summary']), flush=True)
            result = api.compare(pair['original'], pair['native'])
            report['comparisons'].append({'trial': trial, **result})
        report['passed'] = all(result['passed'] for result in report['comparisons'])
    finally:
        (output/'matrix.json').write_text(json.dumps(report, indent=2)+'\n')
    return report
