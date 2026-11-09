#!/usr/bin/env python3
"""Isolated, immutable M4 playback experiment. Default prepares/probes only.
All original Dispatcharr application authors and contributors retain credit.
"""
import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import shutil
import signal
import statistics
import subprocess
import sys
import tempfile
import time
import urllib.request
import uuid

from plan import PLAN, authorize_quiet, digest, final_acceptance, freeze, validate_fixture_tokens, validate_limits, validate_plan
from workload import compare_pair, run_continuous, validate_phase
from observer import Observer
ROOT = Path(__file__).resolve().parents[2]
BASELINE = '1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430'


def module(path, name):
    spec = importlib.util.spec_from_file_location(name, path)
    value = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(value)
    return value


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--candidate-build-manifest', type=Path, required=True)
    parser.add_argument('--measure', action='store_true')
    parser.add_argument('--load-smoke', action='store_true', help='Short sustained delivery/WS correctness only; no resource sampling')
    parser.add_argument('--quiet-authorization', type=Path)
    args = parser.parse_args()
    if args.measure and not args.quiet_authorization:
        parser.error('--measure requires explicit Root coordinated --quiet-authorization')
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    manifest_bytes = args.candidate_build_manifest.read_bytes()
    manifest = json.loads(manifest_bytes)
    if manifest.get('passed') is not True or not manifest.get('source_sha256'):
        raise ValueError('Passing immutable build manifest with source map required')
    frozen = freeze(output / 'plan.json')
    plan = validate_plan(frozen)
    if args.measure:
        authorization = authorize_quiet(args.quiet_authorization, frozen['plan_sha256'], manifest['image_id'])
    else:
        authorization = None
    (output / 'candidate-build-manifest.json').write_bytes(manifest_bytes)
    # Copy every executed helper before imports, mounts and setup.
    sys.path.insert(0, str(ROOT / 'scripts/rust-vod-contracts'))
    from vod_tool_snapshot import snapshot_sources
    tools = output / 'tool-source'
    hashes = snapshot_sources(ROOT, tools)
    # Keep new benchmark tools and observer/resource sampler in the same snapshot.
    for directory in ('scripts/rust-m4-perf', 'scripts/rust-perf'):
        for path in sorted((ROOT / directory).rglob('*')):
            if not path.is_file() or '__pycache__' in path.parts or path.suffix == '.pyc':
                continue
            relative = path.relative_to(ROOT)
            target = tools / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(path.read_bytes())
            hashes[str(relative)] = hashlib.sha256(path.read_bytes()).hexdigest()
    (output / 'tool-source-sha256.json').write_text(json.dumps(hashes, indent=2, sort_keys=True) + '\n')
    global Observer, run_continuous, validate_phase, compare_pair
    load = module(tools / 'scripts/rust-m4-perf/workload.py', 'frozen_m4_workload')
    observer_module = module(tools / 'scripts/rust-m4-perf/observer.py', 'frozen_m4_observer')
    Observer = observer_module.Observer
    run_continuous, validate_phase, compare_pair = load.run_continuous, load.validate_phase, load.compare_pair
    sys.path.insert(0, str(tools / 'scripts/rust-integration'))
    integration = module(tools / 'scripts/rust-integration/run.py', 'm4_integration')
    ports = module(tools / 'scripts/rust-integration/fixture_ports.py', 'm4_ports')
    resources = module(tools / 'scripts/rust-perf/perf.py', 'm4_resources')
    client = module(tools / 'scripts/rust-realtime-contracts/ws_client.py', 'm4_ws').Client
    temporary = Path(tempfile.mkdtemp(prefix='dispatcharr-m4-resource-'))
    source = temporary / 'source'
    source.mkdir()
    project = 'dispatcharr-m4-perf-' + uuid.uuid4().hex[:10]
    environment = dict(os.environ, BASELINE_SOURCE_DIR=str(source), BASELINE_EVIDENCE_DIR=str(output))
    candidate_overlay = integration.candidate_overlay(tools / 'docker/compose.rust-candidate.yml',
        output / 'candidate-overlay.yml', manifest.get('tag') or manifest['image_id'])
    port_overlay = output / 'fixture-ports.yml'
    port_metadata = ports.write_port_overlay(port_overlay,
        {'baseline': [5656, 5432, 6379], 'origin': [8080], 'source': [5656, 9191], 'candidate': [9192]})
    compose = ['docker', 'compose', '-p', project,
               '-f', str(tools / 'docker/compose.rust-playback.yml'),
               '-f', str(tools / 'scripts/rust-vod-contracts/compose.yml'),
               '-f', str(candidate_overlay),
               '-f', str(tools / 'scripts/rust-m4-perf/compose.yml'), '-f', str(port_overlay)]
    report = {'format_version': 1, 'scope': plan['scope'], 'project': project,
              'plan_sha256': frozen['plan_sha256'], 'candidate_image_id': manifest['image_id'],
              'source_commit': BASELINE, 'build_manifest_sha256': hashlib.sha256(manifest_bytes).hexdigest(),
              'tool_manifest_sha256': digest(hashes), 'quiet_authorization_sha256': authorization,
              'measurements_started': False, 'pairs': [], 'preflight': [], 'passed': False,
              'performance_verified': False, 'application_performance_verified': False,
              'full_m4_parity_verified': False, 'fixture_ports': port_metadata}
    import platform
    report['host'] = {'platform': platform.platform(), 'python': platform.python_version(),
                      'cpu_count': os.cpu_count(), 'load_driver_included_in_container_resources': False}
    def command(arguments, timeout=180, private=False):
        completed = subprocess.run(arguments, env=environment, capture_output=True, timeout=timeout)
        # Token replies and credential-bearing requests never enter evidence logs.
        if not private:
            with (output / 'commands.log').open('ab') as log:
                log.write(completed.stdout + completed.stderr)
        completed.check_returncode()
        return completed.stdout.decode()
    def endpoint(service, port):
        return 'http://' + command(compose + ['port', service, str(port)]).strip()
    def http(url, body=None, headers=None, limit=65536):
        request = urllib.request.Request(url, data=json.dumps(body).encode() if body is not None else None,
                                         headers=headers or {})
        with urllib.request.urlopen(request, timeout=15) as response:
            data = response.read(limit + 1)
            if len(data) > limit:
                raise ValueError('Bounded JSON fixture response exceeded')
            return json.loads(data)
    def ready(url, seconds=120):
        deadline = time.monotonic() + seconds
        while time.monotonic() < deadline:
            try:
                http(url)
                return
            except Exception:
                time.sleep(.25)
        raise TimeoutError('Actual HTTP readiness deadline')
    def stop_backends():
        command(compose + ['stop', '-t', '15', 'source', 'candidate'], timeout=60)
    def reset(backend):
        stop_backends()
        status = json.loads(command(compose + ['exec', '-T', 'baseline', '/dispatcharrpy/bin/python',
                            '/verification/supervisor.py', 'control', 'status']))
        if status['running']:
            raise RuntimeError('Infrastructure uWSGI must remain stopped during every measured phase')
        command(compose + ['exec', '-T', 'baseline', 'redis-cli', 'FLUSHDB'])
        command(compose + ['up', '-d', '--no-deps', '--force-recreate', backend])
        base = endpoint(backend, 5656 if backend == 'source' else 9192)
        ready(base + '/api/core/version/')
        if backend == 'source':
            ready(endpoint('source', 9191) + '/api/core/version/')
        return base
    def cleanup_state():
        raw = command(compose + ['exec', '-T', 'baseline', '/dispatcharrpy/bin/python', '-c',
            (tools / 'scripts/rust-m4-perf/cleanup_inspect.py').read_text(), str(seed['provider_profile_id'])])
        return json.loads(raw)
    def endpoints(base, protocol, count, expected):
        rows = []
        for index in range(count):
            if protocol == 'vod':
                path = '/proxy/vod/movie/' + seed['movie_uuid'] + '/fixture-m4-vod-' + str(index)
            else:
                value = http(base + '/api/catchup/sessions/',
                    {'channel_uuid': seed['channel_uuid'], 'start': '2026-11-09T11:00:00Z', 'duration': 30},
                    {'Content-Type': 'application/json', 'Authorization': 'Bearer ' + seed['users'][index]['token']})
                path = value['playback_url']
                if not path.startswith('/') or path.startswith('//'):
                    raise ValueError('Fixture session returned nonlocal playback capability')
            rows.append((base + path, {'User-Agent': 'dispatcharr-m4-fixed-load'}, expected))
        return rows
    def observer_for(backend, base):
        return Observer(endpoint('source', 9191) if backend == 'source' else base, seed['observer_token'], client)
    def wait_cleanup():
        deadline = time.monotonic() + 15
        while any(cleanup_state().values()):
            if time.monotonic() >= deadline:
                raise RuntimeError('Provider counter or media metadata did not clean up after viewer disconnect')
            time.sleep(.25)
        time.sleep(1.2)  # Allow final publication and fenced delayed cleanup, outside measurement.
    def selection(backend):
        ids = [command(compose + ['ps', '-q', name]).strip() for name in ('baseline', 'origin', backend)]
        if any(not identifier for identifier in ids):
            raise RuntimeError('Selected service missing')
        return [identifier[:12] for identifier in ids]
    def terminate(*_):
        raise KeyboardInterrupt
    signal.signal(signal.SIGTERM, terminate)
    try:
        archive = subprocess.run(['git', 'archive', BASELINE], cwd=ROOT, check=True, capture_output=True).stdout
        subprocess.run(['tar', '-x', '--same-permissions', '-C', str(source)], input=archive, check=True)
        (source / 'media').mkdir(exist_ok=True)
        command(compose + ['up', '-d', 'baseline'], timeout=180)
        # Long deterministic fixtures may take several minutes. Generation ends before probes/measurements.
        ready(endpoint('origin', 8080) + '/stats', seconds=650)
        ready(endpoint('baseline', 5656) + '/api/core/version/', seconds=180)
        for helper in ('/vod-verification/seed.py', '/timeshift-verification/seed.py'):
            command(compose + ['exec', '-T', 'baseline', '/dispatcharrpy/bin/python', helper], private=True)
        # Use stdin so neither parent command log nor host process argv holds credentials.
        seed_run = subprocess.run(compose + ['exec', '-T', 'baseline', '/dispatcharrpy/bin/python', '-'],
            env=environment, input=(tools / 'scripts/rust-m4-perf/seed.py').read_bytes(), capture_output=True, timeout=60)
        seed_run.check_returncode()
        seed = json.loads(seed_run.stdout.splitlines()[-1])
        report['fixture_auth'] = validate_fixture_tokens(seed, 3600)
        report['fixture_auth']['synthetic_token_lifetime_seconds'] = seed['fixture_access_token_lifetime_seconds']
        command(compose + ['exec', '-T', 'baseline', '/dispatcharrpy/bin/python',
                           '/verification/supervisor.py', 'control', 'stop'])
        command(compose + ['up', '-d', '--no-deps', 'data-init', 'source', 'candidate'])
        # This overlap occurs only during setup to inspect both cgroups, never in measured phases.
        limits = {}
        report['containers'] = {}
        for logical, service in (('infrastructure', 'baseline'), ('origin', 'origin'), ('source', 'source'), ('candidate', 'candidate')):
            identifier = command(compose + ['ps', '-q', service]).strip()
            inspection = json.loads(command(['docker', 'inspect', identifier]))[0]
            config = inspection['HostConfig']
            limits[logical] = {'memory_bytes': config['Memory'], 'nano_cpus': config['NanoCpus']}
            report['containers'][logical] = {'image_id': inspection['Image'], 'limits': limits[logical]}
            if service == 'candidate':
                report['candidate_provenance'] = integration.validate_candidate_image(manifest, inspection)
        validate_limits(limits, plan)
        stop_backends()
        origin = endpoint('origin', 8080)
        report['fixture'] = http(origin + '/stats')
        expected_prefixes = {}
        for protocol, path in [('vod', '/vod.mp4'), ('timeshift', '/timeshift/fixture/fixture/30/2026-11-09:11-00/fixture.ts')]:
            with urllib.request.urlopen(origin + path, timeout=15) as response:
                payload = response.read(plan['prefix_bytes'])
            if len(payload) != plan['prefix_bytes']:
                raise RuntimeError('Deterministic fixture prefix truncated')
            expected_prefixes[protocol] = hashlib.sha256(payload).hexdigest()
            # Actual Source and native prefix plus strict audio/video decoding, before resource trials.
            for backend in ('source', 'candidate'):
                base = reset(backend)
                watcher = observer_for(backend, base)
                try:
                    url, headers, _ = endpoints(base, protocol, 1, expected_prefixes[protocol])[0]
                    with urllib.request.urlopen(urllib.request.Request(url, headers=headers), timeout=15) as response:
                        # Source deliberately retains short startup-probe sessions.
                        # Hold archive playback beyond3s before asserting established cleanup.
                        capture_bytes = 2 * plan['prefix_bytes'] if protocol == 'timeshift' else plan['prefix_bytes']
                        captured = response.read(capture_bytes)
                        if len(captured) != capture_bytes:
                            raise RuntimeError('Preflight capture truncated')
                        data = captured[:plan['prefix_bytes']]
                    if hashlib.sha256(data).hexdigest() != expected_prefixes[protocol]:
                        raise RuntimeError('Preflight media prefix mismatch')
                    name = backend + '-' + protocol + ('.mp4' if protocol == 'vod' else '.ts')
                    (output / name).write_bytes(data)
                    wait_cleanup()
                    decoder_url = endpoints(base, protocol, 1, expected_prefixes[protocol])[0][0]
                    from urllib.parse import urlsplit
                    parsed = urlsplit(decoder_url)
                    internal_url = 'http://' + backend + (':5656' if backend == 'source' else ':9192') + parsed.path + ('?' + parsed.query if parsed.query else '')
                    decoder = subprocess.run(compose + ['exec', '-T', 'baseline', '/dispatcharrpy/bin/python', '-c',
                        (tools / 'scripts/rust-m4-perf/decode.py').read_text()],
                        env=environment, input=json.dumps({'url': internal_url}).encode(), capture_output=True, timeout=50)
                    decoder.check_returncode()
                    decoded = json.loads(decoder.stdout.splitlines()[-1])
                    frames = decoded['frames']
                    if not decoded['strict_decoder_passed']:
                        raise RuntimeError('Preflight actual HTTP audio/video decoder failed')
                    wait_cleanup()
                finally:
                    event_result = watcher.close()
                if event_result['errors'] or not event_result['stats_snapshots']:
                    raise RuntimeError('Real WebSocket lifecycle/statistics observer failed preflight')
                report['preflight'].append({'protocol': protocol, 'backend': backend, 'prefix_sha256': expected_prefixes[protocol],
                    'prefix_bytes': len(data), 'decoded_frames': frames, 'audio_and_video_mapped': True,
                    'decoder': decoded, 'websocket': event_result, 'provider_counter_released': True})
        if args.load_smoke:
            report['load_smoke'] = []
            for work in plan['workloads']:
                for backend in ('source', 'candidate'):
                    base = reset(backend)
                    watcher = observer_for(backend, base)
                    try:
                        targets = endpoints(base, work['protocol'], work['viewers'], expected_prefixes[work['protocol']])
                        # This is functional load only. No Docker stats/top or CPU/memory observers run.
                        phase = run_continuous(targets, 2, 5,
                            lambda: {'containers': [{'id': 'functional-only'}], 'cpu_percent': 0, 'memory_bytes': 0},
                            plan['prefix_bytes'], plan['read_timeout_seconds'], 1)
                        wait_cleanup()
                    finally:
                        event_result = watcher.close()
                    smoke_budgets = dict(plan['budgets'], min_samples=1)
                    rate = plan['fixture'][work['protocol'] + '_bytes_per_second']
                    failures = validate_phase(phase, work['viewers'], rate, smoke_budgets, ['functional-only'])
                    if event_result['errors'] or not event_result['stats_snapshots']:
                        failures.append('websocket_statistics')
                    # Dummy samples are not resource data and never enter evidence.
                    phase.pop('samples')
                    phase.update(protocol=work['protocol'], backend=backend, viewers_count=work['viewers'],
                                 websocket=event_result, correctness_failures=failures, resource_measurements=False)
                    report['load_smoke'].append(phase)
                    if failures:
                        raise RuntimeError('Sustained functional load smoke failed')
        if args.measure:
            # Setup/probes can outlast a quiet token: validate again before any measurement.
            authorize_quiet(args.quiet_authorization, frozen['plan_sha256'], manifest['image_id'])
            report['measurements_started'] = True
            for work in plan['workloads']:
                for trial, order in enumerate(plan['orders'], 1):
                    pair = {'protocol': work['protocol'], 'viewers': work['viewers'], 'trial': trial, 'order': order, 'phases': {}}
                    report['pairs'].append(pair)
                    for backend in order:
                        authorize_quiet(args.quiet_authorization, frozen['plan_sha256'], manifest['image_id'])
                        validate_fixture_tokens(seed, plan['warmup_seconds']+plan['measurement_seconds']+120)
                        base = reset(backend)
                        ids = selection(backend)
                        running = {identifier[:12] for identifier in command(['docker', 'ps', '-q']).splitlines()}
                        if running != set(ids):
                            raise RuntimeError('Quiet measurement requires only the three selected service containers running')
                        watcher = observer_for(backend, base)
                        phase = None
                        samples_seen = 0
                        def sample():
                            nonlocal samples_seen
                            value = resources.resources(ids)
                            if samples_seen % 3 == 0:
                                value['process_rss_diagnostic'] = resources.process_resources(ids)
                            samples_seen += 1
                            return value
                        try:
                            targets = endpoints(base, work['protocol'], work['viewers'], expected_prefixes[work['protocol']])
                            phase = run_continuous(targets, plan['warmup_seconds'], plan['measurement_seconds'], sample,
                                plan['prefix_bytes'], plan['read_timeout_seconds'], plan['sample_seconds'])
                            wait_cleanup()
                        finally:
                            observed = watcher.close()
                            if phase is not None:
                                phase['websocket'] = observed
                        rate = plan['fixture'][work['protocol'] + '_bytes_per_second']
                        failures = validate_phase(phase, work['viewers'], rate, plan['budgets'], ids)
                        if observed['errors'] or not observed['stats_snapshots']:
                            failures.append('websocket_statistics')
                        if work['protocol'] == 'vod':
                            for transition in ('vod_started', 'vod_stopped'):
                                if observed['kinds'].get(transition, 0) != work['viewers']:
                                    failures.append(transition + '_count')
                        elif not observed['empty_snapshots'].get('timeshift_stats'):
                            failures.append('timeshift_missing_empty_snapshot')
                        phase.update(correctness_passed=not failures, correctness_failures=failures, selected_containers=ids)
                        pair['phases'][backend] = phase
                        (output / 'result.json').write_text(json.dumps(report, indent=2) + '\n')
                        if failures:
                            raise RuntimeError('Measurement correctness failed; preserve trial without replacement')
                    pair.update(compare_pair(pair['phases']['source'], pair['phases']['candidate'], plan['budgets']))
                    (output / 'result.json').write_text(json.dumps(report, indent=2) + '\n')
            report['passed'] = final_acceptance(report['pairs'], plan)
            report['median_ratios'] = []
            for work in plan['workloads']:
                pairs = [row for row in report['pairs'] if row['protocol'] == work['protocol'] and row['viewers'] == work['viewers']]
                report['median_ratios'].append({**work, 'pairs': len(pairs),
                    'ratios': {key: statistics.median(row['ratios'][key] for row in pairs)
                               for key in ('cpu_ratio', 'memory_ratio', 'throughput_ratio', 'first_byte_ratio')}})
            report['performance_verified'] = report['passed']
        else:
            report['passed'] = len(report['preflight']) == 4
            report['mode'] = 'Functional preflight only; no resource measurements'
    except BaseException as error:
        report['error_type'] = type(error).__name__
        report['passed'] = False
        # Exceptions may include auth-bearing URLs; their text is deliberately omitted.
        raise
    finally:
        try:
            command(compose + ['down', '--volumes', '--remove-orphans', '--timeout', '15'], timeout=90)
            remaining = command(['docker', 'ps', '-aq', '--filter', 'label=com.docker.compose.project=' + project]).strip()
            report['cleanup_passed'] = not remaining
        except Exception:
            report['cleanup_passed'] = False
            report['passed'] = False
        shutil.rmtree(temporary)
        (output / 'result.json').write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps({'passed': report['passed'], 'cleanup_passed': report['cleanup_passed'],
                      'measurements_started': report['measurements_started']}))
    return 0 if report['passed'] and report['cleanup_passed'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
