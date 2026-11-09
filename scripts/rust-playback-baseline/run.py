#!/usr/bin/env python3
"""Repeated real-media Python playback baseline; never declares Rust gains."""
import argparse
import datetime
import hashlib
import importlib.util
import json
import math
import shutil
import signal
import os
import subprocess
import sys
import tempfile
import time
import urllib.request
import urllib.parse
import urllib.error
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
BASELINE = '1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430'
IMAGE = 'ghcr.io/dispatcharr/dispatcharr@sha256:34f9011514edd31ef0ff111b5f256d2e37614fcb08a6ee1ed9eccec9251a856f'


def run_load_command(command, environment, timeout):
    """A cancelled measurement must not leave host HTTP/load processes running."""
    with subprocess.Popen(command, env=environment, text=True, stdout=subprocess.PIPE,
                          stderr=subprocess.PIPE, start_new_session=True) as process:
        try:
            stdout, stderr = process.communicate(timeout=timeout)
        except BaseException:
            if process.poll() is None:
                os.killpg(process.pid, signal.SIGTERM)
                try:
                    process.communicate(timeout=3)
                except subprocess.TimeoutExpired:
                    os.killpg(process.pid, signal.SIGKILL)
                    process.communicate(timeout=3)
            raise
        return subprocess.CompletedProcess(command, process.returncode, stdout, stderr)


def validate_build_manifest(manifest, image_id, source_label):
    if manifest.get('format_version') != 1 or manifest.get('passed') is not True:
        raise ValueError('candidate build manifest is not a passing version1 build')
    hashes = manifest.get('source_sha256')
    if not isinstance(hashes, dict) or not hashes or not all(
            isinstance(path, str) and isinstance(digest, str) and len(digest) == 64
            and all(char in '0123456789abcdef' for char in digest)
            for path, digest in hashes.items()):
        raise ValueError('candidate build manifest has invalid source hashes')
    digest = hashlib.sha256(json.dumps(hashes, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
    if manifest.get('source_tree_sha256') != digest or source_label != digest:
        raise ValueError('candidate image/source manifest label mismatch')
    if manifest.get('image_id', '').removeprefix('sha256:') != image_id.removeprefix('sha256:'):
        raise ValueError('running candidate image does not match the build manifest')
    return digest


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--docker', default='docker')
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--viewers', nargs='+', type=int, default=[1, 10, 100])
    parser.add_argument('--trials', type=int, default=3)
    parser.add_argument('--warmup', type=float, default=15)
    parser.add_argument('--duration', type=float, default=60)
    parser.add_argument('--warmup-ramp', type=float, default=0)
    parser.add_argument('--timeout', type=float, default=10)
    parser.add_argument('--scenarios', nargs='+', choices=['same-channel', 'many-channel', 'slow-readers'],
                        default=['same-channel'])
    parser.add_argument('--fixture-seconds', type=int, default=1800)
    parser.add_argument('--fixture-codec', choices=['mpeg2', 'h264'], default='mpeg2')
    parser.add_argument('--vod-fixture', action='store_true')
    parser.add_argument('--shutdown-delay', type=int, default=5)
    parser.add_argument('--retention-seconds', type=int, default=60)
    parser.add_argument('--startup-behind', type=int, default=5)
    parser.add_argument('--cleanup-timeout', type=float, default=60)
    parser.add_argument('--start-signal', type=Path, help='After decoder readiness, wait for this new file before measuring')
    parser.add_argument('--candidate-overlay', type=Path, help='Compose overlay with the prebuilt Rust candidate')
    parser.add_argument('--candidate-build-manifest', type=Path)
    parser.add_argument('--max-cpu-ratio', type=float, default=1)
    parser.add_argument('--max-memory-ratio', type=float, default=1)
    parser.add_argument('--max-latency-ratio', type=float, default=1.1)
    parser.add_argument('--require-comparison', action='store_true')
    parser.add_argument('--functional-only', action='store_true')
    parser.add_argument('--functional-probes', nargs='+', choices=['mixed', 'stall', 'failover', 'hls', 'fmp4', 'vod-origin'],
                        default=['mixed', 'stall', 'failover'])
    parser.add_argument('--candidate-service', default='candidate')
    parser.add_argument('--candidate-port', type=int, default=9192)
    parser.add_argument('--fresh-web-per-trial', action='store_true',
                        help='Explicitly restart only benchmark web and clear its isolated Redis before each independent trial')
    parser.add_argument('--measurement-quality', choices=['load-smoke', 'isolated-characterization'],
                        default='load-smoke')
    args = parser.parse_args()
    if any(n < 1 or n > 100 for n in args.viewers) or not 1 <= args.trials <= 3:
        parser.error('viewers must be 1..100; trials must be 1..3')
    if (not all(math.isfinite(value) for value in
                (args.warmup, args.warmup_ramp, args.duration, args.timeout, args.cleanup_timeout))
            or args.warmup < 0 or args.warmup_ramp < 0
            or (args.warmup_ramp and args.warmup_ramp >= args.warmup) or args.duration < 10 or not 0 < args.timeout <= args.duration
            or not 0 < args.cleanup_timeout <= 300 or not 60 <= args.fixture_seconds <= 3600):
        parser.error('invalid timing or fixture limits')
    if not (0 <= args.shutdown_delay <= 60 and 1 <= args.retention_seconds <= 300
            and 0 <= args.startup_behind <= args.retention_seconds):
        parser.error('invalid retention, startup position, or shutdown delay')
    if args.candidate_overlay and args.measurement_quality == 'isolated-characterization' and not args.candidate_build_manifest:
        parser.error('isolated candidate characterization requires --candidate-build-manifest')
    if not all(math.isfinite(value) and value > 0 for value in (args.max_cpu_ratio, args.max_memory_ratio, args.max_latency_ratio)):
        parser.error('comparison ratios must be finite and positive')
    if args.functional_only and 'vod-origin' in args.functional_probes and not args.vod_fixture:
        parser.error('vod-origin probe requires --vod-fixture')
    if args.functional_only and any(value in args.functional_probes for value in ('hls', 'fmp4')) and args.fixture_codec != 'h264':
        parser.error('browser HLS/fMP4 probes require --fixture-codec h264')
    if args.require_comparison and args.functional_only:
        parser.error('functional-only cannot establish a resource comparison')
    if args.require_comparison and (not args.candidate_overlay or args.trials < 3
            or args.measurement_quality != 'isolated-characterization' or args.warmup < 15 or args.duration < 60):
        parser.error('required comparison needs candidate,3trials,isolated quality,and at least15s/60s warmup/measurement')
    # A continuously shared upstream can survive across trials. Do not permit
    # timestamp wraps even when measurements omit optional scenario stages.
    estimated = args.trials * (len(args.viewers) if 'same-channel' in args.scenarios else 0)
    estimated += args.trials * (len(args.scenarios) - ('same-channel' in args.scenarios))
    if args.candidate_overlay:
        estimated *= 2
    if args.fixture_seconds < estimated * (args.warmup + args.duration + args.timeout) + 60:
        parser.error('fixture duration is insufficient for the requested matrix')
    docker_executable = shutil.which(args.docker)
    if not docker_executable:
        parser.error('docker executable not found')
    args.docker = docker_executable
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=True)
    if any(output.iterdir()):
        parser.error('output must be empty')
    project = 'dispatcharr-playback-' + uuid.uuid4().hex[:12]
    evidence = {'source_commit': BASELINE, 'image': IMAGE, 'project': project,
                'scope': ('real-media-native-ts-functional-probes' if args.functional_only
                          else 'actual Python uWSGI TS playback baseline matrix'),
                'configuration': {key: str(value) if isinstance(value, Path) else value
                                  for key, value in vars(args).items()},
                'milestone_completed': False, 'performance_gain_verified': False,
                'exit_code': 1, 'commands': [], 'measurements': []}
    candidate_manifest = None
    if args.candidate_build_manifest:
        raw_manifest = args.candidate_build_manifest.read_bytes()
        candidate_manifest = json.loads(raw_manifest)
        evidence['candidate_build_manifest'] = candidate_manifest
        evidence['candidate_build_manifest_sha256'] = hashlib.sha256(raw_manifest).hexdigest()
    with tempfile.TemporaryDirectory(prefix='dispatcharr-playback-') as temporary:
        source = Path(temporary) / 'source'
        source.mkdir()
        archive = Path(temporary) / 'source.tar'
        with archive.open('wb') as stream:
            subprocess.run(['git', 'archive', BASELINE], cwd=ROOT, stdout=stream, timeout=30, check=True)
        evidence['git_archive_sha256'] = hashlib.sha256(archive.read_bytes()).hexdigest()
        subprocess.run(['tar', '-xf', str(archive), '-C', str(source), '--no-same-owner'], timeout=30, check=True)
        (source / 'media').mkdir(exist_ok=True)
        environment = dict(os.environ, BASELINE_SOURCE_DIR=str(source), BASELINE_EVIDENCE_DIR=str(output),
                           FIXTURE_SECONDS=str(args.fixture_seconds), FIXTURE_CODEC=args.fixture_codec,
                           ENABLE_VOD_FIXTURE='1' if args.vod_fixture else '0',
                           BENCHMARK_SHUTDOWN_DELAY=str(args.shutdown_delay),
                           BENCHMARK_RETENTION_SECONDS=str(args.retention_seconds),
                           BENCHMARK_STARTUP_BEHIND=str(args.startup_behind))
        compose_file = ROOT / 'docker/compose.rust-playback.yml'
        compose = [args.docker, 'compose', '-p', project, '-f', str(compose_file)]
        if args.candidate_overlay:
            compose += ['-f', str(args.candidate_overlay.resolve())]
        tool_paths = [*Path(__file__).parent.glob('*.py'), *Path(__file__).parent.glob('*.sh'),
                      Path(__file__).with_name('uwsgi.ini'), compose_file,
                      ROOT / 'scripts/rust-perf/perf.py', ROOT / 'scripts/rust-baseline/dependencies.py']
        if args.candidate_overlay:
            tool_paths.append(args.candidate_overlay.resolve())
        evidence['tools_sha256'] = {str(p.relative_to(ROOT)) if p.is_relative_to(ROOT) else str(p): hashlib.sha256(p.read_bytes()).hexdigest()
                                    for p in sorted(tool_paths)}

        def execute(command, timeout=180):
            evidence['commands'].append(command)
            return subprocess.run(command, env=environment, check=True, text=True,
                                  capture_output=True, timeout=timeout).stdout

        def endpoint(service, port):
            address = execute(compose + ['port', service, str(port)]).strip()
            if not address:
                raise RuntimeError('Docker did not publish the isolated loopback port')
            return 'http://' + address

        def get_json(url):
            with urllib.request.urlopen(url, timeout=3) as response:
                return json.load(response)

        try:
            execute(compose + ['up', '-d', 'baseline', 'origin'], timeout=180)
            base, origin = endpoint('baseline', 5656), endpoint('origin', 8080)
            ready_deadline = time.monotonic() + 300
            while True:
                try:
                    get_json(origin + '/health')
                    try:
                        urllib.request.urlopen(base + '/proxy/ts/stream/00000000-0000-0000-0000-000000000000', timeout=3)
                        raise RuntimeError('missing channel unexpectedly returned success')
                    except urllib.error.HTTPError as error:
                        if error.code != 404:
                            raise
                    if (output / 'seed.json').exists():
                        break
                except Exception:
                    if time.monotonic() >= ready_deadline:
                        raise TimeoutError('baseline startup readiness exceeded 300 seconds')
                    time.sleep(1)
            seed = json.loads((output / 'seed.json').read_text())
            fixture = get_json(origin + '/stats')
            (output / 'fixture.json').write_text(json.dumps(fixture, indent=2) + '\n')
            # Decode actual proxied bytes in the same image, with no capture-file buffering.
            decoded = execute(compose + ['exec', '-T', 'baseline', 'ffmpeg', '-hide_banner',
                              '-loglevel', 'error', '-xerror', '-progress', 'pipe:1', '-nostats',
                              '-i', 'http://127.0.0.1:5656' + seed['path'], '-t', '3',
                              '-f', 'null', '-'], timeout=45)
            (output / 'decode.log').write_text(decoded)
            if not any(line.startswith('frame=') and int(line.split('=')[1]) > 0
                       for line in decoded.splitlines()):
                raise RuntimeError('decoder returned no video frames')
            (output / 'ready.json').write_text(json.dumps({'decoder_passed': True, 'ready': True}) + '\n')
            if args.start_signal:
                print(f'Playback decoded. Waiting for start signal: {args.start_signal}', flush=True)
                signal_deadline = time.monotonic() + 1800
                while not args.start_signal.exists():
                    if time.monotonic() >= signal_deadline:
                        raise TimeoutError('measurement start signal not received')
                    time.sleep(.5)
            print('Playback decoded. Starting resource measurements; avoid other CPU workloads.', flush=True)
            fixture_id = (fixture['fixture_sha256'] + ':proxy-ts-2mbps-one-worker-codec' + fixture.get('codec', 'mpeg2')
                          + f'-shutdown{seed["proxy_settings"]["channel_shutdown_delay"]}'
                          + f'-retention{seed["proxy_settings"]["redis_chunk_ttl"]}'
                          + f'-behind{seed["proxy_settings"]["new_client_behind_seconds"]}')
            perf_environment = dict(environment)
            perf_environment['PATH'] = str(Path(args.docker).parent) + os.pathsep + environment['PATH']
            matrix = []
            for scenario in args.scenarios:
                counts = args.viewers if scenario == 'same-channel' else [10]
                for viewers in counts:
                    for trial in range(1, args.trials + 1):
                        backends = (['baseline', 'candidate'] if trial % 2 else ['candidate', 'baseline']) if args.candidate_overlay else ['baseline']
                        for backend in backends:
                            matrix.append((backend, scenario, viewers, trial))
            if args.functional_only:
                matrix = [(backend, 'functional', 1, 1) for backend in
                          (['baseline', 'candidate'] if args.candidate_overlay else ['baseline'])]
            for backend, scenario, viewers, trial in matrix:
                name = f'{backend}-{scenario}-viewers-{viewers}-trial-{trial}.json'
                if args.candidate_overlay:
                    execute(compose + ['stop', '--timeout', '15', args.candidate_service], timeout=30)
                selected = seed['channels'][:viewers] if scenario == 'many-channel' else [seed]
                urls = output / f'{scenario}-urls.txt'
                reset = None
                if args.fresh_web_per_trial or args.candidate_overlay:
                    reset = {'mode': 'fresh-web-and-isolated-redis-per-trial',
                             'reason': 'Independent trial characterization; cold-reopen failure tracked separately'}
                    reset['web_stop'] = json.loads(execute(compose + ['exec', '-T', 'baseline',
                        '/dispatcharrpy/bin/python', '/verification/supervisor.py', 'control', 'stop']))
                    deadline = time.monotonic() + args.cleanup_timeout
                    while get_json(origin + '/stats')['active']:
                        if time.monotonic() >= deadline:
                            raise TimeoutError('upstream remained active after controlled web stop')
                        time.sleep(.25)
                    reset['redis_keys_before'] = int(execute(compose + ['exec', '-T', 'baseline', 'redis-cli', 'dbsize']))
                    reset['redis_clear_result'] = execute(compose + ['exec', '-T', 'baseline', 'redis-cli', 'FLUSHDB']).strip()
                    reset['redis_keys_after'] = int(execute(compose + ['exec', '-T', 'baseline', 'redis-cli', 'dbsize']))
                    if backend == 'baseline':
                        reset['web_start'] = json.loads(execute(compose + ['exec', '-T', 'baseline',
                            '/dispatcharrpy/bin/python', '/verification/supervisor.py', 'control', 'start']))
                        deadline = time.monotonic() + 60
                        while True:
                            try:
                                urllib.request.urlopen(base + '/proxy/ts/stream/00000000-0000-0000-0000-000000000000', timeout=3)
                            except urllib.error.HTTPError as error:
                                if error.code == 404:
                                    break
                            except OSError:
                                pass
                            if time.monotonic() >= deadline:
                                raise TimeoutError('web restart readiness exceeded60seconds')
                            time.sleep(.25)
                services = ['baseline', 'origin']
                revision = BASELINE + '@' + IMAGE
                target = base
                if backend == 'candidate':
                    execute(compose + ['up', '-d', '--no-deps', '--no-build', args.candidate_service], timeout=60)
                    target = endpoint(args.candidate_service, args.candidate_port)
                    deadline = time.monotonic() + 60
                    while True:
                        try:
                            health = get_json(target + '/health/ready')
                            if health.get('status') == 'ready':
                                break
                        except (OSError, ValueError):
                            pass
                        if time.monotonic() >= deadline:
                            raise TimeoutError('candidate readiness exceeded60seconds')
                        time.sleep(.25)
                    services.append(args.candidate_service)
                    container_id = execute(compose + ['ps', '-q', args.candidate_service]).strip()
                    inspected = json.loads(execute([args.docker, 'inspect', container_id]))[0]
                    revision = inspected['Image']
                    if candidate_manifest:
                        validate_build_manifest(candidate_manifest, revision,
                            inspected['Config']['Labels'].get('org.opencontainers.image.source-tree-sha256'))
                    evidence['candidate_image_source_bound'] = bool(candidate_manifest)
                    decoded_candidate = execute(compose + ['exec', '-T', 'baseline', 'ffmpeg', '-hide_banner',
                        '-loglevel', 'error', '-xerror', '-progress', 'pipe:1', '-nostats', '-i',
                        'http://' + args.candidate_service + ':' + str(args.candidate_port) + seed['path'],
                        '-t', '3', '-f', 'null', '-'], timeout=45)
                    if not any(line.startswith('frame=') and int(line.split('=')[1]) > 0
                               for line in decoded_candidate.splitlines()):
                        raise RuntimeError('candidate decoder returned no frames')
                    (output / (name + '.decode.log')).write_text(decoded_candidate)
                urls.write_text(''.join(target + channel['path'] + '\n' for channel in selected))
                before = get_json(origin + '/stats')
                if args.functional_only:
                    for probe_name in args.functional_probes:
                        probe_reset = {'mode': 'isolated-web-and-redis-between-functional-cases'}
                        if args.candidate_overlay:
                            execute(compose + ['stop', '--timeout', '15', args.candidate_service], timeout=30)
                        probe_reset['web_stop'] = json.loads(execute(compose + ['exec', '-T', 'baseline',
                            '/dispatcharrpy/bin/python', '/verification/supervisor.py', 'control', 'stop']))
                        deadline = time.monotonic() + args.cleanup_timeout
                        while get_json(origin + '/stats')['active']:
                            if time.monotonic() >= deadline:
                                raise TimeoutError('functional-case reset left upstream active')
                            time.sleep(.25)
                        probe_reset['redis_keys_before'] = int(execute(compose + ['exec', '-T', 'baseline', 'redis-cli', 'dbsize']))
                        probe_reset['redis_clear_result'] = execute(compose + ['exec', '-T', 'baseline', 'redis-cli', 'FLUSHDB']).strip()
                        if backend == 'baseline':
                            probe_reset['web_start'] = json.loads(execute(compose + ['exec', '-T', 'baseline',
                                '/dispatcharrpy/bin/python', '/verification/supervisor.py', 'control', 'start']))
                            deadline = time.monotonic() + 60
                            while True:
                                try:
                                    urllib.request.urlopen(base + '/proxy/ts/stream/00000000-0000-0000-0000-000000000000', timeout=3)
                                except urllib.error.HTTPError as error:
                                    if error.code == 404:
                                        break
                                except OSError:
                                    pass
                                if time.monotonic() >= deadline:
                                    raise TimeoutError('functional web reset not ready')
                                time.sleep(.25)
                        else:
                            execute(compose + ['up', '-d', '--no-deps', '--no-build', args.candidate_service], timeout=60)
                            # Docker may allocate a new host port after stop/start.
                            target = endpoint(args.candidate_service, args.candidate_port)
                            probe_reset['candidate_endpoint_refreshed'] = True
                            deadline = time.monotonic() + 60
                            while True:
                                try:
                                    if get_json(target + '/health/ready').get('status') == 'ready':
                                        break
                                except (OSError, ValueError):
                                    pass
                                if time.monotonic() >= deadline:
                                    raise TimeoutError('functional candidate reset not ready')
                                time.sleep(.25)
                        directory_name = backend + '-' + probe_name
                        probe_output = output / directory_name
                        protocol_probe = probe_name in ('hls', 'fmp4', 'vod-origin')
                        if protocol_probe:
                            if probe_name == 'vod-origin':
                                urls.write_text(origin + '/vod.mp4\n')
                            else:
                                parts = urllib.parse.urlsplit(target + seed['path'])
                                query = urllib.parse.parse_qsl(parts.query)
                                query = [(key, value) for key, value in query if key != 'output_format']
                                query.append(('output_format', probe_name))
                                urls.write_text(urllib.parse.urlunsplit((parts.scheme, parts.netloc, parts.path,
                                                urllib.parse.urlencode(query), parts.fragment)) + '\n')
                            command = [sys.executable, str(Path(__file__).with_name('protocol_probe.py')),
                                '--url-file', str(urls), '--origin-base', origin,
                                '--mode', probe_name, '--output', str(probe_output)]
                        elif probe_name == 'mixed':
                            urls.write_text(target + seed['path'] + '\n')
                            command = [sys.executable, str(Path(__file__).with_name('mixed_probe.py')),
                                '--url-file', str(urls), '--output', str(probe_output / 'mixed.json'),
                                '--warmup', '15', '--ramp', '4', '--duration', '30',
                                '--project', project, '--services', *services,
                                '--compose-file', str(compose_file),
                                *(['--compose-file', str(args.candidate_overlay.resolve())] if args.candidate_overlay else [])]
                        else:
                            urls.write_text(target + seed['path'] + '\n')
                            command = [sys.executable, str(Path(__file__).with_name('fault_probe.py')),
                                '--url-file', str(urls), '--origin-base', origin,
                                '--primary-path', seed['origin_path'], '--backup-path', seed['backup_origin_path'],
                                '--case', probe_name, '--output', str(probe_output)]
                        evidence['commands'].append(command)
                        probe_result = run_load_command(command, perf_environment, 130 if protocol_probe else 100)
                        probe_output.mkdir(parents=True, exist_ok=True)
                        (probe_output / 'command.log').write_text(probe_result.stdout + probe_result.stderr)
                        record = {'backend': backend, 'probe': probe_name, 'exit_code': probe_result.returncode,
                                  'directory': directory_name, 'passed': probe_result.returncode == 0,
                                  'reset': probe_reset}
                        if protocol_probe:
                            record['protocol_scope'] = ('origin fixture only; no VOD backend route exercised'
                                                        if probe_name == 'vod-origin' else 'backend live ' + probe_name)
                            capture_name = 'capture.ts' if probe_name == 'hls' else 'capture.mp4'
                            if (probe_output / capture_name).exists():
                                decoder_command = compose + ['exec', '-T', 'baseline', 'ffmpeg', '-hide_banner',
                                    '-loglevel', 'error', '-xerror', '-progress', 'pipe:1', '-nostats',
                                    '-i', '/evidence/' + directory_name + '/' + capture_name,
                                    '-map', '0:v:0', '-map', '0:a:0', '-t', '3', '-f', 'null', '-']
                                evidence['commands'].append(decoder_command)
                                decoder = subprocess.run(decoder_command, env=environment, text=True,
                                                         capture_output=True, timeout=45)
                                (probe_output / 'decode.log').write_text(decoder.stdout + decoder.stderr)
                                frames = [int(line.split('=')[1]) for line in decoder.stdout.splitlines()
                                          if line.startswith('frame=')]
                                record['decoder_exit_code'] = decoder.returncode
                                record['decoder_frames'] = max(frames, default=0)
                                record['video_and_audio_verified'] = decoder.returncode == 0 and record['decoder_frames'] >= 50
                                record['passed'] = record['passed'] and record['video_and_audio_verified']
                            else:
                                record['passed'] = False
                        elif probe_name != 'mixed' and (probe_output / 'capture.ts').exists():
                            # Exclude the arbitrary packet-aligned capture tail, which may
                            # end inside a coded frame. This preserves the fault boundary.
                            interior_seconds = max(0, (probe_output / 'capture.ts').stat().st_size / 250000 - 2)
                            record['decoder_interior_seconds'] = interior_seconds
                            decoder_command = compose + ['exec', '-T', 'baseline', 'ffmpeg', '-hide_banner',
                                '-loglevel', 'error', '-xerror', '-progress', 'pipe:1', '-nostats',
                                '-i', '/evidence/' + directory_name + '/capture.ts', '-map', '0:v:0', '-map', '0:a:0',
                                '-t', str(interior_seconds), '-f', 'null', '-']
                            evidence['commands'].append(decoder_command)
                            decoder = subprocess.run(decoder_command, env=environment, text=True,
                                                     capture_output=True, timeout=45)
                            (probe_output / 'decode.log').write_text(decoder.stdout + decoder.stderr)
                            frames = [int(line.split('=')[1]) for line in decoder.stdout.splitlines()
                                      if line.startswith('frame=')]
                            record['decoder_exit_code'] = decoder.returncode
                            record['decoder_frames'] = max(frames, default=0)
                            record['decoder_verified'] = decoder.returncode == 0 and bool(record['decoder_frames'])
                            # A fresh decoder begins after the alternate has been stable for
                            # seven seconds (>two GOPs/PAT/PMT repetitions for this fixture).
                            # This separates persistent failure from an abrupt boundary warning.
                            inside_target = ('http://127.0.0.1:5656' if backend == 'baseline' else
                                             'http://' + args.candidate_service + ':' + str(args.candidate_port))
                            recovery_decoder_command = compose + ['exec', '-T', 'baseline', 'ffmpeg',
                                '-hide_banner', '-loglevel', 'error', '-xerror', '-progress', 'pipe:1', '-nostats',
                                '-i', inside_target + seed['path'], '-map', '0:v:0', '-map', '0:a:0',
                                '-t', '3', '-f', 'null', '-']
                            evidence['commands'].append(recovery_decoder_command)
                            recovered_decoder = subprocess.run(recovery_decoder_command, env=environment,
                                text=True, capture_output=True, timeout=45)
                            (probe_output / 'post-recovery-decode.log').write_text(recovered_decoder.stdout + recovered_decoder.stderr)
                            recovery_frames = [int(line.split('=')[1]) for line in recovered_decoder.stdout.splitlines()
                                               if line.startswith('frame=')]
                            record['post_recovery_decoder_exit_code'] = recovered_decoder.returncode
                            record['post_recovery_decoder_frames'] = max(recovery_frames, default=0)
                            record['post_recovery_video_and_audio_verified'] = (
                                recovered_decoder.returncode == 0 and record['post_recovery_decoder_frames'] >= 50)
                            record['passed'] = record['passed'] and record['post_recovery_video_and_audio_verified']
                            if probe_name == 'failover':
                                record['transient_boundary_warning'] = not record['decoder_verified']
                            else:
                                record['passed'] = record['passed'] and record['decoder_verified']
                        cleanup_deadline = time.monotonic() + args.cleanup_timeout
                        while get_json(origin + '/stats')['active']:
                            if time.monotonic() >= cleanup_deadline:
                                record['passed'] = False
                                record['cleanup_failure'] = 'upstream did not stop within cleanup deadline'
                                break
                            time.sleep(.5)
                        evidence.setdefault('functional_probes', []).append(record)
                        print(f'{backend}-{probe_name}: passed {record["passed"]}', flush=True)
                    continue
                command = [sys.executable, str(ROOT / 'scripts/rust-perf/perf.py'), 'run',
                           '--compose-file', str(compose_file),
                           *(['--compose-file', str(args.candidate_overlay.resolve())] if args.candidate_overlay else []),
                           '--project', project,
                           '--services', *services, '--urls-file', str(urls),
                           '--scenario', 'many-channel' if scenario == 'many-channel' else 'same-channel',
                           '--viewers', str(viewers), '--warmup', str(args.warmup),
                           '--duration', str(args.duration), '--timeout', str(args.timeout),
                           '--warmup-ramp', str(args.warmup_ramp),
                           '--sample-interval', '1', '--fixture-id', fixture_id,
                           '--revision', revision, '--output', str(output / name)]
                if scenario == 'slow-readers':
                    # Explicitly all slow readers; mixed slow/fast is a separate required scenario.
                    command += ['--slow-delay', '0.25']
                evidence['commands'].append(command)
                result = run_load_command(command, perf_environment,
                                          args.warmup + args.duration + args.timeout + 45)
                (output / (name + '.log')).write_text(result.stdout + result.stderr)
                after = get_json(origin + '/stats')
                row = {'file': name, 'backend': backend, 'scenario': scenario, 'viewers': viewers, 'trial': trial,
                       'exit_code': result.returncode, 'origin_before': before, 'origin_after': after,
                       'reset': reset, 'measurement_quality': args.measurement_quality,
                       'selected_services': services, 'revision': revision}
                evidence['measurements'].append(row)
                print(f'{name}: exit {result.returncode}', flush=True)
                if result.returncode:
                    raise RuntimeError('performance harness validity checks failed')
                if after['eof_connections']:
                    raise RuntimeError('fixture exhausted; timestamp-stable duration was insufficient')
                # Observe client shutdown before the next trial instead of mixing lingering sessions.
                cleanup_deadline = time.monotonic() + args.cleanup_timeout
                while True:
                    cleanup_stats = get_json(origin + '/stats')
                    if cleanup_stats['active'] == 0:
                        break
                    if time.monotonic() >= cleanup_deadline:
                        raise TimeoutError('upstream remained active after viewers disconnected')
                    time.sleep(.5)
                row['cleanup_origin'] = cleanup_stats
                row['cleanup_completed'] = True
            for path, digest in evidence['tools_sha256'].items():
                if hashlib.sha256((ROOT / path).read_bytes()).hexdigest() != digest:
                    raise RuntimeError('benchmark tool source changed during the matrix: ' + path)
            if args.functional_only and not all(row['passed'] for row in evidence.get('functional_probes', [])):
                raise RuntimeError('one or more functional probes failed; inspect every probe artifact')
            if args.candidate_overlay and not args.functional_only:
                module_spec = importlib.util.spec_from_file_location('playback_perf', ROOT / 'scripts/rust-perf/perf.py')
                perf = importlib.util.module_from_spec(module_spec)
                module_spec.loader.exec_module(perf)
                comparisons = []
                for scenario, viewers, trial in sorted({(row['scenario'], row['viewers'], row['trial'])
                                                       for row in evidence['measurements']}):
                    reports = {backend: json.loads((output / f'{backend}-{scenario}-viewers-{viewers}-trial-{trial}.json').read_text())
                               for backend in ('baseline', 'candidate')}
                    comparison = perf.compare(reports['baseline'], reports['candidate'],
                        {'cpu_percent_mean': args.max_cpu_ratio, 'memory_bytes_peak': args.max_memory_ratio,
                         'first_byte_ms_p95': args.max_latency_ratio})
                    comparisons.append({'scenario': scenario, 'viewers': viewers, 'trial': trial, **comparison})
                evidence['resource_comparisons'] = comparisons
                evidence['resource_comparison_passed'] = all(row['passed'] for row in comparisons)
                (output / 'comparisons.json').write_text(json.dumps(comparisons, indent=2) + '\n')
                if args.require_comparison and not evidence['resource_comparison_passed']:
                    raise RuntimeError('required native TS resource comparison failed declared gates')
            evidence['matrix_completed'] = True
            evidence['scope_notes'] = [
                'Route-level native TS subset; no full-application parity or gain claim',
                'Baseline web stopped before Rust trials; PostgreSQL and Redis remain inside baseline container',
                'Origin fixture included identically; inspect per-container resource samples',
                'Fresh web/isolated Redis reset is explicit and cannot satisfy cold-reopen gate',
                'Warmup ramp settings must match; failed cold burst is retained separately',
            ]
            evidence['exit_code'] = 0
        except KeyboardInterrupt:
            evidence.update(exit_code=130, failure='interrupted')
        except subprocess.CalledProcessError as error:
            evidence.update(failure='command_failed', failure_command=error.cmd,
                            failure_stdout=error.stdout, failure_stderr=error.stderr)
        except (OSError, subprocess.SubprocessError, RuntimeError, TimeoutError) as error:
            evidence.update(failure=type(error).__name__, failure_message=str(error))
        finally:
            try:
                (output / 'compose.log').write_text(execute(compose + ['logs', '--no-color'], timeout=30))
            except Exception as error:
                evidence['log_collection_failure'] = type(error).__name__
            try:
                execute(compose + ['down', '--volumes', '--remove-orphans', '--timeout', '5'], timeout=60)
                evidence['cleanup'] = 'isolated project removed'
            except Exception as error:
                evidence['cleanup_failure'] = type(error).__name__
                evidence['exit_code'] = 1
            evidence['timestamp_utc'] = datetime.datetime.now(datetime.timezone.utc).isoformat()
            (output / 'run.json').write_text(json.dumps(evidence, indent=2) + '\n')
    print(json.dumps({'exit_code': evidence['exit_code'], 'output': str(output),
                      'failure': evidence.get('failure')}, indent=2))
    raise SystemExit(evidence['exit_code'])


if __name__ == '__main__':
    main()
