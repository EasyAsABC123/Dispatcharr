#!/usr/bin/env python3
"""Run HLS transport gates against isolated real Redis. Original author credit retained."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import uuid

ROOT = Path(__file__).resolve().parents[2]
IMAGE = 'ghcr.io/dispatcharr/dispatcharr@sha256:34f9011514edd31ef0ff111b5f256d2e37614fcb08a6ee1ed9eccec9251a856f'


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--docker', default='docker')
    parser.add_argument('--output', required=True, type=Path)
    parser.add_argument('--suite', choices=['http', 'output', 'inspect', 'shutdown', 'input', 'admission', 'era'], default='http')
    parser.add_argument('--fixture', type=Path, help='Real valid TS fixture; decode each HTTP segment independently')
    args = parser.parse_args()
    if args.fixture and args.suite != 'http':
        parser.error('--fixture is only valid for the HTTP media suite')
    args.output.mkdir(parents=True, exist_ok=True)
    if any(args.output.iterdir()):
        parser.error('output must be empty')
    name = 'dispatcharr-hls-test-' + uuid.uuid4().hex[:12]
    inputs = [ROOT / 'rust/Cargo.lock', ROOT / 'rust/Cargo.toml', Path(__file__).resolve()]
    suites = {
        'input': ('input-store', 'output-store'),
        'output': ('output-store',),
        'inspect': ('clients', 'contracts'),
        'admission': ('clients','contracts'),
        'era': ('clients','contracts'),
        'http': ('hls-http', 'hls-core', 'stream-http', 'stream-core', 'stream-runtime', 'contracts', 'clients', 'output-store', 'coordination'),
        'shutdown': ('hls-http', 'hls-core', 'stream-http', 'stream-core', 'stream-runtime', 'contracts', 'clients', 'output-store', 'coordination'),
    }
    for crate in suites[args.suite]:
        inputs += list((ROOT / 'rust/crates' / crate).rglob('*.rs'))
        inputs += list((ROOT / 'rust/crates' / crate).rglob('Cargo.toml'))
    if args.suite == 'input':
        inputs.append(ROOT / 'apps/proxy/live_proxy/input/buffer.py')
    hashes = {str(path.relative_to(ROOT)): hashlib.sha256(path.read_bytes()).hexdigest() for path in sorted(inputs)}
    scope = {'input': 'shared TS input storage with real Redis', 'output': 'shared output storage with real Redis', 'era': 'client input-era fencing with real Redis', 'admission': 'client admission and local cleanup with real Redis', 'inspect': 'read-only HLS session inspection with real Redis', 'shutdown': 'HLS global shutdown with real Redis and loopback HTTP', 'http': 'HLS capability HTTP transport with real Redis and loopback origin'}[args.suite]
    evidence = {'scope': scope,
                'application_performance_verified': False, 'milestone_complete': False,
                'image': IMAGE, 'source_sha256': hashes, 'passed': False}
    package, target = {'http': ('dispatcharr-hls-http', 'http'), 'output': ('dispatcharr-output-store', 'redis'), 'inspect': ('dispatcharr-clients', 'redis'), 'shutdown': ('dispatcharr-hls-http', 'shutdown'), 'input': ('dispatcharr-input-store', 'redis'), 'admission': ('dispatcharr-clients','admission'), 'era': ('dispatcharr-clients','era')}[args.suite]
    command = ['cargo', 'test', '--manifest-path', str(ROOT / 'rust/Cargo.toml'), '-p',
               package, '--locked', '--offline', '--test', target, '--', '--ignored']
    if args.suite in ('output', 'input'):
        command += ['--test-threads=1']
    if args.suite == 'inspect':
        command += ['hls_readonly_inspection']
    try:
        subprocess.run([args.docker, 'run', '-d', '--rm', '--name', name, '-p', '127.0.0.1::6379',
                        '--entrypoint', 'redis-server', IMAGE, '--save', '', '--appendonly', 'no',
                        '--bind', '0.0.0.0', '--protected-mode', 'no'], check=True, capture_output=True, timeout=90)
        address = subprocess.check_output([args.docker, 'port', name, '6379'], text=True, timeout=10).strip()
        environment = dict(os.environ, REDIS_TEST_URL='redis://' + address)
        if args.fixture:
            environment.update(HLS_TEST_FIXTURE=str(args.fixture.resolve()), HLS_TEST_CAPTURE=str((args.output / 'segments').resolve()))
            evidence['fixture_sha256'] = hashlib.sha256(args.fixture.read_bytes()).hexdigest()
        with (args.output / 'tests.log').open('w') as log:
            result = subprocess.run(command, env=environment, stdout=log, stderr=subprocess.STDOUT, timeout=180)
        evidence.update(command=command, exit_code=result.returncode, passed=result.returncode == 0)
        if evidence['passed'] and args.fixture:
            segments = sorted((args.output / 'segments').glob('*.ts'))
            if not segments:
                raise RuntimeError('No actual HTTP segments captured')
            decoded = []
            for segment in segments:
                decode = [args.docker, 'run', '--rm', '--network', 'none', '-v',
                          str(segment.resolve().parent) + ':/media:ro', '--entrypoint', 'ffmpeg', IMAGE,
                          '-hide_banner', '-loglevel', 'error', '-xerror', '-progress', 'pipe:1',
                          '-nostats', '-i', '/media/' + segment.name, '-f', 'null', '-']
                result = subprocess.run(decode, text=True, capture_output=True, timeout=30)
                (args.output / (segment.name + '.decode.log')).write_text(result.stdout + result.stderr)
                frames = [int(line.split('=')[1]) for line in result.stdout.splitlines() if line.startswith('frame=')]
                passed = result.returncode == 0 and frames and max(frames) > 0
                decoded.append({'segment': segment.name, 'sha256': hashlib.sha256(segment.read_bytes()).hexdigest(),
                                'decoded_frames': max(frames, default=0), 'passed': bool(passed)})
            evidence['decoded_http_segments'] = decoded
            evidence['passed'] = all(item['passed'] for item in decoded)
        changed = [path for path, old in hashes.items() if hashlib.sha256((ROOT / path).read_bytes()).hexdigest() != old]
        if changed:
            evidence.update(passed=False, changed_during_run=changed)
    except (OSError, subprocess.SubprocessError, RuntimeError) as error:
        evidence['error'] = str(error)
    finally:
        subprocess.run([args.docker, 'rm', '-f', name], capture_output=True, timeout=30)
        (args.output / 'evidence.json').write_text(json.dumps(evidence, indent=2) + '\n')
    raise SystemExit(0 if evidence['passed'] else 1)


if __name__ == '__main__':
    main()
