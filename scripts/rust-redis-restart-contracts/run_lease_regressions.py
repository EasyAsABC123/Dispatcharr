#!/usr/bin/env python3
"""Replay provider fencing and canceled ACQUIRE against a frozen lease fix.
Original Dispatcharr and redis-rs authors retain application/client credit.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import uuid
from run import IMAGE


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source-proof', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    proof = json.loads(args.source_proof.read_text())
    if not proof['passed'] or not proof['cleanup_passed']:
        raise ValueError('A completed passing source proof is required')
    base = args.source_proof.resolve().parent
    source = base / 'native-source'
    for relative, digest in proof['rust_source_sha256'].items():
        if hashlib.sha256((source / relative).read_bytes()).hexdigest() != digest:
            raise ValueError('Frozen source changed: ' + relative)
    out = args.output.resolve()
    out.mkdir(parents=True, exist_ok=False)
    binaries = out / 'binaries'
    binaries.mkdir(mode=0o755)
    report = {'passed': False, 'scope': 'Frozen actual provider/credential fencing and ACQUIRE cancellation regression',
              'source_proof_sha256': hashlib.sha256(args.source_proof.read_bytes()).hexdigest(),
              'rust_source_archive_sha256': proof['rust_source_archive_sha256'], 'tests': [],
              'runner_sha256': hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
              'image': IMAGE, 'performance_verified': False}
    name = 'dispatcharr-lease-regressions-' + uuid.uuid4().hex[:12]
    environment = dict(os.environ, CARGO_TARGET_DIR=str(base / 'cargo-target'))
    try:
        for target, count in [('redis', 10), ('acquire_cancel', 1)]:
            command = ['cargo', 'test', '--manifest-path', str(source / 'rust/Cargo.toml'),
                       '-p', 'dispatcharr-coordination', '--test', target, '--no-run',
                       '--message-format=json', '--locked', '--offline']
            built = subprocess.run(command, env=environment, text=True, capture_output=True, timeout=180)
            (out / (target + '-build.log')).write_text(built.stderr)
            (out / (target + '-build.jsonl')).write_text(built.stdout)
            built.check_returncode()
            paths = [json.loads(line)['executable'] for line in built.stdout.splitlines()
                     if line.startswith('{') and json.loads(line).get('reason') == 'compiler-artifact'
                     and json.loads(line).get('executable')]
            if len(paths) != 1:
                raise ValueError('One test binary required')
            shutil.copyfile(paths[0], binaries / target)
            (binaries / target).chmod(0o755)
            report['tests'].append({'target': target, 'expected_count': count,
                                    'binary_sha256': hashlib.sha256((binaries / target).read_bytes()).hexdigest()})
        subprocess.run(['docker', 'run', '-d', '--rm', '--name', name, '--network', 'none',
                        '--read-only', '--user', '65532:65532', '--cpus', '1', '--memory', '128m',
                        '--tmpfs', '/tmp:rw,size=16m,mode=1777', '--workdir', '/tmp',
                        '--mount', f'type=bind,src={binaries},dst=/native-peers,readonly',
                        '-e', 'REDIS_TEST_URL=redis://:playback-fixture-only@127.0.0.1:6379/0',
                        '--entrypoint', 'redis-server', IMAGE, '--save', '', '--appendonly', 'no',
                        '--bind', '127.0.0.1', '--requirepass', 'playback-fixture-only'],
                       env=environment, check=True, capture_output=True, timeout=30)
        # The server has already started before docker run returns; bounded ping
        # handles scheduling without assuming readiness from container creation.
        import time
        for attempt in range(100):
            ready = subprocess.run(['docker', 'exec', '-e', 'REDISCLI_AUTH=playback-fixture-only',
                                    name, 'redis-cli', '--no-auth-warning', 'ping'],
                                   env=environment, capture_output=True, text=True, timeout=5)
            if ready.returncode == 0 and ready.stdout.strip() == 'PONG':
                break
            time.sleep(.05)
        else:
            raise TimeoutError('Redis readiness')
        for test in report['tests']:
            result = subprocess.run(['docker', 'exec', name, '/native-peers/' + test['target'],
                                     '--ignored', '--nocapture', '--test-threads=1'],
                                    env=environment, capture_output=True, text=True, timeout=120)
            text = result.stdout + result.stderr
            (out / (test['target'] + '-tests.log')).write_text(text)
            test['passed'] = result.returncode == 0 and f"test result: ok. {test['expected_count']} passed;" in text
        report['passed'] = all(test.get('passed') for test in report['tests'])
    except Exception as error:
        report['error'] = type(error).__name__ + ': ' + str(error)[:1024]
    finally:
        removed = subprocess.run(['docker', 'rm', '-f', name], env=environment,
                                 capture_output=True, text=True, timeout=20)
        report['cleanup_passed'] = removed.returncode == 0
        report['passed'] &= report['cleanup_passed']
        (out / 'proof.json').write_text(json.dumps(report, indent=2) + '\n')
    return 0 if report['passed'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
