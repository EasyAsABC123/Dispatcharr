#!/usr/bin/env python3
"""Verify native task leases against original helpers on disposable Redis."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import time
import uuid

ROOT = Path(__file__).resolve().parents[2]
IMAGE = 'ghcr.io/dispatcharr/dispatcharr@sha256:34f9011514edd31ef0ff111b5f256d2e37614fcb08a6ee1ed9eccec9251a856f'

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--docker', default='docker')
    args = parser.parse_args()
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    name = 'dispatcharr-task-locks-'+uuid.uuid4().hex[:12]
    prefix = 'rust_task_fixture_'+uuid.uuid4().hex
    report = {'scope': 'original-task-helper-and-native-fenced-job-leases',
              'passed': False, 'image': IMAGE, 'fixture': name,
              'public_jobs_migrated': False, 'performance_verified': False}
    peer = None
    def docker(*parts, **kwargs):
        return subprocess.run([args.docker, *parts], check=True,
                              capture_output=True, text=True, **kwargs).stdout.strip()
    try:
        docker('run', '--rm', '-d', '--name', name,
               '-p', '127.0.0.1::6379', '-v', str(ROOT)+':/source:ro',
               '--entrypoint', 'redis-server', IMAGE,
               '--save', '', '--appendonly', 'no', '--bind', '0.0.0.0',
               '--protected-mode', 'no')
        address = docker('port', name, '6379/tcp')
        for _ in range(100):
            try:
                if docker('exec', name, 'redis-cli', 'PING') == 'PONG':
                    break
            except subprocess.CalledProcessError:
                pass
            time.sleep(.05)
        else:
            raise RuntimeError('disposable Redis did not become ready')
        captured = {str(p.relative_to(ROOT)): p.read_bytes()
                    for p in sorted((ROOT/'rust').rglob('*'))
                    if p.is_file() and not set(p.relative_to(ROOT/'rust').parts)
                    & {'target', '.git', '__pycache__'}}
        report['rust_source_sha256'] = {name: hashlib.sha256(data).hexdigest()
                                        for name, data in captured.items()}
        source = (ROOT/'scripts/rust-ingest-parity/task_lock_source.py').read_bytes()
        (output/'source-peer.py').write_bytes(source)
        report['source_peer_sha256'] = hashlib.sha256(source).hexdigest()
        report['original_utils_sha256'] = hashlib.sha256((ROOT/'core/utils.py').read_bytes()).hexdigest()
        peer = subprocess.Popen([args.docker, 'exec', '-i', '-e',
            'TASK_LOCK_FIXTURE_PREFIX='+prefix, name, '/dispatcharrpy/bin/python', '-'],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        peer.stdin.write(source.decode())
        peer.stdin.close()
        peer.stdin = None
        with tempfile.TemporaryDirectory(prefix='dispatcharr-task-locks-') as temp:
            frozen = Path(temp)
            for relative, data in captured.items():
                p = frozen/relative
                p.parent.mkdir(parents=True, exist_ok=True)
                p.write_bytes(data)
            env = dict(os.environ, TASK_LOCK_DISPOSABLE_FIXTURE='local-test-only',
                       TASK_LOCK_FIXTURE_PREFIX=prefix, REDIS_TEST_URL='redis://'+address+'/0',
                       CARGO_TARGET_DIR=str(ROOT/'rust/target/task-lock-verification'))
            result = subprocess.run(['cargo', 'test', '--manifest-path', str(frozen/'rust/Cargo.toml'),
                '-p', 'dispatcharr-coordination', '--test', 'task_locks', '--offline', '--locked',
                '--', '--ignored', '--nocapture'], env=env, capture_output=True, text=True, timeout=180)
            (output/'native.log').write_text(result.stdout+result.stderr)
            result.check_returncode()
        stdout, stderr = peer.communicate(timeout=10)
        (output/'source.log').write_text(stdout+stderr)
        if peer.returncode != 0:
            raise RuntimeError('original helper peer failed')
        report['original_helper'] = json.loads(stdout.splitlines()[-1])
        report['passed'] = report['original_helper']['passed']
    except Exception as error:
        report['error'] = str(error)
    finally:
        if peer is not None and peer.poll() is None:
            peer.terminate()
            stdout, stderr = peer.communicate(timeout=10)
            (output/'source-failed.log').write_text(stdout+stderr)
        try:
            docker('rm', '-f', name)
            report['cleanup_passed'] = True
        except subprocess.CalledProcessError:
            report['cleanup_passed'] = False
        (output/'task-locks.json').write_text(json.dumps(report, indent=2)+'\n')
    print(json.dumps({key: report.get(key) for key in ['passed', 'cleanup_passed', 'error']}))
    return 0 if report['passed'] and report['cleanup_passed'] else 1

if __name__ == '__main__':
    raise SystemExit(main())
