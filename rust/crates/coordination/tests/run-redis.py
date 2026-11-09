#!/usr/bin/env python3
"""Run coordination integration tests against a disposable, digest-pinned Redis."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import time
import uuid

ROOT = Path(__file__).resolve().parents[4]
IMAGE = 'ghcr.io/dispatcharr/dispatcharr@sha256:34f9011514edd31ef0ff111b5f256d2e37614fcb08a6ee1ed9eccec9251a856f'


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--docker', default='docker')
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--http', action='store_true', help='Also execute real-Redis HTTP integration tests')
    parser.add_argument('--clients', action='store_true', help='Also execute live client/user-limit integration tests')
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    if any(args.output.iterdir()):
        parser.error('Output must be empty')
    name = 'dispatcharr-coordination-' + uuid.uuid4().hex[:12]
    report = {'scope': 'real-Redis-coordination-integration', 'passed': False,
              'image': IMAGE, 'container': name, 'application_performance_verified': False}
    report['source_commit'] = subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=ROOT, text=True).strip()
    inputs = [ROOT / 'rust/Cargo.toml', ROOT / 'rust/Cargo.lock']
    for crate in ('coordination', 'stream-http', 'stream-core', 'stream-runtime', 'contracts', 'clients'):
        inputs.extend(path for path in (ROOT / 'rust/crates' / crate).rglob('*')
                      if path.is_file() and path.suffix in ('.rs', '.toml', '.py'))
    report['source_sha256'] = {str(path.relative_to(ROOT)): hashlib.sha256(path.read_bytes()).hexdigest()
                               for path in sorted(inputs)}
    status = 1

    def docker(*command):
        return subprocess.check_output([args.docker, *command], text=True, timeout=90).strip()

    try:
        docker('run', '-d', '--rm', '--name', name, '-p', '127.0.0.1::6379', '--entrypoint',
               'redis-server', IMAGE, '--save', '', '--appendonly', 'no', '--bind', '0.0.0.0',
               '--protected-mode', 'no')
        address = docker('port', name, '6379').splitlines()[0]
        deadline = time.monotonic() + 20
        while True:
            try:
                if docker('exec', name, 'redis-cli', 'ping') == 'PONG':
                    break
            except subprocess.CalledProcessError:
                pass
            if time.monotonic() >= deadline:
                raise RuntimeError('Redis readiness timeout')
            time.sleep(.1)
        environment = dict(os.environ, REDIS_TEST_URL='redis://' + address)
        command = ['cargo', 'test', '--manifest-path', str(ROOT / 'rust/Cargo.toml'), '-p',
                   'dispatcharr-coordination', '--locked', '--offline', '--test', 'redis', '--', '--ignored']
        with (args.output / 'tests.log').open('w') as log:
            result = subprocess.run(command, env=environment, text=True, stdout=log,
                                    stderr=subprocess.STDOUT, timeout=180)
        report['command'] = command
        report['exit_code'] = result.returncode
        report['passed'] = result.returncode == 0
        status = result.returncode
        if args.http and status == 0:
            http_command = ['cargo', 'test', '--manifest-path', str(ROOT / 'rust/Cargo.toml'), '-p',
                            'dispatcharr-stream-http', '--locked', '--offline', '--test', 'http', '--', '--ignored']
            with (args.output / 'http-tests.log').open('w') as log:
                http_result = subprocess.run(http_command, env=environment, text=True, stdout=log,
                                             stderr=subprocess.STDOUT, timeout=180)
            report['http_command'] = http_command
            report['http_exit_code'] = http_result.returncode
            report['passed'] = http_result.returncode == 0
            status = http_result.returncode
        if args.clients and status == 0:
            client_command = ['cargo', 'test', '--manifest-path', str(ROOT / 'rust/Cargo.toml'), '-p',
                              'dispatcharr-clients', '--locked', '--offline', '--test', 'redis', '--', '--ignored']
            with (args.output / 'client-tests.log').open('w') as log:
                client_result = subprocess.run(client_command, env=environment, text=True, stdout=log,
                                               stderr=subprocess.STDOUT, timeout=180)
            report['client_command'] = client_command
            report['client_exit_code'] = client_result.returncode
            report['passed'] = client_result.returncode == 0
            status = client_result.returncode
        for path, fingerprint in report['source_sha256'].items():
            if hashlib.sha256((ROOT / path).read_bytes()).hexdigest() != fingerprint:
                raise RuntimeError('Source changed during test execution')
    except (OSError, subprocess.SubprocessError, RuntimeError) as error:
        report['error'] = type(error).__name__
        report['passed'] = False
        status = 1
    except KeyboardInterrupt:
        report['error'] = 'Interrupted'
        report['passed'] = False
        status = 130
    finally:
        try:
            docker('rm', '-f', name)
            report['cleanup'] = 'owned test container removed'
        except (OSError, subprocess.SubprocessError):
            report['cleanup_failed'] = True
            report['passed'] = False
            status = 1
        (args.output / 'evidence.json').write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps(report, indent=2))
    return status


if __name__ == '__main__':
    raise SystemExit(main())
