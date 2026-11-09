#!/usr/bin/env python3
"""Run coordination integration tests against a disposable, digest-pinned Redis."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import time
import tempfile
import uuid

ROOT = Path(__file__).resolve().parents[4]
sys.path.insert(0,str(ROOT/'scripts/rust-integration'))
from fixture_ports import write_port_overlay
IMAGE = 'ghcr.io/dispatcharr/dispatcharr@sha256:34f9011514edd31ef0ff111b5f256d2e37614fcb08a6ee1ed9eccec9251a856f'


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--docker', default='docker')
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--http', action='store_true', help='Also execute real-Redis HTTP integration tests')
    parser.add_argument('--clients', action='store_true', help='Also execute live client/user-limit integration tests')
    parser.add_argument('--upstream', action='store_true', help='Also verify shared HTTP codecs and real-Redis upstream cancellation/retention')
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    if any(args.output.iterdir()):
        parser.error('Output must be empty')
    name = 'dispatcharr-coordination-' + uuid.uuid4().hex[:12]
    report = {'scope': 'real-Redis-coordination-integration', 'passed': False,
              'image': IMAGE, 'container': name, 'application_performance_verified': False}
    report['source_commit'] = subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=ROOT, text=True).strip()
    inputs=[path for path in (ROOT/'rust').rglob('*') if path.is_file() and not set(path.relative_to(ROOT/'rust').parts)&{'target','.git','__pycache__'}]
    inputs.append(Path(__file__).resolve())
    # The input-store age oracle executes the unchanged original Lua source.
    inputs.append(ROOT/'apps/proxy/live_proxy/input/buffer.py')
    inputs.append(ROOT/'scripts/rust-integration/fixture_ports.py')
    captured={str(path.relative_to(ROOT)):path.read_bytes() for path in sorted(inputs)}
    temporary=tempfile.TemporaryDirectory(prefix='dispatcharr-coordination-source-')
    frozen=Path(temporary.name)
    for relative,content in captured.items():
        path=frozen/relative;path.parent.mkdir(parents=True,exist_ok=True);path.write_bytes(content)
    report['source_sha256']={name:hashlib.sha256(content).hexdigest() for name,content in captured.items()}
    report['source_scope']='immutable captured Rust workspace and integration runner'
    status = 1

    def docker(*command):
        return subprocess.check_output([args.docker, *command], text=True, timeout=90).strip()

    try:
        ports=write_port_overlay(args.output/'fixture-ports.yml',{'redis':[6379]})
        report['fixture_ports']=ports
        binding='127.0.0.1:'+str(ports['services']['redis'][0]['host'])+':6379'
        docker('run', '-d', '--rm', '--name', name, '-p', binding, '--entrypoint',
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
        environment = dict(os.environ, CARGO_TARGET_DIR=str(ROOT/"rust/target"), REDIS_TEST_URL='redis://' + address)
        command = ['cargo', 'test', '--manifest-path', str(frozen / 'rust/Cargo.toml'), '-p',
                   'dispatcharr-coordination', '--locked', '--offline', '--test', 'redis', '--test', 'acquire_cancel', '--', '--ignored']
        with (args.output / 'tests.log').open('w') as log:
            result = subprocess.run(command, env=environment, text=True, stdout=log,
                                    stderr=subprocess.STDOUT, timeout=180)
        report['command'] = command
        report['exit_code'] = result.returncode
        report['passed'] = result.returncode == 0
        status = result.returncode
        if args.http and status == 0:
            http_command = ['cargo', 'test', '--manifest-path', str(frozen / 'rust/Cargo.toml'), '-p',
                            'dispatcharr-stream-http', '--locked', '--offline', '--test', 'http', '--', '--ignored']
            with (args.output / 'http-tests.log').open('w') as log:
                http_result = subprocess.run(http_command, env=environment, text=True, stdout=log,
                                             stderr=subprocess.STDOUT, timeout=180)
            report['http_command'] = http_command
            report['http_exit_code'] = http_result.returncode
            report['passed'] = http_result.returncode == 0
            status = http_result.returncode
        if args.clients and status == 0:
            client_command = ['cargo', 'test', '--manifest-path', str(frozen / 'rust/Cargo.toml'), '-p',
                              'dispatcharr-clients', '--locked', '--offline', '--test', 'redis', '--test', 'vod', '--test', 'admission', '--test', 'era', '--', '--ignored']
            with (args.output / 'client-tests.log').open('w') as log:
                client_result = subprocess.run(client_command, env=environment, text=True, stdout=log,
                                               stderr=subprocess.STDOUT, timeout=180)
            report['client_command'] = client_command
            report['client_exit_code'] = client_result.returncode
            report['passed'] = client_result.returncode == 0
            status = client_result.returncode
        if args.upstream and status == 0:
            invocations=[('upstream-codecs',['-p','dispatcharr-upstream-http','--lib']),
                         ('upstream-redis',['-p','dispatcharr-stream-http','--test','upstream_native','--','--ignored','--test-threads=1'])]
            report['upstream_checks']=[]
            for label,selection in invocations:
                invocation=['cargo','test','--manifest-path',str(frozen/'rust/Cargo.toml'),'--locked','--offline',*selection]
                with (args.output/(label+'.log')).open('w') as log:
                    check=subprocess.run(invocation,env=environment,text=True,stdout=log,stderr=subprocess.STDOUT,timeout=180)
                report['upstream_checks'].append({'scope':label,'exit_code':check.returncode})
                report['passed']=check.returncode==0;status=check.returncode
                if status:break
        if args.http and status == 0:
            input_command = ['cargo', 'test', '--manifest-path', str(frozen / 'rust/Cargo.toml'), '-p',
                             'dispatcharr-input-store', '--locked', '--offline', '--test', 'redis', '--', '--ignored', '--test-threads=1']
            with (args.output / 'input-tests.log').open('w') as log:
                input_result = subprocess.run(input_command, env=environment, text=True, stdout=log,
                                              stderr=subprocess.STDOUT, timeout=180)
            report['input_command'] = input_command
            report['input_exit_code'] = input_result.returncode
            report['passed'] = input_result.returncode == 0
            status = input_result.returncode
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
        temporary.cleanup()
        (args.output / 'evidence.json').write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps(report, indent=2))
    return status


if __name__ == '__main__':
    raise SystemExit(main())
