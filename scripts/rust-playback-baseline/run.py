#!/usr/bin/env python3
"""Short actual Python playback characterization; does not declare milestone completion."""
import argparse
import datetime
import hashlib
import json
import os
import subprocess
import sys
import tempfile
import time
import urllib.request
import urllib.error
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
BASELINE = '1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430'
IMAGE = 'ghcr.io/dispatcharr/dispatcharr@sha256:34f9011514edd31ef0ff111b5f256d2e37614fcb08a6ee1ed9eccec9251a856f'


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--docker', default='docker')
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--viewers', nargs='+', type=int, default=[1, 10, 100])
    parser.add_argument('--trials', type=int, default=3)
    args = parser.parse_args()
    if any(n < 1 or n > 100 for n in args.viewers) or not 1 <= args.trials <= 3:
        parser.error('viewers must be 1..100; trials must be 1..3')
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=True)
    if any(output.iterdir()):
        parser.error('output must be empty')
    project = 'dispatcharr-playback-' + uuid.uuid4().hex[:12]
    evidence = {'source_commit': BASELINE, 'image': IMAGE, 'project': project,
                'scope': 'short actual Python uWSGI TS playback characterization',
                'milestone_completed': False, 'performance_gain_verified': False,
                'exit_code': 1, 'commands': [], 'measurements': []}
    with tempfile.TemporaryDirectory(prefix='dispatcharr-playback-') as temporary:
        source = Path(temporary) / 'source'
        source.mkdir()
        archive = Path(temporary) / 'source.tar'
        with archive.open('wb') as stream:
            subprocess.run(['git', 'archive', BASELINE], cwd=ROOT, stdout=stream, timeout=30, check=True)
        evidence['git_archive_sha256'] = hashlib.sha256(archive.read_bytes()).hexdigest()
        subprocess.run(['tar', '-xf', str(archive), '-C', str(source), '--no-same-owner'], timeout=30, check=True)
        (source / 'media').mkdir(exist_ok=True)
        environment = dict(os.environ, BASELINE_SOURCE_DIR=str(source), BASELINE_EVIDENCE_DIR=str(output))
        compose_file = ROOT / 'docker/compose.rust-playback.yml'
        compose = [args.docker, 'compose', '-p', project, '-f', str(compose_file)]
        tool_paths = [*Path(__file__).parent.glob('*.py'), *Path(__file__).parent.glob('*.sh'),
                      Path(__file__).with_name('uwsgi.ini'), compose_file,
                      ROOT / 'scripts/rust-perf/perf.py', ROOT / 'scripts/rust-baseline/dependencies.py']
        evidence['tools_sha256'] = {str(p.relative_to(ROOT)): hashlib.sha256(p.read_bytes()).hexdigest()
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
            execute(compose + ['up', '-d'], timeout=180)
            base, origin = endpoint('baseline', 5656), endpoint('origin', 8080)
            ready_deadline = time.monotonic() + 180
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
                        raise TimeoutError('baseline startup readiness exceeded 180 seconds')
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
            print('Playback decoded. Starting resource measurements; avoid other CPU workloads.', flush=True)
            urls = output / 'urls.txt'
            urls.write_text(base + seed['path'] + '\n')
            fixture_id = fixture['fixture_sha256'] + ':proxy-ts-2mbps-one-worker'
            perf_environment = dict(environment)
            perf_environment['PATH'] = str(Path(args.docker).resolve().parent) + os.pathsep + environment['PATH']
            # The immutable perf harness invokes `docker` itself through PATH.
            for viewers in args.viewers:
                for trial in range(1, args.trials + 1):
                    name = f'viewers-{viewers}-trial-{trial}.json'
                    before = get_json(origin + '/stats')
                    command = [sys.executable, str(ROOT / 'scripts/rust-perf/perf.py'), 'run',
                               '--compose-file', str(compose_file), '--project', project,
                               '--services', 'baseline', 'origin', '--urls-file', str(urls),
                               '--scenario', 'same-channel', '--viewers', str(viewers),
                               '--warmup', '2', '--duration', '10', '--timeout', '10',
                               '--sample-interval', '1', '--fixture-id', fixture_id,
                               '--revision', BASELINE + '@' + IMAGE, '--output', str(output / name)]
                    evidence['commands'].append(command)
                    result = subprocess.run(command, env=perf_environment, text=True,
                                            capture_output=True, timeout=90)
                    (output / (name + '.log')).write_text(result.stdout + result.stderr)
                    after = get_json(origin + '/stats')
                    evidence['measurements'].append({'file': name, 'exit_code': result.returncode,
                        'origin_before': before, 'origin_after': after})
                    print(f'{name}: exit {result.returncode}', flush=True)
                    if result.returncode:
                        raise RuntimeError('performance harness validity checks failed')
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
