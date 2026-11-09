#!/usr/bin/env python3
"""Observe pinned-source cold TS wire behavior before changing Rust contracts.

Full original Dispatcharr author/contributor credit is retained. No media decoder
or performance gain is claimed for error/no-data responses.
"""
import argparse
import datetime
import hashlib
import http.client as wire_http
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import tempfile
import time
import urllib.parse
import urllib.request
import uuid

from shared_ts import ROOT, BASELINE, IMAGE, fixture_source, http, wait


def capture(url, maximum, headers=None, max_bytes=188*32, method="GET"):
    parsed = urllib.parse.urlsplit(url)
    connection = wire_http.HTTPConnection(parsed.hostname, parsed.port, timeout=maximum)
    began = time.monotonic()
    result = {'status': None, 'header_seconds': None, 'first_body_seconds': None, 'bytes': 0}
    data = bytearray()
    try:
        connection.request(method, urllib.parse.urlunsplit(('', '', parsed.path, parsed.query, '')),headers=headers or {})
        response = connection.getresponse()
        result['header_seconds'] = time.monotonic() - began
        result['status'] = response.status
        result['headers'] = {name: response.getheader(name) for name in ('Content-Type', 'Content-Length', 'Transfer-Encoding', 'Cache-Control', 'Connection', 'X-Accel-Buffering', 'WWW-Authenticate', 'Allow') if response.getheader(name) is not None}
        while len(data) < max_bytes and time.monotonic() - began < maximum:
            remaining = maximum - (time.monotonic() - began)
            active_socket=connection.sock
            if active_socket is None:
                active_socket=getattr(getattr(getattr(response,'fp',None),'raw',None),'_sock',None)
            if active_socket is not None:
                active_socket.settimeout(max(.1, remaining))
            block = response.read1(min(4096, max_bytes - len(data)))
            if not block:
                result['eof'] = True
                break
            if result['first_body_seconds'] is None:
                result['first_body_seconds'] = time.monotonic() - began
            data.extend(block)
    except Exception as error:
        result['read_error'] = type(error).__name__
    finally:
        connection.close()
        result['elapsed_seconds'] = time.monotonic() - began
    result['bytes'] = len(data)
    result['body_sha256'] = hashlib.sha256(data).hexdigest()
    result['body_hex'] = data.hex()
    result['body_utf8_replacement'] = data.decode('utf-8', errors='replace')
    result['packet_aligned'] = bool(data) and len(data) % 188 == 0
    result['packet_sync'] = bool(data) and all(data[index] == 0x47 for index in range(0, len(data), 188))
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--docker', default='docker')
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--backends', nargs='+', choices=['baseline', 'candidate'], default=['baseline', 'candidate'])
    parser.add_argument('--modes', nargs='+', choices=['fail', 'stall', 'empty'], default=['fail', 'stall', 'empty'])
    parser.add_argument('--candidate-image')
    parser.add_argument('--candidate-build-manifest', type=Path)
    parser.add_argument('--max-observation', type=float, default=75)
    args = parser.parse_args()
    if not 10 <= args.max_observation <= 150:
        parser.error('observation must be10..150seconds')
    if 'candidate' in args.backends and (not args.candidate_image or not args.candidate_build_manifest):
        parser.error('candidate requires unique image and immutable build manifest')
    if args.candidate_image and not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9._:/@-]{0,255}', args.candidate_image):
        parser.error('invalid candidate image')
    args.docker = shutil.which(args.docker)
    if not args.docker:
        parser.error('Docker executable not found')
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=True)
    if any(output.iterdir()):
        parser.error('output must be empty')
    report = {'scope': 'cold native TS wire characterization', 'passed': False, 'milestone_complete': False,
              'application_performance_verified': False, 'baseline_commit': BASELINE, 'baseline_image': IMAGE,
              'observations': [], 'commands': []}
    manifest = None
    if args.candidate_build_manifest:
        raw = args.candidate_build_manifest.read_bytes()
        manifest = json.loads(raw)
        hashes = manifest.get('source_sha256')
        if manifest.get('passed') is not True or not isinstance(hashes, dict) or hashlib.sha256(json.dumps(hashes, sort_keys=True, separators=(',', ':')).encode()).hexdigest() != manifest.get('source_tree_sha256'):
            parser.error('invalid immutable build manifest')
        report['candidate_build_manifest_sha256'] = hashlib.sha256(raw).hexdigest()
    project = 'dispatcharr-cold-ts-' + uuid.uuid4().hex[:12]
    report['project'] = project
    overlay = output / 'cold-overlay.json'
    overlay.write_text(json.dumps({'services': {'origin': {'entrypoint': ['/dispatcharrpy/bin/python', '/integration/cold_origin.py'], 'volumes': [str(ROOT / 'scripts/rust-integration') + ':/integration:ro']},
                                               'candidate': {'image': args.candidate_image or 'dispatcharr-rust-candidate:milestone2'}}}))
    tools = [Path(__file__).resolve(), Path(__file__).with_name('cold_origin.py'), Path(__file__).with_name('shared_ts.py'),
             ROOT / 'docker/compose.rust-playback.yml', ROOT / 'docker/compose.rust-candidate.yml',
             *[ROOT / 'scripts/rust-playback-baseline' / name for name in ('seed.py', 'start.sh', 'supervisor.py', 'uwsgi.ini')]]
    report['tool_sha256'] = {str(path.relative_to(ROOT)): hashlib.sha256(path.read_bytes()).hexdigest() for path in tools}
    with fixture_source(report, output, 'cold-ts.json') as temporary:
        source = Path(temporary) / 'source'
        source.mkdir()
        archive = Path(temporary) / 'source.tar'
        with archive.open('wb') as stream:
            subprocess.run(['git', 'archive', BASELINE], cwd=ROOT, stdout=stream, check=True, timeout=30)
        report['baseline_archive_sha256'] = hashlib.sha256(archive.read_bytes()).hexdigest()
        subprocess.run(['tar', '-xf', str(archive), '-C', str(source), '--no-same-owner'], check=True, timeout=30)
        (source / 'media').mkdir(exist_ok=True)
        environment = dict(os.environ, BASELINE_SOURCE_DIR=str(source), BASELINE_EVIDENCE_DIR=str(output),
                           BENCHMARK_SHUTDOWN_DELAY='2', BENCHMARK_RETENTION_SECONDS='60', BENCHMARK_STARTUP_BEHIND='5')
        compose = [args.docker, 'compose', '-p', project, '-f', str(ROOT / 'docker/compose.rust-playback.yml'),
                   '-f', str(ROOT / 'docker/compose.rust-candidate.yml'), '-f', str(overlay)]
        def command(values, timeout=120):
            report['commands'].append(values)
            return subprocess.run(values, cwd=ROOT, env=environment, text=True, capture_output=True, check=True, timeout=timeout).stdout
        def endpoint(service, port):
            return 'http://' + command(compose + ['port', service, str(port)]).strip()
        try:
            command(compose + ['up', '-d', 'baseline', 'origin'], 180)
            origin, baseline = endpoint('origin', 8080), endpoint('baseline', 5656)
            wait(lambda: (output / 'seed.json').exists() and http(origin + '/health')[0] == 200 and http(baseline + '/proxy/ts/stream/missing')[0] == 404, 300)
            seed = json.loads((output / 'seed.json').read_text())
            for backend in args.backends:
                command(compose + ['stop', '--timeout', '15', 'candidate'])
                command(compose + ['exec', '-T', 'baseline', '/dispatcharrpy/bin/python', '/verification/supervisor.py', 'control', 'stop'])
                command(compose + ['exec', '-T', 'baseline', 'redis-cli', 'FLUSHDB'])
                if backend == 'baseline':
                    command(compose + ['exec', '-T', 'baseline', '/dispatcharrpy/bin/python', '/verification/supervisor.py', 'control', 'start'])
                    target = baseline
                    wait(lambda: http(target + '/proxy/ts/stream/missing')[0] == 404)
                else:
                    command(compose + ['up', '-d', '--no-build', 'candidate'])
                    target = endpoint('candidate', 9192)
                    wait(lambda: http(target + '/health/ready')[0] == 200)
                    container = command(compose + ['ps', '-q', 'candidate']).strip()
                    inspection = json.loads(command([args.docker, 'inspect', container]))[0]
                    if inspection['Image'] != manifest['image_id'] or inspection['Config']['Labels'].get('org.opencontainers.image.source-tree-sha256') != manifest['source_tree_sha256']:
                        raise RuntimeError('candidate immutable source binding mismatch')
                    report['candidate_image_id'] = inspection['Image']
                for index, mode in enumerate(args.modes):
                    selected = seed['channels'][index]
                    for path in (selected['origin_path'], selected['backup_origin_path']):
                        request = urllib.request.Request(origin + '/control', data=json.dumps({'path': path, 'mode': mode}).encode(), headers={'Content-Type': 'application/json'})
                        with urllib.request.urlopen(request, timeout=3) as response:
                            assert json.load(response)['applied'] is True
                    before = http(origin + '/stats')[1]
                    observation = {'backend': backend, 'mode': mode, 'maximum_seconds': args.max_observation,
                                   'initial_origin': before, **capture(target + selected['path'], args.max_observation)}
                    observation['final_origin'] = http(origin + '/stats')[1]
                    report['observations'].append(observation)
                    (output / (backend + '-' + mode + '.bin')).write_bytes(bytes.fromhex(observation['body_hex']))
                    wait(lambda: http(origin + '/stats')[1]['active'] == 0, 40, 'no-media upstream cleanup')
            for mode in args.modes:
                rows = {row['backend']: row for row in report['observations'] if row['mode'] == mode}
                if len(rows) == 2:
                    old, new = rows['baseline'], rows['candidate']
                    report.setdefault('comparisons', []).append({'mode': mode, 'status_equal': old['status'] == new['status'],
                        'content_type_equal': old.get('headers', {}).get('Content-Type','').lower() == new.get('headers', {}).get('Content-Type','').lower(),
                        'body_equal': old['body_sha256'] == new['body_sha256'],
                        'header_seconds_delta_candidate_minus_baseline': None if old['header_seconds'] is None or new['header_seconds'] is None else new['header_seconds'] - old['header_seconds'],
                        'first_body_seconds_delta_candidate_minus_baseline': None if old['first_body_seconds'] is None or new['first_body_seconds'] is None else new['first_body_seconds'] - old['first_body_seconds']})
            # A successful characterization can prove a mismatch; it is not a parity gate.
            report['passed'] = len(report['observations']) == len(args.backends) * len(args.modes)
            report['wire_parity_passed'] = bool(report.get('comparisons')) and all(item['status_equal'] and item['content_type_equal'] and item['body_equal'] for item in report.get('comparisons', []))
        except Exception as error:
            report['failure'] = type(error).__name__
            report['failure_message'] = str(error)
        finally:
            try:
                (output / 'compose.log').write_text(command(compose + ['logs', '--no-color'], 30))
            except Exception:
                pass
            try:
                command(compose + ['down', '--volumes', '--remove-orphans', '--timeout', '5'], 60)
                report['cleanup'] = 'isolated Compose project removed'
            except Exception as error:
                report['cleanup_error'] = type(error).__name__
                report['passed'] = False
            report['changed_tool_sources'] = [name for name, old in report['tool_sha256'].items() if hashlib.sha256((ROOT / name).read_bytes()).hexdigest() != old]
            if report['changed_tool_sources']:
                report['passed'] = False
            report['timestamp_utc'] = datetime.datetime.now(datetime.timezone.utc).isoformat()
            (output / 'cold-ts.json').write_text(json.dumps(report, indent=2) + '\n')
    return 0 if report['passed'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
