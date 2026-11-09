#!/usr/bin/env python3
"""Functional full-retention accounting; never a performance comparison.

Original Dispatcharr authors retain full application credit. This runs a fresh
isolated candidate, checks ten real streams, and samples retained media bytes
and PostgreSQL memory. Observer work is intentionally outside any resource gate.
"""
import argparse
import concurrent.futures
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import time
import urllib.request
import uuid

from fixture_ports import write_port_overlay
from shared_ts import BASELINE, IMAGE, ROOT, Reader, fixture_source, http, wait


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--docker', default='docker')
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--candidate-image', required=True)
    parser.add_argument('--candidate-build-manifest', type=Path, required=True)
    parser.add_argument('--expected-local-limit', type=int, default=16_777_216,
                        help='Expected per-channel local cache, separate from full Redis history')
    parser.add_argument('--late-history-viewers', type=int, choices=[0, 100], default=0)
    args = parser.parse_args()
    if not 188 <= args.expected_local_limit <= 16_777_216:
        parser.error('expected local cache limit must be188..16777216bytes')
    docker = shutil.which(args.docker)
    if not docker:
        parser.error('Docker not found')
    raw = args.candidate_build_manifest.read_bytes()
    manifest = json.loads(raw)
    hashes = manifest.get('source_sha256', {})
    if (not manifest.get('passed') or not hashes or hashlib.sha256(json.dumps(
            hashes, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
            != manifest.get('source_tree_sha256')):
        parser.error('invalid passing source-bound manifest')
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=True)
    if any(output.iterdir()):
        parser.error('output must be empty')
    report = {
        'scope': 'candidate-only functional retained-media and database accounting',
        'passed': False, 'application_performance_verified': False,
        'milestone_complete': False, 'baseline_commit': BASELINE,
        'baseline_image': IMAGE, 'candidate_image_id': manifest['image_id'],
        'candidate_source_tree_sha256': manifest['source_tree_sha256'],
        'candidate_build_manifest_sha256': hashlib.sha256(raw).hexdigest(),
        'commands': [], 'samples': [],
        'observer_scope': 'bounded sequential admin/Redis/PG snapshots, not isolated CPU or memory measurements',
    }
    project = 'dispatcharr-retention-' + uuid.uuid4().hex[:12]
    report['project'] = project
    overlay = output / 'candidate-image.json'
    overlay.write_text(json.dumps({'services': {'candidate': {'image': args.candidate_image}}}))
    ports = output / 'ports.yml'
    report['ports'] = write_port_overlay(ports, {'baseline': [5656], 'origin': [8080], 'candidate': [9192]})
    tools = [Path(__file__).resolve(), Path(__file__).with_name('collect_fixture_memory.py'),
             Path(__file__).with_name('shared_ts.py'), Path(__file__).with_name('fixture_ports.py'),
             Path(__file__).with_name('seed_accounts.py'), ROOT / 'docker/compose.rust-playback.yml',
             ROOT / 'docker/compose.rust-candidate.yml',
             *[ROOT / 'scripts/rust-playback-baseline' / name for name in
               ('origin.py', 'range_http.py', 'seed.py', 'start.sh', 'supervisor.py', 'uwsgi.ini')]]
    report['tool_sha256'] = {str(p.relative_to(ROOT)): hashlib.sha256(p.read_bytes()).hexdigest() for p in tools}
    readers = []
    with fixture_source(report, output, 'retention.json') as temporary:
        source = Path(temporary) / 'source'
        source.mkdir()
        archive = Path(temporary) / 'source.tar'
        with archive.open('wb') as stream:
            subprocess.run(['git', 'archive', BASELINE], cwd=ROOT, stdout=stream, check=True, timeout=30)
        report['baseline_archive_sha256'] = hashlib.sha256(archive.read_bytes()).hexdigest()
        subprocess.run(['tar', '-xf', str(archive), '-C', str(source), '--no-same-owner'], check=True, timeout=30)
        (source / 'media').mkdir(exist_ok=True)
        environment = dict(os.environ, BASELINE_SOURCE_DIR=str(source), BASELINE_EVIDENCE_DIR=str(output),
                           BENCHMARK_SHUTDOWN_DELAY='5', BENCHMARK_RETENTION_SECONDS='60',
                           BENCHMARK_STARTUP_BEHIND='5', FIXTURE_SECONDS='300')
        compose = [docker, 'compose', '-p', project, '-f', str(ROOT / 'docker/compose.rust-playback.yml'),
                   '-f', str(ROOT / 'docker/compose.rust-candidate.yml'), '-f', str(overlay), '-f', str(ports)]

        def command(values, timeout=120):
            report['commands'].append(values)
            return subprocess.run(values, cwd=ROOT, env=environment, text=True, capture_output=True,
                                  check=True, timeout=timeout).stdout

        def endpoint(service, port):
            return 'http://' + command(compose + ['port', service, str(port)]).strip()

        def origin_stats():
            return http(origin + '/stats')[1]

        def collect_pg(name):
            file = output / (name + '.postgres.json')
            command([os.sys.executable, str(Path(__file__).with_name('collect_fixture_memory.py')),
                     '--docker', docker, '--container', baseline_container, '--output', str(file)], 20)
            return json.loads(file.read_text())

        try:
            command(compose + ['up', '-d', 'baseline', 'origin'], 180)
            baseline, origin = endpoint('baseline', 5656), endpoint('origin', 8080)
            wait(lambda: (output / 'seed.json').exists() and http(origin + '/health')[0] == 200
                 and http(baseline + '/proxy/ts/stream/missing')[0] == 404, 300, 'fixture ready')
            seed = json.loads((output / 'seed.json').read_text())
            accounts = json.loads(command(compose + ['exec', '-T', 'baseline', '/dispatcharrpy/bin/python', '/integration/seed_accounts.py']))
            admin = accounts['admin']['access']
            baseline_container = command(compose + ['ps', '-q', 'baseline']).strip()
            command(compose + ['exec', '-T', 'baseline', '/dispatcharrpy/bin/python', '/verification/supervisor.py', 'control', 'stop'])
            command(compose + ['exec', '-T', 'baseline', 'redis-cli', 'FLUSHDB'])
            command(compose + ['up', '-d', '--no-build', '--force-recreate', 'candidate'])
            target = endpoint('candidate', 9192)
            wait(lambda: http(target + '/health/ready')[0] == 200, 60, 'candidate ready')
            container = command(compose + ['ps', '-q', 'candidate']).strip()
            inspected = json.loads(command([docker, 'inspect', container]))[0]
            assert inspected['Image'] == manifest['image_id']
            assert inspected['Config']['Labels'].get('org.opencontainers.image.source-tree-sha256') == manifest['source_tree_sha256']
            assert http(target + '/internal/proxy/ts/v1/memory')[0] == 401
            report['idle_postgres'] = collect_pg('idle')
            selected = seed['channels'][:10]
            priming_start = time.monotonic()

            def prime(item):
                number, channel = item
                log = command(compose + ['exec', '-T', 'baseline', 'ffmpeg', '-hide_banner', '-loglevel', 'error',
                              '-xerror', '-progress', 'pipe:1', '-nostats', '-i',
                              'http://candidate:9192' + channel['path'], '-t', '3', '-f', 'null', '-'], 45)
                frames = max((int(line.split('=')[1]) for line in log.splitlines() if line.startswith('frame=')), default=0)
                (output / ('prime-' + str(number) + '.log')).write_text(log)
                assert frames >= 50
                return {'channel_id': channel['channel_uuid'], 'frames': frames,
                        'completed_seconds': time.monotonic() - priming_start}

            with concurrent.futures.ThreadPoolExecutor(max_workers=10) as pool:
                report['priming'] = list(pool.map(prime, enumerate(selected)))
            times = [row['completed_seconds'] for row in report['priming']]
            report['priming_spread_seconds'] = max(times) - min(times)
            assert report['priming_spread_seconds'] < 5
            for number, channel in enumerate(selected):
                readers.append(Reader(target + channel['path'], output / ('capture-' + str(number) + '.ts')))
            wait(lambda: all(row.snapshot()['media_bytes'] >= 750_000 for row in readers), 25, 'all readers live')
            start = time.monotonic()
            for mark in (5, 30, 60, 75):
                while time.monotonic() - start < mark:
                    time.sleep(min(.5, mark - (time.monotonic() - start)))
                status, memory = http(target + '/internal/proxy/ts/v1/memory', token=admin)
                assert status == 200 and memory['format_version'] == 1
                assert memory['sampled_channels'] == 10 and not memory['truncated']
                assert memory['configured_retention_seconds'] == 60
                assert memory['configured_local_limit_bytes_per_channel'] == args.expected_local_limit
                assert all(row['local_retained_payload_bytes'] <= args.expected_local_limit for row in memory['channels'])
                assert all(row['shared_input']['fence_matches_owner'] for row in memory['channels'])
                state = origin_stats()
                assert state['active'] == 10
                snapshots = [reader.snapshot() for reader in readers]
                assert all(not row['ended'] and not row['errors'] and not row['sync_errors'] for row in snapshots)
                pg = collect_pg('at-' + str(mark))
                report['samples'].append({'elapsed_seconds': time.monotonic() - start, 'retained': memory,
                                          'postgres': pg, 'origin': state, 'readers': snapshots})
                (output / 'retention.json').write_text(json.dumps(report, indent=2) + '\n')
                print('retention sample', mark, 'local', memory['local_retained_payload_bytes'],
                      'Redis', memory['redis_logical_payload_bytes'], flush=True)
            last = report['samples'][-1]['retained']
            assert all(row['local_retained_payload_bytes'] >= args.expected_local_limit // 2
                       and row['shared_input']['logical_payload_bytes'] >= 8 * 1024 * 1024 for row in last['channels'])
            if args.late_history_viewers:
                # Only this disposable database is changed. Existing viewers
                # continue at their original positions; new ones seek45s back.
                change = "import os,sys;sys.path.insert(0,'/app');os.environ.setdefault('DJANGO_SETTINGS_MODULE','dispatcharr.settings');import django;django.setup();from core.models import CoreSettings,PROXY_SETTINGS_KEY;s=CoreSettings.get_proxy_settings().copy();s['new_client_behind_seconds']=45;CoreSettings.objects.filter(key=PROXY_SETTINGS_KEY).update(value=s);print('fixture behind45')"
                command(compose + ['exec', '-T', 'baseline', '/dispatcharrpy/bin/python', '-c', change])
                collect = """import os,json,time,hashlib,redis
r=redis.Redis(host='localhost',password=os.environ['REDIS_PASSWORD']);now=time.time();out={}
for channel in json.loads(os.environ['RETENTION_CHANNELS']):
 p='live:channel:'+channel+':input:buffer:';rows=r.zrangebyscore(p+'chunk_timestamps',now-55,now-35,withscores=True)
 if len(rows)>32:raise RuntimeError('historical prefix bound exceeded')
 out[channel]=[]
 for member,stamp in rows:
  data=r.get(p+'chunk:'+member.decode())
  if data is not None:
   if len(data)>4194304:raise RuntimeError('chunk bound exceeded')
   out[channel].append({'sequence':int(member),'received_at':stamp,'prefix_sha256':hashlib.sha256(data[:65536]).hexdigest()})
print(json.dumps(out))"""
                prefixes = json.loads(command(compose + ['exec', '-T', '-e', 'RETENTION_CHANNELS=' +
                    json.dumps([row['channel_uuid'] for row in selected]), 'baseline', '/dispatcharrpy/bin/python', '-c', collect]))
                assert all(prefixes[row['channel_uuid']] for row in selected)
                before_late = origin_stats()

                def late(number):
                    channel = selected[number % 10]
                    file = output / ('late-' + str(number) + '.ts')
                    capture = file.open('wb') if number < 10 else None
                    try:
                        with urllib.request.urlopen(target + channel['path'], timeout=15) as response:
                            prefix = response.read(65536)
                            assert response.status == 200 and len(prefix) == 65536
                            digest = hashlib.sha256(prefix).hexdigest()
                            assert digest in {row['prefix_sha256'] for row in prefixes[channel['channel_uuid']]}
                            assert all(prefix[offset] == 0x47 for offset in range(0, len(prefix) - 187, 188))
                            if capture:
                                capture.write(prefix)
                            size = len(prefix)
                            while size < 1_500_000:
                                data = response.read(min(65536, 1_500_000 - size))
                                if not data:
                                    raise RuntimeError('late history ended early')
                                if capture:
                                    capture.write(data)
                                size += len(data)
                        return {'channel_id': channel['channel_uuid'], 'bytes': size, 'prefix_sha256': digest,
                                'historical_prefix_matches': True, 'capture': file.name if capture else None}
                    finally:
                        if capture:
                            capture.close()

                with concurrent.futures.ThreadPoolExecutor(max_workers=32) as pool:
                    late_rows = list(pool.map(late, range(100)))
                after_late = origin_stats()
                assert after_late['connections'] == before_late['connections'] and after_late['active'] == 10
                late_decoders = []
                for number in range(10):
                    # A45-second seek can start mid-GOP just as in the source.
                    # Allow initial missing-reference warnings, then require
                    # actual video/audio decoding from the unchanged TS bytes.
                    log = command(compose + ['exec', '-T', 'baseline', 'ffmpeg', '-hide_banner', '-loglevel', 'error',
                                  '-progress', 'pipe:1', '-nostats', '-i', '/evidence/late-' + str(number) + '.ts',
                                  '-map', '0:v:0', '-map', '0:a:0', '-t', '3', '-f', 'null', '-'], 30)
                    (output / ('late-decode-' + str(number) + '.log')).write_text(log)
                    frames = max((int(line.split('=')[1]) for line in log.splitlines() if line.startswith('frame=')), default=0)
                    assert frames >= 50
                    late_decoders.append(frames)
                report['late_history'] = {'viewers': 100, 'channels': 10, 'concurrency': 32, 'behind_seconds': 45,
                    'prefix_reference_age_range_seconds': [35, 55], 'reference_prefixes': prefixes,
                    'readers': late_rows, 'decoder_frames': late_decoders, 'no_extra_upstream': True,
                    'decoder_scope': 'video/audio maps with initial mid-GOP reference warnings permitted; byte-prefix proof is exact', 'passed': True}
            for reader in readers:
                reader.close()
            wait(lambda: origin_stats()['active'] == 0, 30, 'all origins close')
            for number in range(10):
                log = command(compose + ['exec', '-T', 'baseline', 'ffmpeg', '-hide_banner', '-loglevel', 'error',
                              '-xerror', '-progress', 'pipe:1', '-nostats', '-i', '/evidence/capture-' + str(number)
                              + '.ts', '-map', '0:v:0', '-map', '0:a:0', '-t', '3', '-f', 'null', '-'], 30)
                (output / ('decode-' + str(number) + '.log')).write_text(log)
                assert max((int(line.split('=')[1]) for line in log.splitlines() if line.startswith('frame=')), default=0) >= 50
            report['passed'] = True
        except Exception as error:
            report['failure'] = type(error).__name__
            report['failure_message'] = str(error)
        finally:
            for reader in readers:
                try:
                    reader.close()
                except Exception:
                    report['passed'] = False
            try:
                (output / 'compose.log').write_text(command(compose + ['logs', '--no-color'], 30))
            except Exception:
                pass
            try:
                command(compose + ['down', '--volumes', '--remove-orphans', '--timeout', '5'], 60)
                report['cleanup'] = 'isolated Compose project removed'
            except Exception:
                report['cleanup_error'] = True
                report['passed'] = False
            report['changed_tool_sources'] = [name for name, digest in report['tool_sha256'].items()
                                             if hashlib.sha256((ROOT / name).read_bytes()).hexdigest() != digest]
            if report['changed_tool_sources']:
                report['passed'] = False
    return 0 if report['passed'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
