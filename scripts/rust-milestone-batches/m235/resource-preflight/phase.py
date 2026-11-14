#!/usr/bin/env python3
"""Two-worker native TS source contracts, using isolated full Compose fixtures.

All original application credit belongs to Dispatcharr's authors/contributors.
This captures behavior and decoder/resource-lifecycle evidence, not performance.
"""
import argparse
from contextlib import contextmanager
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import socket
import subprocess
import tempfile
import threading
import time
import urllib.error
import urllib.request
import uuid
from playback_methods import probe as probe_methods
from fixture_ports import write_port_overlay
from sampler import measure

ROOT = Path(os.environ['DISPATCHARR_PROFILE_SUPPORT_ROOT']).resolve()
OWN = Path(__file__).resolve().parent
BASELINE = '1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430'
IMAGE = 'ghcr.io/dispatcharr/dispatcharr@sha256:34f9011514edd31ef0ff111b5f256d2e37614fcb08a6ee1ed9eccec9251a856f'


def http(url, method='GET', token=None):
    headers = {'Authorization': 'Bearer ' + token} if token else {}
    try:
        response = urllib.request.urlopen(urllib.request.Request(url, method=method, headers=headers), timeout=10)
    except urllib.error.HTTPError as error:
        response = error
    with response:
        data = response.read(128 * 1024)
        return response.status, json.loads(data)


def wait(check, seconds=60, message='condition deadline'):
    deadline = time.monotonic() + seconds
    last = None
    while time.monotonic() < deadline:
        try:
            result = check()
            if result:
                return result
        except (OSError, ValueError, subprocess.SubprocessError) as error:
            last = type(error).__name__
        time.sleep(.1)
    raise TimeoutError(message + (': ' + last if last else ''))


class Reader:
    """Incremental socket reader; bounded capture, synchronized counters and close."""
    def __init__(self, url, path):
        self.url, self.path = url, path
        self.stop = threading.Event()
        self.lock = threading.Lock()
        self.response = None
        self.state = {'bytes': 0, 'media_bytes': 0, 'packets': 0, 'sync_errors': 0,
                      'partial_bytes': 0, 'ended': False, 'errors': []}
        self.thread = threading.Thread(target=self.run, daemon=True)
        self.thread.start()

    def run(self):
        partial = bytearray()
        try:
            with urllib.request.urlopen(self.url, timeout=100) as response, self.path.open('wb') as stream:
                with self.lock:
                    self.response = response
                    self.state['http_status'] = response.status
                    self.state['content_type'] = response.headers.get_content_type()
                while not self.stop.is_set():
                    data = response.read1(65536)
                    if not data:
                        break
                    partial.extend(data)
                    length = len(partial) // 188 * 188
                    complete = bytes(partial[:length])
                    del partial[:length]
                    stream.write(complete)
                    with self.lock:
                        self.state['bytes'] += len(complete)
                        self.state['packets'] += length // 188
                        for offset in range(0, length, 188):
                            self.state['sync_errors'] += complete[offset] != 0x47
                            if ((complete[offset + 1] & 31) << 8 | complete[offset + 2]) != 8191:
                                self.state['media_bytes'] += 188
                    if stream.tell() > 32 * 1024 * 1024:
                        raise RuntimeError('capture exceeds32MiB')
        except Exception as error:
            if not self.stop.is_set():
                with self.lock:
                    self.state['errors'].append(type(error).__name__)
                    self.state['http_error_status'] = getattr(error, 'code', None)
        finally:
            with self.lock:
                self.state['partial_bytes'] = len(partial)
                self.state['ended'] = True
                self.response = None

    def snapshot(self):
        with self.lock:
            return dict(self.state, errors=list(self.state['errors']))

    def close(self):
        self.stop.set()
        with self.lock:
            response = self.response
        if response is not None:
            try:
                response.fp.raw._sock.shutdown(socket.SHUT_RDWR)
            except (AttributeError, OSError):
                pass
        self.thread.join(timeout=3)
        if self.thread.is_alive():
            raise RuntimeError('reader failed bounded socket shutdown')


@contextmanager
def fixture_source(report,output,filename="shared-ts.json"):
    temporary=Path(tempfile.mkdtemp(prefix='dispatcharr-shared-ts-'))
    try:
        yield str(temporary)
    except BaseException as error:
        report['passed']=False
        report.setdefault('failure',type(error).__name__)
        raise
    finally:
        if report.get('cleanup_error'):
            report['resources_retained']=True
            report['retained_fixture_directory']=str(temporary)
        else:
            shutil.rmtree(temporary)
            report['resources_retained']=False
        (output/filename).write_text(json.dumps(report,indent=2)+'\n')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--docker', default='docker')
    parser.add_argument('--input-profile-source18',action='store_true',help='NEW real custom-input FFmpeg media domain; prior model18 authority reused')
    parser.add_argument('--output', required=True, type=Path)
    parser.add_argument('--candidate-image')
    parser.add_argument('--candidate-build-manifest', type=Path)
    parser.add_argument('--backends', nargs='+', choices=['baseline', 'candidate'], default=['baseline', 'candidate'])
    parser.add_argument('--cases', nargs='+', choices=['shared-demand', 'stop', 'owner-crash', 'all-workers-down'], default=['shared-demand', 'stop', 'owner-crash'])
    parser.add_argument('--cleanup-timeout', type=float, default=90)
    parser.add_argument('--require-active-metadata',action='store_true')
    args = parser.parse_args()
    if len(args.backends)!=1 or args.cases!=['shared-demand'] or not args.input_profile_source18:parser.error('One fixed shared-demand backend per fresh diagnostic phase required')
    if 'all-workers-down' in args.cases and (args.cases[-1] != 'all-workers-down' or args.backends != ['candidate']):
        parser.error('all-workers-down requires candidate-only and must be final')
    if 'owner-crash' in args.cases and args.cases[-1] != 'owner-crash':
        parser.error('owner-crash must be the final case')
    if not 10 <= args.cleanup_timeout <= 150:
        parser.error('cleanup timeout must be10..150seconds')
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
    project = 'dispatcharr-shared-ts-' + uuid.uuid4().hex[:12]
    report = {'scope': 'two-worker-native-TS-functional-contracts', 'passed': False,
              'application_performance_verified': False, 'diagnostic_resource_preflight_only': True, 'milestone_complete': False,
              'require_active_metadata':args.require_active_metadata, 'project': project, 'baseline_commit': BASELINE, 'baseline_image': IMAGE,
              'checks': [], 'cases': [], 'commands': []}
    report['unmeasured_required_domains']=['Source-media-authority','VLC-runtime','active-OutputProfile-delivery','provider-regex','credential-pool','custom-input-HLS-fMP4']
    report['full_M3_passed']=False
    report['Source18_model_calls_replayed']=False
    (output/'phase-identity.json').write_text(json.dumps({'project':project})+'\n')
    manifest = None
    if args.candidate_build_manifest:
        raw = args.candidate_build_manifest.read_bytes()
        manifest = json.loads(raw)
        hashes=manifest.get('source_sha256')
        if (manifest.get('format_version')!=1 or manifest.get('passed') is not True or not isinstance(hashes,dict) or not hashes
                or any(not isinstance(name,str) or not isinstance(value,str) or not re.fullmatch('[0-9a-f]{64}',value) for name,value in hashes.items())):
            parser.error('invalid passing immutable source manifest')
        digest=hashlib.sha256(json.dumps(hashes,sort_keys=True,separators=(',',':')).encode()).hexdigest()
        if digest!=manifest.get('source_tree_sha256'):
            parser.error('candidate source manifest digest mismatch')
        report['candidate_build_manifest_sha256'] = hashlib.sha256(raw).hexdigest()
        report['candidate_build_manifest'] = str(args.candidate_build_manifest)
    tag_overlay = output / 'candidate-image.json'
    tag_overlay.write_text(json.dumps({'services': {name: {'image': args.candidate_image or 'dispatcharr-rust-candidate:milestone2'}
                                                 for name in ('candidate', 'candidate-replica')}}))
    ports = output / 'ports.yml'
    report['ports'] = write_port_overlay(ports, {'baseline':[5656], 'origin':[8080],
        'candidate':[9192], 'candidate-replica':[9192], 'python-replica':[5656]})
    tool_paths = [Path(__file__).resolve(), ROOT / 'docker/compose.rust-playback.yml', ROOT / 'docker/compose.rust-candidate.yml',
                  ROOT / 'docker/compose.rust-replicas.yml', *[ROOT / 'scripts/rust-playback-baseline' / name
                  for name in ('seed.py', 'origin.py', 'range_http.py', 'start.sh', 'supervisor.py', 'uwsgi.ini')],
                  ROOT / 'scripts/rust-baseline/dependencies.py', ROOT / 'scripts/rust-integration/seed_accounts.py', ROOT / 'scripts/rust-integration/playback_methods.py', ROOT / 'scripts/rust-integration/fixture_ports.py']
    if args.input_profile_source18:tool_paths += [OWN/'profile_input_fixture.py']
    tool_paths.append(OWN/'sampler.py')
    report['tool_sha256'] = {str(path.relative_to(ROOT)): hashlib.sha256(path.read_bytes()).hexdigest() for path in sorted(tool_paths)}

    def record(name, passed, details=None, fatal=True):
        report['checks'].append({'name': name, 'passed': bool(passed), 'details': details})
        (output/'cohort-progress.json').write_text(json.dumps(report,indent=2)+'\n')
        if not passed and fatal:
            raise RuntimeError(name)

    with fixture_source(report,output) as temporary:
        source = Path(temporary) / 'source'
        source.mkdir()
        archive = Path(temporary) / 'source.tar'
        with archive.open('wb') as stream:
            subprocess.run(['git', 'archive', BASELINE], cwd=ROOT, stdout=stream, check=True, timeout=30)
        report['baseline_archive_sha256'] = hashlib.sha256(archive.read_bytes()).hexdigest()
        if report['baseline_archive_sha256']!='f2323ac705be2af5d1c264f26b1d297cb2bede53874e6bceccee4bf6603fa7ff':raise RuntimeError('ExactPinnedOriginalArchive')
        subprocess.run(['tar', '-xf', str(archive), '-C', str(source), '--no-same-owner'], check=True, timeout=30)
        # Public pinned Original source must stay readable across container UIDs,
        # independent of the host's restrictive umask. Preserve executable bits.
        source.chmod(0o755)
        for entry in source.rglob('*'):
            if entry.is_dir():entry.chmod(0o755)
            elif entry.is_file():entry.chmod(0o444 | (entry.stat().st_mode & 0o111))
        (source / 'media').mkdir(mode=0o755,exist_ok=True)
        (source / 'media').chmod(0o755)
        environment = dict(os.environ, BASELINE_SOURCE_DIR=str(source), BASELINE_EVIDENCE_DIR=str(output),
                           FIXTURE_SECONDS='300', FIXTURE_CODEC='h264', BENCHMARK_SHUTDOWN_DELAY='2',
                           BENCHMARK_RETENTION_SECONDS='60', BENCHMARK_STARTUP_BEHIND='5')
        compose = [args.docker, 'compose', '-p', project,
                   '-f', str(ROOT / 'docker/compose.rust-playback.yml'),
                   '-f', str(ROOT / 'docker/compose.rust-candidate.yml'),
                   '-f', str(ROOT / 'docker/compose.rust-replicas.yml'), '-f', str(tag_overlay), '-f', str(ports)]

        def command(values, timeout=120, payload=None):
            report['commands'].append(values)
            return subprocess.run(values, cwd=ROOT, env=environment, input=payload, text=True,
                                  capture_output=True, check=True, timeout=timeout).stdout

        def endpoint(service, port):
            return 'http://' + command(compose + ['port', service, str(port)]).strip()

        def python(script, timeout=30):
            return command(compose + ['exec', '-T', 'baseline', '/dispatcharrpy/bin/python', '-c', script], timeout=timeout)

        def redis(script, *values):
            return json.loads(command(compose + ['exec', '-T', 'baseline', 'redis-cli', '--json', 'EVAL', script, '0', *map(str, values)], timeout=15))

        def snapshot(channel, profile):
            row = redis("""local p='live:channel:'..ARGV[1];return {redis.call('GET',p..':owner') or '',redis.call('GET','profile_connections:'..ARGV[2]) or '0',redis.call('SCARD',p..':clients'),redis.call('HLEN',p..':metadata'),redis.call('GET',p..':input:buffer:index') or '',redis.call('ZCARD',p..':input:buffer:chunk_timestamps'),redis.call('HGET',p..':metadata','state') or ''}""", channel, profile)
            return {'owner': row[0] or None, 'provider_slots': int(row[1]), 'clients': row[2], 'metadata_fields': row[3],
                    'input_index': int(row[4]) if row[4] else None, 'timestamps': row[5], 'state': row[6] or None}

        def origin_snapshot():
            return http(origin + '/stats')[1]

        def clean(channel, profile):
            state = snapshot(channel, profile)
            return not state['owner'] and state['provider_slots'] == 0 and state['clients'] == 0 and state['metadata_fields'] == 0 and state['input_index'] is None and state['timestamps'] == 0 and origin_snapshot()['active'] == 0

        def decode(path):
            local = '/evidence/' + str(path.relative_to(output))
            ffprobe = json.loads(command(compose + ['exec', '-T', 'baseline', 'ffprobe', '-v', 'error', '-show_entries', 'stream=codec_type,codec_name', '-of', 'json', local], timeout=30))
            kinds = {stream['codec_type'] for stream in ffprobe.get('streams', [])}
            result = command(compose + ['exec', '-T', 'baseline', 'ffmpeg', '-hide_banner', '-loglevel', 'error', '-xerror', '-progress', 'pipe:1', '-nostats', '-i', local, '-t', '3', '-map', '0:v:0', '-map', '0:a:0', '-f', 'null', '-'], timeout=30)
            path.with_suffix('.decode.log').write_text(result)
            frames = [int(line.split('=')[1]) for line in result.splitlines() if line.startswith('frame=')]
            return {'frames': max(frames, default=0), 'video_and_audio': {'video', 'audio'} <= kinds,
                    'sha256': hashlib.sha256(path.read_bytes()).hexdigest()}

        readers = []
        try:
            command(compose + ['up', '-d', 'baseline', 'origin'], timeout=180)
            origin, baseline = endpoint('origin', 8080), endpoint('baseline', 5656)
            wait(lambda: (output / 'seed.json').is_file() and http(origin + '/health')[0] == 200 and http(baseline + '/proxy/ts/stream/missing')[0] == 404, 300, 'fixture startup')
            seed = json.loads((output / 'seed.json').read_text())
            setup = """import os,sys,json;sys.path.insert(0,'/app');os.environ.setdefault('DJANGO_SETTINGS_MODULE','dispatcharr.settings');import django;django.setup();from apps.m3u.models import M3UAccount;a=M3UAccount.get_custom_account();p=a.profiles.get(is_default=True);p.max_streams=1;p.save(update_fields=['max_streams']);print(json.dumps({'provider_profile_id':p.pk,'max_streams':p.max_streams}))"""
            if args.input_profile_source18:
                report['input_profile_fixture']=json.loads(python((OWN/'profile_input_fixture.py').read_text()))
            provider = json.loads(python(setup))
            report['provider_fixture'] = provider
            accounts = json.loads(command(compose + ['exec', '-T', 'baseline', '/dispatcharrpy/bin/python', '/integration/seed_accounts.py']))
            admin = accounts['admin']['access']
            report['fixture'] = origin_snapshot()
            for backend in args.backends:
                command(compose + ['stop', '--timeout', '15', 'candidate', 'candidate-replica', 'python-replica'], timeout=60)
                command(compose + ['exec', '-T', 'baseline', '/dispatcharrpy/bin/python', '/verification/supervisor.py', 'control', 'stop'])
                command(compose + ['exec', '-T', 'baseline', 'redis-cli', 'FLUSHDB'])
                if backend == 'baseline':
                    command(compose + ['exec', '-T', 'baseline', '/dispatcharrpy/bin/python', '/verification/supervisor.py', 'control', 'start'])
                    command(compose + ['up', '-d', '--no-build', 'python-replica'])
                    service_a, service_b, port = 'baseline', 'python-replica', 5656
                    base_a, base_b = endpoint(service_a, port), endpoint(service_b, port)
                    wait(lambda: http(base_b + '/proxy/ts/stream/missing')[0] == 404, 60)
                else:
                    command(compose + ['up', '-d', '--no-build', 'candidate', 'candidate-replica'])
                    service_a, service_b, port = 'candidate', 'candidate-replica', 9192
                    base_a, base_b = endpoint(service_a, port), endpoint(service_b, port)
                    wait(lambda: http(base_a + '/health/ready')[0] == 200 and http(base_b + '/health/ready')[0] == 200, 60)
                    for service in (service_a, service_b):
                        container = command(compose + ['ps', '-q', service]).strip()
                        inspection = json.loads(command([args.docker, 'inspect', container]))[0]
                        record(service + ':immutable-image', manifest.get('passed') is True and inspection['Image'] == manifest['image_id'] and inspection['Config']['Labels'].get('org.opencontainers.image.source-tree-sha256') == manifest['source_tree_sha256'])
                # No standalone model18/HEAD/credential replay.
                for number, case in enumerate(args.cases):
                    selected = seed['channels'][number]
                    channel = selected['channel_uuid']
                    profile = provider['provider_profile_id']
                    directory = output / (backend + '-' + case)
                    directory.mkdir()
                    result = {'backend': backend, 'case': case}
                    report['cases'].append(result)
                    before = origin_snapshot()
                    reader_a = Reader(base_a + selected['path'], directory / 'owner.ts')
                    readers.append(reader_a)
                    wait(lambda: reader_a.snapshot()['bytes'] >= 750000, 25, 'owner media readiness')
                    result['owner_first_media_metadata']=snapshot(channel,profile)
                    if args.require_active_metadata:record(backend+':'+case+':owner-first-media-active',result['owner_first_media_metadata']['state']=='active',result['owner_first_media_metadata'])
                    reader_b = Reader(base_b + selected['path'], directory / 'follower.ts')
                    readers.append(reader_b)
                    wait(lambda: reader_b.snapshot()['bytes'] >= 750000, 25, 'follower media readiness')
                    result['follower_first_media_metadata']=snapshot(channel,profile)
                    if args.require_active_metadata:record(backend+':'+case+':follower-first-media-active',result['follower_first_media_metadata']['state']=='active',result['follower_first_media_metadata'])
                    def input_process_rows():
                        found=[]
                        for service in (service_a,service_b):
                            cid=command(compose+['ps','--all','-q',service]).strip()
                            if not re.fullmatch('[a-f0-9]{64}',cid):raise RuntimeError('ExactOwnedProfileRoleCID')
                            state=json.loads(command([args.docker,'inspect','--format','{{json .State}}',cid]))
                            result.setdefault('input_process_role_state',{})[service]={'CID':cid,'state':state}
                            if state['Running']is False:
                                if state['Pid']!=0:raise RuntimeError('ExitedProfileRoleStillHasPID')
                                continue
                            text=command([args.docker,'top',cid,'-eo','pid,ppid,pgid,args'])
                            for line in text.splitlines()[1:]:
                                if 'ffmpeg' in line and 'http://origin:8080/live/' in line:
                                    found.append({'service':service,'row':line})
                        return found
                    if args.input_profile_source18:
                        result['actual_input_processes']=input_process_rows()
                        record(backend+':'+case+':one-real-input-profile-process',len(result['actual_input_processes'])==1,result['actual_input_processes'])
                    state = snapshot(channel, profile)
                    result['shared'] = state
                    current = origin_snapshot()
                    record(backend + ':' + case + ':one-origin-and-provider', current['connections'] - before['connections'] == 1 and current['active'] == 1 and state['provider_slots'] == 1 and state['clients'] == 2 and state['input_index'] and state['timestamps'], state)
                    result['resource_preflight']=measure(command,compose,args.docker,backend,[reader_a,reader_b],origin_snapshot,snapshot,channel,profile,output)
                    began = time.monotonic()
                    cleanup_passed=None
                    if case == 'shared-demand':
                        reader_a.close()
                        at_close = reader_b.snapshot()['media_bytes']
                        wait(lambda: reader_b.snapshot()['media_bytes'] >= at_close + 300000, 10, 'follower after owner-local close')
                        after = snapshot(channel, profile)
                        record(backend + ':follower-keeps-owner', after['owner'] == state['owner'] and after['provider_slots'] == 1 and origin_snapshot()['active'] == 1 and origin_snapshot()['connections'] - before['connections'] == 1, after)
                        reader_b.close()
                    elif case == 'stop':
                        status, response = http(base_b + '/proxy/ts/stop/' + channel, method='POST', token=admin)
                        record(backend + ':follower-global-stop-response', status == 200 and response == {'message':'Channel stop request sent','channel_id':channel,'previous_state':{'state':state['state']}}, {'status': status, 'body': response})
                        wait(lambda: reader_a.snapshot()['ended'] and reader_b.snapshot()['ended'], 20, 'global stop reaches both viewers')
                    elif case == 'all-workers-down':
                        command(compose + ['kill', '-s', 'SIGKILL', service_a, service_b])
                        wait(lambda: origin_snapshot()['active'] == 0, 10, 'all crashed upstream sockets close')
                        # Observe an arbitrarily later application restart, after
                        # both the coordinator lease and expiring input fence.
                        while time.monotonic() - began < 65:
                            time.sleep(max(0,min(.5,65-(time.monotonic()-began))))
                        abandoned=snapshot(channel,profile)
                        result['before_restart']=abandoned
                        record(backend+':durable-orphan-index-witness',abandoned['owner'] is None and abandoned['input_index'] is not None and abandoned['timestamps']==0,abandoned)
                        command(compose + ['rm','-f',service_b])
                        restart=time.monotonic()
                        command(compose + ['up','-d','--no-build',service_b])
                        base_b=endpoint(service_b,port)
                        wait(lambda:http(base_b+'/health/ready')[0]==200,30,'late replica restart ready')
                        container=command(compose+['ps','-q',service_b]).strip()
                        inspection=json.loads(command([args.docker,'inspect',container]))[0]
                        record(service_b+':restart-immutable-image',inspection['Image']==manifest['image_id'] and inspection['Config']['Labels'].get('org.opencontainers.image.source-tree-sha256')==manifest['source_tree_sha256'])
                        wait(lambda:clean(channel,profile),10,'late automatic catalog and coordinator cleanup')
                        result['restart_cleanup_seconds']=time.monotonic()-restart
                        record(backend+':late-restart-resource-recovery',result['restart_cleanup_seconds']<=15,{'seconds':result['restart_cleanup_seconds'],'state':snapshot(channel,profile)})
                        record(backend+':late-restart-no-invented-takeover',origin_snapshot()['connections']-before['connections']==1)
                        cleanup_passed=True
                    else:
                        if backend == 'baseline':
                            pid = json.loads(command(compose + ['exec', '-T', 'baseline', '/dispatcharrpy/bin/python', '/verification/supervisor.py', 'control', 'status']))['pid']
                            python('import os,signal;os.killpg(' + str(int(pid)) + ',signal.SIGKILL)')
                        else:
                            command(compose + ['kill', '-s', 'SIGKILL', service_a])
                        wait(lambda: origin_snapshot()['active'] == 0, 10, 'crashed owner upstream closes')
                        result['crash_no_automatic_takeover'] = True
                        try:
                            wait(lambda: clean(channel, profile),args.cleanup_timeout,'owner-crash metadata/slot/input cleanup')
                            cleanup_passed=True
                        except TimeoutError:
                            if backend!='baseline':raise
                            cleanup_passed=False
                            result['baseline_resource_defect']=snapshot(channel,profile)
                            report.setdefault('baseline_resource_defects',[]).append({'case':case,'deadline_seconds':args.cleanup_timeout,'state':snapshot(channel,profile)})
                        record(backend+':owner-crash-resource-recovery',cleanup_passed,{'deadline_seconds':args.cleanup_timeout,'state':snapshot(channel,profile)},fatal=backend!='baseline')
                        record(backend + ':crash-does-not-invent-takeover', origin_snapshot()['connections'] - before['connections'] == 1)
                    if cleanup_passed is None:
                        wait(lambda: clean(channel, profile), args.cleanup_timeout, 'complete shared channel cleanup')
                    result['cleanup_seconds'] = time.monotonic() - began
                    if args.input_profile_source18:
                        wait(lambda:not input_process_rows(),15,'input profile process-group retirement')
                        record(backend+':'+case+':input-profile-process-retired',not input_process_rows())
                    result['after'] = snapshot(channel, profile)
                    if case != 'shared-demand':
                        wait(lambda: reader_a.snapshot()['ended'] and reader_b.snapshot()['ended'],10,'both responses terminate after global cleanup')
                    for reader in (reader_a, reader_b):
                        reader.close()
                    result['readers'] = [reader_a.snapshot(), reader_b.snapshot()]
                    record(backend + ':' + case + ':packet-sync', all(item['sync_errors'] == 0 and item.get('content_type') == 'video/mp2t' for item in result['readers']))
                    result['decoders'] = [decode(directory / name) for name in ('owner.ts', 'follower.ts')]
                    record(backend + ':' + case + ':actual-decoder', all(item['frames'] >= 50 and item['video_and_audio'] for item in result['decoders']), result['decoders'])
                    if case in ('owner-crash','all-workers-down'):
                        break
            report['passed'] = all(item['passed'] for item in report['checks'])
            baseline_checks=[item for item in report['checks'] if item['name'].startswith('baseline:') and item['name']!='baseline:owner-crash-resource-recovery']
            report['baseline_protocol_functional_passed']=all(item['passed'] for item in baseline_checks) if baseline_checks else None
            candidate_checks=[item for item in report['checks'] if item['name'].startswith('candidate')]
            report['candidate_functional_and_recovery_passed']=all(item['passed'] for item in candidate_checks) if candidate_checks else None
        except Exception as error:
            report['failure'] = type(error).__name__
            report['failure_message'] = str(error)
            try:
                report['last_origin'] = origin_snapshot()
                report['last_redis'] = snapshot(channel, profile)
            except Exception:
                pass
        finally:
            for reader in readers:
                try:
                    reader.close()
                except Exception as error:
                    report.setdefault('cleanup_errors', []).append(type(error).__name__)
                    report['passed'] = False
            try:
                (output / 'compose.log').write_text(command(compose + ['logs', '--no-color'], timeout=30))
            except Exception:
                pass
            try:
                command(compose + ['down', '--volumes', '--remove-orphans', '--timeout', '5'], timeout=60)
                report['cleanup'] = 'isolated Compose project removed'
            except Exception as error:
                report['cleanup_error'] = type(error).__name__
                report['passed'] = False
            report['changed_tool_sources'] = [name for name, digest in report['tool_sha256'].items() if hashlib.sha256((ROOT / name).read_bytes()).hexdigest() != digest]
            if report['changed_tool_sources']:
                report['passed'] = False
            (output / 'shared-ts.json').write_text(json.dumps(report, indent=2) + '\n')
    return 0 if report['passed'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
