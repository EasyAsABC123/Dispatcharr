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
    parser.add_argument('--source-topology',choices=['two-replicas','one-worker'],default='two-replicas',help='Predeclared topology; never selected after observing a response')
    parser.add_argument('--input-profile-source18',action='store_true',help='NEW real custom-input FFmpeg media domain; prior model18 authority reused')
    parser.add_argument('--output', required=True, type=Path)
    parser.add_argument('--candidate-image')
    parser.add_argument('--candidate-build-manifest', type=Path)
    parser.add_argument('--backends', nargs='+', choices=['baseline', 'candidate'], default=['baseline', 'candidate'])
    parser.add_argument('--cases', nargs='+', choices=['shared-demand', 'stop', 'owner-crash', 'all-workers-down'], default=['shared-demand', 'stop', 'owner-crash'])
    parser.add_argument('--cleanup-timeout', type=float, default=90)
    parser.add_argument('--require-active-metadata',action='store_true')
    args = parser.parse_args()
    if args.backends != ['baseline'] or not args.input_profile_source18 or args.candidate_build_manifest or args.candidate_image:
        parser.error('Original-only custom-input capture: no Native image or18model replay')
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
    report = {'scope': 'Actual pinned Original custom-FFmpeg HTTP/media characterization', 'passed': False,
              'application_performance_verified': False, 'milestone_complete': False,
              'require_active_metadata':args.require_active_metadata, 'project': project, 'baseline_commit': BASELINE, 'baseline_image': IMAGE,
              'checks': [], 'cases': [], 'commands': []}
    report.update(native_verified=False,Source18_model_calls_replayed=False,Source_media_characterization=True,source_topology=args.source_topology,matching_Native_topology=args.source_topology=='two-replicas',performance_verified=False)
    report['unmeasured_required_domains']=['VLC-runtime','active-OutputProfiles','provider-regex','credential-pools','HLS-fMP4-custom-input']
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
    if args.input_profile_source18:tool_paths.append(OWN/'profile_input_fixture.py')
    report['tool_sha256'] = {str(path.relative_to(ROOT)): hashlib.sha256(path.read_bytes()).hexdigest() for path in sorted(tool_paths)}

    def record(name, passed, details=None, fatal=True):
        report['checks'].append({'name': name, 'passed': bool(passed), 'details': details})
        (output/'capture-progress.json').write_text(json.dumps(report,indent=2)+'\n')
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
        source.chmod(0o755)
        for entry in source.rglob('*'):
            if entry.is_dir():entry.chmod(0o755)
            elif entry.is_file():entry.chmod(0o444 | (entry.stat().st_mode & 0o111))
        (source / 'media').mkdir(exist_ok=True)
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
                    if args.source_topology=='two-replicas':command(compose + ['up', '-d', '--no-build', 'python-replica'])
                    service_a,service_b,port='baseline',('python-replica'if args.source_topology=='two-replicas'else'baseline'),5656
                    base_a,base_b=endpoint(service_a,port),endpoint(service_b,port)
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
                expected_image=json.loads(command([args.docker,'image','inspect',IMAGE]))[0]['Id']
                for service in dict.fromkeys((service_a,service_b)):
                    cid=command(compose+['ps','-q',service]).strip()
                    info=json.loads(command([args.docker,'inspect',cid]))[0]
                    if info['Image']!=expected_image or info['Config']['Image']!=IMAGE:raise RuntimeError('ActualOriginalRoleImageBinding')
                    report.setdefault('Original_roles',{})[service]={'CID':cid,'image_id':info['Image'],'configured_image':info['Config']['Image'],'running':info['State']['Running']}
                report['actual_Original_FFmpeg_version']=command(compose+['exec','-T','baseline','ffmpeg','-version']).splitlines()[0]
                # Previously qualified HEAD/credential/model vectors are reused;
                # only the declared new actual custom-input media cases run.
                fixture_valid = True
                for number, case in enumerate(args.cases):
                    result = {'backend': backend, 'case': case, 'status': 'not_run'}
                    report['cases'].append(result)
                    if not fixture_valid:
                        result.update(status='blocked', reason='previous context retained shared provider/process/upstream resources')
                        continue
                    selected = seed['channels'][number]
                    channel, profile = selected['channel_uuid'], provider['provider_profile_id']
                    directory = output / (backend + '-' + case)
                    directory.mkdir()
                    local_readers = []
                    def observe(name, operation):
                        try:
                            value = operation()
                            result[name] = value
                            return value
                        except Exception as error:
                            result[name] = {'error': type(error).__name__, 'message': str(error)}
                            return None
                        finally:
                            (output/'capture-progress.json').write_text(json.dumps(report,indent=2)+'\n')
                    def input_process_rows():
                        found=[]
                        for service in dict.fromkeys((service_a,service_b)):
                            cid=command(compose+['ps','--all','-q',service]).strip()
                            if not re.fullmatch('[a-f0-9]{64}',cid):raise RuntimeError('ExactOwnedProfileRoleCID')
                            role=json.loads(command([args.docker,'inspect','--format','{{json .State}}',cid]))
                            result.setdefault('input_process_role_state',{})[service]={'CID':cid,'state':role}
                            if not role['Running']:
                                if role['Pid']!=0:raise RuntimeError('ExitedProfileRoleStillHasPID')
                                continue
                            for line in command([args.docker,'top',cid,'-eo','pid,ppid,pgid,args']).splitlines()[1:]:
                                if 'ffmpeg' in line and 'http://origin:8080/live/' in line:found.append({'service':service,'row':line})
                        return found
                    before = origin_snapshot()
                    result['origin_before'] = before
                    for label,base in (('owner',base_a),('follower',base_b)):
                        reader=Reader(base+selected['path'],directory/(label+'.ts'))
                        readers.append(reader);local_readers.append(reader)
                        observe(label+'_media_readiness',lambda reader=reader:wait(lambda:reader.snapshot()['bytes']>=750000 or reader.snapshot()['ended'],25,'bounded media observation'))
                        observe(label+'_first_metadata',lambda:snapshot(channel,profile))
                    observe('actual_input_processes',input_process_rows)
                    observe('shared',lambda:snapshot(channel,profile))
                    current=observe('origin_shared',origin_snapshot)
                    if current is not None:result['upstream_connection_delta']=current['connections']-before['connections']
                    began=time.monotonic()
                    if case=='shared-demand':
                        observe('owner_reader_close',local_readers[0].close)
                        at_close=local_readers[1].snapshot()['media_bytes']
                        observe('follower_after_owner_close',lambda:wait(lambda:local_readers[1].snapshot()['media_bytes']>=at_close+300000 or local_readers[1].snapshot()['ended'],10,'follower continuation observation'))
                        observe('after_reader_loss',lambda:snapshot(channel,profile))
                        observe('origin_after_reader_loss',origin_snapshot)
                    elif case=='stop':
                        observe('stop_response',lambda:http(base_b+'/proxy/ts/stop/'+channel,method='POST',token=admin))
                        observe('responses_ended_after_stop',lambda:wait(lambda:all(r.snapshot()['ended'] for r in local_readers),20,'stop response observation'))
                    elif case=='owner-crash':
                        pid=json.loads(command(compose+['exec','-T','baseline','/dispatcharrpy/bin/python','/verification/supervisor.py','control','status']))['pid']
                        result['killed_Original_worker_group']=int(pid)
                        python('import os,signal;os.killpg('+str(int(pid))+',signal.SIGKILL)')
                        observe('origin_closed_after_crash',lambda:wait(lambda:origin_snapshot()['active']==0,10,'crashed upstream observation'))
                    for reader in local_readers:observe('reader_close_'+str(local_readers.index(reader)),reader.close)
                    observe('automatic_resource_retirement',lambda:wait(lambda:clean(channel,profile),args.cleanup_timeout,'automatic resource retirement observation'))
                    result['retirement_observation_seconds']=time.monotonic()-began
                    observe('after',lambda:snapshot(channel,profile))
                    observe('origin_after',origin_snapshot)
                    observe('processes_after',input_process_rows)
                    result['readers']=[reader.snapshot() for reader in local_readers]
                    result['decoders']={}
                    for label in ('owner','follower'):
                        observed=observe('decode_'+label,lambda label=label:decode(directory/(label+'.ts')))
                        result['decoders'][label]=observed
                    # Counts and decoder results characterize Original behavior;
                    # they are never compared against Native expectations here.
                    result['status']='captured'
                    fixture_valid=clean(channel,profile) and not input_process_rows()
                    result['shared_fixture_clean_for_next_context']=fixture_valid
                    (output/'capture-progress.json').write_text(json.dumps(report,indent=2)+'\n')
                report['capture_complete']=len(report['cases'])==len(args.cases) and all(row['status']=='captured' for row in report['cases'])
                report['capture_valid']=report['capture_complete']
                report['passed']=False
                report['qualification']='Original characterization only; parity and broader milestone acceptance are not transferred'
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
    return 0 if report.get('capture_valid') and not report.get('cleanup_error') and not report.get('cleanup_errors') and not report.get('changed_tool_sources') else 1


if __name__ == '__main__':
    raise SystemExit(main())
