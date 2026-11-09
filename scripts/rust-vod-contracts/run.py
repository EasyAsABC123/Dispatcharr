#!/usr/bin/env python3
"""Run isolated Compose native VOD Python characterization, then clean up."""
import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import time
import urllib.request
import uuid
from vod_tool_snapshot import snapshot_sources

SCRIPT_ROOT = Path(__file__).resolve().parents[2]
ROOT = Path(os.environ.get('DISPATCHARR_CONTRACT_REPO',str(SCRIPT_ROOT))).resolve()
BASELINE = '1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430'


def rootless_port_collision(stderr):
    return isinstance(stderr,str) and 'RootlessKit PortManager.AddPort()' in stderr and 'bind: address already in use' in stderr


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--docker', default='docker')
    parser.add_argument('--source-frontend-from-candidate', action='store_true', help='Mount unchanged built React from immutable candidate into source fixture; requires candidate manifest')
    parser.add_argument('--profile-aliases', action='store_true', help='Separately characterize bare-profile redirects and their follow status')
    parser.add_argument('--timeshift-redirect-session', action='store_true', help='Use Redirect channel with empty allowed profiles to verify established capability still proxies')
    parser.add_argument('--timeshift-management-active', action='store_true', help='Include actual active/idle media admin-stop controls')
    parser.add_argument('--timeshift-rust-management', action='store_true', help='Compare native Rust catchup management against original actual SQL/Redis wire contracts')
    parser.add_argument('--timeshift-management', action='store_true', help='Characterize original catchup stats/programmes/admin-stop contracts')
    parser.add_argument('--timeshift-rust-xc', action='store_true', help='Compare real Store XC Timeshift HTTP routes with original source')
    parser.add_argument('--timeshift-xc', action='store_true', help='Capture original XC catch-up PATH/QUERY wire contracts')
    parser.add_argument('--timeshift-rust-direct', action='store_true', help='Compare actual Store/Auth native direct-auth HTTP against original source')
    parser.add_argument('--xc-store-auth', action='store_true', help='Compare Store XC authentication/ACL/cache/lockout contracts with original Django helpers')
    parser.add_argument('--timeshift-direct-auth', action='store_true', help='Capture original native direct-auth validation, redirect and client-session contracts')
    parser.add_argument('--timeshift-busy-seek', action='store_true', help='Characterize original busy middle seek, full reconnect and near-EOF probe boundaries')
    parser.add_argument('--timeshift-idle-reopen', action='store_true', help='Characterize original same-capability idle Range reopen and cached URL state')
    parser.add_argument('--timeshift-native-limit', action='store_true', help='Capture actual JWT native positive-limit refusal/retry on source and optional frozen candidate')
    parser.add_argument('--timeshift-admission-rust-test', action='store_true', help='Compare new Rust atomic admission effects against captured original source with real fixture Redis')
    parser.add_argument('--timeshift-admission-source', action='store_true', help='Capture original Timeshift shared-limit/hop policy using actual Redis fixture records')
    parser.add_argument('--candidate-timeshift-media', action='store_true', help='Compare source/candidate native session API and six actual archive media/lifecycle scenarios')
    parser.add_argument('--candidate-overlay', type=Path)
    parser.add_argument('--candidate-build-manifest', type=Path)
    parser.add_argument('--timeshift-rust-media', action='store_true', help='Run actual Store/Auth native Rust archive HTTP and decoder against generated source fixture')
    parser.add_argument('--timeshift-media', action='store_true', help='Capture paced native archive ranges, URL fallback, and cleanup')
    parser.add_argument('--timeshift-rust-test', action='store_true', help='Run actual Store/Auth/Redis catch-up contract; implies --timeshift-sessions')
    parser.add_argument('--timeshift-sessions', action='store_true', help='Capture original native catch-up session API contracts')
    parser.add_argument('--rust-resolver-test', action='store_true', help='Run the actual-schema Rust resolver test against this disposable database')
    parser.add_argument('--rust-ingest-test', action='store_true', help='Compare the native M3U batch sink to original Django writes in this disposable database')
    args = parser.parse_args()
    if args.timeshift_management_active:
        args.timeshift_management = True
    if args.timeshift_rust_management:
        args.timeshift_management = True
    if args.timeshift_rust_xc:
        args.timeshift_xc = True
    if args.timeshift_rust_direct:
        args.timeshift_direct_auth = True
    if args.timeshift_redirect_session:
        args.timeshift_native_limit = True
    if args.timeshift_admission_rust_test:
        args.timeshift_admission_source = True
    if args.candidate_timeshift_media:
        if not args.candidate_overlay or not args.candidate_build_manifest:
            parser.error('candidate Timeshift comparison requires immutable candidate overlay and manifest')
        args.timeshift_media = True
    if args.timeshift_rust_media:
        args.timeshift_rust_test = True
        args.timeshift_media = True
    if args.timeshift_rust_test or args.timeshift_media or args.timeshift_admission_source or args.timeshift_native_limit or args.timeshift_idle_reopen or args.timeshift_busy_seek or args.timeshift_direct_auth or args.timeshift_xc or args.timeshift_management:
        args.timeshift_sessions = True
    if bool(args.candidate_overlay) != bool(args.candidate_build_manifest):
        parser.error('candidate overlay and immutable build manifest must be provided together')
    if args.source_frontend_from_candidate and not args.candidate_build_manifest:
        parser.error('source React fixture requires immutable candidate manifest')
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=True)
    if any(output.iterdir()):
        parser.error('output must be empty')
    tool_root=output/'tool-source'
    tool_hashes=snapshot_sources(SCRIPT_ROOT,tool_root)
    tool_manifest=json.dumps(tool_hashes,sort_keys=True,indent=2)+'\n'
    (output/'tool-source-sha256.json').write_text(tool_manifest)
    sys.path.insert(0,str(tool_root/'scripts/rust-vod-contracts'))
    spec=importlib.util.spec_from_file_location('frozen_vod_wire_probe',tool_root/'scripts/rust-vod-contracts/probe.py')
    frozen_probe=importlib.util.module_from_spec(spec)
    spec.loader.exec_module(frozen_probe)
    probe,profile_alias_probe=frozen_probe.probe,frozen_probe.profile_alias_probe
    project = 'dispatcharr-vod-contract-' + uuid.uuid4().hex[:10]
    temporary = Path(tempfile.mkdtemp(prefix='dispatcharr-vod-contract-'))
    source = temporary/'source'
    source.mkdir()
    environment = dict(os.environ, BASELINE_SOURCE_DIR=str(source), BASELINE_EVIDENCE_DIR=str(output))
    compose = [args.docker, 'compose', '-p', project, '-f', str(tool_root/'docker/compose.rust-playback.yml'),
               '-f', str(tool_root/'scripts/rust-vod-contracts/compose.yml')]
    candidate_manifest = None
    candidate_manifest_bytes = None
    if args.candidate_overlay:
        candidate_manifest_bytes = args.candidate_build_manifest.read_bytes()
        (output/'candidate-build-manifest.json').write_bytes(candidate_manifest_bytes)
        candidate_manifest = json.loads(candidate_manifest_bytes)
        sys.path.insert(0,str(tool_root/'scripts/rust-integration'))
        spec = importlib.util.spec_from_file_location('integration_adapter', tool_root/'scripts/rust-integration/run.py')
        integration = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(integration)
        overlay = integration.candidate_overlay(args.candidate_overlay.resolve(), output/'candidate-overlay.yml', candidate_manifest.get('tag') or candidate_manifest['image_id'])
        compose += ['-f',str(overlay)]
    port_source = (tool_root/'scripts/rust-integration/fixture_ports.py').read_bytes()
    (output/'fixture-ports-tool.py').write_bytes(port_source)
    port_namespace = {'__file__':str(tool_root/'scripts/rust-integration/fixture_ports.py')}
    exec(compile(port_source,port_namespace['__file__'],'exec'),port_namespace)
    service_ports = {'baseline':[5656,5432,6379],'origin':[8080]}
    if candidate_manifest:
        service_ports['candidate'] = [9192]
    port_overlay = output/'fixture-ports.yml'
    port_metadata = port_namespace['write_port_overlay'](port_overlay,service_ports)
    port_metadata.update(tool_sha256=hashlib.sha256(port_source).hexdigest(),overlay_sha256=hashlib.sha256(port_overlay.read_bytes()).hexdigest())
    compose += ['-f',str(port_overlay)]
    report = {'format_version': 1, 'scope': 'original Python native XC VOD wire contracts',
              'source_commit': BASELINE, 'passed': False, 'project': project,
              'performance_verified': False, 'parity_verified': False, 'fixture_ports':port_metadata, 'tool_source_sha256':tool_hashes, 'tool_manifest_sha256':hashlib.sha256(tool_manifest.encode()).hexdigest(), 'tool_source_scope':'All VOD/Timeshift probes, helpers, integration/baseline fixtures and Compose files copied before setup; later imports and script mounts use these bytes.'}
    if candidate_manifest:
        report['candidate_overlay_sha256']=hashlib.sha256((output/'candidate-overlay.yml').read_bytes()).hexdigest()
    def command(arguments, timeout=180):
        result = subprocess.run(arguments, env=environment, capture_output=True, text=True, timeout=timeout)
        with (output/'commands.log').open('a') as log:
            log.write(result.stdout+result.stderr)
        result.check_returncode()
        return result.stdout
    def container(service):
        return command(compose+['ps','-q',service]).strip()
    def address(service, port):
        return 'http://'+command(compose+['port',service,str(port)]).strip()
    try:
        archive = subprocess.run(['git','archive',BASELINE],cwd=ROOT,check=True,capture_output=True).stdout
        subprocess.run(['tar','-x','--same-permissions','-C',str(source)],input=archive,check=True)
        (source/'media').mkdir(exist_ok=True)
        if args.source_frontend_from_candidate:
            frontend = source/'frontend/dist'
            frontend.mkdir(parents=True,exist_ok=True)
            holder = command([args.docker,'create','--entrypoint','/bin/true',candidate_manifest['image_id']]).strip()
            try:
                command([args.docker,'cp',holder+':/opt/dispatcharr-frontend/.',str(frontend)])
            finally:
                command([args.docker,'rm',holder])
            report['source_frontend_fixture'] = {'image_id':candidate_manifest['image_id'],'index_sha256':hashlib.sha256((frontend/'index.html').read_bytes()).hexdigest(),'purpose':'Remove missing-template confound when source redirects fall through React catch-all'}
        report['fixture_start_attempts'] = []
        for attempt in range(1,5):
            try:
                command(compose+['up','-d','--force-recreate','baseline'])
                report['fixture_start_attempts'].append({'attempt':attempt,'started':True})
                break
            except subprocess.CalledProcessError as error:
                bind_collision = rootless_port_collision(error.stderr)
                report['fixture_start_attempts'].append({'attempt':attempt,'started':False,'rootless_dynamic_port_collision':bind_collision})
                if not bind_collision or attempt == 4:
                    raise
        deadline = time.monotonic()+180
        while True:
            try:
                base = address('baseline',5656)
                with urllib.request.urlopen(base+'/api/core/version/',timeout=2) as response:
                    if response.status == 200:
                        break
            except Exception:
                if time.monotonic()>deadline:
                    raise TimeoutError('baseline readiness timed out')
                time.sleep(1)
        command(compose+['exec','-T','baseline','/dispatcharrpy/bin/python','/vod-verification/seed.py'])
        seed = json.loads((output/'vod-seed.json').read_text())
        if args.timeshift_sessions:
            private = subprocess.run(compose+['exec','-T','baseline','/dispatcharrpy/bin/python','/timeshift-verification/seed.py'], env=environment, capture_output=True, text=True, timeout=60)
            private.check_returncode()
            session_seed = json.loads(private.stdout.splitlines()[-1])
            spec = __import__('importlib.util', fromlist=['spec_from_file_location']).spec_from_file_location('timeshift_session_probe', tool_root/'scripts/rust-timeshift-contracts/probe.py')
            module = __import__('importlib.util', fromlist=['module_from_spec']).module_from_spec(spec)
            spec.loader.exec_module(module)
            report['timeshift_sessions'] = module.capture(base,session_seed)
            (output/'timeshift-sessions.json').write_text(json.dumps(report['timeshift_sessions'],indent=2)+'\n')
            if not report['timeshift_sessions']['capture_complete']:
                raise RuntimeError('Timeshift session source capture incomplete')

        if args.timeshift_management:
            sys.path.insert(0,str(tool_root/'scripts/rust-timeshift-contracts'))
            spec=importlib.util.spec_from_file_location('timeshift_management_probe',tool_root/'scripts/rust-timeshift-contracts/management_probe.py')
            management=importlib.util.module_from_spec(spec)
            spec.loader.exec_module(management)
            def management_control(action,label,session_id=None):
                result=command(compose+['exec','-T','baseline','/dispatcharrpy/bin/python','/timeshift-verification/management_control.py',action,label]+([session_id] if session_id else []),timeout=30)
                return json.loads(result.splitlines()[-1])
            report['timeshift_management_source']=management.capture(base,session_seed,management_control,'source',active=args.timeshift_management_active)
            (output/'timeshift-management-source.json').write_text(json.dumps(report['timeshift_management_source'],indent=2)+'\n')
        if args.timeshift_xc:
            sys.path.insert(0,str(tool_root/'scripts/rust-timeshift-contracts'))
            spec=importlib.util.spec_from_file_location('timeshift_xc_probe',tool_root/'scripts/rust-timeshift-contracts/xc_probe.py')
            xc=importlib.util.module_from_spec(spec)
            spec.loader.exec_module(xc)
            def xc_control(action,label):
                result=command(compose+['exec','-T','baseline','/dispatcharrpy/bin/python','/timeshift-verification/xc_control.py',action,label],timeout=30)
                return json.loads(result.splitlines()[-1])
            report['timeshift_xc_source']=xc.capture(base,xc_control,'source')
            (output/'timeshift-xc-source.json').write_text(json.dumps(report['timeshift_xc_source'],indent=2)+'\n')
        if args.timeshift_direct_auth:
            spec=importlib.util.spec_from_file_location('timeshift_direct_probe',tool_root/'scripts/rust-timeshift-contracts/direct_probe.py')
            direct=importlib.util.module_from_spec(spec)
            spec.loader.exec_module(direct)
            def direct_control(action,label):
                result=command(compose+['exec','-T','baseline','/dispatcharrpy/bin/python','/timeshift-verification/direct_control.py',action,label],timeout=30)
                return json.loads(result.splitlines()[-1])
            report['timeshift_direct_auth_source']=direct.capture(base,session_seed,direct_control,'source')
            (output/'timeshift-direct-auth-source.json').write_text(json.dumps(report['timeshift_direct_auth_source'],indent=2)+'\n')
        if args.timeshift_admission_source:
            captured = command(compose+['exec','-T','baseline','/dispatcharrpy/bin/python','/timeshift-verification/admission_source.py'],timeout=60)
            report['timeshift_admission_source'] = json.loads(captured.splitlines()[-1])
            (output/'timeshift-admission-source.json').write_text(json.dumps(report['timeshift_admission_source'],indent=2)+'\n')
            if not report['timeshift_admission_source']['capture_complete']:
                raise RuntimeError('source Timeshift admission capture incomplete')
            if args.timeshift_admission_rust_test:
                redis_address = command(compose+['port','baseline','6379']).strip()
                test_environment = dict(environment,REDIS_TEST_URL='redis://:playback-fixture-only@'+redis_address+'/0',TIMESHIFT_ADMISSION_ORACLE=str(output/'timeshift-admission-source.json'))
                result = subprocess.run(['cargo','test','--manifest-path',str(ROOT/'rust/Cargo.toml'),'-p','dispatcharr-clients','--test','timeshift_source','--offline','--','--ignored','--nocapture'],env=test_environment,capture_output=True,text=True,timeout=180)
                (output/'rust-timeshift-admission.log').write_text(result.stdout+result.stderr)
                report['rust_timeshift_admission_source_passed'] = result.returncode == 0
                result.check_returncode()
        if args.timeshift_media or args.timeshift_native_limit or args.timeshift_idle_reopen or args.timeshift_busy_seek:
            def observe(session_id,details=False):
                result = subprocess.run(compose+['exec','-T','baseline','/dispatcharrpy/bin/python','/timeshift-verification/inspect_session.py'],env=environment,input=json.dumps({'session_id':session_id,'profile_id':seed['provider_profile_id'],'details':details}),capture_output=True,text=True,timeout=15)
                result.check_returncode()
                return json.loads(result.stdout.splitlines()[-1])
        if args.timeshift_busy_seek:
            sys.path.insert(0,str(tool_root/'scripts/rust-timeshift-contracts'))
            spec=importlib.util.spec_from_file_location('timeshift_busy_probe',tool_root/'scripts/rust-timeshift-contracts/busy_probe.py')
            busy=importlib.util.module_from_spec(spec)
            spec.loader.exec_module(busy)
            report['timeshift_busy_seek_source']=busy.capture(base,address('origin',8080),session_seed,observe)
            (output/'timeshift-busy-seek-source.json').write_text(json.dumps(report['timeshift_busy_seek_source'],indent=2)+'\n')
        if args.timeshift_idle_reopen:
            spec=importlib.util.spec_from_file_location('timeshift_idle_probe',tool_root/'scripts/rust-timeshift-contracts/idle_probe.py')
            idle=importlib.util.module_from_spec(spec)
            spec.loader.exec_module(idle)
            def decode_idle(capture):
                event=capture['events'][1]
                if 'media_file' not in event:
                    return
                decoded=command(compose+['exec','-T','baseline','ffmpeg','-hide_banner','-loglevel','error','-xerror','-progress','pipe:1','-nostats','-i','/evidence/'+event['media_file'],'-map','0:v:0','-map','0:a:0','-t','3','-f','null','-'],timeout=45)
                (output/(event['media_file']+'.decode.log')).write_text(decoded)
                frames=max((int(line.split('=')[1]) for line in decoded.splitlines() if line.startswith('frame=')),default=0)
                event['decoder']={'frames':frames,'strict_audio_video_decode_passed':frames>=50}
                if frames<50:raise RuntimeError('idle reopen strict decoder failed')
            report['timeshift_idle_reopen']=idle.capture(base,address('origin',8080),session_seed,output,observe)
            decode_idle(report['timeshift_idle_reopen'])
            (output/'timeshift-idle-reopen-source.json').write_text(json.dumps(report['timeshift_idle_reopen'],indent=2)+'\n')
        if args.timeshift_media:
            spec = importlib.util.spec_from_file_location('timeshift_media_probe',tool_root/'scripts/rust-timeshift-contracts/media_probe.py')
            media = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(media)
            report['timeshift_media'] = media.capture(base,address('origin',8080),session_seed,output,observe)
            (output/'timeshift-media.json').write_text(json.dumps(report['timeshift_media'],indent=2)+'\n')
            for event in report['timeshift_media']['events']:
                if 'media_file' not in event:
                    continue
                decoded = command(compose+['exec','-T','baseline','ffmpeg','-hide_banner','-loglevel','error','-xerror','-progress','pipe:1','-nostats','-i','/evidence/'+event['media_file'],'-map','0:v:0','-map','0:a:0','-t','3','-f','null','-'],timeout=45)
                (output/(event['media_file']+'.decode.log')).write_text(decoded)
                frames = max((int(line.split('=')[1]) for line in decoded.splitlines() if line.startswith('frame=')),default=0)
                event['decoder'] = {'frames':frames,'audio_and_video_mapped':True,'strict_decoder_passed':frames>=50}
                if frames<50:
                    raise RuntimeError('archive media decoded insufficient video frames')
            (output/'timeshift-media.json').write_text(json.dumps(report['timeshift_media'],indent=2)+'\n')
        if args.timeshift_native_limit:
            spec = importlib.util.spec_from_file_location('native_limit_probe',tool_root/'scripts/rust-timeshift-contracts/limit_probe.py')
            native_limit = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(native_limit)
            def limit_control(action,label):
                result=command(compose+['exec','-T','baseline','/dispatcharrpy/bin/python','/timeshift-verification/limit_control.py',action,label]+(['redirect'] if args.timeshift_redirect_session else []),timeout=30)
                return json.loads(result.splitlines()[-1])
            def decode_limit(capture):
                for event in capture['events']:
                    if 'media_file' not in event:
                        continue
                    decoded=command(compose+['exec','-T','baseline','ffmpeg','-hide_banner','-loglevel','error','-xerror','-progress','pipe:1','-nostats','-i','/evidence/'+event['media_file'],'-map','0:v:0','-map','0:a:0','-t','3','-f','null','-'],timeout=45)
                    (output/(event['media_file']+'.decode.log')).write_text(decoded)
                    frames=max((int(line.split('=')[1]) for line in decoded.splitlines() if line.startswith('frame=')),default=0)
                    event['decoder']={'frames':frames,'strict_audio_video_decode_passed':frames>=50}
                    if frames<50:
                        raise RuntimeError('native positive-limit decoded insufficient frames')
            report['timeshift_native_limit']=native_limit.capture(base,address('origin',8080),session_seed,output,observe,limit_control,'source')
            decode_limit(report['timeshift_native_limit'])
            (output/'timeshift-native-limit-source.json').write_text(json.dumps(report['timeshift_native_limit'],indent=2)+'\n')
        if args.timeshift_rust_test:
            database_address = command(compose+['port','baseline','5432']).strip()
            redis_address = command(compose+['port','baseline','6379']).strip()
            test_environment = dict(environment, DATABASE_TEST_URL='postgresql://dispatch:playback-fixture-only@'+database_address+'/dispatcharr', REDIS_TEST_URL='redis://:playback-fixture-only@'+redis_address+'/0', VOD_DISPOSABLE_FIXTURE='playback-fixture-only', TIMESHIFT_OWNER_ACCESS=session_seed['users']['owner']['token'])
            if args.timeshift_rust_media:
                origin = address('origin',8080)
                reset = urllib.request.Request(origin+'/archive-control',method='POST',headers={'Content-Type':'application/json'},data=json.dumps({'mode':'all'}).encode())
                with urllib.request.urlopen(reset,timeout=5) as response:
                    if response.status != 200:
                        raise RuntimeError('native Rust archive fixture reset failed')
                test_environment.update(TIMESHIFT_ARCHIVE_ORIGIN=origin,TIMESHIFT_RUST_MEDIA_FILE=str(output/'timeshift-rust.ts'))
                if args.timeshift_idle_reopen:
                    test_environment['TIMESHIFT_RUST_IDLE']='1'
                if args.timeshift_busy_seek:
                    test_environment['TIMESHIFT_RUST_BUSY']='1'
            result = subprocess.run(['cargo','test','--manifest-path',str(ROOT/'rust/Cargo.toml'),'-p','dispatcharr-timeshift-http','--test','sql','--offline','--','--ignored','--nocapture'],env=test_environment,capture_output=True,text=True,timeout=180)
            (output/'rust-timeshift-sql.log').write_text(result.stdout+result.stderr)
            report['rust_timeshift_sql_passed'] = result.returncode == 0
            result.check_returncode()
            if args.timeshift_rust_media:
                captured = output/'timeshift-rust.ts'
                if captured.stat().st_size != 1_880_000:
                    raise RuntimeError('native Rust archive byte count mismatch')
                decoded = command(compose+['exec','-T','baseline','ffmpeg','-hide_banner','-loglevel','error','-xerror','-progress','pipe:1','-nostats','-i','/evidence/timeshift-rust.ts','-map','0:v:0','-map','0:a:0','-t','3','-f','null','-'],timeout=45)
                (output/'timeshift-rust.ts.decode.log').write_text(decoded)
                frames=max((int(line.split('=')[1]) for line in decoded.splitlines() if line.startswith('frame=')),default=0)
                report['rust_timeshift_media']={'native_http_passed':True,'positive_limit_live_refusal_and_retry_verified':True,'store_auth':'real Django schema, AuthService and JWT handshake','bytes':captured.stat().st_size,'sha256':hashlib.sha256(captured.read_bytes()).hexdigest(),'strict_audio_and_video_decode_passed':frames>=50,'frames':frames,'source_bound_candidate_image':False,'same_session_idle_reopen_actual_sql_passed':args.timeshift_idle_reopen,'same_session_busy_controls_actual_sql_passed':args.timeshift_busy_seek}
                if frames<50:
                    raise RuntimeError('native Rust archive decode insufficient frames')
        if args.timeshift_rust_management:
            database_address=command(compose+['port','baseline','5432']).strip()
            redis_address=command(compose+['port','baseline','6379']).strip()
            xc_output=output/'timeshift-management-native.json'
            test_environment=dict(environment,DATABASE_TEST_URL='postgresql://dispatch:playback-fixture-only@'+database_address+'/dispatcharr',REDIS_TEST_URL='redis://:playback-fixture-only@'+redis_address+'/0',DIRECT_DISPOSABLE_FIXTURE='playback-fixture-only',TIMESHIFT_NATIVE_KIND='management',TIMESHIFT_MANAGEMENT_ACTIVE='1' if args.timeshift_management_active else '0',TIMESHIFT_DIRECT_COMPOSE=json.dumps(compose),TIMESHIFT_DIRECT_SEED=json.dumps(session_seed),TIMESHIFT_DIRECT_DRIVER=str(tool_root/'scripts/rust-timeshift-contracts/native_direct_driver.py'),TIMESHIFT_DIRECT_OUTPUT=str(xc_output),TIMESHIFT_DIRECT_PHYSICAL_ORIGIN=address('origin',8080),TIMESHIFT_DIRECT_ORACLE=str(output/'timeshift-management-source.json'))
            result=subprocess.run(['cargo','test','--manifest-path',str(ROOT/'rust/Cargo.toml'),'-p','dispatcharr-timeshift-http','--test','direct_sql','--locked','--offline','--','--ignored','--nocapture'],env=test_environment,capture_output=True,text=True,timeout=180)
            (output/'rust-timeshift-management.log').write_text(result.stdout+result.stderr)
            report['rust_timeshift_management']={'passed':result.returncode==0,'source_bound_candidate_image':False,'application_parity_verified':False,'performance_verified':False}
            result.check_returncode()
            report['native_timeshift_management']=json.loads(xc_output.read_text())
            report['timeshift_native_management_comparison']=report['native_timeshift_management']['source_comparison']
        if args.timeshift_rust_xc:
            database_address=command(compose+['port','baseline','5432']).strip()
            redis_address=command(compose+['port','baseline','6379']).strip()
            xc_output=output/'timeshift-xc-native.json'
            test_environment=dict(environment,DATABASE_TEST_URL='postgresql://dispatch:playback-fixture-only@'+database_address+'/dispatcharr',REDIS_TEST_URL='redis://:playback-fixture-only@'+redis_address+'/0',DIRECT_DISPOSABLE_FIXTURE='playback-fixture-only',TIMESHIFT_NATIVE_KIND='xc',TIMESHIFT_DIRECT_COMPOSE=json.dumps(compose),TIMESHIFT_DIRECT_SEED=json.dumps(session_seed),TIMESHIFT_DIRECT_DRIVER=str(tool_root/'scripts/rust-timeshift-contracts/native_direct_driver.py'),TIMESHIFT_DIRECT_OUTPUT=str(xc_output),TIMESHIFT_DIRECT_PHYSICAL_ORIGIN=address('origin',8080),TIMESHIFT_DIRECT_ORACLE=str(output/'timeshift-xc-source.json'))
            result=subprocess.run(['cargo','test','--manifest-path',str(ROOT/'rust/Cargo.toml'),'-p','dispatcharr-timeshift-http','--test','direct_sql','--locked','--offline','--','--ignored','--nocapture'],env=test_environment,capture_output=True,text=True,timeout=180)
            (output/'rust-timeshift-xc.log').write_text(result.stdout+result.stderr)
            report['rust_timeshift_xc']={'passed':result.returncode==0,'source_bound_candidate_image':False,'application_parity_verified':False,'performance_verified':False}
            result.check_returncode()
            report['native_timeshift_xc']=json.loads(xc_output.read_text())
            report['timeshift_native_xc_comparison']=report['native_timeshift_xc']['source_comparison']
        if args.timeshift_rust_direct:
            database_address=command(compose+['port','baseline','5432']).strip()
            redis_address=command(compose+['port','baseline','6379']).strip()
            direct_output=output/'timeshift-direct-native.json'
            test_environment=dict(environment,DATABASE_TEST_URL='postgresql://dispatch:playback-fixture-only@'+database_address+'/dispatcharr',REDIS_TEST_URL='redis://:playback-fixture-only@'+redis_address+'/0',DIRECT_DISPOSABLE_FIXTURE='playback-fixture-only',TIMESHIFT_DIRECT_COMPOSE=json.dumps(compose),TIMESHIFT_DIRECT_SEED=json.dumps(session_seed),TIMESHIFT_DIRECT_DRIVER=str(tool_root/'scripts/rust-timeshift-contracts/native_direct_driver.py'),TIMESHIFT_DIRECT_OUTPUT=str(direct_output),TIMESHIFT_DIRECT_PHYSICAL_ORIGIN=address('origin',8080),TIMESHIFT_DIRECT_ORACLE=str(output/'timeshift-direct-auth-source.json'))
            result=subprocess.run(['cargo','test','--manifest-path',str(ROOT/'rust/Cargo.toml'),'-p','dispatcharr-timeshift-http','--test','direct_sql','--locked','--offline','--','--ignored','--nocapture'],env=test_environment,capture_output=True,text=True,timeout=180)
            (output/'rust-timeshift-direct.log').write_text(result.stdout+result.stderr)
            report['rust_timeshift_direct']={'passed':result.returncode==0,'source_bound_candidate_image':False,'application_parity_verified':False,'performance_verified':False}
            result.check_returncode()
            report['native_timeshift_direct_auth']=json.loads(direct_output.read_text())
            report['timeshift_native_direct_auth_comparison']=direct.compare(report['timeshift_direct_auth_source'],report['native_timeshift_direct_auth'])
        if args.xc_store_auth:
            fixture_tool=(tool_root/'scripts/rust-integration/xc_fixture.py').read_bytes()
            (output/'xc-fixture-tool.py').write_bytes(fixture_tool)
            fixture_result=subprocess.run(compose+['exec','-T','baseline','/dispatcharrpy/bin/python','-'],env=environment,input=fixture_tool.decode(),capture_output=True,text=True,timeout=120)
            (output/'xc-source.log').write_text(fixture_result.stderr)
            fixture_result.check_returncode()
            fixture=json.loads(fixture_result.stdout.splitlines()[-1])
            fixture_path=output/'xc-store-fixture.json'
            fixture_path.write_text(json.dumps(fixture,indent=2)+'\n')
            captured={str(path.relative_to(ROOT)):path.read_bytes() for path in sorted((ROOT/'rust').rglob('*')) if path.is_file() and not set(path.relative_to(ROOT/'rust').parts)&{'target','.git','__pycache__'}}
            frozen=temporary/'xc-rust-source'
            for relative,content in captured.items():
                path=frozen/relative;path.parent.mkdir(parents=True,exist_ok=True);path.write_bytes(content)
            database_address=command(compose+['port','baseline','5432']).strip()
            redis_address=command(compose+['port','baseline','6379']).strip()
            test_environment=dict(environment,DATABASE_TEST_URL='postgresql://dispatch:playback-fixture-only@'+database_address+'/dispatcharr',REDIS_TEST_URL='redis://:playback-fixture-only@'+redis_address+'/0',XC_DISPOSABLE_FIXTURE='playback-fixture-only',XC_AUTH_FIXTURE=str(fixture_path),CARGO_TARGET_DIR=str(ROOT/'rust/target/xc-sql-verification'))
            result=subprocess.run(['cargo','test','--manifest-path',str(frozen/'rust/Cargo.toml'),'-p','dispatcharr-store','--test','xc_sql','--locked','--offline','--','--ignored','--nocapture'],env=test_environment,capture_output=True,text=True,timeout=180)
            (output/'rust-xc-sql.log').write_text(result.stdout+result.stderr)
            report['xc_store_auth']={'passed':result.returncode==0,'fixture_tool_sha256':hashlib.sha256(fixture_tool).hexdigest(),'fixture_sha256':hashlib.sha256(fixture_path.read_bytes()).hexdigest(),'rust_source_sha256':{relative:hashlib.sha256(content).hexdigest() for relative,content in captured.items()},'auth_cases':len(fixture['auth_cases']),'channel_cases':len(fixture['channel_cases']),'event_transport':'original actual Channels layer; native test sink envelope comparison only','source_bound_candidate_image':False,'application_parity_verified':False,'performance_verified':False}
            result.check_returncode()
        if args.rust_ingest_test:
            # Capture both executable sides before invocation. Other agents may
            # keep editing the repository without changing this running trial.
            fixture_tool=(tool_root/'scripts/rust-ingest-parity/sql_fixture.py').read_bytes()
            (output/'ingest-fixture-tool.py').write_bytes(fixture_tool)
            fixture_result=subprocess.run(compose+['exec','-T','baseline','/dispatcharrpy/bin/python','-'],env=environment,input=fixture_tool.decode(),capture_output=True,text=True,timeout=60)
            (output/'ingest-source.log').write_text(fixture_result.stderr)
            fixture_result.check_returncode()
            fixture=json.loads(fixture_result.stdout.splitlines()[-1])
            fixture_path=output/'ingest-batch-fixture.json'
            fixture_path.write_text(json.dumps(fixture,indent=2)+'\n')
            captured={str(path.relative_to(ROOT)):path.read_bytes() for path in sorted((ROOT/'rust').rglob('*')) if path.is_file() and not set(path.relative_to(ROOT/'rust').parts)&{'target','.git','__pycache__'}}
            frozen=temporary/'ingest-rust-source'
            for relative,content in captured.items():
                path=frozen/relative;path.parent.mkdir(parents=True,exist_ok=True);path.write_bytes(content)
            database_address=command(compose+['port','baseline','5432']).strip()
            test_environment=dict(environment,DATABASE_TEST_URL='postgresql://dispatch:playback-fixture-only@'+database_address+'/dispatcharr',INGEST_DISPOSABLE_FIXTURE='playback-fixture-only',INGEST_BATCH_FIXTURE=str(fixture_path),CARGO_TARGET_DIR=str(ROOT/'rust/target/ingest-sql-verification'))
            result=subprocess.run(['cargo','test','--manifest-path',str(frozen/'rust/Cargo.toml'),'-p','dispatcharr-store','--test','import_sql','--locked','--offline','--','--ignored','--nocapture'],env=test_environment,capture_output=True,text=True,timeout=600)
            (output/'rust-ingest-sql.log').write_text(result.stdout+result.stderr)
            report['rust_ingest_sql']={'passed':result.returncode==0,'fixture_tool_sha256':hashlib.sha256(fixture_tool).hexdigest(),'fixture_sha256':hashlib.sha256(fixture_path.read_bytes()).hexdigest(),'rust_source_sha256':{relative:hashlib.sha256(content).hexdigest() for relative,content in captured.items()},'cases':[step['name'] for step in fixture['steps']],'group_cases':[step['name'] for step in fixture.get('group_steps',[])],'application_performance_verified':False}
            report['rust_ingest_sql']['cleanup_cases']=[step['name'] for step in fixture.get('cleanup',{}).get('steps',[])]
            report['rust_ingest_sql']['group_cleanup_cases']=[step['name'] for step in fixture.get('group_cleanup',{}).get('steps',[])]
            result.check_returncode()
        inspection = json.loads(command([args.docker,'inspect',container('baseline')]))[0]
        report['source_image_id'] = inspection['Image']
        report['wire'] = probe(base,seed['native_path'],capture_fullfile=output/'baseline-full.mp4')
        if args.profile_aliases:
            report['profile_aliases'] = profile_alias_probe(base,seed['native_path'],seed['provider_profile_id'])
            (output/'profile-aliases.json').write_text(json.dumps(report['profile_aliases'],indent=2)+'\n')
        if args.rust_resolver_test:
            database_address = command(compose+['port','baseline','5432']).strip()
            database_environment = dict(environment, DATABASE_TEST_URL='postgresql://dispatch:playback-fixture-only@'+database_address+'/dispatcharr', VOD_DISPOSABLE_FIXTURE='playback-fixture-only')
            result = subprocess.run(['cargo','test','--manifest-path',str(ROOT/'rust/Cargo.toml'),'-p','dispatcharr-vod-http','--test','sql','--offline','--','--ignored','--nocapture'],env=database_environment,capture_output=True,text=True,timeout=180)
            (output/'rust-sql-resolver.log').write_text(result.stdout+result.stderr)
            report['rust_sql_resolver_passed'] = result.returncode == 0
            result.check_returncode()
        if candidate_manifest:
            report['candidate_reset'] = {'mode':'baseline web stopped; only isolated Redis cleared'}
            report['candidate_reset']['web_stop'] = json.loads(command(compose+['exec','-T','baseline','/dispatcharrpy/bin/python','/verification/supervisor.py','control','stop']))
            report['candidate_reset']['redis_keys_before'] = int(command(compose+['exec','-T','baseline','redis-cli','dbsize']))
            report['candidate_reset']['redis_clear'] = command(compose+['exec','-T','baseline','redis-cli','FLUSHDB']).strip()
            command(compose+['up','-d','--no-deps','--no-build','candidate'])
            candidate = address('candidate',9192)
            deadline = time.monotonic()+60
            while True:
                try:
                    with urllib.request.urlopen(candidate+'/health/ready',timeout=2) as response:
                        if json.load(response).get('status') == 'ready':
                            break
                except (OSError,ValueError):
                    pass
                if time.monotonic()>deadline:
                    raise TimeoutError('candidate readiness timed out')
                time.sleep(.25)
            inspected = json.loads(command([args.docker,'inspect',container('candidate')]))[0]
            spec = importlib.util.spec_from_file_location('playback_build_adapter',tool_root/'scripts/rust-playback-baseline/run.py')
            playback_adapter = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(playback_adapter)
            source_hash = playback_adapter.validate_build_manifest(candidate_manifest,inspected['Image'],(inspected['Config'].get('Labels') or {}).get('org.opencontainers.image.source-tree-sha256'))
            report['candidate_provenance'] = {'image_id':inspected['Image'],'source_tree_sha256':source_hash,'build_manifest_sha256':hashlib.sha256(candidate_manifest_bytes).hexdigest()}
            report['candidate_wire'] = probe(candidate,seed['native_path'],capture_fullfile=output/'candidate-full.mp4')
            (output/'candidate-wire.json').write_text(json.dumps(report['candidate_wire'],indent=2)+'\n')
            if args.profile_aliases:
                report['candidate_profile_aliases'] = profile_alias_probe(candidate,seed['native_path'],seed['provider_profile_id'])
            spec = importlib.util.spec_from_file_location('vod_comparison',tool_root/'scripts/rust-vod-contracts/compare.py')
            comparison_adapter = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(comparison_adapter)
            report['wire_comparison'] = comparison_adapter.compare(report['wire'],report['candidate_wire'])
            if args.profile_aliases and args.source_frontend_from_candidate:
                alias_differences = []
                for source_alias,candidate_alias in zip(report['profile_aliases']['results'],report['candidate_profile_aliases']['results'],strict=True):
                    for key in ['profile_id','mint_status','mint_headers','follow_status','follow_headers','follow_bytes','follow_body_sha256']:
                        if comparison_adapter.semantic(source_alias.get(key)) != comparison_adapter.semantic(candidate_alias.get(key)):
                            alias_differences.append({'profile_id':source_alias['profile_id'],'field':key,'source':source_alias.get(key),'candidate':candidate_alias.get(key)})
                report['profile_alias_comparison'] = {'captured_wire_matches':not alias_differences,'differences':alias_differences,'application_parity_verified':False,'performance_verified':False}
            if args.timeshift_management:
                report['candidate_timeshift_management']=management.capture(candidate,session_seed,management_control,'candidate',active=args.timeshift_management_active)
                report['timeshift_management_comparison']=management.compare(report['timeshift_management_source'],report['candidate_timeshift_management'])
                (output/'timeshift-management-candidate.json').write_text(json.dumps(report['candidate_timeshift_management'],indent=2)+'\n')
            if args.timeshift_xc:
                report['candidate_timeshift_xc']=xc.capture(candidate,xc_control,'candidate')
                report['timeshift_xc_comparison']=xc.compare(report['timeshift_xc_source'],report['candidate_timeshift_xc'])
                (output/'timeshift-xc-candidate.json').write_text(json.dumps(report['candidate_timeshift_xc'],indent=2)+'\n')
            if args.timeshift_direct_auth:
                report['candidate_timeshift_direct_auth']=direct.capture(candidate,session_seed,direct_control,'candidate')
                report['timeshift_direct_auth_comparison']=direct.compare(report['timeshift_direct_auth_source'],report['candidate_timeshift_direct_auth'])
                (output/'timeshift-direct-auth-candidate.json').write_text(json.dumps(report['candidate_timeshift_direct_auth'],indent=2)+'\n')
            if args.timeshift_busy_seek:
                report['candidate_timeshift_busy_seek']=busy.capture(candidate,address('origin',8080),session_seed,observe)
                report['timeshift_busy_seek_comparison']=busy.compare(report['timeshift_busy_seek_source'],report['candidate_timeshift_busy_seek'])
                (output/'timeshift-busy-seek-candidate.json').write_text(json.dumps(report['candidate_timeshift_busy_seek'],indent=2)+'\n')
            if args.timeshift_idle_reopen:
                report['candidate_timeshift_idle_reopen']=idle.capture(candidate,address('origin',8080),session_seed,output,observe,prefix='timeshift-candidate-idle')
                decode_idle(report['candidate_timeshift_idle_reopen'])
                report['timeshift_idle_reopen_comparison']=idle.compare(report['timeshift_idle_reopen'],report['candidate_timeshift_idle_reopen'])
                (output/'timeshift-idle-reopen-candidate.json').write_text(json.dumps(report['candidate_timeshift_idle_reopen'],indent=2)+'\n')
            if args.timeshift_native_limit:
                report['candidate_timeshift_native_limit']=native_limit.capture(candidate,address('origin',8080),session_seed,output,observe,limit_control,'candidate')
                decode_limit(report['candidate_timeshift_native_limit'])
                (output/'timeshift-native-limit-candidate.json').write_text(json.dumps(report['candidate_timeshift_native_limit'],indent=2)+'\n')
                differences=[]
                for original,rewritten in zip(report['timeshift_native_limit']['events'],report['candidate_timeshift_native_limit']['events'],strict=True):
                    for key in ['case','status','headers','bytes','sha256','error_body']:
                        if original.get(key)!=rewritten.get(key):
                            differences.append({'case':original['case'],'field':key,'source':original.get(key),'candidate':rewritten.get(key)})
                    for key in ['api_exists','pool_exists','pool_busy','profile_connections','stats_client_count']:
                        if original['state'].get(key)!=rewritten['state'].get(key):
                            differences.append({'case':original['case'],'field':'state.'+key,'source':original['state'].get(key),'candidate':rewritten['state'].get(key)})
                report['timeshift_native_limit_comparison']={'captured_wire_matches':not differences,'differences':differences,'application_parity_verified':False,'performance_verified':False}
            if args.candidate_timeshift_media:
                spec = importlib.util.spec_from_file_location('timeshift_comparison',tool_root/'scripts/rust-timeshift-contracts/compare.py')
                timeshift_compare = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(timeshift_compare)
                report['candidate_timeshift_sessions'] = module.capture(candidate,session_seed)
                report['timeshift_session_comparison'] = timeshift_compare.compare_sessions(report['timeshift_sessions'],report['candidate_timeshift_sessions'])
                report['candidate_timeshift_media'] = media.capture(candidate,address('origin',8080),session_seed,output,observe,prefix='timeshift-candidate')
                for event in report['candidate_timeshift_media']['events']:
                    if 'media_file' not in event:
                        continue
                    decoded = command(compose+['exec','-T','baseline','ffmpeg','-hide_banner','-loglevel','error','-xerror','-progress','pipe:1','-nostats','-i','/evidence/'+event['media_file'],'-map','0:v:0','-map','0:a:0','-t','3','-f','null','-'],timeout=45)
                    (output/(event['media_file']+'.decode.log')).write_text(decoded)
                    frames=max((int(line.split('=')[1]) for line in decoded.splitlines() if line.startswith('frame=')),default=0)
                    event['decoder']={'frames':frames,'audio_and_video_mapped':True,'strict_decoder_passed':frames>=50}
                    if frames<50:
                        raise RuntimeError('candidate native archive decoded insufficient frames')
                report['timeshift_media_comparison'] = timeshift_compare.compare_media(report['timeshift_media'],report['candidate_timeshift_media'])
                (output/'candidate-timeshift-sessions.json').write_text(json.dumps(report['candidate_timeshift_sessions'],indent=2)+'\n')
                (output/'candidate-timeshift-media.json').write_text(json.dumps(report['candidate_timeshift_media'],indent=2)+'\n')
        report['decoders'] = []
        for backend in ['baseline']+(['candidate'] if candidate_manifest else []):
            decoded = command(compose+['exec','-T','baseline','ffmpeg','-hide_banner','-loglevel','error','-xerror','-progress','pipe:1','-nostats','-i','/evidence/'+backend+'-full.mp4','-map','0:v:0','-map','0:a:0','-t','3','-f','null','-'],timeout=45)
            (output/(backend+'-decode.log')).write_text(decoded)
            frames = max((int(line.split('=')[1]) for line in decoded.splitlines() if line.startswith('frame=')),default=0)
            report['decoders'].append({'backend':backend,'frames':frames,'audio_and_video_mapped':True,'strict_decoder_passed':frames>=50})
            if frames<50:
                raise RuntimeError('native VOD decoded insufficient video frames')
        origin = address('origin',8080)
        with urllib.request.urlopen(origin+'/stats',timeout=5) as response:
            report['origin'] = json.load(response)
        # A characterization succeeds only when every request was recorded.
        # It does not claim that baseline defects satisfy RFC range behavior.
        report['passed'] = len(report['wire']['events']) == 7 and (not candidate_manifest or report['wire_comparison']['captured_wire_matches'])
        if args.timeshift_management:
            report['passed'] = report['passed'] and report['timeshift_management_source']['capture_complete'] and (not candidate_manifest or report['timeshift_management_comparison']['captured_management_matches'])
        if args.timeshift_xc:
            report['passed'] = report['passed'] and report['timeshift_xc_source']['capture_complete'] and (not candidate_manifest or report['timeshift_xc_comparison']['captured_direct_auth_matches'])
        if args.timeshift_rust_management:
            report['passed'] = report['passed'] and report['timeshift_native_management_comparison']['captured_management_matches']
        if args.timeshift_rust_xc:
            report['passed'] = report['passed'] and report['timeshift_native_xc_comparison']['captured_direct_auth_matches']
        if args.timeshift_rust_direct:
            report['passed'] = report['passed'] and report['timeshift_native_direct_auth_comparison']['captured_direct_auth_matches']
        if args.timeshift_direct_auth:
            report['passed'] = report['passed'] and report['timeshift_direct_auth_source']['capture_complete'] and (not candidate_manifest or report['timeshift_direct_auth_comparison']['captured_direct_auth_matches'])
        if args.timeshift_busy_seek:
            report['passed'] = report['passed'] and report['timeshift_busy_seek_source']['capture_complete'] and (not candidate_manifest or report['timeshift_busy_seek_comparison']['captured_busy_controls_match'])
        if args.timeshift_idle_reopen:
            report['passed'] = report['passed'] and report['timeshift_idle_reopen']['capture_complete'] and (not candidate_manifest or report['timeshift_idle_reopen_comparison']['captured_idle_reopen_matches'])
        if args.candidate_timeshift_media:
            report['passed'] = report['passed'] and report['timeshift_session_comparison']['captured_wire_matches'] and report['timeshift_media_comparison']['captured_media_matches']
        if args.timeshift_native_limit:
            report['passed'] = report['passed'] and report['timeshift_native_limit']['expected_control_behaviors_passed'] and (not candidate_manifest or report['candidate_timeshift_native_limit']['expected_control_behaviors_passed'] and report['timeshift_native_limit_comparison']['captured_wire_matches'])
        if args.profile_aliases and args.source_frontend_from_candidate:
            report['passed'] = report['passed'] and report['profile_alias_comparison']['captured_wire_matches']
    except BaseException as error:
        report['failure'] = type(error).__name__
        raise
    finally:
        try:
            command(compose+['logs','--no-color'],timeout=30)
            command(compose+['down','-v','--remove-orphans','--timeout','15'],timeout=60)
            report['cleanup_passed'] = True
        except Exception as error:
            report['cleanup_passed'] = False
            report['cleanup_error'] = type(error).__name__
            report['passed'] = False
        if report['cleanup_passed']:
            shutil.rmtree(temporary)
        else:
            report['retained_source_directory'] = str(source)
        (output/'vod-contracts.json').write_text(json.dumps(report,indent=2)+'\n')
    print(json.dumps({'recorded': report['passed'], 'cleanup_passed': report['cleanup_passed']}))
    if not report['passed']:
        raise SystemExit(1)

if __name__ == '__main__':
    main()
