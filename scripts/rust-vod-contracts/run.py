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
from probe import probe, profile_alias_probe

ROOT = Path(__file__).resolve().parents[2]
BASELINE = '1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430'


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--docker', default='docker')
    parser.add_argument('--source-frontend-from-candidate', action='store_true', help='Mount unchanged built React from immutable candidate into source fixture; requires candidate manifest')
    parser.add_argument('--profile-aliases', action='store_true', help='Separately characterize bare-profile redirects and their follow status')
    parser.add_argument('--candidate-overlay', type=Path)
    parser.add_argument('--candidate-build-manifest', type=Path)
    parser.add_argument('--timeshift-media', action='store_true', help='Capture paced native archive ranges, URL fallback, and cleanup')
    parser.add_argument('--timeshift-rust-test', action='store_true', help='Run actual Store/Auth/Redis catch-up contract; implies --timeshift-sessions')
    parser.add_argument('--timeshift-sessions', action='store_true', help='Capture original native catch-up session API contracts')
    parser.add_argument('--rust-resolver-test', action='store_true', help='Run the actual-schema Rust resolver test against this disposable database')
    args = parser.parse_args()
    if args.timeshift_rust_test or args.timeshift_media:
        args.timeshift_sessions = True
    if bool(args.candidate_overlay) != bool(args.candidate_build_manifest):
        parser.error('candidate overlay and immutable build manifest must be provided together')
    if args.source_frontend_from_candidate and not args.candidate_build_manifest:
        parser.error('source React fixture requires immutable candidate manifest')
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=True)
    if any(output.iterdir()):
        parser.error('output must be empty')
    project = 'dispatcharr-vod-contract-' + uuid.uuid4().hex[:10]
    temporary = Path(tempfile.mkdtemp(prefix='dispatcharr-vod-contract-'))
    source = temporary/'source'
    source.mkdir()
    environment = dict(os.environ, BASELINE_SOURCE_DIR=str(source), BASELINE_EVIDENCE_DIR=str(output))
    compose = [args.docker, 'compose', '-p', project, '-f', str(ROOT/'docker/compose.rust-playback.yml'),
               '-f', str(ROOT/'scripts/rust-vod-contracts/compose.yml')]
    candidate_manifest = None
    if args.candidate_overlay:
        candidate_manifest = json.loads(args.candidate_build_manifest.read_text())
        sys.path.insert(0,str(ROOT/'scripts/rust-integration'))
        spec = importlib.util.spec_from_file_location('integration_adapter', ROOT/'scripts/rust-integration/run.py')
        integration = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(integration)
        overlay = integration.candidate_overlay(args.candidate_overlay.resolve(), output/'candidate-overlay.yml', candidate_manifest.get('tag') or candidate_manifest['image_id'])
        compose += ['-f',str(overlay)]
    report = {'format_version': 1, 'scope': 'original Python native XC VOD wire contracts',
              'source_commit': BASELINE, 'passed': False, 'project': project,
              'performance_verified': False, 'parity_verified': False}
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
        command(compose+['up','-d','baseline'])
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
            spec = __import__('importlib.util', fromlist=['spec_from_file_location']).spec_from_file_location('timeshift_session_probe', ROOT/'scripts/rust-timeshift-contracts/probe.py')
            module = __import__('importlib.util', fromlist=['module_from_spec']).module_from_spec(spec)
            spec.loader.exec_module(module)
            report['timeshift_sessions'] = module.capture(base,session_seed)
            (output/'timeshift-sessions.json').write_text(json.dumps(report['timeshift_sessions'],indent=2)+'\n')
            if not report['timeshift_sessions']['capture_complete']:
                raise RuntimeError('Timeshift session source capture incomplete')

        if args.timeshift_media:
            spec = importlib.util.spec_from_file_location('timeshift_media_probe',ROOT/'scripts/rust-timeshift-contracts/media_probe.py')
            media = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(media)
            def observe(session_id):
                result = subprocess.run(compose+['exec','-T','baseline','/dispatcharrpy/bin/python','/timeshift-verification/inspect_session.py'],env=environment,input=json.dumps({'session_id':session_id,'profile_id':seed['provider_profile_id']}),capture_output=True,text=True,timeout=15)
                result.check_returncode()
                return json.loads(result.stdout.splitlines()[-1])
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
        if args.timeshift_rust_test:
            database_address = command(compose+['port','baseline','5432']).strip()
            redis_address = command(compose+['port','baseline','6379']).strip()
            test_environment = dict(environment, DATABASE_TEST_URL='postgresql://dispatch:playback-fixture-only@'+database_address+'/dispatcharr', REDIS_TEST_URL='redis://:playback-fixture-only@'+redis_address+'/0', VOD_DISPOSABLE_FIXTURE='playback-fixture-only', TIMESHIFT_OWNER_ACCESS=session_seed['users']['owner']['token'])
            result = subprocess.run(['cargo','test','--manifest-path',str(ROOT/'rust/Cargo.toml'),'-p','dispatcharr-timeshift-http','--test','sql','--offline','--','--ignored','--nocapture'],env=test_environment,capture_output=True,text=True,timeout=180)
            (output/'rust-timeshift-sql.log').write_text(result.stdout+result.stderr)
            report['rust_timeshift_sql_passed'] = result.returncode == 0
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
            spec = importlib.util.spec_from_file_location('playback_build_adapter',ROOT/'scripts/rust-playback-baseline/run.py')
            playback_adapter = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(playback_adapter)
            source_hash = playback_adapter.validate_build_manifest(candidate_manifest,inspected['Image'],(inspected['Config'].get('Labels') or {}).get('org.opencontainers.image.source-tree-sha256'))
            report['candidate_provenance'] = {'image_id':inspected['Image'],'source_tree_sha256':source_hash,'build_manifest_sha256':hashlib.sha256(args.candidate_build_manifest.read_bytes()).hexdigest()}
            report['candidate_wire'] = probe(candidate,seed['native_path'],capture_fullfile=output/'candidate-full.mp4')
            (output/'candidate-wire.json').write_text(json.dumps(report['candidate_wire'],indent=2)+'\n')
            if args.profile_aliases:
                report['candidate_profile_aliases'] = profile_alias_probe(candidate,seed['native_path'],seed['provider_profile_id'])
            spec = importlib.util.spec_from_file_location('vod_comparison',ROOT/'scripts/rust-vod-contracts/compare.py')
            comparison_adapter = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(comparison_adapter)
            report['wire_comparison'] = comparison_adapter.compare(report['wire'],report['candidate_wire'])
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
