#!/usr/bin/env python3
"""Run isolated Compose native VOD Python characterization, then clean up."""
import argparse
import hashlib
import io
import importlib.util
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import tarfile
import time
import urllib.request
import urllib.parse
import uuid
from vod_tool_snapshot import snapshot_sources

SCRIPT_ROOT = Path(__file__).resolve().parents[2]
ROOT = Path(os.environ.get('DISPATCHARR_CONTRACT_REPO',str(SCRIPT_ROOT))).resolve()
BASELINE = '1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430'


def rootless_port_collision(stderr):
    return isinstance(stderr,str) and 'RootlessKit PortManager.AddPort()' in stderr and 'bind: address already in use' in stderr


class ActiveSourceComplete(Exception):
    """Only the new isolated Source active-session capture completed."""

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--docker', default='docker')
    parser.add_argument('--source-frontend-from-candidate', action='store_true', help='Mount unchanged built React from immutable candidate into source fixture; requires candidate manifest')
    parser.add_argument('--profile-aliases', action='store_true', help='Separately characterize bare-profile redirects and their follow status')
    parser.add_argument('--timeshift-redirect-session', action='store_true', help='Use Redirect channel with empty allowed profiles to verify established capability still proxies')
    parser.add_argument('--m3u-rust-http', action='store_true', help='Compare actual native M3U HTTP/auth/cache with original source')
    parser.add_argument('--m3u-rust-renderer', action='store_true', help='Compare native SQL/renderer positive export bytes with original source')
    parser.add_argument('--vod-active-long-prefix', action='store_true', help='Modifier: retainactualold/youngprefixesbounded1MiBeachfordecoderinteriorverification')
    parser.add_argument('--vod-active-fast-reopen', action='store_true', help='Modifier: reopenimmediatelyafterRedisready, beforeold34blocksample/monitorgrace')
    parser.add_argument('--vod-active-recovery-characterize', action='store_true', help='Modifier: recordpolicy/faultoutcomeswithoutrequiringanonymouscontinuitycontract; preservesallchecks')
    parser.add_argument('--vod-header-stripped-native', action='store_true', help='Compare actual Original/native authenticated redirect and headerless follows; optional source seek flag')
    parser.add_argument('--vod-header-stripped-mapping-loss-source', action='store_true', help='Source-only expired/deleted mapping before first headerless stream')
    parser.add_argument('--vod-header-stripped-state-loss-source', action='store_true', help='Source-only controlled public metadata/mapping/provider loss between headerless siblings')
    parser.add_argument('--vod-header-stripped-seek-source', action='store_true', help='Source-only authenticated301 then headerless follow and same-SID range seek')
    parser.add_argument('--vod-header-stripped-follow-source', action='store_true', help='Source-only actual Bearer redirect then two headerless same-SID bodies')
    parser.add_argument('--vod-optional-session-lookup-source', action='store_true', help='Source-only exact optional Redis-to-user lookup error and parsing corpus')
    parser.add_argument('--vod-active-admin-stop-entry-source', action='store_true', help='Source-only additional same-ID request while actual admin stop remains pending')
    parser.add_argument('--vod-active-admin-stop-source', action='store_true', help='Source-only actual authenticated admin stop while old and younger bodies remain open')
    parser.add_argument('--vod-active-auth-loss', action='store_true', help='Modifier: omit initially validBearer after actualRedisstate-loss; recordsrequest/publicidentitychange')
    parser.add_argument('--vod-active-capped-viewer', action='store_true', help='Modifier: actualBearerJWT fixtureviewer withstream_limit1 foractiveVODcapture')
    parser.add_argument('--vod-active-idle-same-entry', action='store_true', help='Qualify each backend freshpublicactive0 window before actualsameSIDrequest')
    parser.add_argument('--vod-active-idle-follow-source', action='store_true', help='Source-only followRedirectidle-adoption andobserveproviderreservation/oldtimer')
    parser.add_argument('--vod-active-idle-redirect-source', action='store_true', help='Source-only Redirect idle-adoption afterstate-lossreopen whileyoungerbodyheld')
    parser.add_argument('--vod-active-idle-mint-source', action='store_true', help='Source-only no-SIDmint whilecurrentpublicidle/oldbodyclosed butyoungerbodyheld')
    parser.add_argument('--vod-active-recovery-native', action='store_true', help='Source/native actualHTTP fullrecovery schedule: activeentry, oldprogress/close, retiredreopen; requiresopt-in frozenimage')
    parser.add_argument('--vod-active-retired-reopen-source', action='store_true', help='Source-only same-ID public reopen after oldclose removesmetadata whileyoungerbodycontinues')
    parser.add_argument('--vod-active-extra-entry-source', action='store_true', help='Source-only third same-ID request after state-loss reopen, while old and younger bodies stay open')
    parser.add_argument('--vod-active-old-close-source', action='store_true', help='Source-only old-body close effects while a younger same-ID body stays open')
    parser.add_argument('--vod-active-old-progress-source', action='store_true', help='Source-only old body100th-yield telemetry after same-ID reopen')
    parser.add_argument('--vod-active-restart-http-source', action='store_true', help='Source-only actual HTTP active VOD Redis state-loss characterization')
    parser.add_argument('--vod-active-restart-native', action='store_true', help='Capture Original active state loss then frozen native actual HTTP behavior; no automatic parity acceptance')
    parser.add_argument('--vod-active-restart-source', action='store_true', help='Characterize actual active Original VOD iterator over owned Redis-only state loss; isolated source-only mode')
    parser.add_argument('--vod-rapid-reclose-source', action='store_true', help='Characterize actual original rapid-reclose duplicate-stop timer race separately')
    parser.add_argument('--vod-rapid-reclose-rust', action='store_true', help='Compare original rapid-reclose event multiplicity, native fenced cleanup and retained timer admission')
    parser.add_argument('--vod-rapid-reclose-ws', action='store_true', help='Compare actual rapid-reclose HTTP, packaged WebSocket multiplicity, full DTOs, audits and Redis cleanup')
    parser.add_argument('--vod-progress-source', action='store_true', help='Capture original finalpartial/full100thchunk consumed-byte telemetry boundaries')
    parser.add_argument('--vod-progress-rust', action='store_true', help='Compare native finalpartial100thchunk telemetry with actual original provider/Redis')
    parser.add_argument('--timeshift-playback-ws', action='store_true', help='Capture actual original/native WebSocket fullstats during six decoded archive media/lifecycle cases')
    parser.add_argument('--vod-playback-ws', action='store_true', help='Compare actual original/native HTTP playback and packaged WebSocket lifecycle/fullstats delivery')
    parser.add_argument('--vod-lifecycle-rust', action='store_true', help='Compare native VOD event/body timing and real Redis cleanup against original manager contracts')
    parser.add_argument('--vod-lifecycle-source', action='store_true', help='Capture original actual VOD response-body lifecycle event timing against real upstream HTTP/Redis/Channels')
    parser.add_argument('--vod-producer-rust', action='store_true', help='Compare native VOD statistics projection with actual original ORM/Redis golden rows')
    parser.add_argument('--timeshift-producer-rust', action='store_true', help='Verify native bounded catch-up producer registration and grace cleanup on actual original schema/Redis')
    parser.add_argument('--timeshift-producer-source', action='store_true', help='Capture original actual Redis/Channels Timeshift stats registration publication rules')
    parser.add_argument('--vod-producer-source', action='store_true', help='Capture original real ORM/Redis VOD statistics and Channels producer component contracts')
    parser.add_argument('--m3u-large-output', action='store_true', help='Characterize a 6000-channel multi-megabyte M3U and its Django cache')
    parser.add_argument('--m3u-output', action='store_true', help='Characterize original anonymous and XC M3U exports')
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
    output_peer=None
    if args.vod_header_stripped_native:args.vod_header_stripped_follow_source=True
    if args.vod_header_stripped_native and not(args.candidate_overlay and args.candidate_build_manifest):parser.error('native header flow requiresimmutable image manifest/overlay')
    if args.vod_header_stripped_seek_source or args.vod_header_stripped_mapping_loss_source or args.vod_header_stripped_state_loss_source:args.vod_header_stripped_follow_source=True
    if (args.vod_optional_session_lookup_source or (args.vod_header_stripped_follow_source and not args.vod_header_stripped_native)) and args.candidate_overlay:parser.error('lookup/header corpus is Source-only')
    if args.vod_rapid_reclose_ws:
        args.vod_playback_ws = True
    if args.vod_rapid_reclose_rust:
        args.vod_rapid_reclose_source = True
    if args.vod_progress_rust:
        args.vod_progress_source = True
    if args.timeshift_playback_ws:
        args.timeshift_media = True
        if args.candidate_overlay:
            args.candidate_timeshift_media = True
    if args.timeshift_producer_rust:
        args.timeshift_producer_source = True
    if args.vod_lifecycle_rust:
        args.vod_lifecycle_source = True
    if args.vod_producer_rust:
        args.vod_producer_source = True
    if args.m3u_large_output:
        args.m3u_output = True
    if args.m3u_rust_http:
        args.m3u_output = True
    if args.m3u_rust_renderer:
        args.m3u_output = True
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
    if args.vod_active_admin_stop_entry_source:args.vod_active_admin_stop_source=True
    if args.vod_active_admin_stop_source:
        if args.candidate_overlay:parser.error('admin stop characterization is Source-only')
        args.vod_active_extra_entry_source=True
        args.vod_active_old_close_source=True
    if args.vod_active_recovery_native:
        args.vod_active_restart_native=True
        args.vod_active_restart_http_source=True
        args.vod_active_extra_entry_source=True
        args.vod_active_retired_reopen_source=True
        args.vod_active_old_close_source=True
    if args.vod_active_idle_same_entry:args.vod_active_old_close_source=True
    if args.vod_active_idle_follow_source:args.vod_active_idle_redirect_source=True
    if args.vod_active_idle_redirect_source:args.vod_active_idle_mint_source=True
    if args.vod_active_idle_mint_source:args.vod_active_old_close_source=True
    if args.vod_active_old_close_source:args.vod_active_old_progress_source=True
    if args.vod_active_old_progress_source:
        if args.candidate_overlay and not args.vod_active_recovery_native:parser.error('old-generation telemetry probe is Source-only')
        args.vod_active_restart_http_source=True
    if args.vod_active_restart_native:
        if not args.candidate_overlay or not args.candidate_build_manifest:parser.error('native active restart capture requires immutable candidate overlay/manifest')
        args.vod_active_restart_http_source=True
    if args.vod_active_retired_reopen_source:
        args.vod_active_old_progress_source=True
        args.vod_active_old_close_source=True
        args.vod_active_restart_http_source=True
    if args.vod_active_extra_entry_source:args.vod_active_restart_http_source=True
    if args.vod_active_restart_http_source:args.vod_active_restart_source=True
    if (args.vod_active_capped_viewer or args.vod_active_fast_reopen or args.vod_active_long_prefix or args.vod_active_auth_loss) and not args.vod_active_restart_source:parser.error('activeviewer/fastreopen modifiers requireactiveVODcapture')
    if args.vod_active_recovery_characterize and not args.vod_active_recovery_native:parser.error('characterization modifier requiresSource/native recoverycapture')
    if args.vod_active_restart_source and args.candidate_overlay and not args.vod_active_restart_native:
        parser.error('active restart Source characterization does not yet implement candidate comparison')
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
    if args.m3u_output or args.vod_producer_source or args.timeshift_producer_source or args.vod_lifecycle_source or args.vod_playback_ws or args.timeshift_playback_ws or args.vod_progress_source or args.vod_rapid_reclose_source:
        compose += ['-f',str(tool_root/'scripts/rust-output-contracts/compose.yml')]
    if args.vod_playback_ws:
        compose += ['-f',str(tool_root/'scripts/rust-output-contracts/compose.ws.yml')]
    if args.vod_active_restart_source or args.vod_header_stripped_follow_source:
        active_overlay=output/'active-vod-fixture.yml'
        active_overlay.write_text('services:\n  origin:\n    environment:\n      VOD_FIXTURE_BYTES_PER_SECOND: "32768"\n')
        if args.vod_active_restart_native or args.vod_header_stripped_native:
            with active_overlay.open('a') as fixture:fixture.write('  candidate:\n    environment:\n      DISPATCHARR_TRUSTED_PROXIES: "127.0.0.0/8,10.0.0.0/8,172.16.0.0/12,192.168.0.0/16"\n')
        if args.vod_active_recovery_native or args.vod_header_stripped_native:
            with active_overlay.open('a') as fixture:fixture.write('      RUST_VOD_REDIS_RECOVERY_ENABLED: \"1\"\n')
        # Apply this owned functional fixture override after candidate policy.
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
        if args.m3u_output:
            compose += ['-f',str(tool_root/'scripts/rust-output-contracts/candidate-policy.yml')]
    if args.vod_active_restart_source or args.vod_header_stripped_follow_source:compose += ['-f',str(active_overlay)]
    port_source = (tool_root/'scripts/rust-integration/fixture_ports.py').read_bytes()
    (output/'fixture-ports-tool.py').write_bytes(port_source)
    port_namespace = {'__file__':str(tool_root/'scripts/rust-integration/fixture_ports.py')}
    exec(compile(port_source,port_namespace['__file__'],'exec'),port_namespace)
    service_ports = {'baseline':[5656,5432,6379],'origin':[8080]}
    if args.vod_playback_ws or args.timeshift_playback_ws:
        service_ports['baseline'].append(9191)
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
        # A frozen contract tree can sit below the actual Git worktree. Archive
        # from its Git top level so Git's subdirectory prefix cannot produce an
        # empty baseline tree when ROOT is that frozen nested directory.
        git_root=subprocess.check_output(['git','rev-parse','--show-toplevel'],cwd=ROOT,text=True).strip()
        archive = subprocess.run(['git','archive',BASELINE],cwd=git_root,check=True,capture_output=True).stdout
        with tarfile.open(fileobj=io.BytesIO(archive),mode='r:') as baseline_tar:
            if 'pyproject.toml' not in baseline_tar.getnames():
                raise RuntimeError('Baseline archive missing repository root')
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
        if args.vod_header_stripped_follow_source:
            command(compose+['exec','-T']+(['-e','HEADER_STRIPPED_SEEK=1']if args.vod_header_stripped_seek_source else[])+(['-e','HEADER_MAPPING_LOSS=1']if args.vod_header_stripped_mapping_loss_source else[])+(['-e','HEADER_STATE_LOSS=1']if args.vod_header_stripped_state_loss_source else[])+['baseline','/dispatcharrpy/bin/python','/vod-verification/header_stripped_follow_source.py'],timeout=90)
            captured=json.loads((output/'vod-header-stripped-follow-source.json').read_text())
            report['header_stripped_follow_source']=captured
            report['passed']=captured['capture_complete']and not captured['remaining_metadata']and captured['provider_after_close']in [None,'0']
            report['scope']='Actual Original Bearer redirect/headerless initial+existing-SID follow only; no native/restart acceptance'
            if args.vod_header_stripped_native:
                report['header_candidate_reset']={'web_stop':json.loads(command(compose+['exec','-T','baseline','/dispatcharrpy/bin/python','/verification/supervisor.py','control','stop'])),'redis_clear':command(compose+['exec','-T','baseline','redis-cli','FLUSHDB']).strip()}
                command(compose+['up','-d','--no-build','candidate'])
                candidate=address('candidate',9192);deadline=time.monotonic()+60
                while True:
                    try:
                        with urllib.request.urlopen(candidate+'/health/ready',timeout=2)as ready:
                            if json.load(ready).get('status')=='ready':break
                    except(OSError,ValueError):pass
                    if time.monotonic()>deadline:raise TimeoutError('Native header candidate readiness')
                    time.sleep(.25)
                inspected=json.loads(command([args.docker,'inspect',container('candidate')]))[0]
                spec=importlib.util.spec_from_file_location('header_build_adapter',tool_root/'scripts/rust-playback-baseline/run.py');adapter=importlib.util.module_from_spec(spec);spec.loader.exec_module(adapter)
                tree=adapter.validate_build_manifest(candidate_manifest,inspected['Image'],(inspected['Config'].get('Labels')or{}).get('org.opencontainers.image.source-tree-sha256'))
                report['header_candidate_provenance']={'image_id':inspected['Image'],'source_tree_sha256':tree,'build_manifest_sha256':hashlib.sha256(candidate_manifest_bytes).hexdigest(),'container_id':inspected['Id'],'started_at':inspected['State']['StartedAt']}
                command(compose+['exec','-T','-e','HEADER_NATIVE_BASE_URL=http://candidate:9192']+(['-e','HEADER_STRIPPED_SEEK=1']if args.vod_header_stripped_seek_source else[])+['baseline','/dispatcharrpy/bin/python','/vod-verification/header_stripped_follow_native.py'],timeout=90)
                native=json.loads((output/'vod-header-stripped-follow-native.json').read_text());report['header_stripped_follow_native']=native
                final=json.loads(command([args.docker,'inspect',container('candidate')]))[0]
                checks={'both_complete':captured['capture_complete']and native['capture_complete'],'redirect_status':captured['redirect_status']==native['redirect_status'],'same_process':final['Id']==inspected['Id']and final['State']['StartedAt']==inspected['State']['StartedAt']and final['RestartCount']==0}
                for name in ['first_headerless_follow','second_headerless_existing_session']:
                    a=captured.get(name,{});b=native.get(name,{})
                    checks[name]=a.get('status')==b.get('status')and a.get('bytes')==b.get('bytes')and a.get('sha256')==b.get('sha256')and a.get('content_range')==b.get('content_range')
                checks['cleanup']=not captured['remaining_metadata']and not native['remaining_metadata']and captured['provider_after_close']in[None,'0']and native['provider_after_close']in[None,'0']
                report['header_schedule']={'checks':checks,'passed':all(checks.values()),'scope':'Actual selected redirect/headerless body/range bytes; not full metadata/event/auth/concurrency parity'}
                report['passed']&=report['header_schedule']['passed']
                report['scope']='Actual Original/native headerless capability follow comparison; failures preserved, no default/fullauth promotion'
                if not report['passed']:raise RuntimeError('Selected headerless Source/native flow differs')
            raise ActiveSourceComplete()
        if args.vod_optional_session_lookup_source:
            command(compose+['exec','-T','baseline','/dispatcharrpy/bin/python','/vod-verification/optional_session_lookup_source.py'],timeout=90)
            captured=json.loads((output/'vod-optional-session-lookup-source.json').read_text())
            report['optional_session_lookup_source']=captured
            report['passed']=captured['capture_complete']and captured['cleanup_complete']
            report['scope']='Exact Original optional-session lookup block with actual Redis ACL/type/parse cases; no native/HTTP/restart acceptance'
            raise ActiveSourceComplete()
        if args.vod_active_restart_source:
            active_controller='active_restart_http_source.py'if args.vod_active_restart_http_source else'active_restart_source.py'
            command(compose+['exec','-T']+(['-e','ACTIVE_OBSERVE_OLD_PROGRESS=1']if args.vod_active_old_progress_source else[])+(['-e','ACTIVE_CLOSE_OLD_FIRST=1']if args.vod_active_old_close_source else[])+(['-e','ACTIVE_EXTRA_ENTRY=1']if args.vod_active_extra_entry_source else[])+(['-e','ACTIVE_RETIRED_REOPEN=1']if args.vod_active_retired_reopen_source else[])+(['-e','ACTIVE_ENTRY_AFTER_ADMIN_STOP=1']if args.vod_active_admin_stop_entry_source else[])+(['-e','ACTIVE_ADMIN_STOP=1']if args.vod_active_admin_stop_source else[])+(['-e','ACTIVE_IDLE_SAME_ENTRY=1']if args.vod_active_idle_same_entry else[])+(['-e','ACTIVE_IDLE_MINT=1']if args.vod_active_idle_mint_source else[])+(['-e','ACTIVE_IDLE_REDIRECT=1']if args.vod_active_idle_redirect_source else[])+(['-e','ACTIVE_IDLE_FOLLOW=1']if args.vod_active_idle_follow_source else[])+(['-e','ACTIVE_STRIP_AUTH_AFTER_LOSS=1']if args.vod_active_auth_loss else[])+(['-e','ACTIVE_CAPPED_VIEWER=1']if args.vod_active_capped_viewer else[])+(['-e','ACTIVE_FAST_REOPEN=1']if args.vod_active_fast_reopen else[])+(['-e','ACTIVE_CAPTURE_LONG_PREFIX=1']if args.vod_active_long_prefix else[])+['baseline','/dispatcharrpy/bin/python','/vod-verification/'+active_controller],timeout=150)
            active=json.loads((output/'vod-active-restart-source.json').read_text())
            report['active_vod_restart_source']=active
            report['active_source_transport']='actual HTTP'if args.vod_active_restart_http_source else'full manager responseiterator'
            report['scope']='Actual Original active VOD body/state-loss component; no native/HTTP/auth/performance parity claim'
            report['passed']=active['capture_complete']
            report['active_session_recovery_verified']=False
            if not report['passed']:raise RuntimeError('Incomplete Original active VOD restart capture')
            if args.vod_active_restart_native:
                report['active_candidate_reset']={'web_stop':json.loads(command(compose+['exec','-T','baseline','/dispatcharrpy/bin/python','/verification/supervisor.py','control','stop'])),'redis_clear':command(compose+['exec','-T','baseline','redis-cli','FLUSHDB']).strip()}
                command(compose+['up','-d','--no-build','candidate'])
                candidate=address('candidate',9192);deadline=time.monotonic()+60
                while True:
                    try:
                        with urllib.request.urlopen(candidate+'/health/ready',timeout=2)as response:
                            if json.load(response).get('status')=='ready':break
                    except(OSError,ValueError):pass
                    if time.monotonic()>deadline:raise TimeoutError('Native active candidate readiness')
                    time.sleep(.25)
                inspected=json.loads(command([args.docker,'inspect',container('candidate')]))[0]
                spec=importlib.util.spec_from_file_location('active_build_adapter',tool_root/'scripts/rust-playback-baseline/run.py');adapter=importlib.util.module_from_spec(spec);spec.loader.exec_module(adapter)
                tree=adapter.validate_build_manifest(candidate_manifest,inspected['Image'],(inspected['Config'].get('Labels')or{}).get('org.opencontainers.image.source-tree-sha256'))
                report['active_candidate_provenance']={'image_id':inspected['Image'],'source_tree_sha256':tree,'build_manifest_sha256':hashlib.sha256(candidate_manifest_bytes).hexdigest(),'container_id':inspected['Id'],'started_at':inspected['State']['StartedAt']}
                command(compose+['exec','-T','-e','ACTIVE_NATIVE_BASE_URL=http://candidate:9192']+(['-e','ACTIVE_OBSERVE_OLD_PROGRESS=1','-e','ACTIVE_CLOSE_OLD_FIRST=1','-e','ACTIVE_EXTRA_ENTRY=1','-e','ACTIVE_RETIRED_REOPEN=1']if args.vod_active_recovery_native else[])+(['-e','ACTIVE_IDLE_SAME_ENTRY=1']if args.vod_active_idle_same_entry else[])+(['-e','ACTIVE_STRIP_AUTH_AFTER_LOSS=1']if args.vod_active_auth_loss else[])+(['-e','ACTIVE_CAPPED_VIEWER=1']if args.vod_active_capped_viewer else[])+(['-e','ACTIVE_FAST_REOPEN=1']if args.vod_active_fast_reopen else[])+(['-e','ACTIVE_CAPTURE_LONG_PREFIX=1']if args.vod_active_long_prefix else[])+['baseline','/dispatcharrpy/bin/python','/vod-verification/active_restart_native.py'],timeout=150)
                native=json.loads((output/'vod-active-restart-native.json').read_text());report['active_vod_restart_native']=native
                final=json.loads(command([args.docker,'inspect',container('candidate')]))[0]
                report['active_candidate_process_preserved']=final['Id']==inspected['Id']and final['State']['StartedAt']==inspected['State']['StartedAt']and final['RestartCount']==0
                report['active_vod_comparison']={'both_captures_complete':active['capture_complete']and native['capture_complete'],'old_body_ended_matches':active['old_body']['ended']==native['old_body']['ended'],'old_body_bytes_match':active['old_body']['bytes']==native['old_body']['bytes'],'same_id_reopen_status_matches':active['same_id_reopen']['status']==native['same_id_reopen']['status'],'both_sampled_prefixes_match_origin':active['body_prefix_matches_origin']and native['body_prefix_matches_origin'],'application_parity_verified':False}
                report['passed']&=native['capture_complete']and report['active_candidate_process_preserved']
                if args.vod_active_recovery_native:
                    spec=importlib.util.spec_from_file_location('active_recovery_compare',tool_root/'scripts/rust-vod-contracts/active_recovery_compare.py');recovery_compare=importlib.util.module_from_spec(spec);spec.loader.exec_module(recovery_compare)
                    report['active_recovery_schedule']=recovery_compare.comparison(active,native)
                    report['active_recovery_schedule_verified']=report['active_recovery_schedule']['passed']
                    if not args.vod_active_recovery_characterize:report['passed']&=report['active_recovery_schedule']['passed']
                    else:report['scope']='Characterizationonly: policy/faultSource/native differences retained, no active recovery parity acceptance'
                    if not report['active_recovery_schedule']['passed'] and not args.vod_active_recovery_characterize:raise RuntimeError('Selected Source/native active recovery schedule differs')
                report['scope']='Actual Original full-manager and frozen native HTTP active state-loss characterization; differences are evidence, not parity acceptance'
            raise ActiveSourceComplete()
        if args.vod_rapid_reclose_source:
            rapid=json.loads(command(compose+['exec','-T','baseline','/dispatcharrpy/bin/python','/output-verification/vod_rapid_reclose_fixture.py'],timeout=60).splitlines()[-1])
            (output/'vod-rapid-reclose-source.json').write_text(json.dumps(rapid,indent=2)+'\n')
            report['vod_rapid_reclose_source']=rapid
            if not rapid['within_original_grace'] or rapid['remaining_metadata'] or rapid['provider_connections']!=0:raise RuntimeError('Rapid source race capture incomplete')
            if args.vod_rapid_reclose_rust:
                database_address=command(compose+['port','baseline','5432']).strip()
                redis_address=command(compose+['port','baseline','6379']).strip()
                origin_address=command(compose+['port','origin','8080']).strip()
                test_environment=dict(environment,DATABASE_TEST_URL='postgresql://dispatch:playback-fixture-only@'+database_address+'/dispatcharr',REDIS_TEST_URL='redis://:playback-fixture-only@'+redis_address+'/0',VOD_DISPOSABLE_FIXTURE='playback-fixture-only',VOD_RAPID_RECLOSE_ORACLE=str(output/'vod-rapid-reclose-source.json'),VOD_ORIGIN_TEST_URL='http://'+origin_address)
                result=subprocess.run(['cargo','test','--manifest-path',str(ROOT/'rust/Cargo.toml'),'-p','dispatcharr-vod-http','--test','rapid_reclose_sql','--locked','--offline','--','--ignored','--nocapture','--test-threads=1'],env=test_environment,capture_output=True,text=True,timeout=360)
                (output/'rust-vod-rapid-reclose.log').write_text(result.stdout+result.stderr)
                report['vod_rapid_reclose_rust']={'passed':result.returncode==0,'native_transport_verified':False,'source_bound_candidate_image':False,'timer_interleaving_normalized':True}
                result.check_returncode()
        if args.vod_progress_source:
            progress=json.loads(command(compose+['exec','-T','baseline','/dispatcharrpy/bin/python','/output-verification/vod_progress_fixture.py'],timeout=60).splitlines()[-1])
            (output/'vod-progress-source.json').write_text(json.dumps(progress,indent=2)+'\n')
            report['vod_progress_source']=progress
            if args.vod_progress_rust:
                database_address=command(compose+['port','baseline','5432']).strip();redis_address=command(compose+['port','baseline','6379']).strip();origin_address=command(compose+['port','origin','8080']).strip()
                test_environment=dict(environment,DATABASE_TEST_URL='postgresql://dispatch:playback-fixture-only@'+database_address+'/dispatcharr',REDIS_TEST_URL='redis://:playback-fixture-only@'+redis_address+'/0',VOD_ORIGIN_TEST_URL='http://'+origin_address,VOD_DISPOSABLE_FIXTURE='playback-fixture-only',VOD_PROGRESS_ORACLE=str(output/'vod-progress-source.json'))
                result=subprocess.run(['cargo','test','--manifest-path',str(ROOT/'rust/Cargo.toml'),'-p','dispatcharr-vod-http','--test','lifecycle_sql','--locked','--offline','final_partial_hundredth_source_chunk','--','--ignored','--nocapture'],env=test_environment,capture_output=True,text=True,timeout=180)
                (output/'rust-vod-progress.log').write_text(result.stdout+result.stderr);report['vod_progress_rust']={'passed':result.returncode==0,'source_bound_candidate_image':False};result.check_returncode()
        if args.vod_playback_ws or args.timeshift_playback_ws:
            def ws_control(action,sid=None):
                if action=='rapid-inspect':
                    if not sid or not sid.startswith('fixture-packaged-vod-') or len(sid)>128:raise RuntimeError('Invalid owned rapid fixture ID')
                    script="local k='vod_persistent_connection:'..ARGV[1];return {redis.call('EXISTS',k),redis.call('HGET',k,'active_streams') or ''}"
                    values=json.loads(command(compose+['exec','-T','baseline','redis-cli','--json','EVAL',script,'0',sid],timeout=5))
                    return {'exists':bool(values[0]),'active_streams':int(values[1] or 0)}
                arguments=compose+['exec','-T','baseline','/dispatcharrpy/bin/python','/output-verification/vod_ws_control.py',action]+([sid] if sid else [])
                # Token output remains memory-only and never reaches command logs.
                result=subprocess.run(arguments,env=environment,capture_output=True,text=True,timeout=30)
                result.check_returncode();return json.loads(result.stdout.splitlines()[-1])
            def source_ws():
                access=ws_control('token')['access']
                report.setdefault('source_daphne_starts',[]).append(ws_control('start-daphne'))
                ws_base='http://'+command(compose+['port','baseline','9191']).strip()
                deadline=time.monotonic()+30
                while True:
                    try:
                        with urllib.request.urlopen(ws_base+'/api/core/version/',timeout=1) as ready:
                            if ready.status==200:break
                    except OSError:
                        if time.monotonic()>deadline:raise TimeoutError('Owned source WebSocket server readiness timed out')
                        time.sleep(.1)
                return ws_base,access
        if args.vod_playback_ws:
            spec=importlib.util.spec_from_file_location('frozen_vod_ws_probe',tool_root/'scripts/rust-output-contracts/vod_ws_probe.py')
            vod_ws=importlib.util.module_from_spec(spec);spec.loader.exec_module(vod_ws)
            ws_base,access=source_ws()
            try:
                report['vod_ws_source']=vod_ws.capture(base,ws_base,access,ws_control,error_path=output/'vod-ws-source-partial.json')
                if args.vod_rapid_reclose_ws:
                    spec=importlib.util.spec_from_file_location('frozen_rapid_vod_ws',tool_root/'scripts/rust-output-contracts/vod_rapid_ws_probe.py')
                    rapid_ws=importlib.util.module_from_spec(spec);spec.loader.exec_module(rapid_ws)
                    report['vod_rapid_ws_source']=rapid_ws.capture(base,ws_base,access,ws_control,error_path=output/'vod-rapid-ws-source-partial.json')
                    (output/'vod-rapid-ws-source.json').write_text(json.dumps(report['vod_rapid_ws_source'],indent=2)+'\n')
            finally:ws_control('stop-daphne')
            del access
            (output/'vod-ws-source.json').write_text(json.dumps(report['vod_ws_source'],indent=2)+'\n')
        if args.timeshift_playback_ws:
            spec=importlib.util.spec_from_file_location('frozen_timeshift_ws_probe',tool_root/'scripts/rust-output-contracts/timeshift_ws_probe.py')
            timeshift_ws=importlib.util.module_from_spec(spec);spec.loader.exec_module(timeshift_ws)
        if args.vod_lifecycle_source:
            lifecycle_result=command(compose+['exec','-T','baseline','/dispatcharrpy/bin/python','/output-verification/vod_lifecycle_fixture.py'],timeout=120)
            lifecycle=json.loads(lifecycle_result.splitlines()[-1])
            (output/'vod-lifecycle-source.json').write_text(json.dumps(lifecycle,indent=2)+'\n')
            report['vod_lifecycle_source']={'cases':len(lifecycle['cases']),'passed':len(lifecycle['cases'])==6,'native_implementation_verified':False}
            if not report['vod_lifecycle_source']['passed']:raise RuntimeError('Original VOD body lifecycle fixture incomplete')
            if args.vod_lifecycle_rust:
                database_address=command(compose+['port','baseline','5432']).strip()
                redis_address=command(compose+['port','baseline','6379']).strip()
                origin_address=command(compose+['port','origin','8080']).strip()
                test_environment=dict(environment,DATABASE_TEST_URL='postgresql://dispatch:playback-fixture-only@'+database_address+'/dispatcharr',REDIS_TEST_URL='redis://:playback-fixture-only@'+redis_address+'/0',VOD_DISPOSABLE_FIXTURE='playback-fixture-only',VOD_LIFECYCLE_ORACLE=str(output/'vod-lifecycle-source.json'),VOD_ORIGIN_TEST_URL='http://'+origin_address)
                result=subprocess.run(['cargo','test','--manifest-path',str(ROOT/'rust/Cargo.toml'),'-p','dispatcharr-vod-http','--test','lifecycle_sql','--test','http','--locked','--offline','--','--ignored','--nocapture','--test-threads=1','--skip','final_partial_hundredth_source_chunk'],env=test_environment,capture_output=True,text=True,timeout=240)
                (output/'rust-vod-lifecycle.log').write_text(result.stdout+result.stderr)
                report['vod_lifecycle_rust']={'passed':result.returncode==0,'native_transport_verified':False,'source_bound_candidate_image':False}
                result.check_returncode()
        if args.timeshift_producer_source:
            producer_result=command(compose+['exec','-T','baseline','/dispatcharrpy/bin/python','/output-verification/timeshift_producer_fixture.py'],timeout=120)
            producer=json.loads(producer_result.splitlines()[-1])
            (output/'timeshift-producer-source.json').write_text(json.dumps(producer,indent=2)+'\n')
            report['timeshift_producer_source']={'registration_cases':len(producer['cases']),'passed':len(producer['cases'])==5,'application_parity_verified':False}
            if not report['timeshift_producer_source']['passed']:raise RuntimeError('Original Timeshift producer fixture incomplete')
            if args.timeshift_producer_rust:
                database_address=command(compose+['port','baseline','5432']).strip()
                redis_address=command(compose+['port','baseline','6379']).strip()
                test_environment=dict(environment,DATABASE_TEST_URL='postgresql://dispatch:playback-fixture-only@'+database_address+'/dispatcharr',REDIS_TEST_URL='redis://:playback-fixture-only@'+redis_address+'/0',TIMESHIFT_DISPOSABLE_FIXTURE='playback-fixture-only')
                result=subprocess.run(['cargo','test','--manifest-path',str(ROOT/'rust/Cargo.toml'),'-p','dispatcharr-timeshift-http','--lib','--locked','--offline','original_registration_notifications_and_grace_cleanup','--','--ignored','--nocapture','--test-threads=1'],env=test_environment,capture_output=True,text=True,timeout=180)
                (output/'rust-timeshift-producer.log').write_text(result.stdout+result.stderr)
                report['timeshift_producer_rust']={'passed':result.returncode==0,'native_transport_verified':False,'source_bound_candidate_image':False}
                result.check_returncode()
                result=subprocess.run(['cargo','test','--manifest-path',str(ROOT/'rust/Cargo.toml'),'-p','dispatcharr-timeshift-http','--test','media_pool','--locked','--offline','--','--ignored','--nocapture'],env=test_environment,capture_output=True,text=True,timeout=180)
                (output/'rust-timeshift-pool-regression.log').write_text(result.stdout+result.stderr)
                report['timeshift_pool_regression']={'passed':result.returncode==0}
                result.check_returncode()
        if args.vod_producer_source:
            producer_result=command(compose+['exec','-T','baseline','/dispatcharrpy/bin/python','/output-verification/vod_producer_fixture.py'],timeout=120)
            producer=json.loads(producer_result.splitlines()[-1])
            (output/'vod-producer-source.json').write_text(json.dumps(producer,indent=2)+'\n')
            report['vod_producer_source']={'stats_cases':len(producer['stats_cases']),'channel_messages':len(producer['published']),'audit_rows':len(producer['audit']),'passed':len(producer['stats_cases'])==19 and len(producer['published'])==4 and len(producer['audit'])==2,'application_parity_verified':False}
            if not report['vod_producer_source']['passed']:raise RuntimeError('Original VOD producer fixture incomplete')
            if args.vod_producer_rust:
                database_address=command(compose+['port','baseline','5432']).strip()
                test_environment=dict(environment,DATABASE_TEST_URL='postgresql://dispatch:playback-fixture-only@'+database_address+'/dispatcharr',VOD_DISPOSABLE_FIXTURE='playback-fixture-only',VOD_PRODUCER_ORACLE=str(output/'vod-producer-source.json'),REDIS_TEST_URL='redis://:playback-fixture-only@'+command(compose+['port','baseline','6379']).strip()+'/0')
                result=subprocess.run(['cargo','test','--manifest-path',str(ROOT/'rust/Cargo.toml'),'-p','dispatcharr-vod-http','--lib','--locked','--offline','original_vod_statistics_match_real_orm_source','--','--ignored','--nocapture'],env=test_environment,capture_output=True,text=True,timeout=180)
                (output/'rust-vod-producer-projection.log').write_text(result.stdout+result.stderr)
                report['vod_producer_rust']={'passed':result.returncode==0,'native_lifecycle_publication_verified':False,'source_bound_candidate_image':False}
                result.check_returncode()
        if args.m3u_output:
            spec=importlib.util.spec_from_file_location('frozen_m3u_probe',tool_root/'scripts/rust-output-contracts/m3u_probe.py')
            m3u=importlib.util.module_from_spec(spec)
            spec.loader.exec_module(m3u)
            peer_spec=importlib.util.spec_from_file_location('frozen_output_peer',tool_root/'scripts/rust-realtime-contracts/interop.py')
            peer_module=importlib.util.module_from_spec(peer_spec)
            peer_spec.loader.exec_module(peer_module)
            output_peer=peer_module.Peer(compose+['exec','-T','baseline','/dispatcharrpy/bin/python','/output-verification/control.py','--persistent'],environment,output/'output-control-peer.log')
            def output_control(action,label):
                return output_peer.call({'action':action,'label':label})
            report['m3u_source']=(m3u.capture_large if args.m3u_large_output else m3u.capture)(base,output_control,'source',error_path=output/'source-csrf-failure.html')
            (output/'m3u-source.json').write_text(json.dumps(report['m3u_source'],indent=2)+'\n')
            report['m3u_third_party_license']=output_control('license','source')
            (output/'django-source-license.json').write_text(json.dumps(report['m3u_third_party_license'],indent=2)+'\n')
            if args.m3u_rust_http:
                database_address=command(compose+['port','baseline','5432']).strip()
                redis_address=command(compose+['port','baseline','6379']).strip()
                test_environment=dict(environment,DATABASE_TEST_URL='postgresql://dispatch:playback-fixture-only@'+database_address+'/dispatcharr',REDIS_TEST_URL='redis://:playback-fixture-only@'+redis_address+'/0',M3U_DISPOSABLE_FIXTURE='playback-fixture-only',M3U_SOURCE_ORACLE=str(output/'m3u-source.json'),M3U_HTTP_COMPOSE=json.dumps(compose),M3U_HTTP_DRIVER=str(tool_root/'scripts/rust-output-contracts/native_driver.py'),M3U_HTTP_OUTPUT=str(output/'m3u-native-http.json'),M3U_LARGE_OUTPUT='1' if args.m3u_large_output else '0')
                result=subprocess.run(['cargo','test','--manifest-path',str(ROOT/'rust/Cargo.toml'),'-p','dispatcharr-output-http','--lib','--test','http_sql','--locked','--offline','--','--ignored','--nocapture'],env=test_environment,capture_output=True,text=True,timeout=240)
                (output/'rust-m3u-http.log').write_text(result.stdout+result.stderr)
                report['m3u_rust_http']={'passed':result.returncode==0,'source_bound_candidate_image':False,'application_parity_verified':False,'performance_verified':False}
                result.check_returncode()
                report['m3u_native_http']=json.loads((output/'m3u-native-http.json').read_text())
            if args.m3u_rust_renderer:
                database_address=command(compose+['port','baseline','5432']).strip()
                test_environment=dict(environment,DATABASE_TEST_URL='postgresql://dispatch:playback-fixture-only@'+database_address+'/dispatcharr',M3U_DISPOSABLE_FIXTURE='playback-fixture-only',M3U_SOURCE_ORACLE=str(output/'m3u-source.json'))
                result=subprocess.run(['cargo','test','--manifest-path',str(ROOT/'rust/Cargo.toml'),'-p','dispatcharr-output-http','--test','sql','--locked','--offline','--','--ignored','--nocapture'],env=test_environment,capture_output=True,text=True,timeout=180)
                (output/'rust-m3u-renderer.log').write_text(result.stdout+result.stderr)
                report['m3u_rust_sql_renderer']={'passed':result.returncode==0,'source_bound_candidate_image':False,'http_auth_cache_and_route_parity_verified':False,'application_parity_verified':False,'performance_verified':False}
                result.check_returncode()
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
            if args.timeshift_playback_ws:
                ws_base,access=source_ws()
                try:report['timeshift_media'],report['timeshift_ws_source']=timeshift_ws.capture(ws_base,access,lambda:media.capture(base,address('origin',8080),session_seed,output,observe))
                finally:ws_control('stop-daphne');del access
                (output/'timeshift-ws-source.json').write_text(json.dumps(report['timeshift_ws_source'],indent=2)+'\n')
            else:
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
            if args.vod_progress_source:
                spec=importlib.util.spec_from_file_location('frozen_vod_progress',tool_root/'scripts/rust-output-contracts/vod_progress_probe.py');progress_probe=importlib.util.module_from_spec(spec);spec.loader.exec_module(progress_probe)
                def progress_inspect(sid):return json.loads(command(compose+['exec','-T','baseline','/dispatcharrpy/bin/python','/output-verification/vod_progress_inspect.py',sid],timeout=10).splitlines()[-1])
                report['vod_progress_candidate']=progress_probe.capture(candidate,report['vod_progress_source'],progress_inspect)
                (output/'vod-progress-candidate.json').write_text(json.dumps(report['vod_progress_candidate'],indent=2)+'\n')
            if args.vod_playback_ws:
                access=ws_control('token')['access']
                try:
                    report['vod_ws_candidate']=vod_ws.capture(candidate,candidate,access,ws_control,error_path=output/'vod-ws-candidate-partial.json')
                    if args.vod_rapid_reclose_ws:
                        report['vod_rapid_ws_candidate']=rapid_ws.capture(candidate,candidate,access,ws_control,error_path=output/'vod-rapid-ws-candidate-partial.json')
                        report['vod_rapid_ws_comparison']=rapid_ws.compare(report['vod_rapid_ws_source'],report['vod_rapid_ws_candidate'])
                        (output/'vod-rapid-ws-candidate.json').write_text(json.dumps(report['vod_rapid_ws_candidate'],indent=2)+'\n')
                finally:del access
                report['vod_ws_comparison']=vod_ws.compare(report['vod_ws_source'],report['vod_ws_candidate'])
                (output/'vod-ws-candidate.json').write_text(json.dumps(report['vod_ws_candidate'],indent=2)+'\n')
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
            if args.m3u_output:
                report['m3u_candidate']=(m3u.capture_large if args.m3u_large_output else m3u.capture)(candidate,output_control,'candidate')
                report['m3u_comparison']=m3u.compare(report['m3u_source'],report['m3u_candidate'])
                (output/'m3u-candidate.json').write_text(json.dumps(report['m3u_candidate'],indent=2)+'\n')
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
                if args.timeshift_playback_ws:
                    access=ws_control('token')['access']
                    try:report['candidate_timeshift_media'],report['timeshift_ws_candidate']=timeshift_ws.capture(candidate,access,lambda:media.capture(candidate,address('origin',8080),session_seed,output,observe,prefix='timeshift-candidate'))
                    finally:del access
                    report['timeshift_ws_comparison']=timeshift_ws.compare(report['timeshift_ws_source'],report['timeshift_ws_candidate'])
                    (output/'timeshift-ws-candidate.json').write_text(json.dumps(report['timeshift_ws_candidate'],indent=2)+'\n')
                else:
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
        if args.vod_progress_source:
            report['passed']=report['passed'] and report['vod_progress_source']['capture_complete'] and (not candidate_manifest or report['vod_progress_candidate']['source_progress_matches'])
        if args.timeshift_playback_ws:
            report['passed']=report['passed'] and report['timeshift_ws_source']['capture_complete'] and (not candidate_manifest or report['timeshift_ws_comparison']['captured_packaged_timeshift_events_match'])
        if args.vod_playback_ws:
            report['passed']=report['passed'] and report['vod_ws_source']['capture_complete'] and (not candidate_manifest or report['vod_ws_comparison']['captured_packaged_vod_events_match'])
        if args.vod_rapid_reclose_ws:
            report['passed']=report['passed'] and report['vod_rapid_ws_source']['capture_complete'] and (not candidate_manifest or report['vod_rapid_ws_comparison']['captured_packaged_rapid_reclose_match'])
        if args.m3u_rust_http:
            report['passed'] = report['passed'] and report['m3u_rust_http']['passed']
        if args.m3u_rust_renderer:
            report['passed'] = report['passed'] and report['m3u_rust_sql_renderer']['passed']
        if args.m3u_output:
            report['passed'] = report['passed'] and report['m3u_source']['capture_complete'] and (not candidate_manifest or report['m3u_comparison']['captured_m3u_http_matches'])
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
    except ActiveSourceComplete:
        pass
    except BaseException as error:
        report['failure'] = type(error).__name__
        raise
    finally:
        if output_peer:output_peer.close()
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
