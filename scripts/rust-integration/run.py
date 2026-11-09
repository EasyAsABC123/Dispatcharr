#!/usr/bin/env python3
"""Real database/auth/media compatibility probe; incomplete features remain open gates."""
import argparse
from contextlib import contextmanager
import hashlib
import importlib.util
import json
import os
import re
import shutil
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
import uuid

from tool_snapshot import snapshot_sources

ROOT = Path(__file__).resolve().parents[2]
BASELINE = '1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430'
CONTRACT_SUITES=('accounts','account_crud','app_data','core','profiles','memberships',
                 'channel_reads','channel_crud','groups','channel_stops','group_cleanup','streams','provider_reads')
OPTIONAL_CONTRACT_SUITES=('proxy_auth',)


def contract_suites(value):
    if value=='all':return list(CONTRACT_SUITES)
    names=[name.strip() for name in value.split(',') if name.strip()]
    if not names or set(names)-set(CONTRACT_SUITES+OPTIONAL_CONTRACT_SUITES) or len(names)!=len(set(names)):
        raise ValueError('Expected unique known contract suite names or all')
    return [name for name in CONTRACT_SUITES+OPTIONAL_CONTRACT_SUITES if name in names]


def digest(value):
    return hashlib.sha256(json.dumps(value,sort_keys=True,separators=(',',':'),allow_nan=False).encode()).hexdigest()


@contextmanager
def fixture_source(report,keep,output):
    """Preserve retained container mounts; failed cleanup is retained too."""
    temporary=Path(tempfile.mkdtemp(prefix='dispatcharr-rust-probe-'))
    try:
        yield str(temporary)
    except BaseException as error:
        report['passed']=False
        report.setdefault('failure',type(error).__name__)
        raise
    finally:
        retain=bool(keep or report.get('cleanup_error'))
        if not retain:
            try:
                shutil.rmtree(temporary)
            except OSError as error:
                report['cleanup_error']=type(error).__name__
                report['passed']=False
                retain=True
        report['resources_retained']=retain
        if retain:
            report['retained_fixture_directory']=str(temporary)
            report['retained_source_directory']=str(temporary/'source')
        (output/'integration.json').write_text(json.dumps(report,indent=2)+'\n')


def candidate_overlay(template,destination,image):
    if not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9._:/@-]{0,255}',image):
        raise ValueError('Invalid candidate image reference')
    original='image: dispatcharr-rust-candidate:milestone2'
    text=template.read_text()
    if text.count(original)!=1:
        raise ValueError('Candidate Compose image slot changed; update the fixture adapter')
    destination.write_text(text.replace(original,'image: '+image,1))
    return destination


def validate_candidate_image(manifest,inspection):
    image_id=inspection['Image']
    labels=inspection['Config'].get('Labels') or {}
    source=labels.get('org.opencontainers.image.source-tree-sha256')
    if not source:
        raise ValueError('Candidate image lacks its immutable source binding')
    if manifest is not None:
        if (manifest.get('passed') is not True or manifest.get('image_id')!=image_id
                or manifest.get('source_tree_sha256')!=source):
            raise ValueError('Running candidate does not match its immutable build manifest')
    return {'image_id':image_id,'source_tree_sha256':source,'manifest_verified':manifest is not None}


def http(url, body=None, token=None, method=None, headers=None):
    request_headers = {'Content-Type': 'application/json', **(headers or {})}
    if token:
        request_headers['Authorization'] = 'Bearer ' + token
    request = urllib.request.Request(url, data=json.dumps(body).encode() if body is not None else None,
                                     headers=request_headers, method=method)
    try:
        response = urllib.request.urlopen(request, timeout=15)
    except urllib.error.HTTPError as error:
        response = error
    with response:
        raw = response.read()
        try:
            value = json.loads(raw)
        except ValueError:
            value = {'non_json': True, 'length': len(raw)}
        return response.status, value


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--keep', action='store_true')
    parser.add_argument('--candidate-image',help='Unique prebuilt image tag; keeps earlier measured tags unchanged')
    parser.add_argument('--candidate-build-manifest',type=Path)
    parser.add_argument('--contract-suites',default='all',help='Comma-separated oracle suites or all (default); foundational auth/schema/media checks always run')
    parser.add_argument('--trusted-proxy-fixture',action='store_true',help='Only for the isolated proxy_auth suite: trust peers explicitly on both disposable fixture services')
    args = parser.parse_args()
    try:selected_suites=contract_suites(args.contract_suites)
    except ValueError as error:parser.error(str(error))
    if ('proxy_auth' in selected_suites or args.trusted_proxy_fixture) and not (args.trusted_proxy_fixture and selected_suites==['proxy_auth']):
        parser.error('proxy_auth requires --trusted-proxy-fixture and must run as the only selected suite')
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=True)
    if any(output.iterdir()):
        parser.error('output must be empty')
    project = 'dispatcharr-rust-probe-' + uuid.uuid4().hex[:10]
    report = {'scope': 'real-database-accounts-native-ts-probe', 'passed': False,
              'source_baseline': BASELINE, 'project': project, 'checks': [],
              'application_performance_verified': False,'contract_suites':selected_suites}
    manifest=None
    if args.candidate_build_manifest:
        raw=args.candidate_build_manifest.read_bytes()
        manifest=json.loads(raw)
        report['build_manifest_sha256']=hashlib.sha256(raw).hexdigest()
    tool_source=output/'tool-source'
    report['tool_source_sha256']=snapshot_sources(ROOT,tool_source)
    report['tool_source_scope']='Captured account probes, integration helpers, baseline/playback fixtures and Compose files; all later imports and container script mounts use this copy.'
    overlay=tool_source/'docker/compose.rust-candidate.yml'
    if args.candidate_image:
        overlay=candidate_overlay(overlay,output/'candidate-overlay.yml',args.candidate_image)
    report['candidate_overlay_sha256']=hashlib.sha256(overlay.read_bytes()).hexdigest()
    with fixture_source(report,args.keep,output) as temporary:
        source = Path(temporary) / 'source'
        source.mkdir()
        source.chmod(0o755)
        archive = Path(temporary) / 'source.tar'
        with archive.open('wb') as stream:
            subprocess.run(['git', 'archive', BASELINE], cwd=ROOT, stdout=stream, check=True, timeout=30)
        report['baseline_archive_sha256'] = hashlib.sha256(archive.read_bytes()).hexdigest()
        subprocess.run(['tar', '-xf', str(archive), '-C', str(source), '--no-same-owner', '--same-permissions'], check=True, timeout=30)
        (source / 'media').mkdir(exist_ok=True)
        environment = dict(os.environ, BASELINE_SOURCE_DIR=str(source),
                           BASELINE_EVIDENCE_DIR=str(output), FIXTURE_SECONDS='180')
        compose = ['docker', 'compose', '-p', project, '-f', str(tool_source/'docker/compose.rust-playback.yml'),
                   '-f', str(overlay)]
        if args.trusted_proxy_fixture:
            trust=output/'trusted-proxy-fixture.yml'
            trust.write_text('services:\n  baseline:\n    environment:\n      DISPATCHARR_TRUSTED_PROXIES: "0.0.0.0/0,::/0"\n  candidate:\n    environment:\n      DISPATCHARR_TRUSTED_PROXIES: "0.0.0.0/0,::/0"\n')
            report['trusted_proxy_fixture']=True
            report['trusted_proxy_overlay_sha256']=hashlib.sha256(trust.read_bytes()).hexdigest()
            compose+=['-f',str(trust)]

        def command(args, timeout=120, payload=None):
            return subprocess.check_output(args, cwd=ROOT, env=environment, text=True,
                                           timeout=timeout, input=payload, stderr=subprocess.PIPE)

        def execute(service, script, payload=None):
            return command(compose + ['exec', '-T', service, '/dispatcharrpy/bin/python', script], payload=payload)

        def endpoint(service, port):
            return 'http://' + command(compose + ['port', service, str(port)]).strip()

        def wait(check, seconds=120):
            deadline = time.monotonic() + seconds
            while time.monotonic() < deadline:
                try:
                    if check():
                        return
                except (OSError, ValueError, subprocess.SubprocessError):
                    pass
                time.sleep(.5)
            raise TimeoutError('Readiness deadline exceeded')

        def record(name, passed, details=None):
            report['checks'].append({'name': name, 'passed': bool(passed), 'details': details})

        status = 1
        try:
            command(compose + ['up', '-d', 'baseline'], timeout=180)
            baseline = endpoint('baseline', 5656)
            origin = endpoint('origin', 8080)
            wait(lambda: http(origin + '/health')[0] == 200 and (output / 'seed.json').is_file())
            wait(lambda: http(baseline + '/proxy/ts/stream/not-a-channel')[0] == 404)
            accounts = json.loads(execute('baseline', '/integration/seed_accounts.py'))
            command(compose + ['up', '-d', '--no-build', 'candidate'])
            candidate = endpoint('candidate', 9192)
            wait(lambda: http(candidate + '/health/ready')[0] == 200)
            report['images'] = json.loads(command(compose + ['images', '--format', 'json']))
            container=command(compose+['ps','-q','candidate']).strip()
            inspection=json.loads(command(['docker','inspect',container]))[0]
            report['candidate_image_binding']=validate_candidate_image(manifest,inspection)
            for path in ('/proxy/ts/status', '/proxy/ts/stream/not-a-channel'):
                old, new = http(baseline + path), http(candidate + path)
                record('status-parity:' + path, old[0] == new[0] and digest(old[1])==digest(new[1]),
                       {'python_status':old[0],'rust_status':new[0],
                        'python_body_sha256':digest(old[1]),'rust_body_sha256':digest(new[1])})
            admin = accounts['admin']
            viewer = accounts['viewer']
            for role, paths in [
                ('admin', ['/api/accounts/users/', '/api/accounts/users/me/',
                           '/api/accounts/groups/', '/api/accounts/permissions/', '/api/accounts/api-keys/']),
                ('viewer', ['/api/accounts/users/me/', '/api/accounts/api-keys/', '/api/accounts/users/'])]:
                for path in paths:
                    old = http(baseline + path, token=accounts[role]['access'])
                    new = http(candidate + path, token=accounts[role]['access'])
                    # Do not persist serialized fixture API keys or personal fields.
                    record('account-body-parity:' + role + ':' + path, old[0]==new[0] and digest(old[1])==digest(new[1]),
                           {'python_status': old[0], 'rust_status': new[0],
                            'python_body_sha256': digest(old[1]), 'rust_body_sha256': digest(new[1])})
            for name, credentials, expected in [('admin', admin, 200), ('viewer', viewer, 403),
                                                 ('inactive', accounts['inactive'], 401)]:
                response = http(candidate + '/proxy/ts/status', token=credentials['access'])
                record('django-token-role:' + name, response[0] == expected, {'status': response[0]})
            rust_login = http(candidate + '/api/accounts/token/', {'username': admin['username'], 'password': admin['password']})
            record('django-password-auth', rust_login[0] == 200 and set(rust_login[1]) == {'access', 'refresh'}, {'status': rust_login[0]})
            if rust_login[0] == 200:
                claims = json.loads(execute('baseline', '/integration/verify_tokens.py', json.dumps(rust_login[1])))
                record('rust-tokens-accepted-by-django', all(str(value['user_id']) == str(admin['id']) for value in claims.values()), claims)
                refreshed = http(candidate + '/api/accounts/token/refresh/', {'refresh': admin['refresh']})
                record('django-refresh-accepted-by-rust', refreshed[0] == 200 and 'access' in refreshed[1], {'status': refreshed[0]})
            seed = json.loads((output / 'seed.json').read_text())
            # Real FFmpeg decoder resides in the baseline image, not on the host.
            decoded = command(compose + ['exec', '-T', 'baseline', 'ffmpeg', '-hide_banner', '-loglevel', 'error',
                                         '-xerror', '-progress', 'pipe:1', '-nostats', '-i',
                                         'http://candidate:9192' + seed['path'], '-t', '3', '-f', 'null', '-'], timeout=90)
            (output / 'decode.log').write_text(decoded)
            frames = [int(line.split('=')[1]) for line in decoded.splitlines() if line.startswith('frame=')]
            record('rust-real-media-decoder', bool(frames) and max(frames) >= 75, {'frames': max(frames, default=0)})
            # An invalid presented credential must never attach anonymously to a running stream.
            invalid = http(candidate + '/proxy/ts/status', token='invalid')
            record('invalid-presented-token-rejected', invalid[0] == 401, {'status': invalid[0]})
            spec = importlib.util.spec_from_file_location('account_probes', tool_source / 'scripts/rust-account-contracts/probes.py')
            account_probes = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(account_probes)
            for suite,method in [('accounts','run'),('account_crud','run_crud'),('app_data','run_app_data'),
                                 ('core','run_core'),('profiles','run_profiles'),('memberships','run_memberships'),
                                 ('channel_reads','run_channel_reads'),('channel_crud','run_channel_crud'),
                                 ('groups','run_groups'),('channel_stops','run_channel_stops'),
                                 ('group_cleanup','run_group_cleanup'),('streams','run_streams'),
                                 ('provider_reads','run_provider_reads'),
                                 ('proxy_auth','run_proxy_auth')]:
                if suite in selected_suites:
                    if suite=='core' and 'account_crud' in selected_suites:
                        # Both independent suites deliberately test password
                        # sign-in. The fixture's source login budget is 3/min,
                        # and foundation spends one extra Rust attempt. Let
                        # those real budgets expire instead of resetting or
                        # bypassing application state between contracts.
                        report.setdefault('suite_isolation',[]).append({'before_suite':'core','reason':'independent login contracts share real per-IP 3/min budgets','wait_seconds':62})
                        for _ in range(2):time.sleep(31)
                    extra=(seed,) if suite=='channel_stops' else ()
                    getattr(account_probes,method)(http,baseline,candidate,accounts,record,*extra)
            status = 0 if all(check['passed'] for check in report['checks']) else 1
            report['passed'] = status == 0
        except (OSError, ValueError, RuntimeError, subprocess.SubprocessError) as error:
            report['failure'] = type(error).__name__
            if isinstance(error, subprocess.CalledProcessError):
                # Commands never include tokens; stderr may help diagnose schema/decoder failures.
                report['failure_stderr'] = error.stderr
        except KeyboardInterrupt:
            status = 130
            report['failure'] = 'interrupted'
        finally:
            try:
                (output / 'compose.log').write_text(command(compose + ['logs', '--no-color'], timeout=30))
                if not args.keep:
                    command(compose + ['down', '--volumes', '--remove-orphans', '--timeout', '10'], timeout=60)
                    report['cleanup'] = 'project removed'
            except (OSError, subprocess.SubprocessError) as error:
                report['cleanup_error'] = type(error).__name__
                report['passed'] = False
                status = 1
    print(json.dumps({'passed': report['passed'], 'evidence': str(output), 'failure': report.get('failure')}))
    return status if report['passed'] else (status or 1)


if __name__ == '__main__':
    raise SystemExit(main())
