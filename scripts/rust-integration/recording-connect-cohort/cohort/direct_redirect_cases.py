"""Source-first unresolved saved-Redirect Stream branch in existing Fixture.
Capture validity is distinct from behavior compatibility; no bootstrap on import.
"""
import hashlib
import http.client
import json
import time
import urllib.parse
import uuid
from pathlib import Path

from output_profile_reader import Reader


def save(path, value):
    Path(path).write_text(json.dumps(value, indent=2) + '\n')


def run(f):
    """Run after redirect prepare/startup, before redirect Origin is stopped.

    Only Original is an authority capture. Native can optionally record its
    current explicit guard; no cross-backend equivalence is asserted here.
    """
    out = f.output / 'direct-Stream-Redirect-capture'
    out.mkdir(mode=0o755)
    out.chmod(0o755)
    seed = f._redirect_seed
    stream = int(seed['stream'])
    profile = str(seed['profile'])
    worker = 'owned-saved-redirect-' + uuid.uuid4().hex
    report = {
        'status': 'running', 'scope': 'saved Redirect direct Stream without skip: new transport/error characterization',
        'backend': f.backend, 'required_behavior_qualified': False,
        'full_M3_passed': False, 'performance_verified': False,
        'no_automatic_Proxy_or_302_expectation': True,
    }
    original = f.sql("from apps.channels.models import Stream;s=Stream.objects.get(pk=" + str(stream) + ");print(json.dumps({'profile':s.stream_profile_id,'hash':s.stream_hash,'url':s.url}))")
    prepared = f.sql("from apps.channels.models import Stream;from core.models import StreamProfile;p=StreamProfile.objects.get(name='Redirect',locked=True);s=Stream.objects.get(pk=" + str(stream) + ");s.stream_profile=p;s.stream_hash=" + repr(worker) + ";s.url=" + repr(seed['media_url']) + ";s.save(update_fields=['stream_profile','stream_hash','url']);print(json.dumps({'profile_id':p.pk,'name':p.name,'locked':p.locked,'active':p.is_active,'command_empty':p.command=='','parameters_empty':p.parameters=='','built_command':p.build_command(s.url,'owned-direct-Redirect',s.pk),'stream_id':s.pk}))", secret=True)
    # Never publish generated credentials/URLs from a command payload.
    prepared.pop('built_command', None)
    report['actual_saved_profile'] = prepared
    save(out / 'report.json', report)

    def snapshot(key):
        rows = f.redis(0, "return {redis.call('GET','stream_profile:'..ARGV[1]..':preview')or'',redis.call('GET','stream_profile:'..ARGV[1]..':p'..ARGV[3])or'',redis.call('GET','profile_connections:'..ARGV[3])or'0',redis.call('SCARD','live:channel:'..ARGV[2]..':clients'),redis.call('GET','live:channel:'..ARGV[2]..':owner')or'',redis.call('HGET','live:channel:'..ARGV[2]..':metadata','state')or'',redis.call('HGET','live:channel:'..ARGV[2]..':metadata','stream_profile')or'',redis.call('GET','channel_stream:'..ARGV[4])or'',redis.call('GET','stream_profile:'..ARGV[1])or''}", str(stream), key, profile, str(f.channel_id))
        return {'default_preview_alias': rows[0], 'scoped_preview_alias': rows[1], 'provider_count': int(rows[2]), 'clients': rows[3], 'owner_present': bool(rows[4]), 'state': rows[5], 'stream_profile_metadata': rows[6], 'bare_Channel_alias': rows[7], 'bare_Stream_alias': rows[8]}

    def origin():
        return json.loads(f.get(f.origin + '/stats')[1])

    def retire(key, label):
        receipt = {'actual_stop': f.api('POST', '/proxy/ts/stop/' + key, {}, 'admin')}
        save(out / (label + '-stop.json'), receipt)
        f.wait(lambda: not snapshot(key)['owner_present'] and snapshot(key)['clients'] == 0 and snapshot(key)['provider_count'] == 0 and origin()['active'] == 0, 30, 'actual-direct-Redirect-worker-slot-retirement')
        receipt['actual_retired'] = snapshot(key)
        save(out / (label + '-stop.json'), receipt)
        return receipt

    try:
        if not (prepared['locked'] and prepared['active'] and prepared['command_empty'] and prepared['parameters_empty']):
            raise AssertionError('ActualUnmodifiedLockedRedirectRequired')
        before = origin()
        before_state = snapshot(worker)
        if before_state['provider_count'] or before_state['owner_present'] or before_state['clients'] or before['active']:
            raise AssertionError('ExistingRedirectContextMustBeIdle')
        observed = {'before': before_state, 'samples': [], 'status': None, 'location_present': False}
        report['without_skip'] = observed
        connection = None
        response = None
        capture = out / 'without-skip-body.bin'
        try:
            base = urllib.parse.urlsplit(f.base_url)
            if base.scheme != 'http' or base.username or base.password or base.path not in ('', '/'):
                raise ValueError('ExistingOwnedHTTPFixtureOnly')
            connection = http.client.HTTPConnection(base.hostname, base.port, timeout=f.remaining(6))
            connection.request('GET', '/proxy/ts/stream/' + worker, headers={'Authorization': 'Bearer ' + f.actual_admin_token(), 'User-Agent': 'owned-saved-Redirect-characterization'})
            response = connection.getresponse()
            observed.update(status=response.status, content_type=response.getheader('Content-Type'), location_present=response.getheader('Location') is not None)
            save(out / 'report.json', report)
            deadline = time.monotonic() + f.remaining(8)
            with capture.open('wb') as body:
                while time.monotonic() < deadline and body.tell() < 2 * 1024 * 1024 and len(observed['samples']) < 64:
                    observed['samples'].append(snapshot(worker))
                    save(out / 'report.json', report)
                    if connection.sock is not None:
                        connection.sock.settimeout(min(1, f.remaining(max(.001, deadline - time.monotonic()))))
                    try:
                        chunk = response.read1(min(65536, 2 * 1024 * 1024 - body.tell()))
                    except (TimeoutError, OSError) as error:
                        observed['read_stop_type'] = type(error).__name__
                        break
                    if not chunk:
                        observed['actual_body_EOF'] = True
                        break
                    body.write(chunk)
                observed['capture_limit_reached'] = body.tell() == 2 * 1024 * 1024
                observed['sample_limit_reached'] = len(observed['samples']) == 64
            observed.update(body_bytes=capture.stat().st_size, body_sha256=hashlib.sha256(capture.read_bytes()).hexdigest())
        except Exception as error:
            observed['actual_transport_exception_type'] = type(error).__name__
        finally:
            if response is not None:
                response.close()
            if connection is not None:
                connection.close()
            observed['controller_HTTP_socket_closed'] = True
            observed['after_request_close'] = snapshot(worker)
            now = origin()
            observed['actual_origin_connection_delta'] = now['connections'] - before['connections']
            observed['actual_origin_active'] = now['active']
            save(out / 'report.json', report)
            observed['retirement'] = retire(worker, 'without-skip')
        # Positive control uses the real privileged skip route; it does not
        # redefine the observed no-skip result as success or failure.
        scoped = worker + '.p' + profile
        reader = Reader(f.base_url + '/proxy/ts/stream/' + worker + '?skip_redirect=true', out / 'privileged-skip-control.ts', f.actual_admin_token())
        try:
            f.wait(lambda: reader.snapshot()['media_bytes'] >= 750000 and not reader.snapshot()['errors'], 35, 'actual-saved-Redirect-privileged-Proxy-control')
            report['privileged_skip_control'] = {'reader': reader.snapshot(), 'actual': snapshot(scoped), 'scope': 'fresh non-null TS/control only, no codec or performance qualification'}
            actual = report['privileged_skip_control']['actual']
            if actual['provider_count'] != 1 or actual['clients'] != 1 or not actual['owner_present'] or actual['scoped_preview_alias'] != profile or actual['bare_Channel_alias'] or actual['bare_Stream_alias']:
                raise AssertionError('ActualPrivilegedSkipScopedOneSlot')
        finally:
            reader.close()
            report.setdefault('privileged_skip_control', {})['reader_final'] = reader.snapshot()
            save(out / 'report.json', report)
            report['privileged_skip_control']['retirement'] = retire(scoped, 'privileged-skip')
        report['capture_valid'] = True
        report['status'] = 'passed'
        report['status_meaning'] = 'bounded characterization and physical cleanup completed; unresolved no-skip behavior not qualified'
    except BaseException as error:
        report['primary_failure'] = {'type': type(error).__name__, 'label': str(error)[:160]}
        report['status'] = 'failed'
    finally:
        try:
            if snapshot(worker)['owner_present'] or snapshot(worker + '.p' + profile)['owner_present'] or snapshot(worker)['provider_count']:
                raise RuntimeError('OwnedStreamRestoreBlockedByActualOwnerOrProvider')
            f.sql("from apps.channels.models import Stream;Stream.objects.filter(pk=" + str(stream) + ").update(stream_profile_id=" + repr(original['profile']) + ",stream_hash=" + repr(original['hash']) + ",url=" + repr(original['url']) + ");print(json.dumps({'actual_owned_Stream_restored':True}))")
            report['actual_owned_Stream_restored'] = True
        except Exception as error:
            report['restore_failure'] = type(error).__name__
            report['status'] = 'failed'
        save(out / 'report.json', report)
    return report
