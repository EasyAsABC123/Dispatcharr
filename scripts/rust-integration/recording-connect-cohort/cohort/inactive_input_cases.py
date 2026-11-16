"""Source/Native saved inactive INPUT profile, actual TS/hash-HLS lifecycle.
Join existing Redirect fixture; no app/network/bootstrap work on import.
"""
import hashlib
import json
import re
import urllib.error
import urllib.parse
import urllib.request
import uuid
from pathlib import Path

from output_profile_reader import Reader


def save(path, value):
    Path(path).write_text(json.dumps(value, indent=2) + '\n')


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        return None


def run(f):
    out = f.output / 'inactive-input-profile-capture'
    out.mkdir(mode=0o755)
    out.chmod(0o755)
    seed = f._redirect_seed
    worker = 'owned-inactive-input-' + uuid.uuid4().hex
    report = {'status': 'running', 'cells': [], 'full_M3_passed': False, 'performance_verified': False,
              'scope': 'saved inactive custom FFmpeg input profile on Channel TS and direct Stream HLS; provider/output profile active policy unchanged'}
    original = f.sql("from apps.channels.models import Channel,Stream;c=Channel.objects.get(pk=" + str(f.channel_id) + ");s=Stream.objects.get(pk=" + str(seed['stream']) + ");print(json.dumps({'Channel_profile':c.stream_profile_id,'Stream_profile':s.stream_profile_id,'hash':s.stream_hash,'url':s.url}))")
    selected = f.sql("from apps.channels.models import Channel,Stream;from core.models import StreamProfile;t=StreamProfile.get_locked('FFmpeg');p=StreamProfile.objects.create(name=" + repr(worker) + ",command=t.command,parameters=t.parameters,user_agent_id=t.user_agent_id,is_active=False,locked=False);Channel.objects.filter(pk=" + str(f.channel_id) + ").update(stream_profile=p);Stream.objects.filter(pk=" + str(seed['stream']) + ").update(stream_profile=p,stream_hash=" + repr(worker) + ",url=" + repr(seed['media_url']) + ");print(json.dumps({'id':p.pk,'is_active':p.is_active,'locked':p.locked,'command_sha256':__import__('hashlib').sha256(p.command.encode()).hexdigest(),'parameters_sha256':__import__('hashlib').sha256(p.parameters.encode()).hexdigest(),'actual_FFmpeg_template_id':t.pk}))")
    report['actual_profile'] = selected

    def persist():
        save(out / 'report.json', report)

    def origin():
        return json.loads(f.get(f.origin + '/stats')[1])

    def snapshot(key):
        rows = f.redis(0, "local p='live:channel:'..ARGV[1];return {redis.call('SCARD',p..':clients'),redis.call('GET',p..':owner')or'',redis.call('GET','profile_connections:'..ARGV[2])or'0',redis.call('HGET',p..':metadata','stream_profile')or'',redis.call('GET','stream_profile:'..ARGV[3]..':preview')or'',redis.call('EXISTS',p..':stopping'),redis.call('HGET',p..':metadata','state')or''}", key, str(seed['profile']), str(seed['stream']))
        return {'clients': rows[0], 'owner_present': bool(rows[1]), 'provider_count': int(rows[2]), 'input_profile_metadata': rows[3], 'Stream_preview_alias': rows[4], 'stopping_signal_present': bool(rows[5]), 'metadata_state': rows[6]}

    def input_pids():
        cid = f.native if f.backend == 'Native' else f.command(f.compose + ['ps', '-q', 'baseline']).strip()
        rows = f.command([f.docker, 'top', cid, '-eo', 'pid,args'], secret=True)
        return [int(row.split()[0]) for row in rows.splitlines()[1:] if 'ffmpeg' in row and '-i ' + seed['media_url'] in row]

    def request(path, method='GET'):
        # The only absolute URI accepted here must remain this existing Main.
        url = urllib.parse.urljoin(f.base_url + '/', path)
        if urllib.parse.urlsplit(url).netloc != urllib.parse.urlsplit(f.base_url).netloc:
            raise ValueError('HLSCapabilityMustRemainOwnedMain')
        req = urllib.request.Request(url, method=method, headers={'Authorization': 'Bearer ' + f.actual_admin_token()})
        try:
            reply = urllib.request.build_opener(NoRedirect()).open(req, timeout=f.remaining(12))
        except urllib.error.HTTPError as error:
            reply = error
        with reply:
            body = reply.read(8 * 1024 * 1024 + 1)
            if len(body) > 8 * 1024 * 1024:
                raise ValueError('ExistingHLSSegment8MiBCaptureBound')
            return reply.status, dict(reply.headers), body

    def decode(path, label):
        local = '/evidence/' + str(path.relative_to(f.output))
        probe = json.loads(f.command(f.compose + ['exec', '-T', 'baseline', 'ffprobe', '-v', 'error', '-show_entries', 'stream=codec_type,codec_name', '-of', 'json', local], 30))
        progress = f.command(f.compose + ['exec', '-T', 'baseline', 'ffmpeg', '-hide_banner', '-loglevel', 'error', '-xerror', '-threads', '1', '-filter_threads', '1', '-filter_complex_threads', '1', '-progress', 'pipe:1', '-nostats', '-i', local, '-t', '3', '-map', '0:v:0', '-map', '0:a:0', '-threads:v', '1', '-threads:a', '1', '-f', 'null', '-'], 30, allow=True)
        actual = f.cli[-1]
        stderr = Path(actual['stderr']).read_bytes()
        proof = {'actual_exit': actual['actual_exit'], 'stdout_EOF': actual['stdout_EOF'], 'stderr_EOF': actual['stderr_EOF'], 'process_group_settled': actual['process_group_settled'], 'stderr_bytes': len(stderr), 'stderr_sha256': hashlib.sha256(stderr).hexdigest(), 'video_frames': max([int(line.split('=')[1]) for line in progress.splitlines() if line.startswith('frame=')], default=0), 'AV': {'video', 'audio'} <= {v['codec_type'] for v in probe['streams']}, 'media_sha256': hashlib.sha256(path.read_bytes()).hexdigest()}
        save(out / (label + '-decoder.json'), proof)
        if not (proof['actual_exit'] == 0 and proof['stdout_EOF'] and proof['stderr_EOF'] and proof['process_group_settled'] and not stderr and proof['AV'] and proof['video_frames'] >= 70):
            raise AssertionError('ExistingStrictThreeSecondAVPrefix')
        return proof

    def stop(key, label):
        receipt = f.api('POST', '/proxy/ts/stop/' + key, {}, 'admin')
        save(out / (label + '-stop.json'), receipt)
        f.wait(lambda: not snapshot(key)['owner_present'] and snapshot(key)['provider_count'] == 0 and snapshot(key)['clients'] == 0 and not snapshot(key)['stopping_signal_present'] and snapshot(key)['metadata_state'] != 'stopping' and not input_pids() and origin()['active'] == 0, 40, 'actual-inactive-input-worker-process-provider-retirement')
        return {'actual_stop': receipt, 'actual_retired': snapshot(key)}

    def channel_ts():
        before = origin()
        readers = []
        proof = {}
        try:
            for n in range(2):
                reader = Reader(f.base_url + '/proxy/ts/stream/' + f.channel_uuid, out / ('Channel-' + str(n) + '.ts'), f.actual_admin_token())
                readers.append(reader)
                f.wait(lambda: reader.snapshot()['media_bytes'] >= 750000 and not reader.snapshot()['errors'], 35, 'actual-inactive-profile-Channel-TS')
            proof.update(actual=snapshot(f.channel_uuid), actual_input_pids=input_pids(), actual_origin_delta=origin()['connections'] - before['connections'])
            save(out / 'Channel-active.json', proof)
            if proof['actual']['clients'] != 2 or proof['actual']['provider_count'] != 1 or not proof['actual']['owner_present'] or len(proof['actual_input_pids']) != 1 or proof['actual_origin_delta'] != 1:
                raise AssertionError('InactiveSavedInputMustRetainSharedOnePhysicalProcess')
            readers[0].close()
            f.wait(lambda: snapshot(f.channel_uuid)['clients'] == 1, 10, 'actual-inactive-profile-selective-reader-retirement')
            count = readers[1].snapshot()['media_bytes']
            f.wait(lambda: readers[1].snapshot()['media_bytes'] >= count + 188000 and not readers[1].snapshot()['errors'], 10, 'actual-inactive-profile-sibling-advance')
        except BaseException as error:
            save(out / 'Channel-primary-failure.json', {'type': type(error).__name__, 'label': str(error)[:160]})
            raise
        finally:
            for n, reader in enumerate(readers):
                reader.close()
                save(out / ('Channel-reader-' + str(n) + '.json'), reader.snapshot())
            proof['retirement'] = stop(f.channel_uuid, 'Channel')
        proof['decoders'] = [decode(out / ('Channel-' + str(n) + '.ts'), 'Channel-' + str(n)) for n in range(2)]
        return proof

    def stream_hls():
        before = origin()
        token = None
        proof = {}
        try:
            code, headers, body = request('/proxy/ts/stream/' + worker + '?output_format=hls')
            location = headers.get('Location') or headers.get('location')
            token = headers.get('X-Dispatcharr-Session-Token') or headers.get('x-dispatcharr-session-token')
            proof['mint'] = {'status': code, 'body_bytes': len(body), 'body_sha256': hashlib.sha256(body).hexdigest(), 'capability_present': bool(token), 'location_present': bool(location)}
            save(out / 'Stream-HLS-mint.json', proof)
            if code != 302 or not token or not re.fullmatch(r'/proxy/hls/[A-Za-z0-9_-]{32}/index\.m3u8', location or ''):
                raise AssertionError('ActualStreamHashHLSCapability')
            current = {}
            def ready():
                status, _, value = request(location)
                current.update(status=status, body=value)
                return status == 200 and b'#EXTINF:' in value
            f.wait(ready, 35, 'actual-inactive-input-Stream-HLS-segments')
            playlist = current['body'].decode()
            uris = [line for line in playlist.splitlines() if line and not line.startswith('#')]
            if not uris:
                raise AssertionError('ActualStreamHashHLSSegment')
            capture = out / 'Stream-HLS.ts'
            with capture.open('wb') as target:
                for uri in uris[-2:]:
                    path = urllib.parse.urljoin(location, uri)
                    if not re.fullmatch(r'/proxy/hls/[A-Za-z0-9_-]{32}/[0-9]+\.ts', path):
                        raise AssertionError('OwnedRelativeHLSCapabilitySegment')
                    status, _, value = request(path)
                    if status != 200 or not value or len(value) % 188 or value[0] != 0x47:
                        raise AssertionError('ActualStreamHashHLSTS')
                    target.write(value)
            proof.update(actual=snapshot(worker), actual_input_pids=input_pids(), actual_origin_delta=origin()['connections'] - before['connections'], playlist_sha256=hashlib.sha256(current['body']).hexdigest())
            save(out / 'Stream-HLS-active.json', proof)
            if proof['actual']['clients'] != 1 or proof['actual']['provider_count'] != 1 or not proof['actual']['owner_present'] or proof['actual']['Stream_preview_alias'] != str(seed['profile']) or len(proof['actual_input_pids']) != 1 or proof['actual_origin_delta'] != 1:
                raise AssertionError('InactiveInputStreamHashHLSMustShareActualInput')
            proof['decoded'] = decode(capture, 'Stream-HLS')
            return proof
        except BaseException as error:
            save(out / 'Stream-HLS-primary-failure.json', {'type': type(error).__name__, 'label': str(error)[:160]})
            raise
        finally:
            if token:
                status, _, value = request('/api/proxy/hls/sessions/' + token + '/', method='DELETE')
                save(out / 'Stream-HLS-session-stop.json', {'status': status, 'body_bytes': len(value), 'body_sha256': hashlib.sha256(value).hexdigest()})
                if status not in (200, 204):
                    proof['session_stop_failure'] = status
            proof['retirement'] = stop(worker, 'Stream-HLS')
            save(out / 'Stream-HLS-final.json', proof)
            if 'session_stop_failure' in proof:
                raise AssertionError('ActualStreamHashHLSSessionStop')

    persist()
    try:
        for label, operation in [('inactive-custom-input-Channel-TS-sharing-retirement', channel_ts), ('inactive-custom-input-direct-Stream-HLS-capability-decoder-retirement', stream_hls)]:
            row = {'name': label, 'status': 'running'}
            report['cells'].append(row)
            persist()
            if input_pids() or origin()['active']:
                row.update(status='blocked', reason='prior actual input/upstream did not retire')
                persist()
                continue
            try:
                row['observation'] = operation()
                row['status'] = 'passed'
            except Exception as error:
                row.update(status='failed', error_type=type(error).__name__, error_label=str(error)[:160])
            persist()
    finally:
        try:
            if snapshot(f.channel_uuid)['owner_present'] or snapshot(worker)['owner_present'] or input_pids() or snapshot(worker)['provider_count']:
                raise RuntimeError('ActualLiveInputBlocksOwnedRowRestore')
            f.sql("from apps.channels.models import Channel,Stream;from core.models import StreamProfile;Channel.objects.filter(pk=" + str(f.channel_id) + ").update(stream_profile_id=" + repr(original['Channel_profile']) + ");Stream.objects.filter(pk=" + str(seed['stream']) + ").update(stream_profile_id=" + repr(original['Stream_profile']) + ",stream_hash=" + repr(original['hash']) + ",url=" + repr(original['url']) + ");StreamProfile.objects.filter(pk=" + str(selected['id']) + ").delete();print(json.dumps({'actual_owned_rows_restored':True}))")
            report['actual_owned_rows_restored'] = True
        except Exception as error:
            report['restore_error'] = type(error).__name__
        report['status'] = 'passed' if len(report['cells']) == 2 and all(row['status'] == 'passed' for row in report['cells']) and report.get('actual_owned_rows_restored') else 'failed'
        persist()
    return report
