"""One ordinary HLS viewer on the immutable paired fixture."""
import ast
import hashlib
import json
from pathlib import Path
import re
import time
import urllib.error
import urllib.request
import tarfile

from budget import bounded_read
from output_profile_cases import MARKER, NoRedirect



STALL = 7
CASES = ('current', 'legacy-no-timestamp', 'legacy-no-advertised-target', 'aged')

SNAPSHOT = """
local key = ARGV[1]
local playlist = redis.call('GET', key..':playlist')
local owner = redis.call('GET', key..':owner')
if not playlist or not owner then return {} end
if #playlist > 65536 then return redis.error_reply('playlist bound') end
return {playlist, redis.call('PEXPIRETIME', key..':playlist'), redis.sha1hex(owner)}
"""
CAS = """
local key, before, after, expiry, expected_owner = ARGV[1], ARGV[2], ARGV[3], tonumber(ARGV[4]), ARGV[5]
local owner = redis.call('GET', key..':owner')
if not owner or redis.sha1hex(owner) ~= expected_owner then return 'owner-changed' end
local playlist = key..':playlist'
local current = redis.call('GET', playlist)
if redis.call('PEXPIRETIME', playlist) ~= expiry then return 'expiry-changed' end
if current == after then return 'already-exact' end
if current ~= before then return 'bytes-changed' end
redis.call('SET', playlist, after, 'KEEPTTL')
if redis.call('PEXPIRETIME', playlist) ~= expiry or redis.call('GET', playlist) ~= after then
    return redis.error_reply('exact TTL restoration failed')
end
return 'changed-exact'
"""
ACCOUNTING = """
local channel = 'live:channel:'..ARGV[1]
local key = channel..':output:hls:p'..ARGV[2]
local owner = redis.call('GET', key..':owner')
local clients = redis.call('SMEMBERS', channel..':clients')
local matching = 0
for _, id in ipairs(clients) do
  local fields = redis.call('HMGET', channel..':clients:'..id, 'output_format', 'output_profile_id', 'hls_token')
  if fields[1] == 'hls' and fields[2] == ARGV[2] and fields[3] and redis.sha1hex(fields[3]) == ARGV[3] then
    matching = matching + 1
  end
end
return {owner and redis.sha1hex(owner) or '', #clients, matching}
"""


def sha(data):
    return hashlib.sha256(data).hexdigest()


def save(path, value):
    path.write_text(json.dumps(value, indent=2)+'\n')
    path.chmod(0o600)


def oracle(original_archive):
    namespace = {'json': json, 'time': time, 'HLS_STALE_PLAYLIST_SECONDS': 45}
    with tarfile.open(original_archive) as archive:
        for name, definitions in (
            ('segmenter.py', {'render_media_playlist'}),
            ('views.py', {'_StalePlaylist', '_playlist_is_stale', '_render_playlist_text'}),
        ):
            source = 'apps/proxy/live_proxy/output/hls/'+name
            nodes = [node for node in ast.parse(archive.extractfile(source).read()).body
                     if getattr(node, 'name', None) in definitions]
            assert {node.name for node in nodes} == definitions
            exec(compile(ast.Module(nodes, type_ignores=[]), source, 'exec'), namespace)
    return namespace['_render_playlist_text'], namespace['_StalePlaylist']


def variant(raw, label, now):
    if label == 'current':
        return raw
    state = json.loads(raw)
    if label == 'legacy-no-timestamp':
        assert 'ts' in state
        del state['ts']
    elif label == 'legacy-no-advertised-target':
        assert 'adv_target' in state
        del state['adv_target']
    else:
        assert label == 'aged'
        state['ts'] = now-max(45, 3*state['adv_target'])-60
    return json.dumps(state, separators=(',', ':'))


def request(fixture, path, method='GET', data=None, limit=65536, timeout=5, origin=False, authenticated=False):
    assert path.startswith('/') and not path.startswith('//') and '?' not in path
    endpoint = fixture.origin if origin else fixture.base_url
    headers = {'User-Agent': 'owned-valid-hls-legacy-playlist'}
    if origin:
        headers['Content-Type'] = 'application/json'
    if authenticated:
        assert not origin
        headers['Authorization'] = 'Bearer '+fixture.actual_admin_token()
    body = json.dumps(data).encode() if data is not None else None
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), NoRedirect())
    query = urllib.request.Request(endpoint+path, data=body, headers=headers, method=method)
    deadline = time.monotonic()+fixture.remaining(timeout)

    def remaining(seconds):
        left = deadline-time.monotonic()
        if left <= 0:
            raise TimeoutError('OwnedHTTPDeadline')
        return fixture.remaining(min(left, seconds))

    try:
        response = opener.open(query, timeout=remaining(timeout))
    except urllib.error.HTTPError as error:
        response = error
    with response:
        return response.status, {key.lower(): value for key, value in response.headers.items()}, bounded_read(
            response, limit, remaining
        )


def media_sequences(body):
    uris = [line for line in body.decode().splitlines() if line and not line.startswith('#')]
    assert all(re.fullmatch(r'[0-9]+\.ts', name) for name in uris)
    return [int(name[:-3]) for name in uris]


def snapshot(fixture, key):
    [value, expiry, owner] = json.loads(fixture.command(
        fixture.compose+['exec', '-T', 'baseline', 'redis-cli', '-n', '0', '--json',
                         'EVAL', SNAPSHOT, '0', key], 20, secret=True
    ))
    assert isinstance(expiry, int) and (expiry == -1 or expiry > int(time.time()*1000)+1500)
    assert re.fullmatch('[0-9a-f]{40}', owner)
    return value, expiry, owner


def origin_stats(fixture):
    code, _, body = request(fixture, '/stats', origin=True)
    assert code == 200
    return json.loads(body)


def origin_control(fixture, origin_path, action):
    status, _, data = request(fixture, '/control', method='POST', origin=True,
                              data={'path': origin_path, 'action': action, 'seconds': STALL if action == 'stall' else 0})
    assert status == 200 and json.loads(data) == {'applied': True}


def read_cell(fixture, key, capability, origin_path, label, item, persist, source_oracle):
    expected_body, stale = source_oracle
    began = time.monotonic()
    try:
        origin_control(fixture, origin_path, 'stall')
        sent = origin_stats(fixture)['bytes']
        fixture.sleep(0.35)
        assert origin_stats(fixture)['bytes'] == sent, 'OwnedOriginDidNotHold'
        before, expiry, owner = snapshot(fixture, key)
        patched = variant(before, label, time.time())
        item.update(input_sha256=sha(patched.encode()), original_sha256=sha(before.encode()),
                    absolute_expiration_ms=expiry, owner_sha1=owner, origins_stalled=True)
        persist()
        try:
            assert fixture.redis(0, CAS, key, before, patched, expiry, owner) in ('changed-exact', 'already-exact')
            assert snapshot(fixture, key) == (patched, expiry, owner)
            status, headers, body = request(fixture, capability, timeout=3)
            assert snapshot(fixture, key) == (patched, expiry, owner), 'DescriptorChangedDuringRead'
            assert time.monotonic()-began < STALL, 'HoldExpiredDuringRead'
            assert origin_stats(fixture)['bytes'] == sent, 'OriginResumedDuringRead'
            item.update(status=status, content_type=headers.get('content-type'), cache_control=headers.get('cache-control'),
                        body_bytes=len(body), body_sha256=sha(body))
            try:
                expected = expected_body(patched).encode()
            except stale:
                item['expected_status'] = 404
                assert label == 'aged' and status == 404 and json.loads(body) == {'error': 'Playlist stale'}
            else:
                item['expected_status'] = 200
                item['expected_body_sha256'] = sha(expected)
                assert status == 200 and body == expected
                assert headers['content-type'] == 'application/vnd.apple.mpegurl' and headers['cache-control'] == 'no-cache'
        finally:
            # Attempt even when a timed-out command may have installed the value.
            restoration = fixture.redis(0, CAS, key, patched, before, expiry, owner)
            item['restoration'] = restoration
            item['exact_restoration'] = (restoration in ('changed-exact', 'already-exact') and
                                         snapshot(fixture, key) == (before, expiry, owner))
            persist()
            assert item['exact_restoration'], 'DescriptorRestoreOwnershipUnsettled'
    finally:
        origin_control(fixture, origin_path, 'recover')


def process_generations(fixture):
    cid = fixture.native if fixture.backend == 'Native' else fixture.command(fixture.compose+['ps', '-q', 'baseline']).strip()
    output = fixture.command([fixture.docker, 'top', cid, '-eo', 'pid,args'])
    ids = [int(line.split()[0]) for line in output.splitlines()[1:] if 'ffmpeg' in line and MARKER in line and 'pipe:0' in line]
    generations = [[pid, fixture.pid_start(pid)] for pid in ids]
    assert len(generations) == 1 and generations[0][1]
    return generations


def decode(fixture, output, stream, sequence):
    status, headers, data = request(fixture, f'{stream}/{sequence}.ts', limit=4*1024*1024)
    assert status == 200 and headers['content-type'] == 'video/mp2t' and data and len(data) % 188 == 0
    assert headers['cache-control'] == 'no-cache'
    output.write_bytes(data)
    output.chmod(0o644)
    guest = '/evidence/'+output.relative_to(fixture.output).as_posix()
    prefix = fixture.compose+['exec', '-T', 'baseline']
    probe = json.loads(fixture.command(prefix+['ffprobe', '-v', 'error', '-count_frames',
                                               '-show_entries', 'stream=codec_type,nb_read_frames',
                                               '-of', 'json', guest], 20))
    fixture.command(prefix+['ffmpeg', '-hide_banner', '-loglevel', 'error', '-xerror', '-threads', '1',
                            '-i', guest, '-map', '0:v:0', '-map', '0:a:0', '-f', 'null', '-'], 20)
    receipt = fixture.cli[-1]
    assert receipt['actual_exit'] == 0 and receipt['process_group_settled'] and receipt['stdout_EOF'] and receipt['stderr_EOF']
    assert Path(receipt['stderr']).stat().st_size == 0
    counts = {stream['codec_type']: int(stream['nb_read_frames']) for stream in probe['streams']
              if stream.get('nb_read_frames', '').isdigit()}
    assert counts.get('video', 0) > 0 and counts.get('audio', 0) > 0
    return {'sequence': sequence, 'bytes': len(data), 'sha256': sha(data), 'actual_decoder_exit': receipt['actual_exit'],
            'cache_control': headers['cache-control'], 'decoded_frames': counts}


def run(fixture):
    source_oracle = oracle(fixture.binding['Original_archive'])
    out = fixture.output/'legacy-hls'
    out.mkdir(mode=0o755)
    out.chmod(0o755)
    report = {'status': 'running', 'backend': fixture.backend, 'cells': [],
              'full_M3_passed': False, 'performance_verified': False}

    def persist():
        save(out/'report.json', report)

    persist()
    before = origin_stats(fixture)
    assert before['active'] == 0
    profile = str(fixture._actual_output_profile_id)
    channel = fixture.channel_uuid
    key = f'live:channel:{channel}:output:hls:p{profile}'
    # The mint request itself has two benign query parameters.
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), NoRedirect())
    mint = urllib.request.Request(fixture.base_url+f'/proxy/ts/stream/{channel}?output_format=hls&output_profile={profile}',
                                  headers={'Authorization': 'Bearer '+fixture.actual_admin_token(),
                                           'User-Agent': 'owned-valid-hls-legacy-playlist'})
    try:
        minted = opener.open(mint, timeout=fixture.remaining(10))
    except urllib.error.HTTPError as error:
        minted = error
    with minted:
        status = minted.status
        token = minted.headers.get('X-Dispatcharr-Session-Token')
        location = minted.headers.get('Location')
    assert token and re.fullmatch('[A-Za-z0-9_-]{32}', token)
    fingerprint = hashlib.sha1(token.encode(), usedforsecurity=False).hexdigest()

    def warm():
        code, _, body = request(fixture, location, timeout=8)
        return media_sequences(body) if code == 200 and b'#EXTINF:' in body else None

    try:
        assert status == 302 and location == f'/proxy/hls/{token}/index.m3u8'
        sequences = fixture.wait(warm, 35, 'ActualWarmProfiledHLS')
        [origin_path] = [path for path, row in origin_stats(fixture)['paths'].items() if row.get('active') == 1]
        assert re.fullmatch(r'/live(?:/[0-9]+)?\.ts', origin_path)
        original = fixture.redis(0, ACCOUNTING, channel, profile, fingerprint)
        assert original[0] and original[1:] == [1, 1]
        generations = process_generations(fixture)
        report['warm_decoder'] = decode(fixture, out/'warm.ts', location.rsplit('/', 1)[0], sequences[-1])
        for label in CASES:
            item = {'case': label}
            report['cells'].append(item)
            persist()
            read_cell(fixture, key, location, origin_path, label, item, persist, source_oracle)
        current = fixture.wait(lambda: [seq for seq in (warm() or []) if seq > sequences[-1]], 25,
                               'ActualHLSSegmentAdvancedAfterRestore')
        report['recovery_decoder'] = decode(fixture, out/'recovery.ts', location.rsplit('/', 1)[0], current[-1])
        after = origin_stats(fixture)
        assert after['active'] == 1 and after['connections']-before['connections'] == 1
        assert fixture.redis(0, ACCOUNTING, channel, profile, fingerprint) == original
        assert process_generations(fixture) == generations
        report['accounting'] = {'origin_connections_added': 1, 'profiled_hls_clients': 1,
                                'same_HLS_owner_and_transform_generation': True}
        report['status'] = 'passed'
    except Exception as error:
        report.update(status='failed', failure_type=type(error).__name__)
    finally:
        try:
            code, _, _ = request(fixture, f'/api/proxy/hls/sessions/{token}/', 'DELETE', authenticated=True)
            report['viewer_stop_status'] = code
            assert code == 204
            if report['status'] == 'passed':
                code, _, _ = request(fixture, location, timeout=3)
                report['stopped_playlist_status'] = code
                assert code == 410
                fixture.wait(lambda: fixture.redis(0, ACCOUNTING, channel, profile, fingerprint)[1:] == [0, 0],
                             10, 'ActualStoppedHLSClientAbsent')
                report['stopped_client_absent'] = True
        except Exception as error:
            report.update(status='failed', stop_failure_type=type(error).__name__)
        persist()
    return report
