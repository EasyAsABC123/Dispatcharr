"""Normal DVR list, Browser recording URL, Range seek and HLS media controls."""
import hashlib
from pathlib import Path
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import build_opener, HTTPRedirectHandler, ProxyHandler, Request

ROOT = '/api/channels/recordings/'
READ_LIMIT = 2 * 1024 * 1024


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, request, response, code, message, headers, newurl):
        return None


def media(base, path, token=None, range_header=None, method='GET'):
    headers = {}
    if token is not None:
        headers['Authorization'] = 'Bearer ' + token
    if range_header is not None:
        headers['Range'] = range_header
    query = Request(base + path, headers=headers, method=method)
    try:
        response = build_opener(ProxyHandler({}), NoRedirect()).open(query, timeout=15)
    except HTTPError as error:
        response = error
    with response:
        body = response.read(READ_LIMIT + 1)
        if len(body) > READ_LIMIT:
            raise RuntimeError('Owned recording response exceeded fixture bound')
        return response.status, response.headers, body


def run(http, baseline, candidate, accounts, record, fixture, digest):
    ids = fixture({'action': 'recording_read_seed'})
    seed = Path(__file__).resolve().parents[1] / 'rust-integration/recording_read_assets'
    files = {name: (seed / name).read_bytes() for name in ids['files']}
    if {name: hashlib.sha256(raw).hexdigest() for name, raw in files.items()} != ids['files']:
        raise RuntimeError('Original and Native media fixture reference drift')
    before = fixture({'action': 'recording_read_snapshot'})
    expected = {row['id']: row for row in before['rows']}
    token = accounts['admin']['access']
    query = '?' + urlencode({'token': token})
    for backend, base in (('source', baseline), ('native', candidate)):
        completed = expected[ids['completed']]['custom_properties']['file_url']
        active = expected[ids['active']]['custom_properties']['file_url']
        status, rows = http(base + ROOT, token=token)
        normalized = {row['id']: row for row in rows} if isinstance(rows, list) else None
        listed = (isinstance(rows, list) and len(rows) == len(expected)
                  and digest(normalized) == digest(expected))
        record(f'recording-read-{backend}:dvr-list', status == 200 and listed,
               {'actual_status': status, 'json_sha256': digest(rows), 'owned_urls_and_rows_match': listed})
        if not listed or status != 200:
            raise RuntimeError('DVR list prerequisite failed')

        def observe(name, response, expected_code, raw, expected_headers):
            actual, headers, payload = response
            same_headers = all(headers.get(key) == value for key, value in expected_headers.items())
            record(f'recording-read-{backend}:{name}', actual == expected_code and payload == raw and same_headers,
                   {'actual_status': actual, 'expected_media_or_manifest_exact': payload == raw,
                    'headers_match': same_headers, 'response_bytes': len(payload),
                    'response_sha256': hashlib.sha256(payload).hexdigest()})

        raw = files['owned-completed.mkv']
        full = {'Content-Type': 'video/x-matroska', 'Content-Length': str(len(raw)), 'Accept-Ranges': 'bytes',
                'Content-Disposition': 'inline; filename="owned-completed.mkv"'}
        observe('watch-completed-with-react-query', media(base, completed + query), 200, raw, full)
        observe('file-head', media(base, completed, token, method='HEAD'), 200, b'', full)
        for name, header, start, end in (('closed-seek', 'bytes=123-337', 123, 337),
                                         ('open-seek', 'bytes=1024-', 1024, len(raw) - 1)):
            expected_range = {**full, 'Content-Length': str(end - start + 1),
                              'Content-Range': f'bytes {start}-{end}/{len(raw)}'}
            observe(name, media(base, completed + query, range_header=header), 206, raw[start:end + 1],
                    expected_range)
        segment = active.removesuffix('index.m3u8') + '000000.ts'
        playlist = files['owned-index.m3u8'].replace(b'000000.ts\n', (base + segment + '\n').encode())
        observe('hls-playlist-bearer', media(base, active, token), 200, playlist,
                {'Content-Type': 'application/x-mpegURL', 'Cache-Control': 'no-cache'})
        ts = files['000000.ts']
        if fixture({'action': 'recording_read_reset_heartbeat', 'id': ids['active']}) != {'absent': True}:
            raise RuntimeError('Expected fresh heartbeat scope before HLS segment')
        observe('hls-segment-bearer', media(base, segment, token), 200, ts,
                {'Content-Type': 'video/mp2t', 'Content-Length': str(len(ts))})
        heartbeat = fixture({'action': 'recording_read_heartbeat', 'id': ids['active']})
        active = heartbeat['active'] and 0 < heartbeat['ttl'] <= 20
        record(f'recording-read-{backend}:hls-heartbeat', active,
               {'owned_heartbeat_active': active, 'actual_ttl_seconds': heartbeat['ttl']})
        after = fixture({'action': 'recording_read_snapshot'})
        record(f'recording-read-{backend}:sql-unchanged', after == before,
               {'all_recording_rows_and_sequence_unchanged': after == before})
