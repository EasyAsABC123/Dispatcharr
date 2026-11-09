#!/usr/bin/env python3
"""Capture normalized live HLS/fMP4 contracts or seekable origin HTTP contracts.

Original application credit belongs to the Dispatcharr authors. These probes do
not claim full backend parity, decoder validity, or performance improvement.
"""
import argparse
import hashlib
import json
import math
from pathlib import Path
import re
import time
import urllib.error
import urllib.parse
import urllib.request


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *_):
        return None


HEADERS = ('Content-Type', 'Content-Length', 'Cache-Control', 'Transfer-Encoding',
           'Location', 'X-Dispatcharr-Session-Token', 'Access-Control-Expose-Headers',
           'Content-Range', 'Accept-Ranges', 'ETag')


def request(url, headers=None, method='GET', no_redirect=False):
    opener = urllib.request.build_opener(NoRedirect()) if no_redirect else urllib.request.build_opener()
    try:
        return opener.open(urllib.request.Request(url, headers=headers or {}, method=method), timeout=30)
    except urllib.error.HTTPError as response:
        return response


def normalized_headers(response, token=None):
    return {name: (response.headers[name].replace(token, '<session-token>') if token else response.headers[name])
            for name in HEADERS if name in response.headers}


def playlist_segments(body):
    if not body.startswith('#EXTM3U\n'):
        raise ValueError('HLS playlist does not begin with EXTM3U')
    lines = [line.strip() for line in body.splitlines() if line.strip()]
    values = [line for line in lines if not line.startswith('#')]
    durations = [float(line.split(':', 1)[1].split(',', 1)[0]) for line in lines if line.startswith('#EXTINF:')]
    targets = [int(line.split(':', 1)[1]) for line in lines if line.startswith('#EXT-X-TARGETDURATION:')]
    if len(durations) != len(values) or not all(math.isfinite(value) and value > 0 for value in durations) or len(targets) != 1 or targets[0] < 1:
        raise ValueError('HLS media durations or target duration are missing/invalid')
    if not values or any(not re.fullmatch(r'[0-9]+\.ts', value) for value in values):
        raise ValueError('Missing/non-native HLS segment URIs')
    return values


def complete_mp4_prefix(data):
    offset, boxes = 0, []
    while len(data) - offset >= 8:
        size, kind = int.from_bytes(data[offset:offset + 4], 'big'), data[offset + 4:offset + 8].decode('ascii')
        header = 8
        if size == 1:
            if len(data) - offset < 16:
                break
            size, header = int.from_bytes(data[offset + 8:offset + 16], 'big'), 16
        if size < header:
            raise ValueError('Invalid/unknown-size live MP4 box')
        if size > len(data) - offset:
            break
        boxes.append({'type': kind, 'size': size, 'offset': offset})
        offset += size
    return offset, boxes


def wait_cleanup(origin, timeout=60):
    if not origin:
        return {'observed': False}
    began = time.monotonic()
    while True:
        with request(origin + '/stats') as response:
            stats = json.load(response)
        if stats['active'] == 0:
            return {'observed': True, 'elapsed_seconds': time.monotonic() - began,
                    'origin_after': stats}
        if time.monotonic() - began >= timeout:
            raise TimeoutError('Origin connections did not close within cleanup deadline')
        time.sleep(.5)


def hls(url, origin, output):
    events = []
    started = time.monotonic()
    with request(url, no_redirect=True) as response:
        token = response.headers.get('X-Dispatcharr-Session-Token')
        location = response.headers.get('Location', '')
        events.append({'kind': 'mint', 'status': response.status,
                       'headers': normalized_headers(response, token)})
        if response.status != 302 or not token or not re.fullmatch(r'[A-Za-z0-9_-]{32}', token):
            raise ValueError('HLS mint status/session token mismatch')
        if location != f'/proxy/hls/{token}/index.m3u8':
            raise ValueError('HLS capability location mismatch')
        playlist_url = urllib.parse.urljoin(url, location)
    with request(playlist_url) as response:
        first = response.read1(8)
        first_latency = time.monotonic() - started
        rest = response.read(128 * 1024)
        body = (first + rest).decode()
        events.append({'kind': 'initial_playlist', 'status': response.status,
                       'headers': normalized_headers(response, token),
                       'first_bytes': first.decode(), 'first_byte_seconds': first_latency})
        if response.status != 200 or response.headers.get_content_type() != 'application/vnd.apple.mpegurl':
            raise ValueError('HLS playlist content type/status mismatch')
    segments = playlist_segments(body)
    (output / 'playlist-initial.m3u8').write_text(body.replace(token, '<session-token>'))
    capture = bytearray()
    for uri in segments[:3]:
        with request(urllib.parse.urljoin(playlist_url, uri)) as response:
            data = response.read(8 * 1024 * 1024)
            events.append({'kind': 'segment', 'uri': uri, 'status': response.status,
                           'headers': normalized_headers(response, token), 'bytes': len(data),
                           'sha256': hashlib.sha256(data).hexdigest()})
            if (response.status != 200 or response.headers.get_content_type() != 'video/mp2t'
                    or not data or len(data) % 188 or any(data[offset] != 0x47 for offset in range(0, len(data), 188))):
                raise ValueError('HLS segment is not packet-aligned TS')
            capture.extend(data)
    (output / 'capture.ts').write_bytes(capture)
    head = max(int(uri[:-3]) for uri in segments)
    deadline = time.monotonic() + 15
    while True:
        with request(playlist_url) as response:
            advanced = response.read(128 * 1024).decode()
        advanced_segments = playlist_segments(advanced)
        if max(int(uri[:-3]) for uri in advanced_segments) > head:
            break
        if time.monotonic() >= deadline:
            raise TimeoutError('HLS playlist did not advance')
        time.sleep(1)
    (output / 'playlist-advanced.m3u8').write_text(advanced.replace(token, '<session-token>'))
    with request(urllib.parse.urljoin(playlist_url, str(head + 999999) + '.ts')) as response:
        error_body = json.load(response)
        events.append({'kind': 'missing_segment', 'status': response.status, 'body': error_body})
        if response.status != 404:
            raise ValueError('Unavailable segment should return404')
    base = urllib.parse.urlsplit(url)
    invalid_url = urllib.parse.urlunsplit((base.scheme, base.netloc, '/proxy/hls/invalid/index.m3u8', '', ''))
    with request(invalid_url) as response:
        events.append({'kind': 'invalid_capability', 'status': response.status, 'body': json.load(response)})
        if response.status != 410:
            raise ValueError('Invalid capability should return410')
    cleanup = wait_cleanup(origin)
    if cleanup['observed']:
        with request(playlist_url) as response:
            events.append({'kind': 'expired_capability', 'status': response.status, 'body': json.load(response)})
            if response.status != 410:
                raise ValueError('Cleaned session should return410')
    return {'events': events, 'cleanup': cleanup, 'token_sha256': hashlib.sha256(token.encode()).hexdigest(),
            'segments_captured': min(3, len(segments))}


def fmp4(url, origin, output):
    data = bytearray()
    began = time.monotonic()
    with request(url) as response:
        headers = normalized_headers(response)
        if response.status != 200 or response.headers.get_content_type() != 'video/mp4':
            raise ValueError('fMP4 response content type/status mismatch')
        first = None
        while len(data) < 16 * 1024 * 1024:
            chunk = response.read1(65536)
            if not chunk:
                break
            data.extend(chunk)
            if first is None:
                first = time.monotonic()
            if time.monotonic() - first >= 10:
                break
            if time.monotonic() - began > 45:
                raise TimeoutError('fMP4 capture exceeded startup/measurement bound')
    length, boxes = complete_mp4_prefix(data)
    types = [box['type'] for box in boxes]
    if 'ftyp' not in types or 'moov' not in types or types.count('moof') < 2 or types.count('mdat') < 2:
        raise ValueError('fMP4 capture lacks init or two complete media fragments')
    (output / 'capture.mp4').write_bytes(data[:length])
    return {'status': 200, 'headers': headers, 'boxes': boxes, 'captured_bytes': length,
            'discarded_incomplete_tail_bytes': len(data) - length, 'cleanup': wait_cleanup(origin)}


def vod_origin(url, output):
    events = []
    with request(url, method='HEAD', headers={'Range': 'bytes=0-9'}) as response:
        events.append({'kind': 'head', 'status': response.status, 'headers': normalized_headers(response)})
        size, etag = int(response.headers['Content-Length']), response.headers['ETag']
        if response.status != 200 or response.read():
            raise ValueError('VOD HEAD has body or wrong status')
    with request(url) as response:
        body = response.read(32 * 1024 * 1024)
        if response.status != 200 or len(body) != size:
            raise ValueError('VOD complete body length mismatch')
    (output / 'capture.mp4').write_bytes(body)
    for value, expected_status, expected in [('bytes=7-1023', 206, body[7:1024]),
                                             ('bytes=-300', 206, body[-300:]),
                                             (f'bytes={size}-', 416, b''),
                                             ('bytes=malformed', 200, body)]:
        with request(url, headers={'Range': value, 'If-Range': etag}) as response:
            content = response.read(32 * 1024 * 1024)
            events.append({'kind': 'range', 'request_range': value, 'status': response.status,
                           'headers': normalized_headers(response), 'bytes': len(content)})
            if response.status != expected_status or content != expected:
                raise ValueError('VOD range body/status mismatch')
    return {'events': events, 'bytes': size, 'sha256': hashlib.sha256(body).hexdigest()}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--url-file', type=Path, required=True)
    parser.add_argument('--origin-base')
    parser.add_argument('--mode', choices=['hls', 'fmp4', 'vod-origin'], required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    if any(args.output.iterdir()):
        parser.error('Output must be empty')
    urls = [line.strip() for line in args.url_file.read_text().splitlines() if line.strip()]
    if len(urls) != 1 or not urls[0].startswith(('http://', 'https://')):
        parser.error('URL file needs exactly one HTTP(S) URL')
    result = {'scope': args.mode + '-protocol-fixture-probe', 'passed': False,
              'decoder_verified': False, 'application_performance_verified': False,
              'normalizations': ['Opaque HLS capability token replaced by placeholder in headers/playlists',
                                 'Volatile Date header omitted; media sequence values retained as observed']}
    try:
        result['observed'] = (hls(urls[0], args.origin_base, args.output) if args.mode == 'hls' else
                              fmp4(urls[0], args.origin_base, args.output) if args.mode == 'fmp4' else
                              vod_origin(urls[0], args.output))
        result['passed'] = True
    except Exception as error:
        result['failure'] = type(error).__name__ + ': ' + str(error)
    (args.output / 'protocol.json').write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps({'passed': result['passed'], 'failure': result.get('failure')}))
    return 0 if result['passed'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
