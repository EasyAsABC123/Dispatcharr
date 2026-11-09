#!/usr/bin/env python3
"""Record native VOD wire contracts; differences are evidence, not parity claims."""
import argparse
import hashlib
import json
import re
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args):
        return None


def fetch(url, method='GET', headers=None, token=None):
    headers = dict(headers or {})
    if token:
        headers['Authorization'] = 'Bearer ' + token
    request = urllib.request.Request(url, headers=headers, method=method)
    try:
        response = urllib.request.build_opener(NoRedirect).open(request, timeout=20)
    except urllib.error.HTTPError as error:
        response = error
    with response:
        body = response.read(8*1024*1024+1)
        if len(body) > 8*1024*1024:
            raise ValueError('Fixture exceeds explicit 8 MiB probe bound')
        return response.status, dict(response.headers), body


def normalized(headers):
    keep = {'content-type', 'content-length', 'content-range', 'accept-ranges', 'etag',
            'cache-control', 'allow', 'last-modified', 'location'}
    result = {key.lower(): value for key, value in headers.items() if key.lower() in keep}
    if 'location' in result:
        parsed = urllib.parse.urlsplit(result['location'])
        if parsed.scheme or parsed.netloc:
            result['location'] = '<provider-redirect-redacted>'
        else:
            result['location'] = re.sub(r'/vod_[^/?]+', '/<session>', result['location'])
    return result


def probe(base, native_path, token=None, capture_fullfile=None):
    events = []
    for name, method, request_headers in [
        ('head', 'HEAD', {}), ('full', 'GET', {}),
        ('first-range', 'GET', {'Range': 'bytes=0-255'}),
        ('middle-range', 'GET', {'Range': 'bytes=1000-1255'}),
        ('suffix-range', 'GET', {'Range': 'bytes=-256'}),
        ('unsatisfiable', 'GET', {'Range': 'bytes=9000000-'}),
        ('if-range-mismatch', 'GET', {'Range': 'bytes=0-255', 'If-Range': '"different"'})]:
        status, headers, body = fetch(base+native_path, method, request_headers, token)
        mint = {'status': status, 'headers': normalized(headers)}
        if method == 'GET' and status == 301:
            location = next(value for key, value in headers.items() if key.lower() == 'location')
            parsed = urllib.parse.urlsplit(location)
            if parsed.scheme or parsed.netloc:
                raise ValueError('Session mint redirected outside the gateway')
            status, headers, body = fetch(urllib.parse.urljoin(base, location), method, request_headers, token)
        if name == 'full' and status == 200 and capture_fullfile is not None:
            Path(capture_fullfile).write_bytes(body)
        events.append({'case': name, 'mint': mint, 'status': status,
                       'headers': normalized(headers), 'bytes': len(body),
                       'body_sha256': hashlib.sha256(body).hexdigest()})
    return {'format_version': 1, 'scope': 'native XC VOD file wire characterization',
            'performance_verified': False, 'parity_verified': False, 'events': events}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--base-url', required=True)
    parser.add_argument('--movie-id', required=True)
    parser.add_argument('--output', required=True, type=Path)
    parser.add_argument('--token-file', type=Path, help='Private temporary token; never included in evidence')
    args = parser.parse_args()
    token = args.token_file.read_text().strip() if args.token_file else None
    report = probe(args.base_url.rstrip('/'), '/proxy/vod/movie/'+str(__import__('uuid').UUID(args.movie_id)), token)
    args.output.write_text(json.dumps(report, indent=2)+'\n')
    print(json.dumps({'recorded_cases': len(report['events']), 'parity_verified': False}))

if __name__ == '__main__':
    main()


def profile_alias_probe(base, native_path, profile_id):
    """Separate characterization: the original bare-profile redirect may fail."""
    results = []
    for selected in [profile_id, 0]:
        status, headers, body = fetch(base+native_path+'/'+str(selected)+'/')
        record = {'profile_id': selected, 'mint_status': status, 'mint_headers': normalized(headers)}
        if status == 301:
            location = next(value for key, value in headers.items() if key.lower() == 'location')
            parsed = urllib.parse.urlsplit(location)
            if parsed.scheme or parsed.netloc:
                raise ValueError('Profile session mint left the gateway')
            followed, followed_headers, followed_body = fetch(urllib.parse.urljoin(base,location))
            record.update(follow_status=followed, follow_headers=normalized(followed_headers),
                          follow_bytes=len(followed_body), follow_body_sha256=hashlib.sha256(followed_body).hexdigest())
        results.append(record)
    return {'scope':'native VOD bare-profile alias source characterization',
            'application_parity_verified':False,'performance_verified':False,'results':results}
