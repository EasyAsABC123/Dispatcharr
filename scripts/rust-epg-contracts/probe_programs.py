#!/usr/bin/env python3
"""Packaged programme JSON/HEAD read replay from an actual original capture.
Original Dispatcharr authors retain application credit; no HTML/CRUD acceptance.
"""
import argparse
import hashlib
import json
from pathlib import Path
from urllib.request import Request, build_opener, HTTPRedirectHandler
from urllib.error import HTTPError


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, *args):
        return None


def capture(base, fixture):
    cases = []
    differences = []
    skipped_html = 0
    opener = build_opener(NoRedirect())
    for index, case in enumerate(fixture['cases']):
        if not case['path'].startswith('/api/epg/programs/'):
            continue
        if case['headers'].get('HTTP_ACCEPT') == 'text/html':
            skipped_html += 1
            continue
        headers = {name.removeprefix('HTTP_').replace('_', '-'): value
                   for name, value in case['headers'].items() if name.startswith('HTTP_')}
        request = Request(base+case['path'], headers=headers, method=case['method'])
        try:
            response = opener.open(request, timeout=20)
        except HTTPError as error:
            response = error
        with response:
            body = response.read(32*1024*1024+1)
            if len(body) > 32*1024*1024:
                raise RuntimeError('Owned fixture unexpectedly exceeded probe capacity')
            status = response.status
            expected = case['expected']
            equal = status == expected['status']
            if case['method'] == 'HEAD':
                equal = equal and not body and response.headers.get('Content-Length') == str(expected['rendered_representation_bytes'])
            else:
                try:
                    actual = json.loads(body)
                except ValueError:
                    actual = None
                equal = equal and actual == expected.get('json')
            if not equal:
                differences.append({'index': index, 'path': case['path'],
                                    'role': case['role'], 'status': status,
                                    'expected_status': expected['status']})
            cases.append({'index': index, 'path': case['path'], 'method': case['method'],
                          'role': case['role'], 'status': status, 'matches': equal,
                          'body_bytes': len(body), 'body_sha256': hashlib.sha256(body).hexdigest()})
    return {'scope': 'Packaged programme JSON/HEAD reads; HTML, CRUD, poster and grid excluded',
            'passed': len(cases) == 37 and not differences,
            'cases': cases, 'differences': differences, 'skipped_browsable_html': skipped_html,
            'application_parity_verified': False, 'performance_verified': False}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--base', required=True)
    parser.add_argument('--fixture', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    report = capture(args.base, json.loads(args.fixture.read_text()))
    args.output.write_text(json.dumps(report, indent=2)+'\n')
    print(json.dumps({'passed': report['passed'], 'cases': len(report['cases']),
                      'differences': len(report['differences'])}))
    return 0 if report['passed'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
