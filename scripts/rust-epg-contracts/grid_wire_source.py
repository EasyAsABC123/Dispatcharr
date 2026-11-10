"""DRAFT original-network grid capture, unexecuted during quiet measurements.

Requires the disposable guide_source.py fixture; no clock/view patch is used.
All original application credit belongs to Dispatcharr authors/contributors.
"""
import datetime
import hashlib
import json
import os
from pathlib import Path
import sys
from urllib.error import HTTPError
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit
from urllib.request import HTTPRedirectHandler, Request, build_opener

if os.environ.get('POSTGRES_PASSWORD') != 'playback-fixture-only':
    raise RuntimeError('Disposable fixture required')
sys.path.insert(0, '/app')
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'dispatcharr.settings')
import django
django.setup()
from django.db.models import F
from django.utils import timezone
from apps.epg.models import EPGData, ProgramData

fixture = json.loads(Path('/evidence/guide-source.json').read_text())
anchor = timezone.now()
hour = anchor.replace(minute=0, second=0, microsecond=0)
if (hour + datetime.timedelta(hours=1) - anchor).total_seconds() < 600:
    raise RuntimeError('Refuse real-clock guide fixture with fewer than ten minutes before UTC hour boundary')
old = datetime.datetime.fromisoformat(fixture['clock'])
delta = anchor - old
owned_ids = fixture['fixtures']['epg_data_ids']
if EPGData.objects.filter(pk__in=owned_ids, name__startswith='rust-probe-guide-http-').count() != len(owned_ids):
    raise RuntimeError('Guide fixture ownership mismatch')
# Shift only this fixture's programme rows. Moving boundary-only rows away
# from now makes this network gate independent of per-request microseconds;
# the separate Factory gate retains literal exact-boundary characterization.
ProgramData.objects.filter(epg_id__in=owned_ids).update(
    start_time=F('start_time')+delta, end_time=F('end_time')+delta)
ProgramData.objects.filter(epg_id__in=owned_ids, title__contains='ends-now-excluded').update(
    end_time=anchor-datetime.timedelta(minutes=10))
ProgramData.objects.filter(epg_id__in=owned_ids, title__contains='starts-now-included').update(
    start_time=anchor+datetime.timedelta(minutes=10))

class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, *args):
        return None

opener = build_opener(NoRedirect())
cases = []

def guard():
    now = timezone.now()
    if not 0 <= (now-anchor).total_seconds() < 300 or now.replace(minute=0, second=0, microsecond=0) != hour:
        raise RuntimeError('Expired or hour-crossing guide wire fixture; do not normalize bodies')
    return now

def shifted_path(path):
    parts = urlsplit(path)
    pairs = []
    for key, value in parse_qsl(parts.query, keep_blank_values=True):
        if key in ('start', 'end') and value:
            try:
                value = (datetime.datetime.fromisoformat(value) + delta).isoformat()
            except ValueError:
                pass
        pairs.append((key, value))
    return urlunsplit(('', '', parts.path, urlencode(pairs), ''))

def capture(case, path):
    before = guard()
    headers = {key.removeprefix('HTTP_').replace('_', '-'): value
               for key, value in case['headers'].items()}
    request = Request('http://127.0.0.1:5656'+path, headers=headers, method=case['method'])
    try:
        response = opener.open(request, timeout=15)
    except HTTPError as error:
        response = error
    with response:
        body = response.read(8*1024*1024+1)
        if len(body) > 8*1024*1024:
            raise RuntimeError('Owned guide response exceeds fixture budget')
        expected = {'status': response.status, 'body_bytes': len(body),
            'body_sha256': hashlib.sha256(body).hexdigest(),
            'content_type': response.getheader('Content-Type'),
            'selected_headers': {name: response.getheader(name) for name in [
                'Content-Length', 'Allow', 'WWW-Authenticate', 'Vary',
                'X-Content-Type-Options', 'X-Frame-Options',
                'Referrer-Policy', 'Cross-Origin-Opener-Policy']}}
    after = guard()
    cases.append({'path': path, 'method': case['method'], 'role': case['role'],
        'headers': case['headers'], 'source_before': before.isoformat(),
        'source_after': after.isoformat(), 'expected': expected})

original_cases = [case for case in fixture['cases'] if case['path'].startswith('/api/epg/grid/')]
if len(original_cases) != 40:
    raise RuntimeError('Expected original baseline40 guide cases')
for case in original_cases:
    capture(case, shifted_path(case['path']))
# Explicit future window fixes the dummy anchor independently of server now.
future = [('start', (hour+datetime.timedelta(hours=1)).isoformat()),
          ('end', (hour+datetime.timedelta(hours=5)).isoformat())]
for role in ['admin', 'standard', 'filtered']:
    case = next(case for case in original_cases if case['role'] == role and case['path'] == '/api/epg/grid/')
    capture(case, '/api/epg/grid/?'+urlencode(future))
print(json.dumps({'scope': 'Actual unpatched original-network guide HTTP; clock-stable owned row fixture',
    'anchor': anchor.isoformat(), 'utc_hour': hour.isoformat(),
    'expires': (anchor+datetime.timedelta(minutes=5)).isoformat(),
    'cases': cases, 'native_verified': False, 'performance_verified': False}, ensure_ascii=False))
