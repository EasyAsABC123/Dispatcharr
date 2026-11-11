"""Captured BrowsableAPI package comparison; one validated random mask slot.
Original Dispatcharr/Django/DRF authors retain all credit and licenses.
"""
import datetime
from email.utils import parsedate_to_datetime
import hashlib
from http.cookies import SimpleCookie
import json
import re

ALPHABET = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789'
PREFIX = '"csrfToken": "'
INTRINSIC = ['content-type', 'content-length', 'vary', 'allow', 'www-authenticate']


def unmask(token):
    if len(token) != 64 or any(c not in ALPHABET for c in token):
        raise ValueError('Exact Django mask required')
    return ''.join(ALPHABET[(ALPHABET.index(token[i + 32]) - ALPHABET.index(token[i])) % 62]
                   for i in range(32))


def body_view(row):
    headers = {key.lower(): value for key, value in row['raw_response_headers']}
    body = row['body_text']
    raw = body.encode('utf-8')
    if len(raw) != row['body_bytes'] or hashlib.sha256(raw).hexdigest() != row['body_sha256']:
        raise ValueError('Captured body length/hash inconsistent')
    jar = SimpleCookie()
    for name, value in row['raw_response_headers']:
        if name.lower() == 'set-cookie':
            jar.load(value)
    cookie = jar.get('csrftoken')
    is_html = row['content_type'].startswith('text/html')
    slots = 0
    if is_html:
        if cookie is None or len(cookie.value) != 32 or any(c not in ALPHABET for c in cookie.value):
            raise ValueError('HTML response must issue valid Source cookie')
        if cookie['path'] != '/' or cookie['samesite'] != 'Lax' or cookie['max-age'] != '31449600' or cookie['secure'] or cookie['httponly'] or cookie['domain']:
            raise ValueError('Cookie attributes differ from captured Source')
        expiry = parsedate_to_datetime(cookie['expires'])
        observed = datetime.datetime.fromisoformat(row['observed_utc'])
        if not 31_449_597 <= (expiry - observed).total_seconds() <= 31_449_602:
            raise ValueError('Cookie expiry differs from Source Max-Age')
        incoming = SimpleCookie(); incoming.load(row['request_cookie'])
        if 'csrftoken' in incoming and incoming['csrftoken'].value != cookie.value:
            raise ValueError('Sticky Source secret changed')
        if row['method'] == 'GET':
            if body.count(PREFIX) != 1:
                raise ValueError('Exactly one declared CSRF slot required')
            start = body.index(PREFIX) + len(PREFIX); end = start + 64
            if body[end:end + 1] != '"' or unmask(body[start:end]) != cookie.value:
                raise ValueError('Body mask and own cookie differ')
            body = body[:start] + '@@VALIDATED_INDEPENDENT_CSRF_MASK@@' + body[end:]; slots = 1
    elif cookie is not None:
        raise ValueError('JSON negotiation must not acquire CSRF cookie')
    if row['method'] == 'HEAD' and row['body_bytes'] != 0:
        raise ValueError('HEAD response contains body bytes')
    return body, {key: headers.get(key) for key in INTRINSIC}, slots


def compare(original, candidate):
    assert original['passed'] and candidate['passed'] and original['target'] == 'Original' and candidate['target'] == 'candidate'
    assert len(original['records']) == len(candidate['records']) == 56
    assert original['fingerprints_before'] == original['fingerprints_after'] == candidate['fingerprints_before'] == candidate['fingerprints_after']
    records = []; differences = []
    for index, (source, native) in enumerate(zip(original['records'], candidate['records'])):
        assert all(source[key] == native[key] for key in ['spec', 'path', 'accept', 'role', 'method', 'repeat', 'cookie_strategy'])
        a, source_headers, source_slots = body_view(source); b, native_headers, native_slots = body_view(native)
        same = source['status'] == native['status'] and source_headers == native_headers and a == b and source_slots == native_slots
        row = {'source_index': index, 'spec': source['spec'], 'role': source['role'], 'method': source['method'], 'repeat': source['repeat'], 'status': native['status'], 'same': same, 'declared_validated_random_slots': source_slots, 'native_body_sha256': native['body_sha256']}
        records.append(row)
        if not same:
            differences.append({'case': row, 'intrinsic_headers_equal': source_headers == native_headers, 'body_outside_validated_slot_equal': a == b})
    return {'passed': not differences, 'compared': len(records), 'records': records, 'differences': differences, 'scope': 'Exact captured56 Source/package observations, deterministic bytes/five headers and validated cookie/mask/expiry; no asset or fullHTML grammar acceptance', 'assets_verified': False, 'full_HTML_parity': False}


def probe(command, compose, evidence, candidate_binding):
    observed = {}
    for target in ['Original', 'candidate']:
        result = command(compose + ['exec', '-T', '-e', 'VOD_API_DISPOSABLE_FIXTURE=playback-fixture-only', '-e', 'VOD_UNIFIED_HTML_TARGET=' + target, 'baseline', '/dispatcharrpy/bin/python', '/vod-api-probe/unified_html_source.py'], timeout=180)
        value = json.loads(result.stdout.splitlines()[-1]); observed[target] = value
        (evidence / ('packaged-HTML-' + target + '-private.json')).write_text(json.dumps(value, indent=2, ensure_ascii=False) + '\n')
    report = compare(observed['Original'], observed['candidate']); report['candidate_binding'] = candidate_binding
    report['private_observation_sha256'] = {key: hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False).encode()).hexdigest() for key, value in observed.items()}
    (evidence / 'packaged-HTML.json').write_text(json.dumps(report, indent=2) + '\n'); return report
