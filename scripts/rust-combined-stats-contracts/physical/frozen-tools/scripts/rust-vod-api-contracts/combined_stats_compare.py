"""Owned normal-Main replay, strict typed JSON/arrays and finite CSRF contexts.
Original Dispatcharr/Django/DRF authors retain algorithm and application credit.
Only exact observed Allow variants and independently checked clock/CSRF values
are compared as qualified variants. All original bytes/headers remain recorded.
"""
import base64
import datetime
from email.utils import parsedate_to_datetime
import hashlib
import http.client
import json
import math
import os
from pathlib import Path
import re
import signal
import subprocess
import time
from http.cookies import SimpleCookie

ALLOW = {'GET, OPTIONS', 'OPTIONS, GET'}


def headers(row):
    result = {}
    for key, value in row['raw_header_pairs']:
        result.setdefault(key.lower(), []).append(value)
    return result


def checked_html(row):
    from django.middleware.csrf import _unmask_cipher_token
    raw = base64.b64decode(row['body_base64']).decode()
    values = headers(row)
    cookies = values.get('set-cookie', [])
    if len(cookies) != 1:
        raise AssertionError('HTML CSRF cookie cardinality')
    cookie = SimpleCookie()
    cookie.load(cookies[0])
    if set(cookie) != {'csrftoken'}:
        raise AssertionError('HTML CSRF cookie name')
    token = cookie['csrftoken']
    if (not re.fullmatch('[A-Za-z0-9]{32}', token.value) or token['path'] != '/' or
            token['samesite'] != 'Lax' or token['max-age'] != '31449600'):
        raise AssertionError('HTML CSRF cookie attributes')
    expiry = parsedate_to_datetime(token['expires']).timestamp()
    if not row['started_at']-2 <= expiry-31449600 <= row['finished_at']+2:
        raise AssertionError('HTML CSRF expiry outside own clock window')
    matched = []

    def replace(match):
        masked = match.group(2)
        if not re.fullmatch('[A-Za-z0-9]{64}', masked) or _unmask_cipher_token(masked) != token.value:
            raise AssertionError('HTML mask does not match response cookie')
        matched.append(masked)
        return match.group(1)+'<checked-independent-mask>'+match.group(3)

    raw = re.sub(r'("csrfToken": ")([A-Za-z0-9]{64})(")', replace, raw)
    raw = re.sub(r'(name="csrfmiddlewaretoken" value=")([A-Za-z0-9]{64})(")', replace, raw)
    if not matched:
        raise AssertionError('HTML CSRF script absent')
    for allow in ALLOW:
        raw = raw.replace('<b>Allow:</b> <span class="lit">'+allow+'</span>',
                          '<b>Allow:</b> <span class="lit"><observed-method-variant></span>')
    return raw


def compare(source, native):
    differences = []
    if source['status'] != native['status']:
        differences.append('status')
    sh, nh = headers(source), headers(native)
    allow_variants = []
    for key in sorted(set(sh) | set(nh)):
        if key in ['set-cookie', 'content-length']:
            continue
        if key == 'allow' and sh.get(key) != nh.get(key):
            if len(sh.get(key, [])) == len(nh.get(key, [])) == 1 and sh[key][0] in ALLOW and nh[key][0] in ALLOW:
                allow_variants.append(dict(source=sh[key], native=nh[key]))
                continue
        if sh.get(key) != nh.get(key):
            differences.append('header/'+key)
    for row in [source, native]:
        length = headers(row).get('content-length')
        if row['method'] != 'HEAD' and length is not None and length != [str(row['body_bytes'])]:
            differences.append('invalid-content-length')
    clock_paths = []

    def walk(a, b, path, ap=None, bp=None):
        if path in [('timestamp',), ('vod', 'timestamp'), ('catchup', 'timestamp')]:
            for value, row in [(a, source), (b, native)]:
                if (type(value) not in [int, float] or not math.isfinite(value) or
                        not row['started_at'] <= value <= row['finished_at']):
                    differences.append('clock/'+repr(path))
            clock_paths.append(path)
            return
        if (path and path[-1] == 'duration' and isinstance(ap, dict) and isinstance(bp, dict)
                and str(path[0]) == 'vod' and ap.get('connected_at') and bp.get('connected_at')):
            for value, parent, row in [(a, ap, source), (b, bp, native)]:
                try:
                    stamp = float(parent['connected_at'])
                except (ValueError, TypeError):
                    differences.append('duration-invalid-connected-at/'+repr(path))
                    continue
                if type(value) is not int or not math.floor(row['started_at']-stamp) <= value <= math.floor(row['finished_at']-stamp):
                    differences.append('duration/'+repr(path))
            clock_paths.append(path)
            return
        if type(a) is not type(b):
            differences.append('type/'+repr(path)+':'+type(a).__name__+'/'+type(b).__name__)
        elif isinstance(a, dict):
            if set(a) != set(b):
                differences.append('keys/'+repr(path))
            for key in a.keys() & b.keys():
                walk(a[key], b[key], path+(key,), a, b)
        elif isinstance(a, list):
            if len(a) != len(b):
                differences.append('length/'+repr(path))
            for index, (aa, bb) in enumerate(zip(a, b)):
                walk(aa, bb, path+(index,))
        elif a != b:
            differences.append('value/'+repr(path))

    if 'json' in source and 'json' in native:
        walk(source['json'], native['json'], ())
        if not clock_paths and sh.get('content-length') != nh.get('content-length'):
            differences.append('header/content-length')
    elif source['body_bytes'] == native['body_bytes'] == 0:
        if sh.get('content-length') != nh.get('content-length'):
            differences.append('header/content-length')
    elif any(value.startswith('text/html') for value in sh.get('content-type', [])) and 'set-cookie' in sh:
        try:
            if checked_html(source) != checked_html(native):
                differences.append('html/source-content')
        except Exception as error:
            differences.append('html/'+type(error).__name__+':'+str(error))
        if sh.get('content-length') != nh.get('content-length'):
            differences.append('header/content-length')
    elif source['body_sha256'] != native['body_sha256']:
        differences.append('raw-body')
    return dict(label=source['label'], passed=not differences, differences=differences[:64],
                difference_count=len(differences), observed_allow_variants=allow_variants,
                independently_checked_clock_paths=clock_paths)


def replay(original, request, scope, redis, owned, database, before, created):
    assert os.environ.get('VOD_API_DISPOSABLE_FIXTURE') == 'playback-fixture-only'
    binary = Path(os.environ['COMBINED_STATS_NATIVE'])
    assert binary.is_file()
    reference = Path('/evidence/combined-stats-original-reference-private.json')
    reference.write_text(json.dumps(original))
    reference.chmod(0o600)
    saved = [(key, redis.dump(key), redis.pttl(key), time.monotonic()) for key in owned]
    assert sum(len(value or b'') for _, value, _, _ in saved) < 65536
    redis.delete(*owned)
    output = Path('/evidence/native-combined-stats-private.log')
    log = output.open('w')
    output.chmod(0o600)
    environment = dict(os.environ, DATABASE_URL='postgresql://dispatch:playback-fixture-only@127.0.0.1:5432/dispatcharr',
        REDIS_URL='redis://:playback-fixture-only@127.0.0.1:6379/0', RUST_BIND='127.0.0.1:9192',
        DJANGO_SECRET_KEY='playback-fixture-only', RUST_DATABASE_POOL_SIZE='8', RUST_MAX_CHANNELS='32',
        RUST_MAX_VIEWERS_PER_CHANNEL='400', RUST_RETAINED_BYTES_PER_CHANNEL='16777216',
        RUST_RETENTION_SECONDS='60', TOKIO_WORKER_THREADS='1', DISPATCHARR_TRUSTED_PROXIES='none',
        RUST_FRONTEND_DIR='/app/frontend/dist', RUST_EPG_REFRESH_ENABLED='0', RUST_VOD_REDIS_RECOVERY_ENABLED='0')
    process = subprocess.Popen([str(binary)], env=environment, stdout=log, stderr=subprocess.STDOUT,
                               start_new_session=True)
    source_records = scope['records']
    native_records = []
    comparisons = []
    try:
        deadline = time.monotonic()+30
        while True:
            if process.poll() is not None:
                raise AssertionError('Native normal Main exited before readiness')
            connection = http.client.HTTPConnection('127.0.0.1', 9192, timeout=1)
            try:
                connection.request('GET', '/health/ready')
                response = connection.getresponse()
                response.read(4096)
                if response.status == 200:
                    break
            except OSError:
                pass
            finally:
                connection.close()
            if time.monotonic() >= deadline:
                raise TimeoutError('Native normal Main readiness')
            time.sleep(.05)
        scope['records'] = native_records
        scope['ACTIVE_PORT'] = 9192
        startup_database = database()
        startup = Path('/evidence/combined-stats-startup-private.json')
        startup.write_text(json.dumps(dict(before=before, after=startup_database,
            unchanged=before == startup_database)))
        startup.chmod(0o600)
        restored = False
        for row in original:
            if row['label'].startswith('nonempty-') and not restored:
                for key, value, ttl, sampled in saved:
                    remaining = ttl-int((time.monotonic()-sampled)*1000)
                    assert value is not None and remaining > 0
                    redis.restore(key, remaining, value, replace=False)
                restored = True
            request(row['label'], role=row['role'], method=row['method'], accept=row['accept'], path=row['path'], extra_headers=row.get('extra_headers'))
            comparisons.append(compare(row, native_records[-1]))
            progress = Path('/evidence/combined-stats-native-progress-private.json')
            progress.write_text(json.dumps(dict(records=native_records, comparisons=comparisons)))
            progress.chmod(0o600)
        unchanged = database() == before
        return dict(passed=all(row['passed'] for row in comparisons) and unchanged,
            scope='Actual normal native application Main HTTP; private ELF, no canonical package/performance claim',
            binary_sha256=hashlib.sha256(binary.read_bytes()).hexdigest(), records=native_records,
            comparisons=comparisons, selected_models_sequences_unchanged=unchanged,
            startup_models_sequences_unchanged=startup_database == before,
            source_injected_created_at=created, no_redis_global_reset=True)
    finally:
        scope['records'] = source_records
        scope['ACTIVE_PORT'] = 5656
        if process.poll() is None:
            os.killpg(process.pid, signal.SIGTERM)
            try:
                process.wait(15)
            except subprocess.TimeoutExpired:
                os.killpg(process.pid, signal.SIGKILL)
                process.wait(5)
        log.close()
