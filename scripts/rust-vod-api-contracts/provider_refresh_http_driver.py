"""UNEXECUTED draft: host-router refresh HTTP replay after actual Source case.
No Main/package/browser acceptance. Original Dispatcharr authors retain credit.
Imported only by the dedicated future full-HTTP Source helper after quiet release.
"""
import base64
import hashlib
import http.client
import json
import os
from pathlib import Path
import subprocess
import time
from urllib.parse import quote
from provider_refresh_http_assertions import compare_effects, compare_reply

HEADER_NAMES = ['Content-Type', 'Content-Length', 'Allow', 'Vary',
                'WWW-Authenticate', 'X-Content-Type-Options', 'X-Frame-Options',
                'Referrer-Policy', 'Cross-Origin-Opener-Policy']


class NativePeer:
    def __init__(self):
        assert os.environ.get('VOD_API_DISPOSABLE_FIXTURE') == 'playback-fixture-only'
        assert os.environ.get('POSTGRES_PASSWORD') == 'playback-fixture-only'
        binary = Path(os.environ['VOD_REFRESH_ACTOR_NATIVE'])
        assert binary.is_file()
        ready, stop = Path('/evidence/native-refresh-ready'), Path('/evidence/native-refresh-stop')
        assert not ready.exists() and not stop.exists(), 'Fresh owned evidence directory required'
        self.stop = stop
        self.log = open('/evidence/full-http-native-server-private.log', 'w')
        os.chmod(self.log.name, 0o600)
        self.process = subprocess.Popen(
            [str(binary), 'vod_api_draft::vod_api_provider_refresh_peer::native_refresh_http_server',
             '--ignored', '--nocapture', '--test-threads=1'],
            env=dict(os.environ, VOD_API_NATIVE_FIXTURE='playback-fixture-only'),
            stdin=subprocess.DEVNULL, stdout=self.log, stderr=subprocess.STDOUT)
        deadline = time.monotonic() + 10
        while not ready.exists():
            assert self.process.poll() is None, 'Native peer exited before readiness'
            assert time.monotonic() < deadline, 'Native peer readiness deadline'
            time.sleep(.02)

    def close(self):
        self.stop.write_text('stop')
        try:
            self.process.wait(timeout=20)
        except subprocess.TimeoutExpired:
            self.process.terminate()
            self.process.wait(timeout=5)
            raise
        finally:
            self.log.close()
        assert self.process.returncode == 0, 'Peer must acknowledge full runtime shutdown'


_peer = None


def close_peer():
    global _peer
    if _peer is not None:
        try:
            _peer.close()
        finally:
            _peer = None


def replay(context, record, options):
    """One live Original case already completed; replay exact pre-state natively.

    Source capture runs first. Native is a standalone peer using the same copied
    database and absolute owned provider origins, not the image44 frontend donor.
    Fixed-clock SourceFactory boundaries are retained as explicit exclusions.
    """
    global _peer
    if options.get('factory'):
        record['native_HTTP_control'] = {
            'supported': False, 'passed': False,
            'exclusion': 'Fixed-clock Source APIRequestFactory boundary requires separate controlled-clock native reference; not live TCP'}
        return
    if _peer is None:
        _peer = NativePeer()
    from django.db import connection
    from rest_framework_simplejwt.tokens import AccessToken
    context['load_phase'](record['before'])
    allowed = context['sequences']()
    with connection.cursor() as cursor:
        for name, value in record['sequence_before'].items():
            assert name in allowed
            cursor.execute('SELECT setval(%s::regclass,%s,%s)',
                           [name, value['last_value'], value['is_called']])
    assert context['fingerprints']() == record['before_fingerprints']
    assert context['sequences']() == record['sequence_before']
    # Preserve the same scenario, resetting retry counters/concurrent-edit latch.
    context['mode'].clear()
    context['mode'].update(record['provider_scenario'])
    context['mode'].pop('concurrent_edit_done', None)
    for field in ['close_auth', 'close_info']:
        context['mode'][field] = options.get(field, 0)
    start = len(context['requests'])
    fault = options.get('sql_fault')
    if fault:
        assert fault in ['vod_movie', 'vod_m3umovierelation', 'vod_episode']
        with connection.cursor() as cursor:
            cursor.execute("CREATE FUNCTION owned_refresh_fault() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'Owned refresh write failure'; END $$")
            cursor.execute('CREATE TRIGGER owned_refresh_fault BEFORE INSERT OR UPDATE ON ' + fault + ' FOR EACH ROW EXECUTE FUNCTION owned_refresh_fault()')
    headers = {'Host': 'localhost', 'Accept': 'application/json'}
    if record['role']:
        user = context['User'].objects.get(username=context['prefix'] + record['role'])
        headers['Authorization'] = 'Bearer ' + str(AccessToken.for_user(user))
    client = http.client.HTTPConnection('127.0.0.1', 9193, timeout=25)
    began = context['timezone'].now()
    try:
        client.request(record['method'], quote(record['path'], safe='/?=&|,'), headers=headers)
        response = client.getresponse()
        raw = response.read(4 * 1024 * 1024 + 1)
        assert len(raw) <= 4 * 1024 * 1024
        actual = {'status': response.status, 'method': record['method'],
                  'headers': {key: response.getheader(key) for key in HEADER_NAMES},
                  'body_base64': base64.b64encode(raw).decode(),
                  'body_sha256': hashlib.sha256(raw).hexdigest()}
        finished = context['timezone'].now()
        native_after, native_fingerprints = context['state'](), context['fingerprints']()
        native_sequences = context['sequences']()
        # Snapshot immediately at API completion, before any delayed origin write
        # could retrospectively turn a premature-return failure into a pass.
        native_requests = context['requests'][start:]
        at_reply_body_flags = [row.get('response', {}).get('body_released') for row in native_requests]
    finally:
        client.close()
        if fault:
            with connection.cursor() as cursor:
                cursor.execute('DROP TRIGGER owned_refresh_fault ON ' + fault)
                cursor.execute('DROP FUNCTION owned_refresh_fault()')
    effects = compare_effects(record['after'], native_after, record['before'],
                              (record['wall_started'], record['wall_finished']),
                              (began, finished), context['Encoder'])
    parent_id = context['movie'].pk if record['kind'] == 'movie' else context['series'].pk
    expected = dict(record['expected'], method=record['method'])
    wire = compare_reply(expected, actual, record['kind'], parent_id,
                         record['before'], record['after'], native_after)
    source_wire = [{'method': row['method'], 'raw_target': row['raw_target'],
                    'headers': {key.lower(): value for key, value in row['headers'].items()}}
                   for row in record['provider_requests']]
    native_wire = [{'method': row['method'], 'raw_target': row['raw_target'],
                    'headers': {key.lower(): value for key, value in row['headers'].items()}}
                   for row in native_requests]
    differences = list(effects['differences'])
    if native_sequences != record['sequence_after']:
        differences.append({'kind': 'exact_sequence_state', 'Source': record['sequence_after'], 'native': native_sequences})
    changed = {table for table in record['before_fingerprints']
               if record['before_fingerprints'][table] != record['after_fingerprints'][table]}
    unexpected = [table for table in native_fingerprints
                  if table not in changed and native_fingerprints[table] != record['before_fingerprints'][table]]
    if unexpected:
        differences.append({'kind': 'unexpected_model_mutations', 'tables': unexpected})
    delay_required = bool(options.get('body_delay') or options.get('auth_body_delay') or options.get('redirect_body_delay'))
    bodies_finished = not delay_required or all(value is True for value in at_reply_body_flags)
    record['native_HTTP_control'] = {
        'supported': True,
        'passed': wire['passed'] and not differences and source_wire == native_wire and bodies_finished,
        'scope': 'Actual Original TCP vs standalone native router HTTP plus complete task SQL; not Main/package/browser/performance',
        'expected_reference_scope': record['clock_scope'],
        'native_reply': actual, 'reply_comparison': wire,
        'SQL_differences': differences,
        'own_interval_SQL_clocks': effects['own_interval_clocks'],
        'new_episode_UUIDv4': effects['new_episode_UUIDv4'],
        'full_provider_request_wire_equal': source_wire == native_wire,
        'native_provider_requests': native_requests,
        'provider_body_completion_at_API_reply': at_reply_body_flags,
        'required_delayed_body_completion': bodies_finished,
        'native_after': native_after,
    }
    context['restore']()
