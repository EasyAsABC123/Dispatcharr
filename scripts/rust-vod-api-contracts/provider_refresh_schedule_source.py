"""UNEXECUTED owned Source/native refresh scheduling/controller draft.
Source uses pinned Django in an owned threaded WSGI worker with a named wrapper
at its real direct task call; this is not a Source upstream-cap2 implementation.
Original Dispatcharr authors retain application and algorithm credit.
"""
import os
assert os.environ.get('VOD_API_DISPOSABLE_FIXTURE') == 'playback-fixture-only'
assert os.environ.get('POSTGRES_PASSWORD') == 'playback-fixture-only'
import base64
from concurrent.futures import ThreadPoolExecutor
import datetime
import hashlib
import http.client
import json
from pathlib import Path
import runpy
import socket
from socketserver import ThreadingMixIn
import threading
import time
from unittest.mock import patch
from urllib.parse import parse_qs, urlsplit
from wsgiref.simple_server import WSGIServer, WSGIRequestHandler, make_server
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from provider_refresh_http_driver import NativePeer, HEADER_NAMES
from provider_refresh_http_assertions import compare_effects, compare_reply

os.environ['VOD_REFRESH_CASE_FILTER'] = '__schedule_bootstrap__'
ctx = runpy.run_path(str(Path(__file__).with_name('provider_refresh_source.py')))
from django.core.wsgi import get_wsgi_application
from django.db import connection
from rest_framework_simplejwt.tokens import AccessToken
from apps.vod import api_views, tasks
scenario = os.environ.get('VOD_REFRESH_SCHEDULE_CASE', 'movie_third_recent_skip')
assert scenario in {'movie_third_recent_skip', 'series_third_old_snapshot',
                    'movie_caller_lost', 'series_caller_lost', 'series_third_old_account_agent_relation'}
kind = 'movie' if scenario.startswith('movie') else 'series'
caller_lost = scenario.endswith('caller_lost')
extended_series = scenario == 'series_third_old_account_agent_relation'
Movie, Series = ctx['Movie'], ctx['Series']
MovieRel, SeriesRel, Account = ctx['M3UMovieRelation'], ctx['M3USeriesRelation'], ctx['M3UAccount']
Parent, Relation = (Movie, MovieRel) if kind == 'movie' else (Series, SeriesRel)
parent_fk = 'movie' if kind == 'movie' else 'series'
lookup = 'movie_id' if kind == 'movie' else 'series_id'
parents = list(Parent.objects.filter(pk__in=Relation.objects.filter(m3u_account__is_active=True).values(lookup)).order_by('id')[:3])
assert len(parents) == 3
relations = [Relation.objects.filter(**{parent_fk: parent}, m3u_account__is_active=True).order_by('-m3u_account__priority', 'id').first() for parent in parents]
assert all(relations)
user = ctx['User'].objects.get(username=ctx['prefix'] + 'standard')
token = str(AccessToken.for_user(user))


def reply(port, path, method='GET', extra=None):
    client = http.client.HTTPConnection('127.0.0.1', port, timeout=20)
    try:
        headers = {'Host': 'localhost', 'Accept': 'application/json', 'Authorization': 'Bearer ' + token}
        headers.update(extra or {})
        client.request(method, path, headers=headers)
        response = client.getresponse()
        raw = response.read(4 * 1024 * 1024 + 1)
        assert len(raw) <= 4 * 1024 * 1024
        return {'status': response.status, 'method': method,
                'headers': {key: response.getheader(key) for key in HEADER_NAMES},
                'body_base64': base64.b64encode(raw).decode(),
                'body_sha256': hashlib.sha256(raw).hexdigest()}
    finally:
        client.close()


def control(path, method='GET'):
    row = reply(9193, '/__owned_provider_refresh__/' + path, method)
    assert row['status'] == 200
    return json.loads(base64.b64decode(row['body_base64']))


def until(predicate, seconds=8):
    deadline = time.monotonic() + seconds
    while True:
        result = predicate()
        if result:
            return result
        assert time.monotonic() < deadline, 'Bounded owned scheduling barrier deadline'
        time.sleep(.01)


class OriginState:
    def __init__(self):
        self.release_info = threading.Event()
        self.release_auth = threading.Event()
        self.auth_entered = threading.Event()
        self.third_info_entered = threading.Event()
        self.release_third_info = threading.Event()
        self.condition = threading.Condition()
        self.pending = set()
        self.requests = []
        self.failures = []
        self.identifiers = [str(getattr(row, 'stream_id' if kind == 'movie' else 'external_series_id')) for row in relations]


state = OriginState()


class Origin(BaseHTTPRequestHandler):
    protocol_version = 'HTTP/1.1'

    def do_GET(self):
        query = parse_qs(urlsplit(self.path).query)
        action = query.get('action', ['authenticate'])[0]
        if urlsplit(self.path).path != '/player_api.php' or action not in {'authenticate', 'get_vod_info', 'get_series_info'}:
            state.failures.append('Unexpected owned-origin route')
        state.requests.append({'raw_target': self.path, 'headers': dict(self.headers), 'peer_address': list(self.client_address)})
        if action == 'authenticate':
            state.auth_entered.set()
            if caller_lost and not state.release_auth.wait(12):
                state.failures.append('Auth barrier safety deadline')
            value = {'user_info': {'auth': 1}}
        else:
            identifier = query.get('vod_id' if kind == 'movie' else 'series_id', [None])[0]
            with state.condition:
                state.pending.add(identifier)
                state.condition.notify_all()
            if not caller_lost and identifier in state.identifiers[:2] and not state.release_info.wait(12):
                state.failures.append('Info barrier safety deadline')
            if extended_series and identifier == state.identifiers[2]:
                state.third_info_entered.set()
                if not state.release_third_info.wait(12):state.failures.append('Third info relation-edit safety deadline')
            value = {'info': {'plot': 'Scheduled refreshed metadata'}, **({'episodes': {}} if kind == 'series' else {})}
        raw = json.dumps(value, separators=(',', ':')).encode()
        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', str(len(raw)))
        if action == 'authenticate':
            self.send_header('Set-Cookie', 'scheduled_session=owned; Path=/')
        self.end_headers()
        self.wfile.write(raw)

    def log_message(self, *args):
        pass


origin = ThreadingHTTPServer(('127.0.0.1', 0), Origin)
threading.Thread(target=origin.serve_forever, daemon=True).start()
origin_base = 'http://127.0.0.1:' + str(origin.server_port)
alternate_origin = ThreadingHTTPServer(('127.0.0.1',0), Origin)
threading.Thread(target=alternate_origin.serve_forever,daemon=True).start()
alternate_base = 'http://127.0.0.1:' + str(alternate_origin.server_port)
from core.models import UserAgent
original_agent_rows = list(UserAgent.objects.order_by('id').values())
with connection.cursor()as cursor:
    cursor.execute("SELECT pg_get_serial_sequence('core_useragent','id')");agent_sequence = cursor.fetchone()[0]
    cursor.execute('SELECT last_value,is_called FROM '+agent_sequence);agent_sequence_before=cursor.fetchone()
owned_agents=[]
if extended_series:
    for label in ['old','other']:
        assert not UserAgent.objects.filter(name=ctx['prefix']+'schedule-'+label).exists()
        owned_agents.append(UserAgent.objects.create(name=ctx['prefix']+'schedule-'+label,user_agent='owned-schedule-'+label,description='Owned disposable scheduling reference'))
    assert ctx['fingerprints']()==ctx['baseline'], 'UserAgent fixture setup must not alter any18baseline model'


class SourceServer(ThreadingMixIn, WSGIServer):
    daemon_threads = True


class QuietHandler(WSGIRequestHandler):
    def log_message(self, *args):
        pass


class TaskBoundary:
    def __init__(self):
        self.entered = threading.Event()
        self.release = threading.Event()
        self.finished = threading.Event()
        self.observations = []
        self.condition = threading.Condition()
        self.active = 0
        self.module = api_views if kind == 'movie' else tasks
        self.name = 'refresh_movie_advanced_data' if kind == 'movie' else 'refresh_series_episodes'
        self.original = getattr(self.module, self.name)

    def __call__(self, *args, **kwargs):
        selected = int(args[0]) == relations[2].pk if kind == 'movie' else args[1].pk == parents[2].pk
        with self.condition:
            self.active += 1
        try:
            if caller_lost or selected:
                row = {'kind': kind, 'function': self.name, 'controlled_boundary': not caller_lost}
                if kind == 'movie':
                    row['relation_id'] = int(args[0])
                else:
                    row.update(series_id=args[1].pk, series_name=args[1].name,
                               series_description=args[1].description, account_id=args[0].pk,
                               account_endpoint_sha256=hashlib.sha256(args[0].server_url.encode()).hexdigest())
                self.observations.append(row)
                self.entered.set()
                if not caller_lost:
                    assert self.release.wait(12), 'Source direct-call wrapper safety deadline'
            return self.original(*args, **kwargs)
        finally:
            with self.condition:
                self.active -= 1
                self.condition.notify_all()
            if caller_lost or selected:
                self.finished.set()

    def wait_idle(self):
        with self.condition:
            assert self.condition.wait_for(lambda: self.active == 0, timeout=20), 'Source task wrappers must acknowledge return before restoration'


setup_now = ctx['timezone'].now()
phases = []
source_server = None
native = None
patcher = None
source_thread = None


def prepare():
    global state
    ctx['restore']()
    state = OriginState()
    Account.objects.filter(name__startswith=ctx['prefix']).update(server_url=origin_base)
    for parent in parents:
        if kind == 'movie':
            Relation.objects.filter(movie=parent).update(custom_properties={'detailed_fetched': True, 'detailed_info': {'plot': 'Old cached'}}, last_advanced_refresh=setup_now-datetime.timedelta(hours=25))
        else:
            Relation.objects.filter(series=parent).update(custom_properties={'episodes_fetched': True, 'detailed_fetched': True}, last_episode_refresh=setup_now-datetime.timedelta(hours=25))
    if kind == 'series' and not caller_lost:
        Parent.objects.filter(pk=parents[2].pk).update(description=None)
    if extended_series:
        UserAgent.objects.filter(pk=owned_agents[0].pk).update(user_agent='owned-schedule-before-queue')
        Account.objects.filter(pk=relations[2].m3u_account_id).update(user_agent_id=owned_agents[0].pk)


def path(index):
    return '/api/vod/' + ('movies' if kind == 'movie' else 'series') + '/' + str(parents[index].pk) + '/provider-info/?relation_id=' + str(relations[index].pk)


try:
    assert connection.settings_dict['NAME'] == 'dispatcharr'
    with connection.cursor() as cursor:
        cursor.execute('SELECT current_database()')
        assert cursor.fetchone()[0] == 'dispatcharr'
    for target in ['Source', 'native']:
        prepare()
        boundary = None
        if target == 'Source':
            boundary = TaskBoundary()
            patcher = patch.object(boundary.module, boundary.name, boundary)
            patcher.start()
            source_server = make_server('127.0.0.1', 0, get_wsgi_application(), server_class=SourceServer, handler_class=QuietHandler)
            source_thread = threading.Thread(target=source_server.serve_forever, daemon=True)
            source_thread.start()
            port = source_server.server_port
        else:
            native = NativePeer()
            port = 9193
        before, before_hash = ctx['state'](), ctx['fingerprints']()
        agents_before=list(UserAgent.objects.order_by('id').values())
        began = ctx['timezone'].now()
        observations = {}
        if caller_lost:
            client = socket.create_connection(('127.0.0.1', port), timeout=5)
            request = ('GET ' + path(0) + ' HTTP/1.1\r\nHost: localhost\r\nAccept: application/json\r\nAuthorization: Bearer ' + token + '\r\n' + ('X-Owned-Observer-Loss: playback-fixture-only\r\n' if target == 'native' else '') + '\r\n')
            client.sendall(request.encode())
            assert state.auth_entered.wait(8), 'Actual auth attempt must enter owned origin'
            if target == 'native':
                observations['before_client_close'] = control('state')
                assert observations['before_client_close']['upstream'] == 1
            client.shutdown(socket.SHUT_RDWR)
            client.close()
            if target == 'native':
                observed = control('state')
                observations['after_actual_client_close'] = observed
                if observed['observers_dropped'] == 0:
                    observations['explicit_receiver_drop'] = control('drop-observer', 'POST')
                    until(lambda: control('state')['observers_dropped'] == 1)
                observations['shutdown_pending'] = control('close-admission', 'POST')
                assert observations['shutdown_pending']['drained'] is False
            state.release_auth.set()
            if target == 'Source':
                assert boundary.finished.wait(8), 'Observed Source direct task return required'
            else:
                observations['drained'] = until(lambda: (lambda value: value if value['jobs'] == value['upstream'] == value['bytes'] == 0 else None)(control('state')))
            responses = []
        else:
            with ThreadPoolExecutor(max_workers=4) as executor:
                first = [executor.submit(reply, port, path(index)) for index in [0, 1]]
                with state.condition:
                    assert state.condition.wait_for(lambda: set(state.identifiers[:2]).issubset(state.pending), timeout=8)
                third = executor.submit(reply, port, path(2))
                if target == 'Source':
                    assert boundary.entered.wait(8)
                    observations['Source_wrapper'] = list(boundary.observations)
                else:
                    observations['queued'] = until(lambda: (lambda value: value if value['pending_new_refresh'] == 1 else None)(control('state')))
                    assert observations['queued']['upstream'] == 2 and observations['queued']['workers'] == 0
                if kind == 'movie':
                    Relation.objects.filter(pk=relations[2].pk).update(last_advanced_refresh=setup_now)
                    Parent.objects.filter(pk=parents[2].pk).update(name=ctx['prefix'] + 'Third Movie edited before task')
                else:
                    Parent.objects.filter(pk=parents[2].pk).update(name=ctx['prefix'] + 'Concurrent Series name')
                    if extended_series:
                        Account.objects.filter(pk=relations[2].m3u_account_id).update(server_url=alternate_base,username='queued-new-user',password='queued-new-password',user_agent_id=owned_agents[1].pk)
                        UserAgent.objects.filter(pk=owned_agents[0].pk).update(user_agent='owned-schedule-after-queue')
                # Unique detail rather than a tied ordering page: no invented PK
                # tie-breaker or historical expectation replacement.
                ordinary = reply(port, '/api/vod/movies/' + str(ctx['movie'].pk) + '/')
                observations['ordinary'] = ordinary
                assert ordinary['status'] == 200
                if boundary:
                    boundary.release.set()
                state.release_info.set()
                if extended_series:
                    assert state.third_info_entered.wait(8), 'Third admitted session must reach owned info barrier'
                    Relation.objects.filter(pk=relations[2].pk).update(custom_properties={'concurrent_after_admission':True,'episodes_fetched':False,'detailed_fetched':False})
                    state.release_third_info.set()
                responses = [future.result(timeout=20) for future in first] + [third.result(timeout=20)]
                assert all(row['status'] == 200 for row in responses)
                expected_identifiers = set(state.identifiers[:2]) if kind == 'movie' else set(state.identifiers)
                assert state.pending == expected_identifiers, 'Movie recent skip must omit third upstream; Series must execute original snapshot'
        finished = ctx['timezone'].now()
        phases.append({'target': target, 'before': before, 'after': ctx['state'](),
                       'before_fingerprints': before_hash, 'after_fingerprints': ctx['fingerprints'](),
                       'sequences': ctx['sequences'](), 'wall_started': began, 'wall_finished': finished,
                       'responses': responses, 'observations': observations,
                       'provider_requests': list(state.requests), 'origin_failures': list(state.failures),
                       'agents_before':agents_before,'agents_after':list(UserAgent.objects.order_by('id').values()),
                       'Source_reference_scope': 'Pinned Django app on owned threaded WSGI, observed direct task call; artificial third-task pause only, not Source cap2' if target == 'Source' else None})
        if source_server:
            source_server.shutdown(); source_server.server_close(); source_thread.join(timeout=3)
            boundary.wait_idle()
            source_server = None; patcher.stop(); patcher = None
        if native:
            native.close(); native = None
    a, b = phases
    controlled = {('vod_m3umovierelation', relations[2].pk, 'last_advanced_refresh')} if kind == 'movie' and not caller_lost else set()
    if controlled:
        for phase in phases:
            edited = next(row for row in phase['after']['vod_m3umovierelation'] if row['id'] == relations[2].pk)
            assert edited['last_advanced_refresh'] == setup_now, 'Exact predeclared fresh input; task must not rewrite it'
    effects = compare_effects(a['after'], b['after'], a['before'],
                              (a['wall_started'], a['wall_finished']), (b['wall_started'], b['wall_finished']), ctx['Encoder'], controlled)
    failures = list(effects['differences'])
    if a['before_fingerprints'] != b['before_fingerprints']:
        failures.append({'kind': 'phase_setup_fingerprints'})
    if a['sequences'] != b['sequences']:
        failures.append({'kind': 'exact_sequence_state'})
    if a['agents_before'] != b['agents_before'] or a['agents_after'] != b['agents_after']:
        failures.append({'kind':'exact_UserAgent_rows'})
    if extended_series:
        for phase in phases:
            assert all(row['headers']['Host']=='127.0.0.1:'+str(origin.server_port) for row in phase['provider_requests']), 'Original account endpoint must survive queue'
            chosen=[row for row in phase['provider_requests']if parse_qs(urlsplit(row['raw_target']).query).get('series_id')==[state.identifiers[2]]]
            assert len(chosen)==1 and chosen[0]['headers']['User-Agent']=='owned-schedule-after-queue', 'Old account FK must lazily load updated old agent, not new account FK'
            rel=next(row for row in phase['after']['vod_m3useriesrelation']if row['id']==relations[2].pk)
            assert rel['custom_properties'].get('concurrent_after_admission')is True, 'Task must re-read relation after provider reply'
    replies = []
    for index, (left, right) in enumerate(zip(a['responses'], b['responses'])):
        result = compare_reply(left, right, kind, parents[index].pk, a['before'], a['after'], b['after'])
        replies.append(result)
        if not result['passed']:
            failures.append({'kind': 'HTTP_reply', 'index': index, 'differences': result['differences']})
    if not caller_lost:
        left, right = a['observations']['ordinary'], b['observations']['ordinary']
        if left != right:
            failures.append({'kind': 'ordinary_detail_raw_body_or_headers'})

    def sessions(rows):
        # The fixture deliberately serves HTTP/1.1 +Content-Length and auth
        # cookie; qualify each side's own connection, never cross-compare ports.
        grouped = {}
        for row in rows:
            grouped.setdefault(tuple(row['peer_address']), []).append(row)
        result = {}
        for connection_rows in grouped.values():
            if len(connection_rows) != 2:
                return None
            first_query, last_query = [parse_qs(urlsplit(row['raw_target']).query) for row in connection_rows]
            if first_query.get('action') or last_query.get('action') != ['get_vod_info' if kind == 'movie' else 'get_series_info']:
                return None
            if connection_rows[1]['headers'].get('Cookie') != 'scheduled_session=owned':
                return None
            identifier = last_query.get('vod_id' if kind == 'movie' else 'series_id', [None])[0]
            if identifier is None or identifier in result:
                return None
            result[identifier] = [{'raw_target': row['raw_target'], 'headers': {key.lower(): value for key, value in row['headers'].items()}} for row in connection_rows]
        return result

    left_sessions, right_sessions = sessions(a['provider_requests']), sessions(b['provider_requests'])
    wire_equal = left_sessions is not None and right_sessions is not None and left_sessions == right_sessions
    if not wire_equal:
        failures.append({'kind': 'per_parent_session_strict_provider_wire_or_cookie_keepalive'})
    report = {'passed': not failures and not any(row['origin_failures'] for row in phases),
              'scope': __doc__, 'scenario': scenario, 'records': phases,
              'differences': failures, 'reply_comparisons': replies,
              'own_interval_SQL_clocks': effects['own_interval_clocks'],
              'predeclared_strict_controlled_clock_paths': sorted(controlled),
              'strict_provider_wire_by_independent_parent_session': wire_equal,
              'provider_order_scope': 'Each auth→info session strict; concurrent independent-parent interleavings are not ordered; global rawarrays retained',
              'still_open': []if extended_series else ['Series queued account changes/lazy UserAgent and reply-phase relation merge'],
              'fixture_UserAgent_setup_rows':len(owned_agents),
              'native_Main_acceptance': False, 'performance_claim': False}

finally:
    partial = Path('/evidence/schedule-partial-private.json')
    partial.write_text(json.dumps({'scenario':scenario,'completed_phases':phases,'current_provider_requests':state.requests,'current_origin_failures':state.failures,'partial_not_acceptance':True},cls=ctx['Encoder'],indent=2))
    partial.chmod(0o600)
    state.release_auth.set(); state.release_info.set(); state.release_third_info.set()
    if patcher: patcher.stop()
    if source_server:
        source_server.shutdown(); source_server.server_close()
        if boundary:
            boundary.release.set()
            boundary.wait_idle()
    if native: native.close()
    origin.shutdown(); origin.server_close(); alternate_origin.shutdown();alternate_origin.server_close();ctx['restore']()
    for agent in owned_agents:
        assert UserAgent.objects.get(pk=agent.pk).name.startswith(ctx['prefix']+'schedule-')
        UserAgent.objects.filter(pk=agent.pk).delete()
    with connection.cursor()as cursor:cursor.execute('SELECT setval(%s::regclass,%s,%s)',[agent_sequence,*agent_sequence_before])
    assert list(UserAgent.objects.order_by('id').values())==original_agent_rows
report.update(fixture_UserAgent_rows_restored=True,restored18=ctx['fingerprints']() == ctx['baseline'], sequences_restored=ctx['sequences']() == ctx['baseline_sequences'])
print(json.dumps(report, cls=ctx['Encoder']))
