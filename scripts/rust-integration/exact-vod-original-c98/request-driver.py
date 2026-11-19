"""Observe exact VOD paths on an owned local backend using disposable fixture data.

Original Dispatcharr authors retain application credit. Public projections omit
credentials, authorization headers, dynamic session paths and literal bodies.
"""
from hashlib import sha256
from http.client import HTTPException
import json
import math
import re
from urllib.error import HTTPError
from urllib.parse import parse_qsl, quote, urlencode, urlsplit
from urllib.request import HTTPRedirectHandler, ProxyHandler, Request, build_opener
from uuid import UUID

BODY_LIMIT = 128 * 1024
OWNED_CLIENT = 'Dispatcharr exact VOD route fixture'


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, request, fp, code, msg, headers, newurl):
        return None


def require(condition, message):
    if not condition:
        raise ValueError(message)


def observe(base, path, *, token=None, headers=None, opener=None):
    target = urlsplit(base)
    require(target.scheme == 'http' and target.hostname == '127.0.0.1' and target.port is not None
            and not any((target.username, target.password, target.path, target.query, target.fragment)),
            'select an owned loopback fixture endpoint')
    require(path.startswith('/') and not path.startswith('//'), 'require a same-service request path')
    fields = {'Accept': '*/*', 'User-Agent': OWNED_CLIENT, **(headers or {})}
    if token:
        fields['Authorization'] = 'Bearer ' + token
    request = Request(base + path, method='GET', headers=fields)
    send = opener if opener is not None else build_opener(ProxyHandler({}), NoRedirect()).open
    try:
        response = send(request, timeout=15)
    except HTTPError as error:
        response = error
    with response:
        raw = response.read(BODY_LIMIT + 1)
        return response.status, response.headers, raw


def project(wire, operation, method='GET'):
    status, headers, raw = wire
    captured = raw[:BODY_LIMIT]
    return {
        'operation': operation,
        'method': method,
        'status': status,
        'content_type': headers.get('Content-Type'),
        'content_range': headers.get('Content-Range'),
        'cache_control': headers.get('Cache-Control'),
        'www_authenticate': headers.get('WWW-Authenticate'),
        'response_bytes': len(captured),
        'response_sha256': sha256(captured).hexdigest(),
        'body_truncated': len(raw) > BODY_LIMIT,
    }


def empty_stats(wire):
    status, headers, raw = wire
    require(status == 200 and headers.get_content_type() == 'application/json',
            'authenticated VOD stats must return JSON')
    result = json.loads(raw)
    require(entry_keys(result) == {'vod_connections', 'total_connections', 'timestamp'},
            'must observe the exact VOD stats fields')
    require(result['vod_connections'] == [] and result['total_connections'] == 0
            and type(result['timestamp']) in (int, float) and math.isfinite(result['timestamp'])
            and result['timestamp'] > 0, 'disposable pre-playback fixture must be idle')
    return {'vod_connections': [], 'total_connections': 0, 'finite_timestamp': True}


def entry_keys(value):
    return set(value) if isinstance(value, dict) else set()


def redirect_destination(wire, xc_path, content_type, content):
    status, headers, raw = wire
    location = headers.get('Location')
    projection = {'location_present': bool(location), 'redirect_kind': None}
    if not location:
        return projection, None
    try:
        destination = urlsplit(location)
        pairs = parse_qsl(destination.query, strict_parsing=True, max_num_fields=4)
    except ValueError:
        projection['redirect_kind'] = 'unrecognized'
        return projection, None
    if destination.scheme or destination.netloc:
        projection['redirect_kind'] = 'absolute'
        return projection, None
    if destination.fragment or status != 301 or raw:
        projection['redirect_kind'] = 'unrecognized'
        return projection, None

    if xc_path and destination.path == xc_path and len(pairs) == 1 and pairs[0][0] == 'session_id':
        session = pairs[0][1]
        if re.fullmatch(r'[A-Za-z0-9_-]{1,128}', session):
            projection['redirect_kind'] = 'same XC path with session_id query'
            return projection, xc_path + '?' + urlencode({'session_id': session})

    prefix = f'/proxy/vod/{content_type}/{content}/'
    if destination.path.startswith(prefix) and not pairs:
        session = destination.path.removeprefix(prefix)
        if re.fullmatch(r'[A-Za-z0-9_-]{1,128}', session):
            projection['redirect_kind'] = 'same-service VOD capability'
            return projection, prefix + session
    projection['redirect_kind'] = 'unrecognized'
    return projection, None


def validate_location(wire, content_type, content):
    projection, _ = redirect_destination(wire, None, content_type, content)
    require(projection['redirect_kind'] == 'same-service VOD capability',
            'normal XC playback must mint a same-service VOD capability for the seeded content')
    return {'owned_content_matched': True, 'redirect_kind': projection['redirect_kind']}


def assess_range(row, expected_hash):
    row['expected_prefix_sha256'] = expected_hash
    row['owned_prefix_matched'] = (
        row['status'] == 206 and not row['body_truncated'] and row['response_bytes'] == 65536
        and row['response_sha256'] == expected_hash
    )
    return row


def capture(base, seed, *, admin, viewer, xc_user, xc_password, session, opener=None,
            emit=None, follow_owned=False):
    """Observe six exact routes and retain statuses even when their behavior differs."""
    movie, episode = [str(UUID(seed[name])) for name in ('movie_uuid', 'episode_uuid')]
    movie_id, episode_id, profile = [int(seed[name]) for name in ('movie_id', 'episode_id', 'provider_profile_id')]
    expected_hash = seed['first_65536_sha256']
    require(all(value > 0 for value in (movie_id, episode_id, profile))
            and re.fullmatch(r'[A-Za-z0-9_-]{1,64}', session)
            and re.fullmatch(r'[0-9a-f]{64}', expected_hash), 'exact owned fixture ids and prefix')
    range_headers = {'Range': 'bytes=0-65535'}
    rows = []

    def request(path, operation, role, **kwargs):
        try:
            wire = observe(base, path, opener=opener, **kwargs)
            return wire, {**project(wire, operation), 'role': role}
        except (OSError, HTTPException) as error:
            return None, {'operation': operation, 'method': 'GET', 'role': role,
                          'capture_error': type(error).__name__}

    def save(row):
        rows.append(row)
        if emit:
            emit(row)

    for role, token in (('admin', admin), ('viewer', viewer), ('anonymous', None)):
        wire, row = request('/proxy/vod/stats/', 'vod-stats', role, token=token)
        if wire and role == 'admin':
            try:
                row['value'] = empty_stats(wire)
            except ValueError:
                row['idle_stats_shape_matches'] = False
            else:
                row['idle_stats_shape_matches'] = True
        save(row)

    wire, row = request(f'/proxy/vod/movie/{movie}/{session}/{profile}/', 'vod-session-with-profile',
                        'anonymous', headers=range_headers)
    save(assess_range(row, expected_hash) if wire else row)

    credentials = '/'.join(quote(value, safe='') for value in (xc_user, xc_password))
    for route, content_type, content_id, content in (('movie', 'movie', movie_id, movie),
                                                      ('series', 'episode', episode_id, episode)):
        path = f'/{route}/{credentials}/{content_id}.mp4'
        wire, row = request(path, f'xc-{route}-ext', 'xc-disposable-user')
        if wire:
            location, follow_path = redirect_destination(wire, path, content_type, content)
            row.update(location)
            if follow_owned and route == 'movie' and follow_path:
                follow, followed = request(follow_path, f'xc-{route}-owned-follow', 'xc-disposable-user',
                                           headers=range_headers)
                row['owned_follow'] = assess_range(followed, expected_hash) if follow else followed
        save(row)
    return rows
