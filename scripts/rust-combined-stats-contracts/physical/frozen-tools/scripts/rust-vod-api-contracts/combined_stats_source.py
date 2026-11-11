"""Actual Original combined-stats HTTP reference in an owned disposable fixture.
Original Dispatcharr authors retain full application and algorithm credit.
No view, auth, renderer, clock or statistics builder is patched.
Synthetic owned Redis records qualify projection, not active playback ownership.
"""
import base64
import hashlib
import http.client
import json
import os
import sys
import time
from pathlib import Path

if (os.environ.get('POSTGRES_PASSWORD') != 'playback-fixture-only' or
        os.environ.get('VOD_API_DISPOSABLE_FIXTURE') != 'playback-fixture-only'):
    raise RuntimeError('Owned disposable fixture required')
sys.path.insert(0, '/app')
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'dispatcharr.settings')
import django
django.setup()
from django.core.serializers.json import DjangoJSONEncoder
from django.db import connection
from rest_framework_simplejwt.tokens import AccessToken
from apps.accounts.models import User
from apps.channels.models import Channel, ChannelGroup, ChannelGroupM3UAccount, Stream
from apps.m3u.models import M3UAccount, M3UAccountProfile, M3UFilter
from apps.vod.models import (Movie, Series, Episode, VODLogo, VODCategory,
    M3UMovieRelation, M3USeriesRelation, M3UEpisodeRelation, M3UVODCategoryRelation)
from core.models import CoreSettings, SystemEvent
from core.utils import RedisClient

MODELS = [User, M3UAccount, M3UAccountProfile, Movie, Series, Episode, VODLogo,
    VODCategory, M3UMovieRelation, M3USeriesRelation, M3UEpisodeRelation,
    M3UVODCategoryRelation, CoreSettings, SystemEvent, M3UFilter, ChannelGroup,
    ChannelGroupM3UAccount, Stream, Channel]
PREFIX = 'rust-probe-vod-rest-'


def database():
    rows, sequences = {}, {}
    with connection.cursor() as cursor:
        for model in MODELS:
            raw = json.dumps(list(model.objects.order_by('pk').values()),
                             sort_keys=True, cls=DjangoJSONEncoder).encode()
            if len(raw) > 8*1024**2:
                raise ValueError('Owned model fingerprint exceeds8MiB')
            rows[model._meta.db_table] = hashlib.sha256(raw).hexdigest()
            cursor.execute("SELECT pg_get_serial_sequence(%s,'id')", [model._meta.db_table])
            name = cursor.fetchone()[0]
            if name:
                cursor.execute('SELECT last_value,is_called FROM '+name)
                sequences[name] = list(cursor.fetchone())
    return dict(rows=rows, sequences=sequences)


tokens = {}
for role in ['admin', 'standard', 'basic', 'inactive']:
    tokens[role] = str(AccessToken.for_user(User.objects.get(username=PREFIX+role)))
records = []
ACTIVE_PORT = 5656
HEADERS = ['Content-Type', 'Content-Length', 'Allow', 'Vary', 'WWW-Authenticate',
    'Set-Cookie', 'Location', 'X-Content-Type-Options', 'X-Frame-Options', 'Referrer-Policy',
    'Cross-Origin-Opener-Policy']


def request(label, role='admin', method='GET', accept='application/json', path='/proxy/stats/', extra_headers=None):
    if len(records) >= 64:
        raise ValueError('Owned case count exceeds64')
    headers = {'Host': 'localhost', 'Accept': accept}
    if role == 'invalid':
        headers['Authorization'] = 'Bearer not-a-valid-fixture-token'
    elif role is not None:
        headers['Authorization'] = 'Bearer '+tokens[role]
    if extra_headers:
        headers.update(extra_headers)
    client = http.client.HTTPConnection('127.0.0.1', ACTIVE_PORT, timeout=10)
    started = time.time()
    try:
        client.request(method, path, headers=headers)
        response = client.getresponse()
        raw = response.read(2*1024**2+1)
        if len(raw) > 2*1024**2:
            raise ValueError('Owned stats response exceeds2MiB')
        row = dict(label=label, role=role, method=method, path=path, accept=accept,
            started_at=started, finished_at=time.time(), status=response.status,
            raw_header_pairs=[(key, value) for key, value in response.getheaders()
                              if key.lower() in {name.lower() for name in HEADERS}],
            body_base64=base64.b64encode(raw).decode(), body_bytes=len(raw),
            body_sha256=hashlib.sha256(raw).hexdigest())
        try:
            row['json'] = json.loads(raw)
        except ValueError:
            pass
        if extra_headers:
            row['extra_headers'] = extra_headers
        records.append(row)
    finally:
        client.close()


redis = RedisClient.get_client()
assert redis is not None
before = database()
owned = []
result = None
try:
    for role in [None, 'invalid', 'inactive', 'basic', 'standard', 'admin']:
        request('empty-auth-'+str(role), role=role)
    for method in ['HEAD', 'OPTIONS', 'POST', 'PUT', 'PATCH', 'DELETE', 'TRACE']:
        for role in [None, 'standard', 'admin']:
            request('empty-method-'+method+'-'+str(role), role=role, method=method)
    for accept in ['text/html', '*/*', 'application/json; indent=4', 'image/png',
                   'application/json;q=0,text/html;q=1']:
        request('empty-accept-'+accept, accept=accept)
    for path in ['/proxy/stats', '/proxy/stats/?format=json', '/proxy/stats/?format=api',
                 '/proxy/stats/?format=invalid']:
        request('empty-format-'+path, path=path)

    for role in [None, 'invalid', 'inactive', 'basic', 'standard']:
        request('empty-html-auth-'+str(role), role=role, accept='text/html')
    for method in ['OPTIONS', 'POST', 'HEAD']:
        request('empty-html-method-'+method, method=method, accept='text/html')
    request('empty-unsupported-accept-unauth', role=None, accept='image/png')

    request('format-json-html', accept='text/html', path='/proxy/stats/?format=json')
    request('format-api-html', accept='text/html', path='/proxy/stats/?format=api')
    request('format-api-options-html', method='OPTIONS', accept='text/html', path='/proxy/stats/?format=api')
    request('format-invalid-html', accept='text/html', path='/proxy/stats/?format=invalid')
    request('options-indent', method='OPTIONS', accept='application/json; indent=4')
    request('error-indent', method='POST', accept='application/json; indent=4')
    request('invalid-accept-before-auth', role='invalid', accept='image/png')

    request('slashless-anonymous', role=None, path='/proxy/stats')
    for method in ['HEAD', 'OPTIONS', 'POST']:
        request('slashless-'+method, method=method, path='/proxy/stats')
    request('slashless-valid-csrf-POST', method='POST', path='/proxy/stats',
        extra_headers={'Cookie':'csrftoken='+'a'*32, 'X-CSRFToken':'a'*32})

    seed = json.loads(Path('/evidence/seed.json').read_text())
    channel = Channel.objects.get(uuid=seed['channel_uuid'])
    movie = Movie.objects.order_by('id').first()
    episode = Episode.objects.order_by('id').first()
    profile = M3UAccountProfile.objects.filter(m3u_account__name__startswith=PREFIX).order_by('id').first()
    assert movie is not None and episode is not None and profile is not None
    created = time.time()-10
    live_key = 'live:channel:'+str(channel.uuid)+':metadata'
    movie_key = 'vod_persistent_connection:combined-stats-owned-movie'
    episode_key = 'vod_persistent_connection:combined-stats-owned-episode'
    catch_id = str(channel.pk)+'_combined-stats-owned-catchup'
    catch_meta = 'timeshift:channel:'+catch_id+':metadata'
    catch_clients = 'timeshift:channel:'+catch_id+':clients'
    catch_client = catch_clients+':combined-stats-owned-client'
    owned = [live_key, movie_key, episode_key, catch_meta, catch_clients, catch_client]
    assert all(not redis.exists(key) for key in owned), 'Owned key collision'
    redis.hset(live_key, mapping={'state':'active', 'url':'http://origin:8080/live/0.ts',
        'stream_profile':'proxy', 'owner':'combined-stats-owned-owner', 'init_time':'0',
        'channel_name':channel.name, 'total_bytes':'1024', 'm3u_profile':str(profile.pk)})
    for key, kind, content in [(movie_key, 'movie', movie), (episode_key, 'episode', episode)]:
        redis.hset(key, mapping={'content_obj_type':kind, 'content_uuid':str(content.uuid),
            'm3u_profile_id':str(profile.pk), 'client_ip':'127.0.0.1', 'client_user_agent':'combined-stats-owned',
            'user_id':str(User.objects.get(username=PREFIX+'admin').pk),
            'created_at':str(created), 'last_activity':str(created), 'bytes_sent':'1880',
            'position_seconds':'0', 'last_seek_percentage':'0', 'last_seek_timestamp':'0',
            'last_seek_byte':'0', 'total_content_size':'100000'})
    redis.hset(catch_meta, mapping={'channel_id':str(channel.pk), 'channel_uuid':str(channel.uuid),
        'channel_name':channel.name, 'init_time':'0', 'total_bytes':'1880', 'm3u_profile':str(profile.pk)})
    redis.sadd(catch_clients, 'combined-stats-owned-client')
    redis.hset(catch_client, mapping={'programme_start':'2026-11-09T12:00:00Z',
        'connected_at':'', 'position_anchor_at':'', 'paused':'true', 'ip_address':'127.0.0.1',
        'user_id':str(User.objects.get(username=PREFIX+'admin').pk), 'username':PREFIX+'admin',
        'user_agent':'combined-stats-owned'})
    for key in owned:
        redis.expire(key, 120)
    request('nonempty-three-families')
    request('nonempty-html', accept='text/html')
    request('nonempty-head', method='HEAD')
    observed = records[-3]
    assert observed['status'] == 200
    body = observed['json']
    assert body['live']['count'] == 1
    assert body['vod']['total_connections'] == 2
    assert body['catchup']['total_connections'] == 1
    after = database()
    assert before == after, 'Readonly statistics changed selected models or sequences'
    native = None
    if os.environ.get('COMBINED_STATS_NATIVE'):
        from combined_stats_compare import replay
        native = replay(records, request, globals(), redis, owned, database, before, created)
    result = dict(passed=native is None or native['passed'], scope=__doc__, records=records, native=native,
        restored18=True, selected_models=19, sequences_restored=True,
        selected_fingerprints_before=before, selected_fingerprints_after=after,
        nonempty_expected_counts=dict(live=1, vod=2, catchup=1))
finally:
    if owned:
        redis.delete(*owned)
    assert all(not redis.exists(key) for key in owned), 'Owned Redis cleanup failed'
    assert database() == before, 'Selected model/sequence state changed'
    if result is not None:
        result['owned_redis_keys_removed'] = len(owned)
print(json.dumps(result, cls=DjangoJSONEncoder))
