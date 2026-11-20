"""Actual original current-programme HTTP with real clocks and owned wide windows.
Original Dispatcharr authors/contributors retain full application credit.
"""
import datetime
import hashlib
from itertools import islice
import json
import os
import sys
from pathlib import Path
from urllib.request import Request, build_opener, HTTPRedirectHandler
from urllib.error import HTTPError
if os.environ.get('POSTGRES_PASSWORD') != 'playback-fixture-only':
    raise RuntimeError('Disposable fixture required')
sys.path.insert(0,'/app');os.environ.setdefault('DJANGO_SETTINGS_MODULE','dispatcharr.settings')
import django
django.setup()
from django.utils import timezone
from django.db.models import F
from apps.epg.models import ProgramData
fixture=json.loads(Path('/evidence/guide-source.json').read_text())
anchor=timezone.now();old=datetime.datetime.fromisoformat(fixture['clock']);delta=anchor-old
# All rows are in this freshly provisioned fixture. Preserve the original
# intervals/overlaps/PKs, move them near the actual clock, then compare actual
# Source and candidate HTTP. The separate frozen-clock gate covers exact edges.
ProgramData.objects.all().update(start_time=F('start_time')+delta,end_time=F('end_time')+delta)
class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self,*args):return None
opener=build_opener(NoRedirect());cases=[]
fixtures = [case for case in fixture['cases'] if case['path']=='/api/epg/current-programs/']
proven_empty = os.environ.get('CURRENT_PROVEN_EMPTY_CAPTURE') == '1'
def queue_state(broker,results):
    # Preserve the owned startup frames byte-for-byte and in order. No queue
    # commands consume work, and the fixture does not start any queue worker.
    names=('celery','dvr')
    with broker.pipeline(transaction=True) as pipe:
        for name in names:
            pipe.llen(name)
            pipe.lrange(name,0,63)
        pipe.hlen('unacked')
        pipe.zcard('unacked_index')
        celery_count,celery_frames,dvr_count,dvr_frames,unacked,unacked_index=pipe.execute()
    queues={}
    for name,length,frames in zip(names,(celery_count,dvr_count),(celery_frames,dvr_frames),strict=True):
        assert length <= 64 and len(frames) == length and all(len(frame) <= 65536 for frame in frames)
        queues[name]={'count':length,'frame_sha256_ordered':[hashlib.sha256(frame).hexdigest() for frame in frames]}
    result_keys=list(islice(results.scan_iter(match='celery-tasks:celery-task-meta-*',count=32),65))
    assert len(result_keys) <= 64
    return {**queues,'unacked':unacked,'unacked_index':unacked_index,
            'result_key_sha256':sorted(hashlib.sha256(key).hexdigest() for key in result_keys)}


if proven_empty:
    from apps.epg.models import EPGData, EPGSource
    from django.conf import settings
    from redis import Redis
    from urllib.parse import urlsplit

    admin = next(case for case in fixtures if case['method'] == 'POST' and case['role'] == 'admin')
    [schedules, xml] = EPGSource.objects.bulk_create([
        EPGSource(name='Owned no-file Schedules Direct', source_type='schedules_direct', is_active=False),
        EPGSource(name='Owned no-TVG XMLTV', source_type='xmltv', is_active=False),
    ])
    entries = EPGData.objects.bulk_create([
        EPGData(name='Owned source-less entry', tvg_id='owned-no-source'),
        EPGData(name='Owned no-airing SD', tvg_id='owned-sd', epg_source=schedules),
        EPGData(name='Owned absent TVG', tvg_id=None, epg_source=xml),
        EPGData(name='Owned empty TVG', tvg_id='', epg_source=xml),
    ])
    ids = [entry.id for entry in entries]
    assert len(set(ids)) == 4 and not ProgramData.objects.filter(epg_id__in=ids).exists()
    stored = fixture['fixtures']['epg_data_ids'][0]
    assert stored not in ids and ProgramData.objects.filter(
        epg_id=stored, start_time__lte=anchor, end_time__gt=anchor
    ).exists()
    keys = ('source-less', 'schedules-direct', 'null-tvg', 'empty-tvg', 'all-four', 'stored-plus-four')
    requests = [[id] for id in ids] + [ids, [stored, *ids]]
    fixtures = [dict(admin, name=name, body={'epg_data_ids': requested})
                for name, requested in zip(keys, requests, strict=True)]
    def owned_client(url):
        parts = urlsplit(url)
        assert parts.scheme == 'redis' and parts.hostname in ('localhost', '127.0.0.1') and parts.port == 6379
        return Redis.from_url(url, socket_connect_timeout=3, socket_timeout=3)

    broker = owned_client(settings.CELERY_BROKER_URL)
    results = owned_client(settings.CELERY_RESULT_BACKEND)
    queue_before = queue_state(broker, results)
    queue_sha = hashlib.sha256(json.dumps(queue_before, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
if os.environ.get('CURRENT_MULTIPART_CAPTURE') == '1':
    def multipart(fields):
        parts = [
            '--dispatcharr\r\n'
            f'Content-Disposition: form-data; name="{name}"'
            + ('; filename="fixture.txt"\r\nContent-Type: application/octet-stream' if file else '')
            + f'\r\n\r\n{value}\r\n'
            for name, value, file in fields
        ]
        return ''.join(parts) + '--dispatcharr--\r\n'

    observed = fixtures[0]
    for fields, boundary in [
        ([('epg_data_ids', '[]')], True),
        ([('channel_uuids', '[]'), ('channel_uuids', 'null')], True),
        ([(' epg_data_ids ', '')], True),
        ([('channel_uuids', 'null'), ('epg_data_ids', 'null')], True),
        ([('unrelated', 'ignored')], True),
        ([('epg_data_ids', '[]')], False),
    ]:
        content_type = 'multipart/form-data'
        if boundary:
            content_type += '; boundary=dispatcharr'
        fixtures.append(dict(observed, content_type=content_type, body=None, raw_body=multipart((name, value, False) for name, value in fields)))
    if os.environ.get('CURRENT_FILE_MULTIPART_CAPTURE') == '1':
        for fields in [
            [('epg_data_ids', '[]', True)],
            [('channel_uuids', '[]', True)],
            [('channel_uuids', '[]', False), ('epg_data_ids', '[]', True)],
            [('epg_data_ids', '', False), ('epg_data_ids', '[]', True)],
            [('unrelated', 'file bytes', True)],
        ]:
            fixtures.append(dict(
                observed, content_type='multipart/form-data; boundary=dispatcharr',
                body=None, raw_body=multipart(fields),
            ))
for case in fixtures:
    headers={name.removeprefix('HTTP_').replace('_','-'):value for name,value in case['headers'].items()}
    headers['Content-Type']=case['content_type']
    body=case['raw_body'] if case['raw_body'] is not None else json.dumps(case['body'],ensure_ascii=False)
    request=Request('http://127.0.0.1:5656'+case['path'],data=body.encode(),headers=headers,method=case['method'])
    try:response=opener.open(request,timeout=10)
    except HTTPError as error:response=error
    with response:
        data=response.read(8*1024*1024+1)
        if len(data)>8*1024*1024:raise RuntimeError('Owned fixture unexpectedly large')
        result={key:case[key] for key in ['path','method','role','body','raw_body','content_type','headers']}
        result['expected']={'status':response.status,'body_bytes':len(data),'body_sha256':hashlib.sha256(data).hexdigest(),
            'content_type':response.getheader('Content-Type'),'content_length':response.getheader('Content-Length')}
        if proven_empty:
            assert response.status == 200
            actual = json.loads(data)
            if case['name'] == 'stored-plus-four':
                assert len(actual) == 1 and actual[0]['epg_data_id'] == stored
            else:
                assert actual == []
            queue_after = queue_state(broker, results)
            assert queue_after == queue_before, 'owned_startup_queue_state_changed'
            result['after_queue_sha256'] = hashlib.sha256(
                json.dumps(queue_after, sort_keys=True, separators=(',', ':')).encode()
            ).hexdigest()
            result['name'] = case['name']
        cases.append(result)
if proven_empty:
    assert len(cases) == 6 and queue_state(broker, results) == queue_before
    assert not ProgramData.objects.filter(epg_id__in=ids).exists()
    assert list(EPGData.objects.filter(pk__in=ids).order_by('id').values_list('tvg_id', 'epg_source_id')) == [
        ('owned-no-source', None), ('owned-sd', schedules.id), (None, xml.id), ('', xml.id)
    ]
print(json.dumps({'scope':'Actual original real-clock current-programme wire on owned shifted intervals',
    'anchor':anchor.isoformat(),'cases':cases,'proven_empty_only':proven_empty,'includes_multipart':os.environ.get('CURRENT_MULTIPART_CAPTURE') == '1','includes_file_multipart':os.environ.get('CURRENT_FILE_MULTIPART_CAPTURE') == '1','startup_queue_state':queue_before if proven_empty else None,
    'startup_queue_sha256':queue_sha if proven_empty else None,
    'native_verified':False,'performance_verified':False}))
