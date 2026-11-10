"""Actual original current-programme/grid HTTP corpus on owned disposable rows.
All original Dispatcharr authors/contributors retain full application credit.
Clock is pinned for boundary cases; native/application acceptance is separate.
"""
import datetime
import hashlib
import json
import os
import sys
from unittest.mock import patch
from urllib.parse import quote
from urllib.request import Request, urlopen
from urllib.error import HTTPError

if os.environ.get('POSTGRES_PASSWORD') != 'playback-fixture-only':
    raise RuntimeError('Disposable fixture required')
sys.path.insert(0, '/app')
sys.argv.append('shell')
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'dispatcharr.settings')
import django
django.setup()
from django.urls import resolve
from django.utils import timezone
from django.db import connection
from django.test.utils import CaptureQueriesContext
from rest_framework.test import APIRequestFactory
from rest_framework_simplejwt.tokens import AccessToken
from apps.accounts.models import User
from apps.channels.models import Channel, ChannelOverride, ChannelProfile, ChannelProfileMembership
from apps.epg.models import EPGSource, EPGData, ProgramData
from core.models import CoreSettings

PREFIX = 'rust-probe-guide-http-'
NOW = datetime.datetime(2026, 11, 9, 12, 37, 29, 123456, tzinfo=datetime.timezone.utc)
if EPGSource.objects.filter(name__startswith=PREFIX).exists():
    raise RuntimeError('Owned namespace must be fresh')
users = {}
for role, level, active, properties in [
    ('admin', 10, True, {}), ('standard', 1, True, {}),
    ('filtered', 1, True, {'hide_adult_content': True}),
    ('basic', 0, True, {}), ('inactive', 10, False, {})]:
    user = User.objects.create(username=PREFIX+role, password='!', user_level=level,
                               is_active=active, custom_properties=properties)
    users[role] = {'user': user, 'token': str(AccessToken.for_user(user))}
source = EPGSource.objects.create(name=PREFIX+'xml', source_type='xmltv', file_path='/data/guide-http-missing.xml')
dummy = EPGSource.objects.create(name=PREFIX+'dummy', source_type='dummy')
entries = [EPGData.objects.create(name=PREFIX+str(i), tvg_id='guide-http-'+str(i),
                                 epg_source=dummy if i == 3 else source) for i in range(4)]
channels = [Channel.objects.create(name=PREFIX+name, channel_number=91000+i,
                                   epg_data=entry, hidden_from_output=hidden,
                                   user_level=level, is_adult=adult)
            for i, (name, entry, hidden, level, adult) in enumerate([
                ('base', entries[0], False, 0, False),
                ('shared', entries[0], False, 0, False),
                ('override', None, False, 0, False),
                ('hidden', entries[2], True, 0, False),
                ('adult', entries[1], False, 0, True),
                ('restricted', entries[2], False, 10, False),
                ('unmapped', None, False, 0, False),
                ('dummy', entries[3], False, 0, False)])]
ChannelOverride.objects.create(channel=channels[2], epg_data=entries[1], name=PREFIX+'override-effective')
profiles = [ChannelProfile.objects.create(name=PREFIX+str(i)) for i in range(2)]
for channel in channels:
    ChannelProfileMembership.objects.update_or_create(channel=channel, channel_profile=profiles[0], defaults={'enabled': True})
ChannelProfileMembership.objects.update_or_create(channel=channels[0],channel_profile=profiles[1],defaults={'enabled':True})
ChannelProfileMembership.objects.update_or_create(channel=channels[2],channel_profile=profiles[1],defaults={'enabled':False})
users['filtered']['user'].channel_profiles.set([profiles[1]])
# Explicit reverse primary keys distinguish QuerySet.first()'s PK ordering
# from the direct EPG branch's first row encountered in an unordered scan.
base_id = (ProgramData.objects.order_by('-id').values_list('id', flat=True).first() or 0)+100
for index, (entry, start, end, label) in enumerate([
    (entries[0], -30, 30, 'overlap-first-heap'),
    (entries[0], -20, 40, 'overlap-low-pk'),
    (entries[1], -10, 10, 'override'),
    (entries[2], -1, 60, 'hidden-restricted'),
    (entries[0], -60, 0, 'ends-now-excluded'),
    (entries[1], 0, 60, 'starts-now-included'),
    (entries[0], 60, 120, 'future'),
    (entries[0], -120, -60, 'past')]):
    ProgramData.objects.create(id=base_id+(20 if index == 0 else index), epg=entry,
        start_time=NOW+datetime.timedelta(minutes=start), end_time=NOW+datetime.timedelta(minutes=end),
        title=PREFIX+label+' 日本語', tvg_id=entry.tvg_id,
        custom_properties={'season': index, 'episode': '2', 'new': bool(index%2),
                           'premiere_text': 'Season FINALE' if index == 0 else '',
                           'fixture': 'guide-current'})
factory = APIRequestFactory()
cases = []


def case(path, body=None, role='admin', method='GET', raw=None, content_type='application/json', extra_headers=None):
    path = quote(path, safe='/?=&')
    headers = {'HTTP_HOST': 'localhost'}
    headers.update(extra_headers or {})
    if role is not None:
        headers['HTTP_AUTHORIZATION'] = 'Bearer '+users[role]['token']
    data = raw if raw is not None else json.dumps(body, ensure_ascii=False)
    request = factory.generic(method, path, data=data.encode(), content_type=content_type,
                              REMOTE_ADDR='127.0.0.1', **headers)
    match = resolve(path.split('?')[0]); request.resolver_match = match
    result = {'path': path, 'method': method, 'role': role, 'body': body,
              'raw_body': raw, 'content_type': content_type,
              'headers': headers}
    with patch.object(timezone, 'now', return_value=NOW), CaptureQueriesContext(connection) as queries:
        try:
            response = match.func(request, *match.args, **match.kwargs)
            content = bytearray();chunks=[];stream_error=None
            if getattr(response, 'streaming', False):
                try:
                    for chunk in response.streaming_content:
                        content.extend(chunk);chunks.append(len(chunk))
                        if len(content)>8*1024*1024:raise RuntimeError('Owned guide fixture unexpectedly large')
                except Exception as error:
                    stream_error = type(error).__name__
            else:
                response.render();content.extend(response.content)
            expected = {'status': response.status_code, 'body_bytes': len(content),
                        'body_sha256': hashlib.sha256(content).hexdigest(),
                        'content_type': response.get('Content-Type'),
                        'streaming': bool(getattr(response,'streaming',False)),
                        'chunk_lengths': chunks, 'stream_error': stream_error,
                        'selected_headers': {key: response.get(key) for key in
                            ['Allow', 'Vary', 'WWW-Authenticate', 'Content-Length']},
                        'observation': 'Source APIRequestFactory; manually consumed response'}
            try:expected['json']=json.loads(content)
            except ValueError:pass
            result['executed_sql_before_close'] = [query['sql'] for query in queries.captured_queries]
            response.close()
        except Exception as error:
            # APIRequestFactory does not install Django's outer500 handler.
            # Preserve unhandled exception characterization explicitly instead
            # of pretending this is an observed HTTP500 body.
            expected = {'unhandled_exception':type(error).__name__,'message':str(error)}
            if path == '/api/epg/current-programs/' and method == 'POST':
                # Invalid input fails before any time-dependent programme read,
                # so the real WSGI error can be observed without a server clock
                # adapter. Do not infer an HTTP body from Factory exceptions.
                wire_headers={key.removeprefix('HTTP_').replace('_','-'):value
                              for key,value in headers.items()}
                wire_headers['Content-Type']=content_type
                wire=Request('http://127.0.0.1:5656'+path,data=data.encode(),
                             headers=wire_headers,method=method)
                try: wire_response=urlopen(wire,timeout=10)
                except HTTPError as wire_error: wire_response=wire_error
                with wire_response:
                    wire_body=wire_response.read(8193)
                    if len(wire_body)>8192:raise RuntimeError('Unexpected owned error response size')
                    expected['actual_http']={'status':wire_response.status,
                        'body_bytes':len(wire_body),'body_sha256':hashlib.sha256(wire_body).hexdigest(),
                        'content_type':wire_response.getheader('Content-Type')}
    # The owned fixture stores no real credentials. These are executed SELECT
    # statements (not result rows); retain them to diagnose observable unordered
    # join/scan order without inventing an ID sort in the port.
    result['executed_sql'] = [query['sql'] for query in queries.captured_queries]
    if path.startswith('/api/epg/grid/') and expected.get('status') == 200:
        from apps.epg.api_grid import _visible_channels_queryset, _parse_channel_profile_id
        channels_for_grid = _visible_channels_queryset(users[role]['user'], _parse_channel_profile_id(request.GET))
        from django.core.exceptions import EmptyResultSet
        try:result['grid_channel_sql'] = str(channels_for_grid.query)
        except EmptyResultSet:result['grid_channel_sql'] = None
        result['grid_channels'] = [dict(id=c.pk, uuid=str(c.uuid), name=c.effective_name,
            epg_id=c.effective_epg_data_obj.pk if c.effective_epg_data_obj else None,
            source_type=c.effective_epg_data_obj.epg_source.source_type if c.effective_epg_data_obj and c.effective_epg_data_obj.epg_source else None,
            source_properties=c.effective_epg_data_obj.epg_source.custom_properties if c.effective_epg_data_obj and c.effective_epg_data_obj.epg_source else None)
            for c in channels_for_grid]
    if path.startswith('/api/epg/grid/') and method == 'HEAD':
        # HEAD body suppression belongs to the actual server, not Factory.
        wire_headers={key.removeprefix('HTTP_').replace('_','-'):value
                      for key,value in headers.items()}
        wire=Request('http://127.0.0.1:5656'+path,headers=wire_headers,method='HEAD')
        try: wire_response=urlopen(wire,timeout=15)
        except HTTPError as error: wire_response=error
        with wire_response:
            wire_body=wire_response.read(1)
            expected['actual_http_head']={'status':wire_response.status,
                'body_bytes':len(wire_body),'content_type':wire_response.getheader('Content-Type'),
                'selected_headers':{key:wire_response.getheader(key) for key in
                    ['Allow','Vary','WWW-Authenticate','Content-Length']},
                'observation':'Actual original HTTP HEAD; real clock; body suppressed by server'}
    result['expected']=expected;cases.append(result)

current='/api/epg/current-programs/'
for body in [{}, {'channel_uuids':None}, {'channel_uuids':[]},
             {'channel_uuids':[str(c.uuid) for c in channels]},
             {'channel_uuids':[str(channels[2].uuid)]},
             {'epg_data_ids':[entry.pk for entry in entries]},
             {'epg_data_ids':[]}, {'epg_data_ids':None},
             {'epg_data_ids':[entries[0].pk]},
             {'channel_uuids':[], 'epg_data_ids':[]},
             {'channel_uuids':None, 'epg_data_ids':[]},
             {'epg_data_ids':[entries[0].pk]*51},
             {'epg_data_ids':[entries[0].pk]*50+['bad']}]:
    case(current,body,method='POST')
for value in [None,True,False,7,1.5,'',[],{},['bad'],[None],[True],['١'],['1_0'],[1e100]]:
    case(current,{'epg_data_ids':value},method='POST')
    case(current,{'channel_uuids':value},method='POST')
for body in [None,[],False,7,'fixture',{'epg_data_ids':[None]}]:
    case(current,body,method='POST')
for role in ['standard','filtered','basic','inactive',None]:
    case(current,{},role=role,method='POST')
    case(current,None,role=role,method='POST',raw='{bad')
for method in ['GET','HEAD','OPTIONS','PUT','DELETE']:
    case(current,{},method=method)
case(current,{},method='POST',raw='{bad')
case(current,{},method='POST',raw='epg_data_ids=1',content_type='application/x-www-form-urlencoded')

grid='/api/epg/grid/'
for role in ['admin','standard','filtered','basic','inactive',None]:
    for query in ['', '?channel_profile_id='+str(profiles[0].pk),
                  '?channel_profile_id='+str(profiles[1].pk), '?channel_profile_id=999999']:
        case(grid+query,role=role)
for query in ['?days=0','?days=1','?days=-1','?prev_days=1','?days=bad',
              '?channel_profile_id=all','?channel_profile_id=ALL','?channel_profile_id=-1',
              '?channel_profile_id=bad','?start=2026-11-09T12:00:00Z&end=2026-11-09T13:00:00Z',
              '?start=2026-11-09T12:37:29.123456Z&end=2026-11-09T12:37:30Z',
              '?start=2026-11-09T13:00:00Z&end=2026-11-09T12:00:00Z',
              '?start=bad','?days=1&days=bad','?days=bad&days=1']:
    case(grid+query)
case(grid,method='HEAD')
baseline_cases=list(cases)
if os.environ.get('GUIDE_NEGOTIATION_CAPTURE') == '1':
    # Additional exploratory Source grammar, deliberately outside the accepted
    #40-case corpus and22 complete-entity component gate. No native acceptance
    #is inferred from this capture, especially BrowsableAPI HTML/CSRF content.
    for role in ['admin', 'standard', 'basic', None]:
        for accept in ['application/json','application/json;q=0','Application/JSON',
                       'application/json;indent=4','application/json;version=2',
                       'text/html','text/*','application/*','*/*','text/plain',
                       'application/xml','application/json,text/html;q=1',
                       'text/plain,application/json;q=0','',',','*']:
            case(grid,role=role,extra_headers={'HTTP_ACCEPT':accept})
        for query in ['?format=json','?format=api','?format=xml','?format=',
                      '?format=JSON','?format=json&format=xml','?format=xml&format=json']:
            case(grid+query,role=role)
        for method in ['OPTIONS','POST','PUT','PATCH','DELETE']:
            case(grid,role=role,method=method)
negotiation_cases=cases[len(baseline_cases):]
print(json.dumps({'scope':'Actual original current-programme and streamed guide-grid view/DB corpus; pinned clock; unhandled exceptions characterized separately',
                  'clock':NOW.isoformat(),'history_max_days':CoreSettings.get_epg_history_policy().max_days,'cases':baseline_cases,
                  'negotiation_cases':negotiation_cases,'fixtures':{'channel_uuids':[str(c.uuid) for c in channels],
                  'epg_data_ids':[entry.pk for entry in entries],'profile_ids':[p.pk for p in profiles]},
                  'native_verified':False,'application_parity_verified':False,'performance_verified':False},ensure_ascii=False))
