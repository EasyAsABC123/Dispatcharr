"""Draft actual-original guide window/helper corpus; no native acceptance.
Original Dispatcharr authors and contributors retain full application credit.
"""
import datetime
import base64
import hashlib
import json
import os
import sys
import types
from unittest.mock import patch

if os.environ.get('POSTGRES_PASSWORD') != 'playback-fixture-only':
    raise RuntimeError('Disposable fixture required')
sys.path.insert(0, '/app')
sys.argv.append('shell')
os.environ.setdefault('DJANGO_SETTINGS_MODULE','dispatcharr.settings')
import django
django.setup()
from apps.epg import api_grid
from core.models import CoreSettings
from django.http import QueryDict

NOW = datetime.datetime(2026,11,9,12,37,29,123456,tzinfo=datetime.timezone.utc)
cases = []
def value(raw):
    if isinstance(raw, datetime.datetime):
        return {'datetime':raw.isoformat()}
    if isinstance(raw, tuple):
        return [value(item) for item in raw]
    if isinstance(raw, list):
        return [value(item) for item in raw]
    if isinstance(raw, dict):
        return {key:value(item) for key,item in raw.items()}
    if isinstance(raw, bytes):
        return {'bytes_base64':base64.b64encode(raw).decode()}
    return raw
def capture(kind, inputs, function):
    try:
        result = {'value':value(function())}
    except Exception as error:
        result = {'error':type(error).__name__, 'message':str(error)}
    cases.append({'kind':kind, 'inputs':inputs, 'expected':result})
def window(params, maximum=30):
    encoded = QueryDict('',mutable=True)
    for name,raw in params:
        encoded.appendlist(name,raw)
    with patch.object(CoreSettings,'get_epg_history_policy',return_value=types.SimpleNamespace(max_days=maximum)):
        capture('window',{'params':params,'history_max_days':maximum,'now':NOW.isoformat()},
                lambda:api_grid._resolve_epg_grid_window(types.SimpleNamespace(query_params=encoded),NOW))

integers = ['', '0','1','-1','365','366','9999999999999999999999999999999999999999',
            '+7',' 7 ','1_0','١٢','１２','²','1.0','1e2','True','NaN','0x10',
            '\x1c7','7\x1f','\u00857\u0085','\u20077\u2007','7\n','+_7','7__0',
            '9'*4301]
for maximum in [0,1,30,365]:
    window([],maximum)
    for raw in integers:
        window([('days',raw)],maximum)
        window([('prev_days',raw)],maximum)
    for raw in ['0','1','365','366']:
        window([('days',raw),('prev_days',raw)],maximum)
    window([('days','invalid'),('days','2')],maximum)
    window([('days','2'),('days','invalid')],maximum)

dates = ['2026-11-09','20261109','2026-313','2026-W46-1','2026W461',
         '2026-11-09T12:37:29Z','20261109T123729Z','2026-11-09 12:37:29',
         '2026-11-09X12:37:29','2026-11-09日本語12:37:29','2026-11-09🌍12:37:29',
         '2026-11-09T12:37','2026-11-09T12','2026-11-09T12:37:29.123456789',
         '2026-11-09T12:37:29,123456','2026-11-09T12:37:29+01:30',
         '2026-11-09T12:37:29+0130','2026-11-09T12:37:29+01',
         '2026-11-09T12:37:29+01:30:02.3','2026-11-09T12:37:29-00:00',
         '2026-11-09T12:37:29+24:00','2026-11-09T12:37:60Z',
         '2026-02-29','2024-02-29','0001-01-01','9999-12-31',
         '0000-01-01','10000-01-01','２０２６-１１-０９','2026-1-9 1:2:3',
         '2026-11-09T12:37:29Z ',' 2026-11-09','', 'invalid']
for maximum in [0,30,365]:
    for raw in dates:
        window([('start',raw)],maximum)
        window([('end',raw)],maximum)
        window([('start',raw),('days','invalid')],maximum)
    for end in ['2026-11-09T12:00:00Z','2026-11-10T12:00:00Z',
                '2027-11-09T12:00:00Z','2027-11-10T12:00:00Z',
                '2028-11-09T12:00:00Z']:
        window([('start','2026-11-09T12:00:00Z'),('end',end)],maximum)

for raw in integers+['all','ALL','All',' all','all ','', '0001']:
    capture('channel_profile',{'raw':raw},lambda raw=raw:api_grid._parse_channel_profile_id({'channel_profile_id':raw}))
capture('channel_profile',{'missing':True},lambda:api_grid._parse_channel_profile_id({}))
for before in [-100,-48,-25,-24,-2,-1,0,1,2,24,25,48,100]:
    for after in [0.1,1,23,24,25,47,48,49,365*24]:
        start=NOW+datetime.timedelta(hours=before)
        end=start+datetime.timedelta(hours=after)
        capture('dummy_span',{'now':NOW.isoformat(),'start':start.isoformat(),'end':end.isoformat()},
                lambda start=start,end=end:api_grid._dummy_generation_span(NOW,start,end))

for microsecond in [0,1,999,1000,123456,999999]:
    for text in [None,'Season FINALE','ſINALE',7,False,[],{'finale':True},['FINALE']]:
        raw={'id':7,'start_time':NOW.replace(microsecond=microsecond),
             'end_time':NOW.replace(microsecond=microsecond)+datetime.timedelta(hours=1),
             'title':'日本語 & <fixture>','sub_title':None,'description':'','tvg_id':'fixture',
             'season':0,'episode':'2','flag_new':'false','flag_live':[],
             'flag_premiere':{'fixture':False},'premiere_text':text}
        capture('stored_grid_row',{'row':value(raw)},lambda raw=raw:api_grid._program_value_to_dict(raw))
        projected=api_grid._program_value_to_dict(raw)
        for first in [False,True]:
            capture('grid_batch',{'programs':value([projected]),'first':first},
                    lambda projected=projected,first=first:api_grid._encode_program_batch([projected],first=first))
for first in [False,True]:
    capture('grid_batch',{'programs':[],'first':first},lambda first=first:api_grid._encode_program_batch([],first=first))
for count in [0,1,499,500,501,1000,1001]:
    sample={'id':1,'start_time':NOW,'end_time':NOW+datetime.timedelta(hours=1),
            'title':'fixture 日本語','sub_title':None,'description':'','tvg_id':'fixture',
            'season':None,'episode':None,'is_new':False,'is_live':False,
            'is_premiere':False,'is_finale':False}
    with patch.object(api_grid,'_iter_real_program_dicts',return_value=iter([sample]*count)), \
         patch.object(api_grid,'_iter_dummy_for_channels',return_value=iter([])), \
         patch.object(api_grid,'spawn_memory_trim',return_value=None):
        chunks=list(api_grid._iter_grid_json_chunks(NOW-datetime.timedelta(hours=1),NOW+datetime.timedelta(days=1),NOW,
                    epg_ids={1},dummy_custom_channels=[],no_epg_channels=[]))
    body=b''.join(chunks)
    cases.append({'kind':'grid_chunk_boundaries','inputs':{'count':count,'sample':value(sample)},
                  'expected':{'body_sha256':hashlib.sha256(body).hexdigest(),'body_bytes':len(body),
                              'chunks':[{'bytes':len(chunk),'sha256':hashlib.sha256(chunk).hexdigest()} for chunk in chunks],
                              'decoded_programs':len(json.loads(body)['data'])}})
print(json.dumps({'scope':'original-guide-window-helper-corpus','cases':cases},separators=(',',':')))
