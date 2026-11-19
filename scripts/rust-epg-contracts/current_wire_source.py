"""Actual original current-programme HTTP with real clocks and owned wide windows.
Original Dispatcharr authors/contributors retain full application credit.
"""
import datetime
import hashlib
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
if os.environ.get('CURRENT_MULTIPART_CAPTURE') == '1':
    def multipart(fields):
        parts = [
            f'--dispatcharr\r\nContent-Disposition: form-data; name="{name}"\r\n\r\n{value}\r\n'
            for name, value in fields
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
        fixtures.append(dict(observed, content_type=content_type, body=None, raw_body=multipart(fields)))
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
        cases.append(result)
print(json.dumps({'scope':'Actual original real-clock current-programme wire on owned shifted intervals',
    'anchor':anchor.isoformat(),'cases':cases,'includes_multipart':os.environ.get('CURRENT_MULTIPART_CAPTURE') == '1','native_verified':False,'performance_verified':False}))
