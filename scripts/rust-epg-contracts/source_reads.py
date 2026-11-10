"""Draft original-runtime stored EPG API read oracle on disposable owned rows.
All original Dispatcharr authors/contributors retain application credit.
"""
import datetime
import json
import os
import sys
from urllib.parse import quote

if os.environ.get('POSTGRES_PASSWORD') != 'playback-fixture-only':
    raise RuntimeError('Disposable fixture required')
sys.path.insert(0, '/app')
sys.argv.append('shell')
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'dispatcharr.settings')
import django
django.setup()
from apps.accounts.models import User
from apps.channels.models import Channel
from apps.epg.models import EPGData, EPGSource, ProgramData
from apps.epg.api_views import EPGDataViewSet, EPGSourceViewSet, ProgramViewSet
from django.urls import resolve
from django_celery_beat.models import CrontabSchedule, PeriodicTask
from rest_framework.test import APIRequestFactory
from rest_framework_simplejwt.tokens import RefreshToken
from core.utils import build_absolute_uri_with_port

PREFIX = 'rust-probe-epg-reads-'
if EPGSource.objects.filter(name__startswith=PREFIX).exists():
    raise RuntimeError('Fixture namespace must be fresh')
tokens = {}
for role, level, active in [('admin', 10, True), ('standard', 1, True),
                            ('basic', 0, True), ('inactive', 10, False)]:
    user = User.objects.create(username=PREFIX+role, password='!',
                               user_level=level, is_active=active)
    tokens[role] = str(RefreshToken.for_user(user).access_token)

now = datetime.datetime(2026, 11, 9, 12, tzinfo=datetime.timezone.utc)
EPGSource.objects.bulk_create([
    EPGSource(name=PREFIX+'xmltv', source_type='xmltv', url='https://fixture.invalid/guide.xml',
              username='fixture-user', password='fixture-password',
              custom_properties={'fixture': ['日本語', 'é', None]}, priority=7),
    EPGSource(name=PREFIX+'inactive', source_type='xmltv', is_active=False,
              status='disabled', last_message='Fixture disabled'),
    EPGSource(name=PREFIX+'dummy', source_type='dummy', file_path='/data/fixture-missing.xml',
              custom_properties=None),
])
sources = list(EPGSource.objects.filter(name__startswith=PREFIX).order_by('id'))
cron = CrontabSchedule.objects.create(minute='5', hour='1,13', day_of_month='*',
                                     month_of_year='*', day_of_week='1-5', timezone='UTC')
task = PeriodicTask.objects.create(name=PREFIX+'cron', task='apps.epg.tasks.refresh_epg_data',
                                  args=json.dumps([sources[0].id]), crontab=cron, enabled=False)
EPGSource.objects.filter(pk=sources[0].pk).update(refresh_task=task, updated_at=now)
EPGData.objects.bulk_create([
    EPGData(tvg_id='fixture-'+str(i), name=PREFIX+str(i), icon_url=None if i%2 else '',
            epg_source=sources[i%3]) for i in range(5)
] + [EPGData(tvg_id=None, name=PREFIX+'null', epg_source=sources[0]),
     EPGData(tvg_id='', name=PREFIX+'empty', epg_source=sources[1])])
epgs = list(EPGData.objects.filter(name__startswith=PREFIX).order_by('id'))
Channel.objects.bulk_create([Channel(name=PREFIX+'mapped', channel_number=90001,
                                    epg_data=epgs[0])])
metadata = [None, {}, {'season': 0, 'episode': '2', 'new': False, 'live': [],
                       'premiere': 'yes', 'premiere_text': 'Season FINALE'},
            {'categories': ['Drama', '日本語'], 'credits': {'actor': ['A', {'name': 'B'}]},
             'rating': 'PG', 'rating_system': 'Fixture', 'star_ratings': [{'value':'3/5'}],
             'video': {'quality':'HD','aspect':'16:9'}, 'audio': {'stereo':'stereo'},
             'previously_shown': True, 'previously_shown_details': {'start':'1999'},
             'length': {'value':'45','units':'minutes'}, 'sd_icon':'https://fixture.invalid/poster?a=é'},
            {'credits': {}, 'video': {}, 'audio': {}, 'length': {},
             'premiere_text': 'ſINALE', 'date': '2026', 'new': 1, 'live': 'false',
             'images': [], 'content_ratings': [], 'content_advisory': [],
             'event_details': {'fixture': True}, 'imdb.com_id': 'tt-fixture'}]
ProgramData.objects.bulk_create([
    ProgramData(epg=epgs[i%len(epgs)], start_time=now+datetime.timedelta(hours=i),
                end_time=now+datetime.timedelta(hours=i+1, microseconds=i),
                title=PREFIX+str(i)+' 日本語', sub_title=None if i%2 else '',
                description='' if i%2 else None, tvg_id=None if i%2 else 'fixture-'+str(i),
                custom_properties=cp) for i,cp in enumerate(metadata)
])
programs = list(ProgramData.objects.filter(title__startswith=PREFIX).order_by('id'))
factory = APIRequestFactory()
cases = []
def case(path, role='admin', method='GET', headers=None):
    path = quote(path, safe='/?=&')
    headers = {'HTTP_HOST':'localhost', **(headers or {})}
    if role is not None:
        headers['HTTP_AUTHORIZATION'] = 'Bearer '+tokens[role]
    request = factory.generic(method, path, REMOTE_ADDR='127.0.0.1', **headers)
    match = resolve(path.split('?')[0])
    # Django normally installs this during URL resolution; the browsable API
    # uses its namespace when generating action links.
    request.resolver_match = match
    response = match.func(request, *match.args, **match.kwargs)
    response.render()
    body = bytes(response.content)
    try:
        expected = {'json':json.loads(body)}
    except ValueError:
        import hashlib
        expected = {'body_sha256':hashlib.sha256(body).hexdigest(), 'body_length':len(body)}
    if method == 'HEAD':
        expected = {'head_body_empty_on_wire':True, 'rendered_representation_bytes':len(body)}
    cases.append({'path':path, 'method':method, 'role':role, 'headers':headers,
                  'component_origin':build_absolute_uri_with_port(request,''),
                  'expected':{'status':response.status_code, **expected}})

for resource, objects in [('sources', sources), ('epgdata', epgs), ('programs', programs)]:
    base = '/api/epg/'+resource+'/'
    for query in ['', '?page=2&page_size=1', '?search=missing', '?ordering=-id',
                  '?name=missing', '?epg_source=999999', '?tvg_id=missing', '?format=json']:
        case(base+query)
    for item in objects:
        case(base+str(item.pk)+'/')
    for suffix in ['999999/', 'bad/', '0/', '-1/', '000'+str(objects[0].pk)+'/']:
        case(base+suffix)
    for role in ['standard','basic','inactive',None]:
        case(base, role)
        case(base+str(objects[0].pk)+'/', role)
        case(base+'bad/', role)
    case(base, headers={'HTTP_ACCEPT':'text/html'})
    case(base, headers={'HTTP_ACCEPT':'application/xml'})
    case(base, method='HEAD')
for item in programs:
    case('/api/epg/programs/'+str(item.pk)+'/', headers={
        'HTTP_X_FORWARDED_PROTO':'https', 'HTTP_X_FORWARDED_HOST':'fixture.invalid',
        'HTTP_X_FORWARDED_PORT':'9443'})
ordering = {}
for name, view in [('sources',EPGSourceViewSet),('epgdata',EPGDataViewSet),('programs',ProgramViewSet)]:
    queryset = view().get_queryset()
    ordering[name] = {'ordered':queryset.ordered,'model_ordering':queryset.model._meta.ordering,
                      'sql':str(queryset.query)}
print(json.dumps({'scope':'original-stored-EPG-read-oracle', 'cases':cases,
                  'collection_ordering':ordering}, separators=(',',':')))
