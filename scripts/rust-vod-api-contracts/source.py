"""Guarded original VOD REST APIRequestFactory/actual-network oracle.

Run only in a fresh disposable Source Compose fixture after quiet ends.
All application credit belongs to original Dispatcharr authors/contributors.
This is not Source-network, native, playback or provider-refresh acceptance.
"""
import datetime
import hashlib
import json
import os
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.error import HTTPError
from urllib.request import Request, build_opener, HTTPRedirectHandler
from contextlib import ExitStack
from unittest.mock import patch
from urllib.parse import quote

if (os.environ.get('POSTGRES_PASSWORD') != 'playback-fixture-only' or
        os.environ.get('VOD_API_DISPOSABLE_FIXTURE') != 'playback-fixture-only'):
    raise RuntimeError('Explicit disposable VOD API fixture guards required')
MODE=os.environ.get('VOD_API_OBSERVATION','factory')
if MODE not in ('factory','http'):raise RuntimeError('Choose factory or http observation')
sys.path.insert(0, '/app')
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'dispatcharr.settings')
import django
django.setup()
from django.db import connection
from django.test.utils import CaptureQueriesContext
from django.urls import resolve
from django.utils import timezone
from rest_framework.test import APIRequestFactory
from rest_framework_simplejwt.tokens import AccessToken
from apps.accounts.models import User
from apps.m3u.models import M3UAccount, M3UAccountProfile
from apps.vod.models import (Movie, Series, Episode, VODLogo, VODCategory,
    M3UMovieRelation, M3USeriesRelation, M3UEpisodeRelation, M3UVODCategoryRelation)

PREFIX = 'rust-probe-vod-rest-'
NOW = datetime.datetime(2026, 11, 9, 12, 37, 29, 123456, tzinfo=datetime.timezone.utc)
REAL_NOW=timezone.now()
provider_attempts=[]
class ProviderTrap(BaseHTTPRequestHandler):
    def deny(self):
        if len(provider_attempts)<32:provider_attempts.append({'method':self.command,'path':self.path[:1024]})
        self.send_response(503);self.send_header('Content-Length','0');self.end_headers()
    do_GET=deny;do_POST=deny;do_HEAD=deny
    def log_message(self,*args):pass
# Any unexpected provider refresh remains inside the owned container and makes
# real HTTP capture fail. This is not a mocked successful refresh contract.
provider_trap=HTTPServer(('127.0.0.1',0),ProviderTrap)
provider_thread=threading.Thread(target=provider_trap.serve_forever,daemon=True)
provider_thread.start()
PROVIDER_URL='http://127.0.0.1:'+str(provider_trap.server_port)
if any(model.objects.filter(name__startswith=PREFIX).exists() for model in
       [Movie, Series, VODCategory, VODLogo, M3UAccount]):
    raise RuntimeError('Owned VOD namespace must be fresh')
if User.objects.filter(username__startswith=PREFIX).exists():
    raise RuntimeError('Owned user namespace must be fresh')
users = {}
for role, level, active, props in [
    ('admin',10,True,{}), ('standard',1,True,{}), ('basic',0,True,{}),
    ('inactive',10,False,{}), ('adult_hidden',1,True,{'hide_adult_content':True}),
    ('movies_off',1,True,{'vod_movies_enabled':False}),
    ('series_off',1,True,{'vod_series_enabled':False}),
    ('both_off',1,True,{'vod_movies_enabled':False,'vod_series_enabled':False}),
    ('zero_flags',1,True,{'vod_movies_enabled':0,'vod_series_enabled':0}),
    ('admin_movies_off',10,True,{'vod_movies_enabled':False})]:
    user=User.objects.create(username=PREFIX+role,password='!',user_level=level,
                             is_active=active,custom_properties=props)
    users[role]={'id':user.pk,'token':str(AccessToken.for_user(user))}
# bulk_create bypasses account save signals that schedule ingestion/profile
# side effects. These fixture rows are not account-creation API acceptance.
accounts=M3UAccount.objects.bulk_create([
    M3UAccount(name=PREFIX+'active',account_type='XC',is_active=True,priority=2,
        server_url=PROVIDER_URL,custom_properties={'enable_vod':True}),
    M3UAccount(name=PREFIX+'second',account_type='XC',is_active=True,priority=1,
        server_url=PROVIDER_URL,custom_properties={'enable_vod':True,'auto_enable_new_groups_vod':False}),
    M3UAccount(name=PREFIX+'inactive',account_type='XC',is_active=False,
        server_url=PROVIDER_URL,custom_properties={'enable_vod':True})])
M3UAccountProfile.objects.bulk_create([
    M3UAccountProfile(m3u_account=account,name=PREFIX+'default',is_default=True,is_active=True,
        search_pattern='^(.*)$',replace_pattern='$1',max_streams=0) for account in accounts])
logo=VODLogo.objects.create(name=PREFIX+'shared-logo',url='https://fixture.invalid/logo.jpg')
categories=[VODCategory.objects.create(name=PREFIX+name,category_type=kind)
    for name,kind in [('Drama','movie'),('Drama','series'),('News|Live','movie')]]
category_links=M3UVODCategoryRelation.objects.bulk_create([
    M3UVODCategoryRelation(category=category,m3u_account=account,enabled=index%2==0,
                           custom_properties={'language':'en'})
    for index,category in enumerate(categories) for account in accounts])
movies=[]
for index in range(27):
    movies.append(Movie.objects.create(name=PREFIX+('Alpha' if index<2 else f'Movie{index:02}')+(' % _ 日本語' if index==2 else ''),
        year=2000+index,description='Drama, space adventure' if index%2 else None,
        genre='Action' if index%2 else None,rating='7.50' if index%2 else None,
        duration_secs=0 if index==0 else 7200,language='en' if index==1 else '',
        is_adult=index==3,logo=logo if index<2 else None,
        custom_properties=[1,'é'] if index==4 else {'fixture':index,'nil':None}))
movie_relations=[]
for index,movie in enumerate(movies):
    account=accounts[2] if index==26 else accounts[0]
    movie_relations.append(M3UMovieRelation.objects.create(movie=movie,m3u_account=account,
        category=categories[index%3],stream_id=str(1000+index),container_extension='mp4',
        custom_properties={'detailed_fetched':True,'quality':'1080p'},last_advanced_refresh=REAL_NOW))
# Same content on two active providers must not duplicate a main list row.
movie_relations.append(M3UMovieRelation.objects.create(movie=movies[0],m3u_account=accounts[1],
    category=categories[1],stream_id='2000',custom_properties={'detailed_fetched':True},last_advanced_refresh=REAL_NOW))
series=[];series_relations=[];episodes=[]
for index in range(4):
    show=Series.objects.create(name=PREFIX+f'Series{index}',year=2010+index,
        description='Space, drama',genre='Science Fiction',rating='8.25',
        logo=logo if index==0 else None,custom_properties={'fixture':index})
    series.append(show)
    relation=M3USeriesRelation.objects.create(series=show,
        m3u_account=accounts[2] if index==3 else accounts[0],category=categories[1],
        external_series_id=str(3000+index),last_episode_refresh=REAL_NOW,
        custom_properties={'episodes_fetched':True,'detailed_fetched':True})
    series_relations.append(relation)
    for episode_index in range(3):
        episode=Episode.objects.create(series=show,name=PREFIX+f'Episode{index}-{episode_index}',
            season_number=None if episode_index==2 else 1,episode_number=episode_index+1,
            duration_secs=1800,rating=None,air_date=datetime.date(2026,1,episode_index+1),
            custom_properties={'fixture':episode_index})
        episodes.append(episode)
        if episode_index!=2:
            M3UEpisodeRelation.objects.create(episode=episode,m3u_account=accounts[0],
                series_relation=relation,stream_id=str(4000+episode.pk),container_extension='mp4')

factory=APIRequestFactory();cases=[]
def owned_effects():
    return {'uncategorized':[list(row) for row in VODCategory.objects.filter(name='Uncategorized').order_by('id').values_list('id','category_type')],
        'owned_category_relations':[list(row) for row in M3UVODCategoryRelation.objects.filter(m3u_account__in=accounts).order_by('id').values_list('id','category_id','m3u_account_id','enabled')],
        'movie_refresh_rows':[dict(row,last_advanced_refresh=row['last_advanced_refresh'].isoformat()if row['last_advanced_refresh']else None)for row in M3UMovieRelation.objects.filter(m3u_account__in=accounts).order_by('id').values('id','custom_properties','last_advanced_refresh')],
        'series_refresh_rows':[dict(row,last_episode_refresh=row['last_episode_refresh'].isoformat()if row['last_episode_refresh']else None)for row in M3USeriesRelation.objects.filter(m3u_account__in=accounts).order_by('id').values('id','custom_properties','last_episode_refresh')],
        'owned_counts':{'movies':Movie.objects.filter(name__startswith=PREFIX).count(),'series':Series.objects.filter(name__startswith=PREFIX).count(),'episodes':Episode.objects.filter(name__startswith=PREFIX).count()},
        'provider_trap_attempts':len(provider_attempts)}

class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self,*args):return None
opener=build_opener(NoRedirect())
def wire_case(path,role,method,body,headers):
    if not 0 <=(timezone.now()-REAL_NOW).total_seconds()<300:
        raise RuntimeError('Real HTTP metadata fixture expired; provider freshness/JWT assumptions invalid')
    before=owned_effects();began=time.monotonic()
    wire_headers={key.removeprefix('HTTP_').replace('_','-'):value for key,value in headers.items()}
    wire_headers['Content-Type']='application/json'
    data=json.dumps(body).encode()if method not in ('GET','HEAD')else None
    request=Request('http://127.0.0.1:5656'+path,data=data,headers=wire_headers,method=method)
    try:response=opener.open(request,timeout=15)
    except HTTPError as error:response=error
    with response:
        payload=response.read(8*1024*1024+1)
        if len(payload)>8*1024*1024:raise RuntimeError('Owned HTTP metadata response exceeds8MiB')
        expected={'status':response.status,'body_bytes':len(payload),'body_sha256':hashlib.sha256(payload).hexdigest(),
            'content_type':response.getheader('Content-Type'),'selected_headers':{key:response.getheader(key)for key in
                ['Content-Length','Allow','Vary','WWW-Authenticate','X-Content-Type-Options','X-Frame-Options','Referrer-Policy','Cross-Origin-Opener-Policy']}}
        try:expected['json']=json.loads(payload)
        except ValueError:pass
    after=owned_effects()
    cases.append({'path':path,'role':role,'method':method,'body':body,'headers':headers,
        'observation':'Actual unpatched original HTTP; real clock; no view/renderer/auth mocking',
        'expected':expected,'elapsed_seconds':time.monotonic()-began,'effects_before':before,'effects_after':after,
        'provider_refresh_attempted':after['provider_trap_attempts']!=before['provider_trap_attempts'],
        'sql_observation':'DB effect snapshots only; actual server-process SQL not captured by observer connection'})

def case(path,role='standard',method='GET',body=None,extra_headers=None):
    if len(cases)>=700:raise RuntimeError('Owned case-count bound')
    path=quote(path,safe='/?=&|,')
    headers={'HTTP_HOST':'localhost','HTTP_ACCEPT':'application/json'}
    if role is not None:headers['HTTP_AUTHORIZATION']='Bearer '+users[role]['token']
    if extra_headers:headers.update(extra_headers)
    if MODE=='http':return wire_case(path,role,method,body,headers)
    request=factory.generic(method,path,data=json.dumps(body).encode(),content_type='application/json',REMOTE_ADDR='127.0.0.1',**headers)
    match=resolve(path.split('?')[0]);request.resolver_match=match
    before=owned_effects()
    result={'path':path,'role':role,'method':method,'body':body,'headers':headers}
    # Metadata-only capture cannot accidentally fetch a provider, send Celery
    # jobs, or turn a mocked refresh into provider-refresh acceptance.
    with ExitStack() as stack:
        stack.enter_context(patch.object(timezone,'now',return_value=NOW))
        blocked=[]
        def deny(*args,**kwargs):
            blocked.append({'args_repr':repr(args)[:512]})
            raise RuntimeError('Provider/network/task operation blocked in metadata-only oracle')
        for target in ['requests.sessions.Session.request','celery.app.task.Task.apply_async',
                       'apps.vod.api_views.refresh_movie_advanced_data',
                       'apps.vod.tasks.refresh_series_episodes']:
            stack.enter_context(patch(target,side_effect=deny))
        queries=stack.enter_context(CaptureQueriesContext(connection))
        try:
            response=match.func(request,*match.args,**match.kwargs)
            if getattr(response,'streaming',False):
                raise RuntimeError('Unexpected streamed metadata response')
            response.render();data=response.content
            if len(data)>8*1024*1024:raise RuntimeError('Owned response exceeds8MiB')
            result['expected']={'status':response.status_code,'body_bytes':len(data),
                'body_sha256':hashlib.sha256(data).hexdigest(),'content_type':response.get('Content-Type'),
                'selected_headers':{key:response.get(key) for key in ['Allow','Vary','WWW-Authenticate']}}
            try:result['expected']['json']=json.loads(data)
            except ValueError:pass
            response.close()
        except Exception as error:
            result['expected']={'unhandled_exception':type(error).__name__,'message':str(error)}
        result['executed_sql']=[q['sql'] for q in queries.captured_queries][:256]
        result['blocked_provider_or_task_operations']=blocked
    result['effects_before']=before;result['effects_after']=owned_effects();cases.append(result)

# First list is empty by access flags but still performs lazy category writes.
initial_effects=owned_effects()
case('/api/vod/categories/',role='both_off')
case('/api/vod/categories/',role='both_off')
for endpoint in ['movies','series','episodes','categories','all']:
    base='/api/vod/'+endpoint+'/'
    for role in [None,*users]:case(base,role=role)
    for query in ['?page=1','?page=2','?page=last','?page=0','?page=-1','?page=bad',
                  '?page=99999','?page_size=1','?page_size=101','?page_size=0',
                  '?page_size=-1','?page_size=bad','?page=2&page=1',
                  '?search=space','?search=Space,drama','?search=%','?search=_',
                  '?ordering=-year,name','?ordering=bad','?name=Alpha',
                  '?year=2001','?year=bad','?year_gte=2001','?year_lte=2005',
                  '?is_adult=true','?is_adult=false','?m3u_account='+str(accounts[2].pk),
                  '?category='+PREFIX+'Drama','?category='+PREFIX+'Drama|movie',
                  '?category='+PREFIX+'News|Live|movie','?category='+PREFIX+'Drama|invalid',
                  '?category_type=movie','?category_type=series','?category_type=bad',
                  '?m3u_account=bad','?season_number=bad','?episode_number=2',
                  '?search=SPACE&ordering=name','?page_size=2&ordering=-name']:
        case(base+query)
    for method in ['HEAD','OPTIONS','POST','PUT','PATCH','DELETE']:
        for role in ['admin','standard','basic',None]:case(base,role=role,method=method,body={})
for endpoint,items in [('movies',movies),('series',series),('episodes',episodes),('categories',categories)]:
    for item in [items[0],items[-1]]:
        for role in ['admin','standard','basic','movies_off','series_off',None]:
            case(f'/api/vod/{endpoint}/{item.pk}/',role=role)
    case(f'/api/vod/{endpoint}/999999999/')
    case(f'/api/vod/{endpoint}/{items[0].pk}/?name=definitely-missing')
    case(f'/api/vod/{endpoint}/{items[0].pk}/?search=definitely-missing')
for endpoint,item in [('movies',movies[0]),('series',series[0])]:
    for role in ['admin','standard','basic','movies_off','series_off',None]:
        case(f'/api/vod/{endpoint}/{item.pk}/providers/',role=role)
    info_queries=['','?relation_id=bad','?relation_id=999999999']
    if MODE=='factory':info_queries+=['?force_refresh=true']
    if endpoint=='series':info_queries+=['?include_episodes=false','?refresh_interval=bad']
    for query in info_queries:
        case(f'/api/vod/{endpoint}/{item.pk}/provider-info/'+query)
for role in ['admin','standard','basic','series_off',None]:
    case(f'/api/vod/series/{series[0].pk}/episodes/',role=role)
for role in ['admin','standard','adult_hidden','movies_off']:
    case(f'/api/vod/movies/{movies[3].pk}/',role=role)
case(f'/api/vod/episodes/{episodes[2].pk}/')  # Providerless top-level detail.
case('/api/vod/episodes/?series='+str(series[0].pk))
if os.environ.get('VOD_API_HTTP_WIRE')=='1':
    if MODE!='http':raise RuntimeError('Additional route/negotiation evidence requires actual HTTP')
    for endpoint,pk in [('movies',movies[0].pk),('series',series[0].pk),('episodes',episodes[0].pk)]:
        base='/api/vod/'+endpoint+'/'
        for role in ['admin','standard','basic',None]:
            case(base+str(pk)+'/',role=role,method='HEAD')
            case(base+str(pk)+'/',role=role,method='OPTIONS')
            case(base+'999999999/',role=role,method='OPTIONS')
        for accept in ['*/*','application/*','application/json;q=0','application/json;q=0.1,text/html;q=1','application/xml','application/json;indent=4']:
            case(base,extra_headers={'HTTP_ACCEPT':accept})
        for role in ['standard',None]:
            case(base+'?format=bad',role=role)
            case(base,role=role,extra_headers={'HTTP_ACCEPT':'application/xml'})
        for suffix in ['?format=json','.json','.json/']:
            case(base+suffix if suffix.startswith('?') else base.rstrip('/')+suffix)
        for role,method,tail in [(None,'GET',''),('basic','GET',''),('standard','OPTIONS',''),('admin','POST',''),('standard','GET','?year=bad'),('standard','HEAD',''),('standard','GET','999999999/')]:
            case(base+tail,role=role,method=method,body={},extra_headers={'HTTP_ACCEPT':'application/json;indent=4'})
        for indent in ['0','-1','8','99','bad','"4"']:
            case(base,extra_headers={'HTTP_ACCEPT':'application/json;indent='+indent})
        case(base.rstrip('/'))
        for pk_text in ['bad','0','-1','9999999999999999999999999999999999']:
            case(base+pk_text+'/')
final_effects=owned_effects()
provider_trap.shutdown();provider_trap.server_close();provider_thread.join(timeout=2)
refresh_unchanged=(initial_effects['movie_refresh_rows']==final_effects['movie_refresh_rows']and
                   initial_effects['series_refresh_rows']==final_effects['series_refresh_rows'])
print(json.dumps({'scope':'Original VOD metadata-only capture; Factory and real HTTP modes require separate fresh fixtures',
    'observation':MODE,'capture_complete':len(cases)>300 and (MODE!='http'or(not provider_attempts and refresh_unchanged)),
    'initial_effects':initial_effects,'final_effects':final_effects,'provider_refresh_rows_unchanged':refresh_unchanged,
    'provider_trap_attempts':provider_attempts,'provider_policy':'Fresh detail flags/timestamps and loopback503 trap; no remote provider URLs',
    'clock':NOW.isoformat(),'real_fixture_created_at':REAL_NOW.isoformat(),'cases':cases,'fixtures':{'prefix':PREFIX,
        'movie_ids':[x.pk for x in movies],'series_ids':[x.pk for x in series],
        'episode_ids':[x.pk for x in episodes],'category_ids':[x.pk for x in categories],
        'account_ids':[x.pk for x in accounts]},'native_verified':False,
    'source_network_capture_complete':MODE=='http'and not provider_attempts and refresh_unchanged,'provider_refresh_verified':False,'performance_verified':False},ensure_ascii=False))
