"""Actual original category ordering SQL and installed DRF search terms.
Original Dispatcharr authors retain application credit; AGPL retained.
Factory/installed-helper observations are distinct from network wire captures.
"""
import os,sys,json,hashlib,inspect
from urllib.parse import urlencode
if os.environ.get('POSTGRES_PASSWORD')!='playback-fixture-only'or os.environ.get('VOD_API_DISPOSABLE_FIXTURE')!='playback-fixture-only':raise RuntimeError('Owned fixture required')
sys.path.insert(0,'/app');os.environ.setdefault('DJANGO_SETTINGS_MODULE','dispatcharr.settings')
import django
django.setup()
from django.db import connection
from django.test.utils import CaptureQueriesContext
from django.urls import resolve
from rest_framework.test import APIRequestFactory
from rest_framework.request import Request
from rest_framework.filters import SearchFilter
from rest_framework_simplejwt.tokens import AccessToken
from apps.accounts.models import User
user=User.objects.get(username='rust-probe-vod-rest-standard');factory=APIRequestFactory();records=[]
for query in [{'ordering':'m3u_relations'},{'ordering':'-m3u_relations'},{'ordering':'m3u_relations,name'},{'ordering':'name,m3u_relations'},{'ordering':'m3u_relations','m3u_account':'2'},{'ordering':'-m3u_relations','m3u_account':'3'},{'ordering':'get_category_type_display'}]:
    request=factory.get('/api/vod/categories/?'+urlencode(query),HTTP_HOST='localhost',HTTP_ACCEPT='application/json',HTTP_AUTHORIZATION='Bearer '+str(AccessToken.for_user(user)),REMOTE_ADDR='127.0.0.1');match=resolve('/api/vod/categories/');request.resolver_match=match
    with CaptureQueriesContext(connection)as captured:
        try:
            response=match.func(request,*match.args,**match.kwargs);response.render();body=response.content;outcome={'status':response.status_code,'body_sha256':hashlib.sha256(body).hexdigest(),'ids':[row['id']for row in json.loads(body)]}
        except Exception as error:outcome={'exception':type(error).__name__,'message':str(error)[:1024]}
    queries=[row['sql']for row in captured.captured_queries if row['sql'].startswith('SELECT ')and'FROM "vod_vodcategory"'in row['sql']and'ORDER BY'in row['sql']]
    records.append({'query':query,'outcome':outcome,'actual_page_SQL':queries})
terms=[]
for value in ['Drama\\ News','"Drama"','Drama,News','"Drama News"','Drama\\News','Drama\\,News',"'Drama News'",'Drama "News"',r'Drama\" News']:
    request=Request(factory.get('/api/vod/categories/',{'search':value}));terms.append({'search':value,'actual_installed_SearchFilter_terms':list(SearchFilter().get_search_terms(request))})
source=inspect.getsource(SearchFilter.get_search_terms)
print(json.dumps({'passed':True,'scope':'Actual original APIRequestFactory category ORM SQL; separate installed DRF SearchFilter helper, not additional network wire parity','ordering':records,'search_terms':terms,'installed_SearchFilter_get_search_terms_source':source,'source_sha256':hashlib.sha256(source.encode()).hexdigest(),'native_verified':False}))
