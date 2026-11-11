"""UNEXECUTED guarded Original unified long-int/user-property error oracle.
Run after current React checkpoint; Factory and HTTP must be separate fresh
fixtures. Original Dispatcharr authors retain credit and AGPL licensing.
"""
import datetime,hashlib,http.client,json,os,sys
from urllib.parse import urlencode
if os.environ.get('POSTGRES_PASSWORD')!='playback-fixture-only'or os.environ.get('VOD_API_DISPOSABLE_FIXTURE')!='playback-fixture-only':raise RuntimeError('Owned disposable fixture required')
MODE=os.environ.get('VOD_UNIFIED_EDGE_MODE');assert MODE in ['factory','http']
sys.path.insert(0,'/app');os.environ.setdefault('DJANGO_SETTINGS_MODULE','dispatcharr.settings')
import django
django.setup()
from django.db import connection
from django.test.utils import CaptureQueriesContext
from django.urls import resolve
from django.core.serializers.json import DjangoJSONEncoder
from rest_framework.test import APIRequestFactory
from rest_framework_simplejwt.tokens import AccessToken
from apps.accounts.models import User
from apps.m3u.models import M3UAccount,M3UAccountProfile
from apps.vod.models import Movie,Series,Episode,VODLogo,VODCategory,M3UMovieRelation,M3USeriesRelation,M3UEpisodeRelation,M3UVODCategoryRelation
from core.models import CoreSettings
PREFIX='rust-probe-vod-rest-';user=User.objects.get(username=PREFIX+'standard');assert user.is_active and user.user_level==1
assert M3UAccount.objects.filter(name__startswith=PREFIX).count()==3 and not M3UAccount.objects.filter(name__startswith=PREFIX).exclude(server_url__startswith='http://127.0.0.1:').exists()
class ExactEncoder(DjangoJSONEncoder):
 def default(self,value):
  if isinstance(value,(datetime.datetime,datetime.date)):return value.isoformat()
  return super().default(value)
models=[User,M3UAccount,M3UAccountProfile,Movie,Series,Episode,VODLogo,VODCategory,M3UMovieRelation,M3USeriesRelation,M3UEpisodeRelation,M3UVODCategoryRelation,CoreSettings]
def fingerprint():return {m._meta.db_table:hashlib.sha256(json.dumps(list(m.objects.order_by('pk').values()),sort_keys=True,separators=(',',':'),cls=ExactEncoder).encode()).hexdigest()for m in models}
before=fingerprint();saved=user.custom_properties;records=[];factory=APIRequestFactory()
def observe(query,label,method='GET'):
 path='/api/vod/all/'+('?'+urlencode(query)if query else'');assert len(path)<60000
 headers={'Host':'localhost','Accept':'application/json','Authorization':'Bearer '+str(AccessToken.for_user(user))};previous=fingerprint()
 record={'path':path,'method':method,'label':label,'observation':'Actual Original HTTP'if MODE=='http'else'Actual Original APIRequestFactory/observer-connectionSQL'}
 if MODE=='http':
  client=http.client.HTTPConnection('127.0.0.1',5656,timeout=15)
  try:client.request(method,path,headers=headers);response=client.getresponse();body=response.read(2*1024*1024+1);assert len(body)<=2*1024*1024;outcome={'status':response.status,'content_type':response.getheader('Content-Type'),'body_bytes':len(body),'body_sha256':hashlib.sha256(body).hexdigest(),'headers':{key:response.getheader(key)for key in ['Allow','Vary','Content-Length','WWW-Authenticate']}}
  finally:client.close()
 else:
  request=factory.generic(method,path,HTTP_HOST='localhost',HTTP_ACCEPT='application/json',HTTP_AUTHORIZATION=headers['Authorization'],REMOTE_ADDR='127.0.0.1');match=resolve('/api/vod/all/');request.resolver_match=match
  with CaptureQueriesContext(connection)as captured:
   try:response=match.func(request,*match.args,**match.kwargs)
   except Exception as error:outcome={'view_unhandled':type(error).__name__,'message':str(error)};body=None
   else:
    outcome={'status_before_render':response.status_code}
    try:response.render();body=response.content;outcome.update(status=response.status_code,content_type=response.get('Content-Type'),body_bytes=len(body),body_sha256=hashlib.sha256(body).hexdigest())
    except Exception as error:outcome.update(renderer_unhandled=type(error).__name__,message=str(error));body=None
    response.close()
  record['actual_unified_SQL_count']=sum(isinstance(row.get('sql'),str)and('WITH unified_content'in row['sql']or'SELECT COUNT(*) FROM ('in row['sql'])for row in captured.captured_queries)
 if body:
  try:outcome['json']=json.loads(body)
  except(ValueError,UnicodeDecodeError):outcome['body_text']=body.decode('utf-8',errors='replace')
 record['expected']=outcome;assert fingerprint()==previous;records.append(record)
try:
 for value in [None,False,0,0.0,'',[],{},True,1,1.5,'x',['x']]:
  User.objects.filter(pk=user.pk).update(custom_properties=value);observe({}, {'user_properties':value});observe({'page':'bad'},{'user_properties_before_badpage':value},'HEAD')
 User.objects.filter(pk=user.pk).update(custom_properties=saved)
 for value in ['a'*199,'a'*200,'a'*201,'a'*1000,'é'*201,'😀'*201,'\x00'*201,'\u200b'*201,'9'*4300,'9'*4301,'١'*4301,'  +'+'9_'*4300+'9  ']:
  observe({'page':value},{'page_raw':value,'raw_characters':len(value)})
 for pairs in [[('page','bad'),('page_size','bad')],[('page','9'*4301),('page_size','bad')],[('page','bad'),('page_size','9'*4301)]]:observe(pairs,{'error_priority':pairs})
 User.objects.filter(pk=user.pk).update(custom_properties={'vod_movies_enabled':False,'vod_series_enabled':False});observe({'page':'9'*4301,'page_size':'bad'},{'both_disabled_earlyreturn':True})
finally:User.objects.filter(pk=user.pk).update(custom_properties=saved)
assert fingerprint()==before
print(json.dumps({'passed':True,'scope':'Original long-int/error-priority/nonmapping-user-property observations; no native parity','mode':MODE,'records':records,'full_datetime13table_fingerprints_before':before,'full_datetime13table_fingerprints_after':fingerprint(),'owned_rows_restored':True,'provider_guard':'Allprovider URLs ownedloopback; noaction endpoint orsave signal, query-only unified view','native_verified':False},cls=ExactEncoder,ensure_ascii=False))
