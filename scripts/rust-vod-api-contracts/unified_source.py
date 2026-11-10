"""Actual Original unified HTTP/Factory SQL/type/error observations.
Only guarded disposable rows are mutated, always restored. No provider action.
Original Dispatcharr authors retain application credit and AGPL licensing.
"""
import hashlib,http.client,json,os,sys,threading
from http.server import BaseHTTPRequestHandler,HTTPServer
from urllib.parse import urlsplit
from urllib.parse import quote,urlencode
if os.environ.get('POSTGRES_PASSWORD')!='playback-fixture-only' or os.environ.get('VOD_API_DISPOSABLE_FIXTURE')!='playback-fixture-only':raise RuntimeError('Owned VOD fixture required')
TARGET=os.environ.get('VOD_UNIFIED_TARGET','Original')
if TARGET not in ['Original','candidate']:raise RuntimeError('Explicit owned target only')
MODE=os.environ.get('VOD_UNIFIED_OBSERVATION')
if MODE not in ['factory','http']:raise RuntimeError('Explicit unified observation mode required')
if TARGET=='candidate'and MODE!='http':raise RuntimeError('Candidate requires actual HTTP mode')
sys.path.insert(0,'/app');os.environ.setdefault('DJANGO_SETTINGS_MODULE','dispatcharr.settings')
import django
django.setup()
from django.core.serializers.json import DjangoJSONEncoder
from django.db import connection
from django.test.utils import CaptureQueriesContext
from django.urls import resolve
from rest_framework.test import APIRequestFactory
from rest_framework_simplejwt.tokens import AccessToken
from apps.accounts.models import User
from apps.m3u.models import M3UAccount,M3UAccountProfile
from apps.vod.models import Movie,Series,Episode,VODLogo,VODCategory,M3UMovieRelation,M3USeriesRelation,M3UEpisodeRelation,M3UVODCategoryRelation
from core.models import CoreSettings
PREFIX='rust-probe-vod-rest-';roles=['admin','standard','basic','inactive','adult_hidden','movies_off','series_off','both_off','zero_flags','admin_movies_off'];users={role:User.objects.get(username=PREFIX+role)for role in roles}
assert Movie.objects.filter(name__startswith=PREFIX).exists()and Series.objects.filter(name__startswith=PREFIX).exists()
assert not M3UAccount.objects.filter(name__startswith=PREFIX).exclude(server_url__startswith='http://127.0.0.1:').exists()
ports={urlsplit(value).port for value in M3UAccount.objects.filter(name__startswith=PREFIX).values_list('server_url',flat=True)}
assert len(ports)==1 and None not in ports
provider_attempts=[]
class ProviderTrap(BaseHTTPRequestHandler):
 def deny(self):
  provider_attempts.append({'method':self.command,'path':self.path[:1024]});self.send_response(503);self.send_header('Content-Length','0');self.end_headers()
 do_GET=deny;do_POST=deny;do_HEAD=deny
 def log_message(self,*args):pass
provider_trap=HTTPServer(('127.0.0.1',next(iter(ports))),ProviderTrap)
provider_thread=threading.Thread(target=provider_trap.serve_forever,daemon=True);provider_thread.start()
MODELS=[User,M3UAccount,M3UAccountProfile,Movie,Series,Episode,VODLogo,VODCategory,M3UMovieRelation,M3USeriesRelation,M3UEpisodeRelation,M3UVODCategoryRelation,CoreSettings]
def fingerprint():
 result={}
 for model in MODELS:
  rows=list(model.objects.order_by('pk').values());raw=json.dumps(rows,cls=DjangoJSONEncoder,sort_keys=True,separators=(',',':')).encode();result[model._meta.db_table]={'rows':len(rows),'sha256':hashlib.sha256(raw).hexdigest()}
 return result
before=fingerprint();records=[];factory=APIRequestFactory();headers_selected=['Content-Length','Allow','Vary','WWW-Authenticate','X-Content-Type-Options','X-Frame-Options','Referrer-Policy','Cross-Origin-Opener-Policy']
def observe(path='/api/vod/all/',role='standard',method='GET',headers=None,label=None):
 if len(records)>=450:raise RuntimeError('Unified case bound')
 path=quote(path,safe='/?=&|,%.+-_~');given={'Host':'localhost','Accept':'application/json'}
 if role is not None:given['Authorization']='Bearer '+str(AccessToken.for_user(users[role]))
 given.update(headers or {})
 record={'path':path,'role':role,'method':method,'headers':{k:v for k,v in given.items()if k!='Authorization'},'label':label,'observation':('actual Original HTTP'if TARGET=='Original'else'actual packaged native HTTP')if MODE=='http'else'actual APIRequestFactory and observer-connection SQL'}
 previous=fingerprint()
 if MODE=='http':
  def network(host,port):
   client=http.client.HTTPConnection(host,port,timeout=15)
   try:
    client.request(method,path,body=b'{}'if method not in ['GET','HEAD']else None,headers=given);response=client.getresponse();body=response.read(2*1024*1024+1);assert len(body)<=2*1024*1024
    outcome={'status':response.status,'body_bytes':len(body),'body_sha256':hashlib.sha256(body).hexdigest(),'content_type':response.getheader('Content-Type'),'selected_headers':{name:response.getheader(name)for name in headers_selected}}
    try:outcome['json']=json.loads(body)
    except(ValueError,UnicodeDecodeError):pass
    return outcome,body
   finally:client.close()
  if TARGET=='candidate'and label=='cross-kind LOWER(name),id tie':
   # Fixed six predeclared label/path cases, never selected from differences.
   policy=json.load(open('/vod-api-probe/unified-tie-policy.json'));assert path in policy['paths']and role=='standard'and method=='GET'
   with connection.cursor()as cursor:
    cursor.execute("SELECT EXISTS(SELECT 1 FROM vod_movie m JOIN vod_series s ON m.id=s.id AND LOWER(m.name)=LOWER(s.name)WHERE m.name LIKE 'rust-probe-vod-rest-%%')");assert cursor.fetchone()[0]
   source_now,source_bytes=network('127.0.0.1',5656);assert source_now['status']==200;record['fresh_Original_HTTP']=source_now;record['fresh_Original_raw_body']=source_bytes.decode('utf-8')
   assert fingerprint()==previous,'Source authority request changed owned data'
  outcome,body=network('127.0.0.1',5656)if TARGET=='Original'else network('candidate',9192)

 else:
  extra={'HTTP_'+k.upper().replace('-','_'):v for k,v in given.items()};request=factory.generic(method,path,data=b'{}',content_type='application/json',REMOTE_ADDR='127.0.0.1',**extra);match=resolve(path.split('?')[0]);request.resolver_match=match
  with CaptureQueriesContext(connection)as captured:
   try:response=match.func(request,*match.args,**match.kwargs)
   except Exception as error:outcome={'view_unhandled':type(error).__name__,'message':str(error)};body=None
   else:
    outcome={'view_status_before_render':response.status_code}
    try:response.render();body=response.content;assert len(body)<=2*1024*1024;outcome.update(status=response.status_code,body_bytes=len(body),body_sha256=hashlib.sha256(body).hexdigest(),content_type=response.get('Content-Type'),selected_headers={name:response.get(name)for name in headers_selected})
    except Exception as error:outcome.update(renderer_unhandled=type(error).__name__,message=str(error));body=None
    response.close()
  sql=[q['sql']for q in captured.captured_queries if isinstance(q.get('sql'),str)and('WITH unified_content'in q['sql']or'SELECT COUNT(*) FROM ('in q['sql'])];assert len(sql)<=2;record['actual_page_then_count_SQL']=sql;record['unrenderable_debug_SQL_entries']=sum(q.get('sql')is None for q in captured.captured_queries)
 if body is not None:
  try:outcome['json']=json.loads(body)
  except(ValueError,UnicodeDecodeError):outcome['body_text']=body.decode('utf-8',errors='replace')
 record['expected']=outcome;record['effects_unchanged']=fingerprint()==previous;assert record['effects_unchanged'];records.append(record)
for role in [None,*roles]:observe(role=role)
for auth_index,value in enumerate(['Bearer invalid','Bearer','Bearer a b','ApiKey invalid','Basic dGVzdDph','']):
 observe(headers={'Authorization':value},label={'presented_auth_scheme':value.split(' ',1)[0],'presented_auth_case':auth_index})
for query in ['page=bad','page_size=-1','page_size=0','page=2&page_size=1']:
 observe('/api/vod/all/?'+query,method='HEAD',label='HEADwire')
queries=['page=1','page=2','page=last','page=','page=0','page=-1','page=bad','page=99999','page=+2','page=%2B2','page=1_0','page=%D9%A1','page=1.0','page=1e0','page=2&page=1','page=1&page=2','page_size=1','page_size=24','page_size=25','page_size=101','page_size=0','page_size=-1','page_size=bad','page_size=','page=0&page_size=0','page=-1&page_size=-1','page_size=1&page_size=2','page_size=999999999999999999999999999999999','page=999999999999999999999999999999999','search=space','search=Space,drama','search=%25','search=_','search=','search=SPACE&ordering=-name','search=Drama%5C%20News','search=%C4%B0','search=%CE%A3%CE%9F%CE%A3','search=%00','search=a&search=SPACE','search=SPACE&search=a','page_size=1&z=&z=2&a=%2B%20~','ordering=-year,name','ordering=bad','year=bad','m3u_account=bad','is_adult=false','category_type=movie','name=Alpha']
for query in queries:observe('/api/vod/all/?'+query)
for value in [PREFIX+'Drama',PREFIX+'Drama|movie',PREFIX+'Drama|series',PREFIX+'Drama|invalid',PREFIX+'News|Live|movie','|movie',' movie ','','Uncategorized']:
 observe('/api/vod/all/?'+urlencode({'category':value}))
for role in ['both_off','movies_off','series_off','adult_hidden','admin_movies_off']:
 for query in ['page=bad','page_size=-1','page_size=1','category='+PREFIX+'Drama|movie','category='+PREFIX+'Drama|series']:observe('/api/vod/all/?'+query,role=role)
for method in ['HEAD','OPTIONS','POST','PUT','PATCH','DELETE']:
 for role in [None,'basic','standard','admin']:observe(role=role,method=method)
for accept in ['application/xml','application/json;indent=4','application/json;indent=8','application/json;q=0','text/html','*/*']:
 observe(headers={'Accept':accept},label='negotiation')
for path in ['/api/vod/all/?format=json','/api/vod/all/?format=api','/api/vod/all/?format=bad','/api/vod/all/1/','/api/vod/all/bad/','/api/vod/all/999999999999999999999999999999999/']:
 for role in ['standard','admin',None]:observe(path,role=role)
 if path.endswith('format=api'):
  for role in ['standard','admin',None]:observe(path,role=role,method='HEAD',label='Source format-api JSON HEAD negotiation')
movie=Movie.objects.filter(name__startswith=PREFIX).order_by('id').first();series=Series.objects.filter(name__startswith=PREFIX).order_by('id').first();saved_movie={key:getattr(movie,key)for key in ['rating','description','genre','custom_properties']};saved_series_name=series.name
try:
 for rating in [None,'','0','0.0','-0','1e-7','1e16','1e309','1e-999','NaN','inf','-Infinity','bad','1,5',' 1_2 ','١٢.٥']:
  assert rating is None or len(rating)<=10;Movie.objects.filter(pk=movie.pk).update(rating=rating);observe(label={'rating':rating})
  if rating in ['bad','NaN','inf']:observe(method='HEAD',label={'rating_HEAD':rating})
 Movie.objects.filter(pk=movie.pk).update(**saved_movie)
 for props in [None,False,0,'',[],{},['x'],'x',{'x':1e-7}]:
  Movie.objects.filter(pk=movie.pk).update(custom_properties=props);observe(label={'properties':props})
 Movie.objects.filter(pk=movie.pk).update(**saved_movie)
 Series.objects.filter(pk=series.pk).update(name=movie.name)
 for query in ['page_size=1','page_size=1&page=2','page_size=2','page_size=3','page_size=1&page=3','page_size=101']:observe('/api/vod/all/?'+query,label='cross-kind LOWER(name),id tie')
finally:
 Movie.objects.filter(pk=movie.pk).update(**saved_movie);Series.objects.filter(pk=series.pk).update(name=saved_series_name)
assert fingerprint()==before
provider_trap.shutdown();provider_trap.server_close();provider_thread.join(timeout=2)
assert not provider_attempts,'Unexpected provider/network action'
with connection.cursor()as cursor:
 cursor.execute('SELECT current_database()');actual_database=cursor.fetchone()[0]
database_witness={'configured_database':connection.settings_dict['NAME'],'actual_database':actual_database,'scope':'Source helper SQL connection; HTTP same-fixture sentinel binding is a separate native test'}
print(json.dumps({'passed':True,'scope':'Actual Original unified HTTP/Factory corpus'if TARGET=='Original'else'Actual packaged nativeHTTP observations using guarded Original SQL fixture controller; comparison/app acceptance separate','observation':MODE,'target':TARGET,'database_witness':database_witness,'records':records,'fingerprints_before':before,'fingerprints_after':fingerprint(),'exact_owned_rows_restored':True,'provider_trap_attempts':provider_attempts,'provider_refresh_or_remote_actions_called':False,'provider_guard':'All owned provider URLs are loopback; only unified/list/retrieve/method requests, no action endpoint. Fingerprints prove refresh columns unchanged.','source_files':{p:hashlib.sha256(open('/app/'+p,'rb').read()).hexdigest()for p in ['apps/vod/api_views.py','apps/vod/serializers.py','apps/vod/utils.py','apps/accounts/permissions.py']},'native_verified':False,'performance_verified':False},ensure_ascii=False,cls=DjangoJSONEncoder))
