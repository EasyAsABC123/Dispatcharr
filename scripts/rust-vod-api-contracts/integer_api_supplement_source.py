"""UNEXECUTED guarded Original API integer/error-phase supplement.
Uses read-only execute_wrapper observation, not debug-cursor SQL formatting,
which itself could stringify huge ints and perturb the error phase. Factory
and actual network observations stay distinct. Original authors retain credit.
"""
import datetime,hashlib,http.client,json,os,sys
from pathlib import Path
from urllib.parse import urlencode
if os.environ.get('POSTGRES_PASSWORD')!='playback-fixture-only'or os.environ.get('VOD_API_DISPOSABLE_FIXTURE')!='playback-fixture-only':raise RuntimeError('Owned disposable fixture required')
mode=os.environ.get('VOD_UNIFIED_EDGE_MODE');assert mode in ['factory','http']
spec=json.loads(Path('/vod-api-probe/supplement-api-spec.json').read_text());assert len(spec['cases'])<=100
sys.path.insert(0,'/app');os.environ.setdefault('DJANGO_SETTINGS_MODULE','dispatcharr.settings')
import django
django.setup()
from django.db import connection
from django.urls import resolve
from django.core.serializers.json import DjangoJSONEncoder
from rest_framework.test import APIRequestFactory
from rest_framework_simplejwt.tokens import AccessToken
from apps.accounts.models import User
from apps.m3u.models import M3UAccount,M3UAccountProfile
from apps.vod.models import Movie,Series,Episode,VODLogo,VODCategory,M3UMovieRelation,M3USeriesRelation,M3UEpisodeRelation,M3UVODCategoryRelation
from core.models import CoreSettings,SystemEvent
prefix='rust-probe-vod-rest-';user=User.objects.get(username=prefix+'standard');assert user.is_active and user.user_level==1
assert M3UAccount.objects.filter(name__startswith=prefix).count()==3 and not M3UAccount.objects.filter(name__startswith=prefix).exclude(server_url__startswith='http://127.0.0.1:').exists()
class Encoder(DjangoJSONEncoder):
 def default(self,value):
  if isinstance(value,(datetime.datetime,datetime.date)):return value.isoformat()
  return super().default(value)
models=[User,M3UAccount,M3UAccountProfile,Movie,Series,Episode,VODLogo,VODCategory,M3UMovieRelation,M3USeriesRelation,M3UEpisodeRelation,M3UVODCategoryRelation,CoreSettings,SystemEvent]
def fingerprint():return {m._meta.db_table:hashlib.sha256(json.dumps(list(m.objects.order_by('pk').values()),sort_keys=True,separators=(',',':'),cls=Encoder).encode()).hexdigest()for m in models}
before=fingerprint();saved=user.custom_properties;factory=APIRequestFactory();records=[]
try:
 for entry in spec['cases']:
  User.objects.filter(pk=user.pk).update(custom_properties=entry.get('properties',saved))
  path='/api/vod/all/?'+urlencode([tuple(pair)for pair in entry['query_pairs']]);assert len(path)<60000
  token='Bearer '+str(AccessToken.for_user(user));body=None;calls=[]
  if mode=='http':
   client=http.client.HTTPConnection('127.0.0.1',5656,timeout=15)
   try:client.request(entry['method'],path,headers={'Host':'localhost','Accept':'application/json','Authorization':token});response=client.getresponse();body=response.read(2*1024*1024+1);assert len(body)<=2*1024*1024;outcome={'status':response.status,'content_type':response.getheader('Content-Type'),'headers':{key:response.getheader(key)for key in ['Allow','Vary','Content-Length','WWW-Authenticate']}}
   finally:client.close()
  else:
   def wrapper(execute,sql,params,many,context):
    phase='page'if isinstance(sql,str)and'WITH unified_content'in sql else'count'if isinstance(sql,str)and'SELECT COUNT(*) FROM ('in sql else'other'
    if phase!='other':calls.append({'phase':phase,'params':[{'type':type(value).__name__,'integer_bits':value.bit_length()if isinstance(value,int)else None}for value in(params or[])]})
    return execute(sql,params,many,context)
   request=factory.generic(entry['method'],path,HTTP_HOST='localhost',HTTP_ACCEPT='application/json',HTTP_AUTHORIZATION=token,REMOTE_ADDR='127.0.0.1');match=resolve('/api/vod/all/');request.resolver_match=match
   with connection.execute_wrapper(wrapper):
    try:response=match.func(request,*match.args,**match.kwargs)
    except Exception as error:outcome={'view_unhandled':type(error).__name__,'message':str(error)}
    else:
     outcome={'status_before_render':response.status_code}
     try:response.render();body=response.content;outcome.update(status=response.status_code,content_type=response.get('Content-Type'))
     except Exception as error:outcome.update(renderer_unhandled=type(error).__name__,message=str(error))
     response.close()
  if body is not None:
   outcome.update(body_bytes=len(body),body_sha256=hashlib.sha256(body).hexdigest())
   try:outcome['json']=json.loads(body)
   except(ValueError,UnicodeDecodeError):outcome['body_text']=body.decode('utf-8',errors='replace')
  records.append({'case':entry,'observation':'Actual Original TCP HTTP'if mode=='http'else'Original APIRequestFactory +execute_wrapper,notSourceNetwork','expected':outcome,'Factory_execute_calls':calls if mode=='factory'else None,'Factory_debug_cursor_forced':False})
finally:User.objects.filter(pk=user.pk).update(custom_properties=saved)
assert fingerprint()==before
print(json.dumps({'passed_observer':True,'mode':mode,'scope':'Predeclared37 supplemental Originalinteger/error-phase cases; no nativeacceptance','records':records,'fullmicro14table_fingerprints_before':before,'fullmicro14table_fingerprints_after':fingerprint(),'owned_rows_restored':True,'provider_refresh_or_remote_action_called':False,'source_file_sha256':hashlib.sha256(Path('/app/apps/vod/api_views.py').read_bytes()).hexdigest()},cls=Encoder,ensure_ascii=False))
