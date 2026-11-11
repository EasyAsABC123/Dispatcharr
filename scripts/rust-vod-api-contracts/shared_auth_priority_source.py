"""Bounded Original-only shared-auth priority matrix, GET-only endpoints.
Only ownedfixture User props and Redis settings cache are temporarily injected
and restored; no persisted policy/runtime patch or provider action. ActualTCP
invalid-forwarded headers are distinct from Factory invalidREMOTE_ADDR.
Original Dispatcharr authors/contributors retain credit and AGPL licensing.
"""
import datetime,hashlib,http.client,json,os,sys,time
if os.environ.get('POSTGRES_PASSWORD')!='playback-fixture-only'or os.environ.get('VOD_API_DISPOSABLE_FIXTURE')!='playback-fixture-only':raise RuntimeError('Owned disposable fixture required')
sys.path.insert(0,'/app');os.environ.setdefault('DJANGO_SETTINGS_MODULE','dispatcharr.settings')
import django
django.setup()
from django.core.cache import cache
from django.core.serializers.json import DjangoJSONEncoder
from django.urls import resolve
from rest_framework.test import APIRequestFactory
from rest_framework_simplejwt.tokens import AccessToken
from apps.accounts.models import User
from apps.m3u.models import M3UAccount,M3UAccountProfile
from apps.vod.models import Movie,Series,Episode,VODLogo,VODCategory,M3UMovieRelation,M3USeriesRelation,M3UEpisodeRelation,M3UVODCategoryRelation
from core.models import CoreSettings,SystemEvent
from dispatcharr.utils import get_client_ip
import redis
assert os.environ.get('REDIS_PASSWORD')=='playback-fixture-only'
prefix='rust-probe-vod-rest-';roles=['basic','standard','admin'];users={key:User.objects.get(username=prefix+key)for key in roles};assert[users[key].user_level for key in roles]==[0,1,10]
assert M3UAccount.objects.filter(name__startswith=prefix).count()==3 and not M3UAccount.objects.filter(name__startswith=prefix).exclude(server_url__startswith='http://127.0.0.1:').exists()
saved={key:user.custom_properties for key,user in users.items()};models=[User,M3UAccount,M3UAccountProfile,Movie,Series,Episode,VODLogo,VODCategory,M3UMovieRelation,M3USeriesRelation,M3UEpisodeRelation,M3UVODCategoryRelation,CoreSettings,SystemEvent]
class ExactEncoder(DjangoJSONEncoder):
 def default(self,value):
  if isinstance(value,(datetime.datetime,datetime.date)):return value.isoformat()
  return super().default(value)
def fingerprint():return {m._meta.db_table:hashlib.sha256(json.dumps(list(m.objects.order_by('pk').values()),sort_keys=True,separators=(',',':'),cls=ExactEncoder).encode()).hexdigest()for m in models}
# Prime only owned cache state, then snapshot exact raw Redis values and TTLs.
CoreSettings.get_network_access_settings();client=redis.Redis(host='127.0.0.1',port=6379,password='playback-fixture-only')
keys=[cache.make_key(CoreSettings.group_cache_key('network_access')),cache.make_key(CoreSettings.group_cache_ver_key('network_access'))];snapshot={key:{'dump':client.dump(key),'ttl':client.pttl(key),'at':time.monotonic()}for key in keys}
before=fingerprint();records=[];factory=APIRequestFactory();endpoints=['/api/vod/movies/','/api/vod/all/','/api/core/system-events/?limit=1'];properties=[None,False,0,'',[],{},True,1,1.5,'x',['x']]
selected=['Allow','Vary','Content-Length','WWW-Authenticate','X-Content-Type-Options','X-Frame-Options','Referrer-Policy','Cross-Origin-Opener-Policy']
def observe(role,props,profile,path,factory_invalid=False):
 if len(records)>=400:raise RuntimeError('Priority matrix bound')
 headers={'Host':'localhost','Accept':'application/json','Authorization':'Bearer '+str(AccessToken.for_user(users[role]))}
 if profile=='invalid-forwarded':headers.update({'X-Real-IP':'not-an-ip','X-Forwarded-For':'not-an-ip'})
 previous=fingerprint();outcome={};body=None
 if factory_invalid:
  request=factory.get(path,REMOTE_ADDR='not-an-ip',HTTP_HOST='localhost',HTTP_ACCEPT='application/json',HTTP_AUTHORIZATION=headers['Authorization']);match=resolve(path.split('?')[0]);request.resolver_match=match
  try:response=match.func(request,*match.args,**match.kwargs);response.render();body=response.content;outcome={'status':response.status_code,'content_type':response.get('Content-Type'),'headers':{key:response.get(key)for key in selected}};response.close()
  except Exception as error:outcome={'unhandled':type(error).__name__,'message':str(error)}
  observation='Actual APIRequestFactory invalidREMOTE_ADDR; NOT SourceNetwork'
 else:
  connection=http.client.HTTPConnection('127.0.0.1',5656,timeout=10)
  try:connection.request('GET',path,headers=headers);response=connection.getresponse();body=response.read(2*1024*1024+1);assert len(body)<=2*1024*1024;outcome={'status':response.status,'content_type':response.getheader('Content-Type'),'headers':{key:response.getheader(key)for key in selected}}
  finally:connection.close()
  observation='Actual Original TCP HTTP, GET only'
 if body is not None:
  outcome.update(body_bytes=len(body),body_sha256=hashlib.sha256(body).hexdigest())
  try:outcome['json']=json.loads(body)
  except(ValueError,UnicodeDecodeError):outcome['body_text']=body.decode('utf-8',errors='replace')
 assert fingerprint()==previous,'GET changed owned SQL rows'
 records.append({'role':role,'user_level':users[role].user_level,'custom_properties':props,'global_profile':profile,'path':path,'observation':observation,'expected':outcome})
try:
 for profile in ['allow','deny','invalid-forwarded']:
  policy={'UI':'192.0.2.0/24'if profile=='deny'else'0.0.0.0/0,::/0'};cache.set(CoreSettings.group_cache_key('network_access'),policy,timeout=120);assert CoreSettings.get_network_access_settings()==policy
  for role in roles:
   for props in properties:
    User.objects.filter(pk=users[role].id).update(custom_properties=props)
    for path in endpoints:observe(role,props,profile,path)
    User.objects.filter(pk=users[role].id).update(custom_properties=saved[role])
 cache.set(CoreSettings.group_cache_key('network_access'),{'UI':'0.0.0.0/0,::/0'},timeout=120)
 for role in roles:
  for props in properties:
   User.objects.filter(pk=users[role].id).update(custom_properties=props)
   for path in endpoints:observe(role,props,'invalid-REMOTE_ADDR',path,True)
   User.objects.filter(pk=users[role].id).update(custom_properties=saved[role])
finally:
 for role in roles:User.objects.filter(pk=users[role].id).update(custom_properties=saved[role])
 for key,value in snapshot.items():
  if value['dump']is None:client.delete(key)
  else:
   ttl=0 if value['ttl']==-1 else max(1,value['ttl']-int((time.monotonic()-value['at'])*1000));client.restore(key,ttl,value['dump'],replace=True)
assert fingerprint()==before
for key,value in snapshot.items():assert client.dump(key)==value['dump']
assert len(records)==396
print(json.dumps({'passed':True,'scope':'Actual Original sharednetwork/auth/permission priority,297realHTTP+99Factory invalidsocketpeer; no native acceptance','records':records,'SQL_fullmicrosecond14table_fingerprints_before':before,'SQL_fullmicrosecond14table_fingerprints_after':fingerprint(),'owned_SQL_rows_restored':True,'owned_cache_values_restored':True,'cache_TTL_scope':'OriginalremainingTTL minuselapsed; permanent stayspermanent; no wildcard/cacheflush','endpoint_methods':'GET only, no endpointmutation/provider/media/refresh','invalid_IP_scope':'Malformedforwardedheaders overTCP fallbackto validpeer; invalidREMOTE_ADDR separatelyFactory only','native_verified':False,'source_sha256':{path:hashlib.sha256(open('/app/'+path,'rb').read()).hexdigest()for path in ['dispatcharr/utils.py','apps/accounts/permissions.py','apps/vod/api_views.py']}} ,cls=ExactEncoder,ensure_ascii=False))
