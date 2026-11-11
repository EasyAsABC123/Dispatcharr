"""Actual Original image/cache oracle; private fixture origin only.
Metadata/provider refresh is never requested by this helper.
All application and algorithm credit belongs to original Dispatcharr authors.
"""
import datetime,hashlib,http.client,json,os,sys,threading,base64,socket,ipaddress,uuid,concurrent.futures
from pathlib import Path
from http.server import BaseHTTPRequestHandler,HTTPServer
from urllib.parse import quote
if os.environ.get('POSTGRES_PASSWORD')!='playback-fixture-only'or os.environ.get('VOD_API_DISPOSABLE_FIXTURE')!='playback-fixture-only':raise RuntimeError('Owned fixture required')
sys.path.insert(0,'/app');os.environ.setdefault('DJANGO_SETTINGS_MODULE','dispatcharr.settings')
import django
django.setup()
from django.core.serializers.json import DjangoJSONEncoder
from django.utils import timezone
from rest_framework_simplejwt.tokens import AccessToken
from apps.accounts.models import User
from apps.m3u.models import M3UAccount,M3UAccountProfile,M3UFilter
from apps.vod.models import Movie,Series,Episode,VODLogo,VODCategory,M3UMovieRelation,M3USeriesRelation,M3UEpisodeRelation,M3UVODCategoryRelation
from apps.channels.models import ChannelGroup,ChannelGroupM3UAccount,Stream
from core.models import CoreSettings,SystemEvent
class ExactEncoder(DjangoJSONEncoder):
 def default(self,value):
  if isinstance(value,(datetime.datetime,datetime.date,datetime.time)):return value.isoformat()
  return super().default(value)
prefix='rust-probe-vod-rest-';users={role:User.objects.get(username=prefix+role)for role in ['admin','standard','basic','movies_off','series_off','adult_hidden']};accounts=list(M3UAccount.objects.filter(name__startswith=prefix).order_by('id'));assert len(accounts)==3 and all(a.server_url.startswith('http://127.0.0.1:')for a in accounts)
movie=Movie.objects.filter(name__startswith=prefix).order_by('id').first();show=Series.objects.filter(name__startswith=prefix).order_by('id').first();movie_relations=list(M3UMovieRelation.objects.filter(movie=movie).order_by('id'));series_relation=M3USeriesRelation.objects.get(series=show);primary=movie_relations[0];models=[User,M3UAccount,M3UAccountProfile,Movie,Series,Episode,VODLogo,VODCategory,M3UMovieRelation,M3USeriesRelation,M3UEpisodeRelation,M3UVODCategoryRelation,CoreSettings,SystemEvent,M3UFilter,ChannelGroup,ChannelGroupM3UAccount,Stream]
def fingerprint():return {m._meta.db_table:hashlib.sha256(json.dumps(list(m.objects.order_by('id').values()),sort_keys=True,separators=(',',':'),cls=ExactEncoder).encode()).hexdigest()for m in models}

before=fingerprint();nonce=uuid.uuid4().hex;requests=[];records=[]
address=socket.gethostbyname(socket.gethostname());parsed=ipaddress.ip_address(address)
assert parsed.is_private and not parsed.is_loopback and not parsed.is_link_local, address
png=base64.b64decode('iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+/l9sAAAAASUVORK5CYII=')
svg=b'<svg xmlns="http://www.w3.org/2000/svg" width="1" height="1"><rect width="1" height="1"/></svg>'
class Origin(BaseHTTPRequestHandler):
 def do_GET(self):
  requests.append({'method':self.command,'path':self.path,'user_agent':self.headers.get('User-Agent')})
  route=self.path.split('?',1)[0]
  if route in ['/redirect','/loop','/blocked']:
   self.send_response(302);self.send_header('Location', '/ok.png' if route=='/redirect' else '/loop' if route=='/loop' else 'http://127.0.0.1:1/blocked');self.send_header('Content-Length','0');self.end_headers();return
  data=svg if route=='/ok.svg'else b'<html>not an image</html>'if route=='/html'else png
  if route=='/large':data=b'\x89PNG\r\n\x1a\n'+b'x'*(5*1024*1024)
  self.send_response(404 if route=='/missing'else 200);self.send_header('Content-Length',str(len(data)));self.send_header('Content-Type','text/plain');self.send_header('Cache-Control','public, max-age=77');self.send_header('Last-Modified','Mon, 01 Jan 2024 00:00:00 GMT');self.end_headers()
  try:self.wfile.write(data)
  except (BrokenPipeError,ConnectionResetError):pass
 def log_message(self,*args):pass
origin=HTTPServer(('0.0.0.0',0),Origin);thread=threading.Thread(target=origin.serve_forever,daemon=True);thread.start();origin_base=f'http://{address}:{origin.server_port}'
def url(route):return origin_base+route+'?fixture='+nonce
objects={'movies':movie,'series':show,'episodes':Episode.objects.filter(series=show).order_by('id').first(),'vodlogos':VODLogo.objects.get(pk=movie.logo_id)};assert all(objects.values())
saved=[]
for obj in objects.values():
 fields=['url']if isinstance(obj,VODLogo)else['custom_properties'];saved.append((type(obj),obj.pk,{field:getattr(obj,field)for field in fields}))
relations=[*movie_relations,series_relation,*M3UEpisodeRelation.objects.filter(episode=objects['episodes'])]
for rel in relations:saved.append((type(rel),rel.pk,{'custom_properties':rel.custom_properties}))
local_root=Path('/data/logos');local_root.mkdir(exist_ok=True);local=local_root/('rust-probe-vod-image-'+nonce+'.svg');local_png=local.with_suffix('.png');local_unknown=local.with_suffix('.unknown');assert all(not path.exists()for path in [local,local_png,local_unknown])
native_port=int(os.environ['VOD_IMAGE_NATIVE_PORT'])if os.environ.get('VOD_IMAGE_NATIVE_PORT')else None
def case(resource,query='',role='standard',method='GET',accept='image/*',label='baseline',extra=None,path_override=None):
 assert len(records)<250
 obj=objects[resource];action='cache'if resource=='vodlogos'else'image';path=path_override or f'/api/vod/{resource}/{obj.pk}/{action}/'+query;headers={'Host':'localhost','Accept':accept,**(extra or{})}
 if role:headers['Authorization']='Bearer '+str(AccessToken.for_user(users[role]))
 sql_before=fingerprint();count=len(requests);client=http.client.HTTPConnection('127.0.0.1',5656,timeout=20)
 try:
  client.request(method,quote(path,safe='/?=&|,%'),headers=headers);response=client.getresponse();data=response.read(5*1024*1024+1);assert len(data)<=5*1024*1024
  selected={key:response.getheader(key)for key in ['Content-Type','Content-Length','Allow','Vary','WWW-Authenticate','Cache-Control','Last-Modified','Content-Disposition','X-Content-Type-Options','Content-Security-Policy','ETag']}
  records.append({'path':path,'method':method,'role':role,'accept':accept,'label':label,'request_extra':extra or{},'expected':{'status':response.status,'headers':selected,'body_base64':base64.b64encode(data).decode(),'body_sha256':hashlib.sha256(data).hexdigest(),'body_bytes':len(data)},'origin_request_count_before':count,'origin_request_count_after':len(requests),'observation':'actual Original HTTP, unpatched views/SSRF/image proxy'})
 finally:client.close()
 assert fingerprint()==sql_before,'Original image GET/method changed SQL state'
 if native_port:
  client=http.client.HTTPConnection('127.0.0.1',native_port,timeout=20);native_before=len(requests)
  try:
   client.request(method,quote(path,safe='/?=&|,%'),headers=headers);response=client.getresponse();data=response.read(5*1024*1024+1);assert len(data)<=5*1024*1024
   actual={'status':response.status,'headers':{key:response.getheader(key)for key in records[-1]['expected']['headers']},'body_base64':base64.b64encode(data).decode(),'body_sha256':hashlib.sha256(data).hexdigest(),'body_bytes':len(data)}
  finally:client.close()
  records[-1]['actual_native']=actual;records[-1]['native_origin_requests']=len(requests)-native_before;records[-1]['differences']=[key for key in actual if actual[key]!=records[-1]['expected'][key]]
  if records[-1]['native_origin_requests']!=records[-1]['origin_request_count_after']-records[-1]['origin_request_count_before']:records[-1]['differences'].append('origin_request_count')
  assert fingerprint()==sql_before,'Native image GET/method changed SQL state'
def relation_art(route):
 for rel in relations:type(rel).objects.filter(pk=rel.pk).update(custom_properties={'info':{'movie_image':url(route),'backdrop_path':[url(route)]}})
try:

 for path,data in [(local,svg),(local_png,png),(local_unknown,png)]:path.write_bytes(data);os.utime(path,(1704067200,1704067200))
 for resource,obj in objects.items():
  if resource=='vodlogos':type(obj).objects.filter(pk=obj.pk).update(url=url('/ok.png'))
  else:type(obj).objects.filter(pk=obj.pk).update(custom_properties={'movie_image':url('/ok.png'),'backdrop_path':[url('/ok.png')],'poster_path':url('/ok.svg')})
 relation_art('/ok.png')
 for resource in objects:
  query=''if resource=='vodlogos'else'?kind=movie_image'
  for role in ['admin','standard','basic','movies_off','series_off','adult_hidden',None]:case(resource,query,role)
  for method in ['HEAD','OPTIONS','POST']:
   for role in ['admin','standard','basic',None]:case(resource,query,role,method)
  for accept in ['image/png','application/xml','text/html','*/*']:case(resource,query,accept=accept,label='raw_negotiation')
 for resource in objects:
  query=''if resource=='vodlogos'else'?kind=movie_image'
  for method in ['OPTIONS','POST','PATCH','DELETE']:
   for role in ['admin','standard','basic',None]:case(resource,query,role,method,accept='application/json',label='JSON_method_permission')
  action='cache'if resource=='vodlogos'else'image'
  for pk in ['bad','-1','999999999']:case(resource,query,path_override=f'/api/vod/{resource}/{pk}/{action}/'+query,label='raw_lookup')
  case(resource,query,role=None,extra={'Authorization':'Bearer broken'},label='presented_invalid_Bearer')
  for suffix in ['format=api','format=json']:case(resource,query+('&'if query else'?')+suffix,label='raw_format_query')
 for resource in ['movies','series','episodes']:
  for query in ['', '?kind=poster_path','?kind=bad','?kind=backdrop&index=bad','?kind=backdrop&index=-1','?kind=backdrop&index=9','?kind=backdrop&index=0&index=9','?kind=movie_image&m3u_account_id=bad','?kind=movie_image&m3u_account_id=999999','?kind=movie_image&m3u_account_id='+str(primary.m3u_account_id)]:case(resource,query,label='image_selection')
 for route in ['/ok.svg','/html','/missing','/redirect','/loop','/blocked','/large']:
  relation_art(route)
  case('movies','?kind=movie_image',label='origin_'+route);case('movies','?kind=movie_image',label='origin_repeat_'+route)
 relation_art('/ok.png')
 for extra in [{'If-Modified-Since':'Mon, 01 Jan 2024 00:00:00 GMT'},{'If-None-Match':'"fixture-etag"'}]:case('movies','?kind=movie_image',label='conditional',extra=extra)
 for value in [str(local),str(local_png),str(local_unknown),'/data/rust-probe-outside-allowed-'+nonce+'.svg','/data/rust-probe-nonexistent-'+nonce+'.png','/data/../etc/passwd','http://127.0.0.1:1/blocked']:
  VODLogo.objects.filter(pk=objects['vodlogos'].pk).update(url=value);case('vodlogos',label='local_or_unsafe');case('vodlogos',method='HEAD',label='local_or_unsafe_HEAD')
 mixed=None
 if native_port:
  # Fixed six-path mixed phase declared before any native observation. Restore
  # fetched relation flags so cached-info never requests provider refresh.
  for model,pk,fields in saved:
   if model in [M3UMovieRelation,M3USeriesRelation,M3UEpisodeRelation]:model.objects.filter(pk=pk).update(**fields)
  VODLogo.objects.filter(pk=objects['vodlogos'].pk).update(url=url('/ok.png'))
  paths=[f'/api/vod/movies/{movie.pk}/image/?kind=movie_image',f'/api/vod/series/{show.pk}/image/?kind=movie_image',f'/api/vod/episodes/{objects["episodes"].pk}/image/?kind=movie_image',f'/api/vod/vodlogos/{objects["vodlogos"].pk}/cache/','/api/vod/movies/?ordering=created_at',f'/api/vod/movies/{movie.pk}/provider-info/']
  sql_before=fingerprint();token='Bearer '+str(AccessToken.for_user(users['standard']))
  def mixed_request(port,path):
   client=http.client.HTTPConnection('127.0.0.1',port,timeout=15)
   try:
    client.request('GET',path,headers={'Host':'localhost','Accept':'application/json','Authorization':token});response=client.getresponse();body=response.read(2*1024*1024+1);assert len(body)<=2*1024*1024
    return {'status':response.status,'headers':{key:response.getheader(key)for key in ['Content-Type','Content-Length','Allow','Vary','WWW-Authenticate','Cache-Control','Last-Modified','Content-Disposition','X-Content-Type-Options','Content-Security-Policy','ETag']},'body_base64':base64.b64encode(body).decode(),'body_sha256':hashlib.sha256(body).hexdigest()}
   finally:client.close()
  references=[mixed_request(5656,path)for path in paths];assert all(row['status']==200 for row in references);assert fingerprint()==sql_before
  phases=[]
  for port in [5656,native_port]:
   barrier=threading.Barrier(11)
   def simultaneous(index):
    barrier.wait(timeout=5);actual=mixed_request(port,paths[index%6]);return {'index':index,'path':paths[index%6],'actual':actual,'matched':actual==references[index%6]}
   with concurrent.futures.ThreadPoolExecutor(max_workers=10)as executor:
    futures=[executor.submit(simultaneous,index)for index in range(10)];barrier.wait(timeout=5);phase=[future.result(timeout=20)for future in futures]
   phases.append(phase);assert fingerprint()==sql_before
  mixed={'passed':all(row['matched']for phase in phases for row in phase),'predeclared_paths':paths,'fresh_all_six_Original_references':references,'Source_unstaggered10':phases[0],'native_unstaggered10':phases[1],'full18_unchanged':True}
finally:
 for model,pk,fields in saved:model.objects.filter(pk=pk).update(**fields)

 for path in [local,local_png,local_unknown]:path.unlink(missing_ok=True)
 origin.shutdown();origin.server_close();thread.join(timeout=2)
assert fingerprint()==before
assert all(row['path'].startswith(('/ok.png','/ok.svg','/html','/missing','/redirect','/loop','/blocked','/large'))for row in requests)
print(json.dumps({'passed':all(not row.get('differences')for row in records)and(mixed is None or mixed['passed']),'scope':'actual Original/native image TCP'if native_port else'actual Original image/cache HTTP only; no native/browser acceptance','records':records,'mixed':mixed,'fixture_origin':origin_base,'origin_requests':requests,'full18_exact_before':before,'full18_exact_after':fingerprint(),'owned_metadata_restored':True,'owned_local_file_removed':all(not path.exists()for path in [local,local_png,local_unknown]),'volatile_headers_not_selected':['Date','Server'],'provider_refresh_requests':0},cls=ExactEncoder,ensure_ascii=False))
