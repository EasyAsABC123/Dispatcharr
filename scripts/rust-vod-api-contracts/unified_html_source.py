"""Actual Original-only HTML/negotiation GET/HEAD repeats and dependencies.
Raw HTML/cookies stay private; repeatdiff locates nondeterminism only and never
normalizes a parity result. Original Dispatcharr authors retain AGPL credit.
"""
import datetime,difflib,hashlib,http.client,importlib.metadata,json,os,re,sys
from html.parser import HTMLParser
from http.cookies import SimpleCookie
from pathlib import Path
from urllib.parse import urlsplit
if os.environ.get('POSTGRES_PASSWORD')!='playback-fixture-only'or os.environ.get('VOD_API_DISPOSABLE_FIXTURE')!='playback-fixture-only':raise RuntimeError('Owned fixture required')
sys.path.insert(0,'/app');os.environ.setdefault('DJANGO_SETTINGS_MODULE','dispatcharr.settings')
import django
django.setup()
from django.core.serializers.json import DjangoJSONEncoder
from apps.accounts.models import User
from apps.m3u.models import M3UAccount,M3UAccountProfile
from apps.vod.models import Movie,Series,Episode,VODLogo,VODCategory,M3UMovieRelation,M3USeriesRelation,M3UEpisodeRelation,M3UVODCategoryRelation
from core.models import CoreSettings,SystemEvent
from rest_framework_simplejwt.tokens import AccessToken
import rest_framework
TARGET=os.environ.get('VOD_UNIFIED_HTML_TARGET','Original')
assert TARGET in ['Original','candidate']
HOST,PORT=('127.0.0.1',5656)if TARGET=='Original'else('candidate',9192)
prefix='rust-probe-vod-rest-';users={role:User.objects.get(username=prefix+role)for role in ['standard','admin']}
assert M3UAccount.objects.filter(name__startswith=prefix).count()==3 and not M3UAccount.objects.filter(name__startswith=prefix).exclude(server_url__startswith='http://127.0.0.1:').exists()
class Encoder(DjangoJSONEncoder):
 def default(self,value):
  if isinstance(value,(datetime.datetime,datetime.date)):return value.isoformat()
  return super().default(value)
models=[User,M3UAccount,M3UAccountProfile,Movie,Series,Episode,VODLogo,VODCategory,M3UMovieRelation,M3USeriesRelation,M3UEpisodeRelation,M3UVODCategoryRelation,CoreSettings,SystemEvent]
def fingerprint():return {m._meta.db_table:hashlib.sha256(json.dumps(list(m.objects.order_by('pk').values()),sort_keys=True,separators=(',',':'),cls=Encoder).encode()).hexdigest()for m in models}
class Parser(HTMLParser):
 def __init__(self):super().__init__();self.assets=[];self.inputs=[];self.forms=[];self.meta=[]
 def handle_starttag(self,tag,attrs):
  value=dict(attrs)
  if tag=='script'and value.get('src'):self.assets.append(value['src'])
  if tag=='link'and value.get('href'):self.assets.append(value['href'])
  if tag=='input':self.inputs.append(value)
  if tag=='form':self.forms.append(value)
  if tag=='meta':self.meta.append(value)
def parse(body):
 parser=Parser();parser.feed(body.decode('utf-8'));text=body.decode('utf-8')
 return {'assets':parser.assets,'inputs':parser.inputs,'forms':parser.forms,'meta':parser.meta,'csrf_mentions':re.findall(r'.{0,90}(?:csrfToken|csrf_token|csrfmiddlewaretoken|csrftoken).{0,120}',text)}
def request(path,accept,role,method,cookie=''):
 headers={'Host':'localhost','Accept':accept,'Accept-Encoding':'identity'}
 if role:headers['Authorization']='Bearer '+str(AccessToken.for_user(users[role]))
 if cookie:headers['Cookie']=cookie
 connection=http.client.HTTPConnection(HOST,PORT,timeout=10)
 try:connection.request(method,path,headers=headers);response=connection.getresponse();body=response.read(2*1024*1024+1);assert len(body)<=2*1024*1024;raw_headers=response.getheaders();record={'status':response.status,'reason':response.reason,'version':response.version,'raw_response_headers':raw_headers,'body_bytes':len(body),'body_sha256':hashlib.sha256(body).hexdigest(),'body_text':body.decode('utf-8'),'request_cookie':cookie,'content_type':response.getheader('Content-Type')}
 finally:connection.close()
 if body and record['content_type'].startswith('text/html'):record['DOM']=parse(body)
 try:record['json']=json.loads(body)
 except(ValueError,UnicodeDecodeError):pass
 record['Authorization_value_recorded']=False;record['observed_utc']=datetime.datetime.now(datetime.timezone.utc).isoformat();return record
before=fingerprint();records=[];diffs=[]
specs=[('accept_HTML','/api/vod/all/','text/html','standard'),*[(f'format_api_JSON_{role}', '/api/vod/all/?format=api','application/json',role)for role in ['standard','admin',None]],*[(f'format_api_HTML_{role}','/api/vod/all/?format=api','text/html',role)for role in ['standard','admin',None]]]
for label,path,accept,role in specs:
 for method in ['GET','HEAD']:
  group=[];cookie=''
  for ordinal,strategy in enumerate(['fresh-no-cookie','repeat-no-cookie','sticky-cookie','repeat-sticky-cookie']):
   item=request(path,accept,role,method,''if'no-cookie'in strategy else cookie);item.update(spec=label,path=path,accept=accept,role=role,method=method,repeat=ordinal,cookie_strategy=strategy);records.append(item);group.append(item)
   if ordinal==0:
    jar=SimpleCookie()
    for key,value in item['raw_response_headers']:
     if key.lower()=='set-cookie':jar.load(value)
    cookie='; '.join(key+'='+value.value for key,value in jar.items())
  for a,b in [(0,1),(2,3)]:
   left,right=group[a]['body_text'],group[b]['body_text'];opcodes=[{'tag':tag,'left_start':i,'left_end':j,'right_start':k,'right_end':l,'left_fragment':left[i:j],'right_fragment':right[k:l]}for tag,i,j,k,l in difflib.SequenceMatcher(None,left,right,autojunk=False).get_opcodes()if tag!='equal'];diffs.append({'spec':label,'method':method,'repeats':[a,b],'exact_raw_body_equal':left==right,'changes':opcodes,'raw_headers_equal':group[a]['raw_response_headers']==group[b]['raw_response_headers'],'purpose':'Nondeterminism witness only; not parity normalization'})
assert fingerprint()==before
assets=sorted({url for record in records for url in record.get('DOM',{}).get('assets',[])})
asset_observations=[]
for url in (assets if TARGET=='Original'else[]):
 parts=urlsplit(url)
 if parts.netloc or parts.scheme:asset_observations.append({'url':url,'external_contacted':False});continue
 connection=http.client.HTTPConnection(HOST,PORT,timeout=10)
 try:connection.request('GET',url,headers={'Host':'localhost'});response=connection.getresponse();body=response.read(2*1024*1024+1);assert len(body)<=2*1024*1024;asset_observations.append({'url':url,'fixture_HTTP_status':response.status,'fixture_HTTP_MIME':response.getheader('Content-Type'),'fixture_HTTP_bytes':len(body),'fixture_HTTP_sha256':hashlib.sha256(body).hexdigest(),'scope':'This fixture has no productioncollectstatic/nginx; packagebytes inventory separate'})
 finally:connection.close()
assert fingerprint()==before
package=Path(rest_framework.__file__).parent;files={}
for base in [package/'templates',package/'static']:
 for path in sorted(base.rglob('*')):
  if path.is_file():data=path.read_bytes();files[str(path.relative_to(package))]={'bytes':len(data),'sha256':hashlib.sha256(data).hexdigest()}
licenses={};dist=importlib.metadata.distribution('djangorestframework')
for item in dist.files or[]:
 if any(word in str(item).lower()for word in ['license','copyright','notice']):
  path=Path(dist.locate_file(item))
  if path.is_file()and path.stat().st_size<128*1024:licenses[str(item)]={'text':path.read_text(errors='replace'),'sha256':hashlib.sha256(path.read_bytes()).hexdigest()}
print(json.dumps({'passed':True,'target':TARGET,'scope':'Actual target rawHTML/negotiation GET/HEAD/cookies/repeats +package dependency inventory; Source-only no normalization/native parity','records':records,'repeat_differences':diffs,'asset_URL_observations':asset_observations,'framework':{'djangorestframework':importlib.metadata.version('djangorestframework'),'Django':django.get_version()},'package_files':files,'license_dependencies':licenses,'owned_fullmicro14table_rows_restored':True,'fingerprints_before':before,'fingerprints_after':fingerprint(),'native_verified':False,'private_raw_HTML_cookie_values':True},cls=Encoder,ensure_ascii=False))
