"""Original job-free UserAgent CRUD/cache/FK contracts on disposable rows.
Original Dispatcharr contributors retain all application credit.
"""
import base64,datetime,hashlib,json,os,sys,time
from pathlib import Path
if os.environ.get('POSTGRES_PASSWORD')!='playback-fixture-only':raise RuntimeError('Disposable fixture required')
sys.path.insert(0,'/app');sys.argv.append('shell');os.environ.setdefault('DJANGO_SETTINGS_MODULE','dispatcharr.settings')
import django
django.setup()
import requests
from django.db import connection
from django.core.cache import cache
from apps.accounts.models import User
from apps.channels.models import Logo
from apps.m3u.models import M3UAccount
from core.models import CoreSettings,UserAgent,StreamProfile
from core.utils import RedisClient
from rest_framework_simplejwt.tokens import RefreshToken
UTC=datetime.timezone.utc
if UserAgent.objects.filter(id__gte=90000).exists()or User.objects.filter(username__startswith='rust-probe-ua-').exists():raise RuntimeError('Fresh owned namespace required')
roles={}
for name,level,active in [('admin',10,True),('above_admin',20,True),('standard',1,True),('basic',0,True),('inactive',10,False)]:
 user=User.objects.create(username='rust-probe-ua-'+name,password='!',user_level=level,is_active=active);roles[name]=str(RefreshToken.for_user(user).access_token)
StreamProfile.objects.bulk_create([StreamProfile(name='rust-probe-ua-profile',command='',parameters='',user_agent_id=None,is_active=True)])
profile=StreamProfile.objects.get(name='rust-probe-ua-profile')
M3UAccount.objects.bulk_create([M3UAccount(name='rust-probe-ua-provider',account_type='STD',is_active=False,user_agent_id=None)])
provider=M3UAccount.objects.get(name='rust-probe-ua-provider')
Logo.objects.bulk_create([Logo(name='rust-probe-ua-image',url='http://origin:8080/image/agent?initial=1')]);logo=Logo.objects.get(name='rust-probe-ua-image')
setting=CoreSettings.objects.get(key='stream_settings');setting.value={**(setting.value or {}),'default_user_agent':90001};setting.save()
base=[{'id':90001,'name':'rust-probe-ua-one','user_agent':'Rust-Probe/one','description':'original description','is_active':True},{'id':90002,'name':'rust-probe-ua-two','user_agent':'Rust-Probe/two','description':'second description','is_active':False}]
def restore():
 UserAgent.objects.filter(id__gte=90000).delete();UserAgent.objects.bulk_create([UserAgent(**row)for row in base]);UserAgent.objects.filter(id__gte=90000).update(created_at=datetime.datetime(2000,1,2,3,4,5,123456,tzinfo=UTC),updated_at=datetime.datetime(2001,2,3,4,5,6,654321,tzinfo=UTC));StreamProfile.objects.filter(pk=profile.pk).update(user_agent_id=90001);M3UAccount.objects.filter(pk=provider.pk).update(user_agent_id=90001)
def normalize(value):
 if isinstance(value,list):return [normalize(v)for v in value]
 if isinstance(value,dict):
  return {k:('<clock>'if k in ['created_at','updated_at']and isinstance(v,str)and v.startswith(str(datetime.datetime.now(UTC).year))else normalize(v))for k,v in value.items()}
 return value

def witness():
 rows=list(UserAgent.objects.filter(id__gte=90000).order_by('id').values('id','name','user_agent','description','is_active','created_at','updated_at'))
 result=[]
 for row in rows:
  created=row.pop('created_at');updated=row.pop('updated_at');row.update(created_at=created.isoformat().replace('+00:00','Z'),updated_at=updated.isoformat().replace('+00:00','Z'),created_is_fixed=created.year==2000,updated_is_fixed=updated.year==2001,dates_ordered=created<=updated,dates_utc=created.utcoffset()==updated.utcoffset()==datetime.timedelta(0));result.append(row)
 return {'agents':normalize(result),'profile_user_agent':StreamProfile.objects.get(pk=profile.pk).user_agent_id,'provider_user_agent':M3UAccount.objects.get(pk=provider.pk).user_agent_id,'default_setting_unchanged':CoreSettings.get_stream_settings().get('default_user_agent')==90001}
def cached():
 version=cache.get('coresettings:default_user_agent:ver');client=RedisClient.get_client()
 return {'value':cache.get('coresettings:default_user_agent'),'version_type':type(version).__name__,'version_old':version==123456,'version_advanced':isinstance(version,int)and version>123456,'version_persistent':client.ttl(':1:coresettings:default_user_agent:ver')==-1}
def wire(spec):
 if spec is None:return b'','application/json'
 if 'json'in spec:return json.dumps(spec['json'],ensure_ascii=False,separators=(',',':')).encode(),'application/json'
 if 'form'in spec:return __import__('urllib.parse',fromlist=['urlencode']).urlencode(spec['form']).encode(),'application/x-www-form-urlencoded'
 if 'raw'in spec:return base64.b64decode(spec['raw']),spec.get('content_type','application/json')
 out=bytearray()
 for key,value in spec.get('multipart',{}).items():out.extend((f'--rust-probe-boundary\r\nContent-Disposition: form-data; name="{key}"\r\n\r\n{value}\r\n').encode())
 out.extend(b'--rust-probe-boundary--\r\n');return bytes(out),'multipart/form-data; boundary=rust-probe-boundary'
HEADERS=['content-type','content-length','www-authenticate','x-content-type-options']
cases=[]
def case(name,method='PATCH',pk='90001',role='admin',spec=None):
 restore();cache.set('coresettings:default_user_agent','Rust-Probe/stale',300);cache.set('coresettings:default_user_agent:ver',123456,timeout=None)
 sequence=100000+len(cases)*10
 with connection.cursor()as cursor:cursor.execute("SELECT setval('core_useragent_id_seq',%s,true)",[sequence])
 path='/api/core/useragents/'+(str(pk)+'/'if pk is not None else'')
 path=__import__('urllib.parse',fromlist=['quote']).quote(path,safe='/?=&')
 headers={'Host':'localhost'}
 if role is not None:headers['Authorization']='Bearer '+roles.get(role,role)
 raw,mime=wire(spec);before=witness();response=requests.request(method,'http://127.0.0.1:5656'+path,data=raw,headers={**headers,'Content-Type':mime},timeout=20,allow_redirects=False);payload=response.content;selected={k:response.headers[k]for k in HEADERS if k in response.headers}
 output={'status':response.status_code,'headers':selected}
 if 'json'in selected.get('content-type',''):output['json']=normalize(response.json())
 else:output.update(body_sha256=hashlib.sha256(payload).hexdigest(),body_length=len(payload))
 after=witness();invalidated=cached();resolved=CoreSettings.get_default_user_agent();after_fill=cached();response.close()
 image=requests.get('http://127.0.0.1:5656/api/channels/logos/'+str(logo.pk)+'/cache/',headers={'Host':'localhost'},timeout=20)
 image_result={'status':image.status_code,'sha256':hashlib.sha256(image.content).hexdigest()};image.close()
 cases.append({'name':name,'method':method,'path':path,'headers':headers,'body':spec,'request_body_sha256':hashlib.sha256(raw).hexdigest(),'sequence':sequence,'pre':before,'expected':{'response':output,'db':after,'cache_before_resolve':invalidated,'resolved_default':resolved,'cache_after_resolve':after_fill,'original_image_reader':image_result}})
for payload in [{},{'name':'one'},{'user_agent':'UA'},{'name':None,'user_agent':None},{'name':' ','user_agent':''},{'name':False,'user_agent':True},{'name':'x'*513,'user_agent':'y'*513,'description':'z'*256},{'name':'rust-probe-ua-one','user_agent':'Rust-Probe/two'},{'name':42,'user_agent':1.5},{'name':'new','user_agent':'New/1','id':90002,'created_at':None,'updated_at':'ignored','unknown':'ignored'},{'name':'new','user_agent':'New/1','description':'','is_active':False},{'name':'new','user_agent':'New/1','is_active':None},{'name':'new','user_agent':'New/1','is_active':' false '},{'name':'new','user_agent':'New/1','is_active':1.0}]:
 case('create-json-'+str(len(cases)),method='POST',pk=None,spec={'json':payload})
for payload in [{'name':'form','user_agent':'Form/1'},{'name':'form','user_agent':'Form/1','is_active':''},{'name':'form','user_agent':'Form/1','is_active':'on'},{'name':'form','user_agent':'Form/1','description':'  hello  ','is_active':'0'}]:
 case('create-form-'+str(len(cases)),method='POST',pk=None,spec={'form':payload})
case('create-multipart',method='POST',pk=None,spec={'multipart':{'name':'multipart','user_agent':'Multipart/1'}})
for payload in [{},{'name':'renamed'},{'user_agent':'Changed/2'},{'description':''},{'is_active':False},{'name':'rust-probe-ua-two','user_agent':'Rust-Probe/two'},{'description':None},{'is_active':[]},{'created_at':'ignored','updated_at':'ignored','id':1},{'user_agent':'  Changed/trim  '},{'user_agent':'Rust-Probe/é'}]:
 case('patch-json-'+str(len(cases)),spec={'json':payload})
case('patch-other-user-agent-invalidates',pk='90002',spec={'json':{'description':'changed other'}})
for payload in [{},{'name':'put'},{'name':'put','user_agent':'Put/2'},{'name':'put','user_agent':'Put/2','is_active':False}]:case('put-json-'+str(len(cases)),method='PUT',spec={'json':payload})
for method,payload in [('PUT',{'name':'putform','user_agent':'PutForm/2'}),('PUT',{'name':'putform','user_agent':'PutForm/2','is_active':''}),('PATCH',{'description':'partial form'}),('PATCH',{'is_active':''})]:case('html-update-'+str(len(cases)),method=method,spec={'form':payload})
for role in [None,'basic','standard','inactive','invalid','above_admin']:
 case('create-role-'+str(role),method='POST',pk=None,role=role,spec={'json':{'name':'role-create','user_agent':'Role/1'}})
for method in ['PATCH','PUT','DELETE']:
 for pk in ['999999','bad','-1','90001_0','+90001',' 90001 ','٩٠٠٠١','9'*100]:case('lookup-'+method+'-'+str(len(cases)),method=method,pk=pk,spec={'json':{'name':'lookup','user_agent':'Lookup/2'}})
case('delete-default',method='DELETE');case('delete-other',method='DELETE',pk='90002')
case('empty-body-post',method='POST',pk=None)
case('malformed-json',method='POST',pk=None,spec={'raw':base64.b64encode(b'{').decode()})
case('unsupported-mime',method='POST',pk=None,spec={'raw':base64.b64encode(b'hello').decode(),'content_type':'text/plain'})
for role in ['admin','standard','basic','inactive',None]:case('retrieve-role-'+str(role),method='GET',role=role)
restore()
# Actual original reader/writer race: a miss loads the old SQL value, while a
# separate real HTTP request mutates the row and fences the shared cache fill.
import threading
UserAgent.objects.filter(pk=90001).update(user_agent='Rust-Probe/race-old')
cache.delete('coresettings:default_user_agent');cache.set('coresettings:default_user_agent:ver',123456,timeout=None)
loaded=threading.Event();resume=threading.Event();original=CoreSettings._load_default_user_agent_string;result=[]
def blocked(cls):
 value=original();loaded.set()
 if not resume.wait(10):raise RuntimeError('Original reader gate deadline')
 return value
CoreSettings._load_default_user_agent_string=classmethod(blocked)
thread=threading.Thread(target=lambda:result.append(CoreSettings.get_default_user_agent()));thread.start()
if not loaded.wait(10):raise RuntimeError('Original reader did not load')
response=requests.patch('http://127.0.0.1:5656/api/core/useragents/90001/',json={'user_agent':'Rust-Probe/race-new'},headers={'Authorization':'Bearer '+roles['admin'],'Host':'localhost'},timeout=10)
resume.set();thread.join(10);CoreSettings._load_default_user_agent_string=classmethod(original.__func__)
race={'writer_status':response.status_code,'old_returned':result==['Rust-Probe/race-old'],'stale_fill_skipped':cache.get('coresettings:default_user_agent')is None,'fresh_resolved':CoreSettings.get_default_user_agent()=='Rust-Probe/race-new','version_advanced':cache.get('coresettings:default_user_agent:ver')>123456}
if not all([race['writer_status']==200,race['old_returned'],race['stale_fill_skipped'],race['fresh_resolved'],race['version_advanced']]):raise RuntimeError('Original race failed')
restore()
default_cases=[]
for raw in [90001.75,'90_001','٩٠٠٠١',' +90001 ',True,False,None,'',[],{},90002.9,'not-an-id',10**100]:
 setting.refresh_from_db();setting.value={**setting.value,'default_user_agent':raw};setting.save()
 value=CoreSettings.get_default_user_agent();default_cases.append({'raw':raw,'expected':value})
setting.refresh_from_db();setting.value={**setting.value,'default_user_agent':90001};setting.save()
restore()
print(json.dumps({'cases':cases,'agents':base,'profile_id':profile.pk,'provider_id':provider.pk,'logo_id':logo.pk,'race':race,'default_id_cases':default_cases},separators=(',',':')))
