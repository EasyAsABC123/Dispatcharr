"""Source logo delivery/CRUD/upload goldens on disposable owned files/rows.
No passwords/tokens/provider bodies enter sanitized reports. Original authors
retain application credit. Run only in the original disposable fixture.
"""
import base64,hashlib,json,os,sys,time,urllib.request
from pathlib import Path
if os.environ.get('POSTGRES_PASSWORD')!='playback-fixture-only':raise RuntimeError('Disposable fixture required')
sys.path.insert(0,'/app');sys.argv.append('shell');os.environ.setdefault('DJANGO_SETTINGS_MODULE','dispatcharr.settings')
import django
django.setup()
from django.db import connection,transaction
from django.core.cache import cache
from django.test.client import encode_multipart
from django.core.files.uploadedfile import SimpleUploadedFile
import requests
from rest_framework_simplejwt.tokens import RefreshToken
from apps.accounts.models import User
from apps.channels.models import Logo,Channel,ChannelOverride
from apps.channels.api_views import _logo_fetch_failures
from core.models import CoreSettings
try:
 from logo_marker_witness import clear_markers,marker_witness
except ImportError:
 clear_markers=marker_witness=None
if Logo.objects.exists() or User.objects.filter(username__startswith='rust-probe-logo-').exists():raise RuntimeError('Fresh owned logo namespace required')
users={}
for name,level,active in [('admin',10,True),('standard',1,True),('basic',0,True),('inactive',10,False)]:
 u=User.objects.create(username='rust-probe-logo-'+name,password='!',user_level=level,is_active=active);users[name]=str(RefreshToken.for_user(u).access_token)
Logo.objects.bulk_create([Logo(id=1,name='rust-probe-logo-one',url='http://origin:8080/image/png'),Logo(id=2,name='rust-probe-logo-duplicate',url='https://fixture.invalid/duplicate.png')])
Channel.objects.bulk_create([Channel(name='rust-probe-logo-channel-'+str(n),channel_number=n+9000,logo_id=1)for n in range(2)])
channels=list(Channel.objects.filter(name__startswith='rust-probe-logo-channel-').order_by('id'));override=ChannelOverride.objects.create(channel=channels[0],logo_id=1)
files={'/data/logos/rust-probe-logo.png':b'\x89PNG\r\n\x1a\nfixture-image','/data/logos/rust-probe-logo.svg':b'<svg/>','/data/logos/rust-probe-logo.txt':b'local-text','/tmp/rust-probe-logo-outside.png':b'\x89PNG\r\n\x1a\nfixture-outside'}
files.update({'/data/logos/rust-probe-logo.html':b'<html>owned fixture</html>','/data/logos/rust-probe-logo-é.png':b'local-latin1-name','/data/logos/rust-probe-logo-漢.png':b'local-unicode-name'})
owned=list(files)+['/data/logos/rust-probe-logo-link.png','/data/logos/rust-probe-logo-dir','/data/logos/rust-probe-upload.png','/data/logos/rust-probe-unicode-é.png']
for path in files:
 if Path(path).exists():raise RuntimeError('Owned file namespace must be fresh')
Path('/data/logos').mkdir(parents=True,exist_ok=True)
def prepare_files():
 if clear_markers is not None:clear_markers(owned)
 for path in owned:
  p=Path(path)
  if p.is_symlink()or p.is_file():p.unlink()
  elif p.is_dir():p.rmdir()
 for path,body in files.items():Path(path).write_bytes(body);os.utime(path,(1700000000.125,1700000000.125))
 Path('/data/logos/rust-probe-logo-link.png').symlink_to('/tmp/rust-probe-logo-outside.png');Path('/data/logos/rust-probe-logo-dir').mkdir()
def file_witness():
 result={}
 for path in owned:
  p=Path(path)
  if p.is_symlink():result[path]={'kind':'symlink','target':os.readlink(path)}
  elif p.is_file():body=p.read_bytes();result[path]={'kind':'file','size':len(body),'sha256':hashlib.sha256(body).hexdigest()}
  elif p.is_dir():result[path]={'kind':'directory'}
  else:result[path]={'kind':'missing'}
 return result
def db_witness():
 return {'logos':list(Logo.objects.order_by('id').values('id','name','url')),'channels':list(Channel.objects.filter(pk__in=[c.pk for c in channels]).order_by('id').values('id','logo_id')),'overrides':list(ChannelOverride.objects.filter(pk=override.pk).values('id','logo_id'))}
def blob(spec):return base64.b64decode(spec.get('prefix',''))+bytes([spec.get('byte',120)])*spec.get('repeat',0)
def wire(spec):
 if 'raw'in spec:return base64.b64decode(spec['raw']),spec.get('content_type','application/json')
 if 'json'in spec:return json.dumps(spec['json'],ensure_ascii=False,separators=(',',':')).encode(),'application/json'
 if 'form'in spec:return __import__('urllib.parse',fromlist=['urlencode']).urlencode(spec['form']).encode(),'application/x-www-form-urlencoded'
 out=bytearray()
 for field in spec.get('multipart',[]):
  out.extend(b'--rust-probe-boundary\r\n');name=field['name']
  if 'filename'in field:
   out.extend((f'Content-Disposition: form-data; name="{name}"; filename="{field["filename"]}"\r\nContent-Type: {field.get("mime","image/png")}\r\n\r\n').encode());out.extend(blob(field))
  else:out.extend((f'Content-Disposition: form-data; name="{name}"\r\n\r\n{field.get("value","")}').encode())
  out.extend(b'\r\n')
 out.extend(b'--rust-probe-boundary--\r\n')
 return bytes(out),'multipart/form-data; boundary=rust-probe-boundary'

HEADERS=['content-type','content-length','cache-control','last-modified','content-disposition','x-content-type-options','content-security-policy','www-authenticate']
base_logos=list(Logo.objects.order_by('id').values('id','name','url'))
def restore_db():
 Logo.objects.all().delete();Logo.objects.bulk_create([Logo(**row)for row in base_logos]);Channel.objects.filter(pk__in=[c.pk for c in channels]).update(logo_id=1);ChannelOverride.objects.filter(pk=override.pk).update(logo_id=1)
cases=[]
def counts():return json.load(urllib.request.urlopen('http://origin:8080/counts'))
def case(name,method='GET',path='/api/channels/logos/1/cache/',url=None,role=None,body=None,headers=None,repeat=1,clock_hash=False,cached_agent=None):
 prefixes=os.environ.get('LOGO_CASE_PREFIX','').split(',')
 if any(prefixes)and not any(name.startswith(prefix)for prefix in prefixes if prefix):return
 prepare_files();restore_db();_logo_fetch_failures.clear();cache.delete(':1:coresettings:default_user_agent');cache.delete('coresettings:default_user_agent')
 if cached_agent is not None:cache.set('coresettings:default_user_agent',cached_agent,300)
 urllib.request.urlopen('http://origin:8080/reset').close()
 if url is not None:Logo.objects.filter(pk=1).update(url=url)
 with connection.cursor()as cursor:cursor.execute("SELECT setval('dispatcharr_channels_logo_id_seq',%s,true)",[1000+len(cases)*10])
 pre=db_witness();sequence=1000+len(cases)*10;responses=[]
 request_headers={'HTTP_HOST':'localhost',**(headers or {})}
 if role is not None:request_headers['HTTP_AUTHORIZATION']='Bearer '+users.get(role,role)
 raw,mime=wire(body)if body is not None else(b'','application/json')
 for _ in range(repeat):
  wire_headers={key.removeprefix('HTTP_').replace('_','-'):value for key,value in request_headers.items()}
  result=requests.request(method,'http://127.0.0.1:5656'+path,data=raw,headers={**wire_headers,'Content-Type':mime},timeout=45,allow_redirects=False)
  payload=result.content
  selected={h:result.headers[h]for h in HEADERS if h in result.headers}
  response={'status':result.status_code,'headers':selected,'body_sha256':hashlib.sha256(payload).hexdigest(),'body_length':len(payload)}
  if 'json'in selected.get('content-type',''):response['json']=json.loads(payload)
  result.close();responses.append(response)
 after=db_witness();postfiles=file_witness()
 cases.append({'name':name,'method':method,'path':path,'headers':request_headers,'body':body,'request_body_sha256':hashlib.sha256(raw).hexdigest(),'sequence':sequence,'pre':pre,'expected':{'responses':responses,'db':after,'files':postfiles,'origin_calls':counts(),**({'markers':marker_witness(owned)}if marker_witness is not None else{})},'clock_hash':clock_hash,'cached_agent':cached_agent})
 restore_db()
for kind in ['png','jpeg','gif','webp','ico','bmp','svg','html','unknown','empty','agent']:case('remote-'+kind,url='http://origin:8080/image/'+kind)
case('cached-user-agent',url='http://origin:8080/image/agent',cached_agent='Rust-Probe-Cached/7')
for route in ['redirect/png','redirect/5','redirect/6','redirect/unsafe','redirect/missing','status/404','gzip','bad-gzip','truncated','limit/exact','limit/over']:
 case('remote-'+route.replace('/','-'),url='http://origin:8080/'+route,repeat=2 if route in ['status/404','limit/over','bad-gzip']else 1)
for route in ['stall','headers-stall','drip']:case('remote-'+route,url='http://origin:8080/'+route)
for url in ['','ftp://origin/a','http://127.0.0.1/logo.png','http://[::1]/logo.png','http://169.254.169.254/latest','http://missing-fixture.invalid/a']:
 case('blocked-'+str(len(cases)),url=url)
for filename in ['rust-probe-logo.png','rust-probe-logo.svg','rust-probe-logo.txt','rust-probe-missing.png','rust-probe-logo-link.png','rust-probe-logo-dir','rust-probe-logo.html','rust-probe-logo-é.png','rust-probe-logo-漢.png']:
 case('local-'+filename,url='/data/logos/'+filename)
case('local-escaped',url='/data/logos/../../tmp/rust-probe-logo-outside.png')
case('cache-accept-image',headers={'HTTP_ACCEPT':'image/*'})
case('cache-filtered',path='/api/channels/logos/1/cache/?used=false')
case('cache-missing',path='/api/channels/logos/999999/cache/')
case('cache-invalid-bearer',role='invalid')
case('cache-inactive',role='inactive')
case('cache-head',method='HEAD')
case('cache-local-head',method='HEAD',url='/data/logos/rust-probe-logo.png')
base='/api/channels/logos/'
for payload in [{},{'name':'new'},{'url':'x'},{'name':None,'url':None},{'name':' ','url':''},{'name':False,'url':True},{'name':42,'url':56},{'name':' '+('N'*260)+' ','url':'not-a-url','channel_count':9,'id':123},{'name':'unique','url':'https://fixture.invalid/duplicate.png'},['not-a-map']]:
 case('create-'+str(len(cases)),method='POST',path=base,role='admin',body={'json':payload})
case('create-form',method='POST',path=base,role='admin',body={'form':{'name':' form name ','url':' plain text url '}})
case('create-multipart',method='POST',path=base,role='admin',body={'multipart':[{'name':'name','value':' multipart name '},{'name':'url','value':'multipart-url'}]})
case('create-malformed',method='POST',path=base,role='admin',body={'raw':base64.b64encode(b'{').decode()})
case('create-unsupported-type',method='POST',path=base,role='admin',body={'raw':base64.b64encode(b'abc').decode(),'content_type':'text/plain'})
for method,payload in [('PATCH',{}),('PATCH',{'name':'changed'}),('PATCH',{'url':'changed-url'}),('PATCH',{'url':'https://fixture.invalid/duplicate.png'}),('PUT',{'name':'missing-url'}),('PUT',{'name':'replacement','url':'replacement-url'})]:
 case('update-'+str(len(cases)),method=method,path=base+'1/',role='admin',body={'json':payload})
for role in [None,'basic','standard','inactive','invalid']:
 case('create-role-'+str(role),method='POST',path=base,role=role,body={'json':{'name':'denied','url':'denied-url'}})
case('delete-relations',method='DELETE',path=base+'1/',role='admin')
case('delete-keep-file',method='DELETE',path=base+'1/',role='admin',url='/data/logos/rust-probe-logo.png')
case('delete-file',method='DELETE',path=base+'1/?delete_file=TRUE',role='admin',url='/data/logos/rust-probe-logo.png')
case('delete-directory-error',method='DELETE',path=base+'1/?delete_file=true',role='admin',url='/data/logos/rust-probe-logo-dir')
case('delete-filtered',method='DELETE',path=base+'1/?used=false',role='admin')
part={'name':'file','filename':'rust-probe-upload.png','mime':'image/png','prefix':base64.b64encode(b'fake-not-even-png').decode()}
upload=base+'upload/'
for role in [None,'basic','standard','inactive','invalid']:
 case('upload-role-'+str(role),method='POST',path=upload,role=role,body={'multipart':[part]})
case('upload-missing',method='POST',path=upload,role='admin',body={'multipart':[]})
case('upload-json',method='POST',path=upload,role='admin',body={'json':{'name':'missing'}})
case('upload-json-array',method='POST',path=upload,role='admin',body={'json':[]})
case('upload-bad-mime',method='POST',path=upload,role='admin',body={'multipart':[{**part,'mime':'text/plain'}]})
case('upload-too-large',method='POST',path=upload,role='admin',body={'multipart':[{**part,'prefix':'','repeat':5*1024*1024+1}]})
case('upload-mime-before-size',method='POST',path=upload,role='admin',body={'multipart':[{**part,'prefix':'','repeat':5*1024*1024+1,'mime':'text/plain'}]})
for filename in ['rust-probe-upload.png','../rust-probe-upload.png','..\\rust-probe-upload.png','rust-probe-unicode-é.png','rust-probe&#45;upload.png','rust-probe-&NewLine;upload.png','rust-probe-X&sol;rust-probe-upload.png']:
 case('upload-name-'+str(len(cases)),method='POST',path=upload,role='admin',body={'multipart':[{**part,'filename':filename},{'name':'name','value':' custom name '}]},clock_hash=True)
case('upload-exact-limit',method='POST',path=upload,role='admin',body={'multipart':[{**part,'prefix':'','repeat':5*1024*1024}]},clock_hash=True)
case('upload-existing-conflict',method='POST',path=upload,role='admin',body={'multipart':[{**part,'filename':'rust-probe-logo.png'}]})
case('upload-overwrite',method='POST',path=upload,role='admin',body={'multipart':[{**part,'filename':'rust-probe-logo.png'},{'name':'overwrite','value':'yes'}]},clock_hash=True)
case('upload-symlink-escape',method='POST',path=upload,role='admin',body={'multipart':[{**part,'filename':'rust-probe-logo-link.png'}]})
prepare_files()
print(json.dumps({'cases':cases,'files':{p:base64.b64encode(b).decode()for p,b in files.items()},'owned_paths':owned,'channel_ids':[c.pk for c in channels],'override_id':override.pk},separators=(',',':')))
