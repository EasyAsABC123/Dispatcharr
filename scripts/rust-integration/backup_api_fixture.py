"""Original job-free backup HTTP/filesystem contracts; disposable original container only.
Original Dispatcharr authors retain full application credit. Raw JWTs stay ignored.
"""
import hashlib,hmac,json,os,sys
from pathlib import Path
if os.environ.get('POSTGRES_PASSWORD')!='playback-fixture-only'or os.environ.get('BACKUP_API_DISPOSABLE_FIXTURE')!='playback-fixture-only':raise RuntimeError('Explicit disposable backup API fixture required')
sys.path.insert(0,'/app');sys.argv.append('shell');os.environ.setdefault('DJANGO_SETTINGS_MODULE','dispatcharr.settings')
import django
django.setup()
import requests
from apps.accounts.models import User
from core.models import CoreSettings
from django.core.cache import cache
from rest_framework_simplejwt.tokens import RefreshToken
ROOT=Path('/data/backups');ROOT.mkdir(exist_ok=True);PREFIX='rust-probe-backup-api-'
if list(ROOT.iterdir())or User.objects.filter(username__startswith=PREFIX).exists():raise RuntimeError('Fresh owned backup API fixture required')
roles={}
for name,level,active in [('admin',10,True),('standard',1,True),('basic',0,True),('inactive',10,False),('blocked',10,True)]:
 props={'allowed_networks':{'UI':'203.0.113.0/24'}}if name=='blocked'else{}
 user=User.objects.create(username=PREFIX+name,password='!',user_level=level,is_active=active,custom_properties=props)
 roles[name]=str(RefreshToken.for_user(user).access_token)
checks=[];seed=[('dispatcharr-backup-rust-probe-a.zip',b'fixture ZIP bytes\x00\xff',1700000000),('dispatcharr-backup-rust-probe-b.zip',b'other fixture bytes',1700000100),('arbitrary-upload.txt',b'not ZIP validated',1700000200),('caf\xe9.zip',b'latin filename',1700000300),('amp&plus+=.zip',b'path punctuation',1700000400)]
def reset():
 for path in ROOT.iterdir():
  if path.is_dir():path.rmdir()
  else:path.unlink()
 for name,data,mtime in seed:
  path=ROOT/name;path.write_bytes(data);os.utime(path,(mtime,mtime))
 (ROOT/'fixture-directory').mkdir()
def files():
 result={}
 for path in sorted(ROOT.iterdir()):

  if path.is_dir():result[path.name]={'directory':True}
  else:
   digest=hashlib.sha256()
   with path.open('rb')as handle:
    for block in iter(lambda:handle.read(65536),b''):digest.update(block)
   result[path.name]={'size':path.stat().st_size,'sha256':digest.hexdigest()}
 return result
def token(name):return hmac.new(b'playback-fixture-only',name.encode(),hashlib.sha256).hexdigest()[:32]
def case(name,path,method='GET',role='admin',data=None,files_input=None,headers=None,query=None):
 reset();pre=files();request_headers={'Host':'localhost'}
 if role is not None:request_headers['Authorization']='Bearer '+('invalid-token'if role=='invalid'else roles[role])
 request_headers.update(headers or{});prepared=requests.Request(method,'http://127.0.0.1:5656/api/backups/'+path,headers=request_headers,data=data,files=files_input,params=query).prepare();raw=prepared.body or b''
 if isinstance(raw,str):raw=raw.encode()
 response=requests.Session().send(prepared,timeout=20,allow_redirects=False)
 selected={key:response.headers[key]for key in ['content-type','content-length','content-disposition','www-authenticate','x-accel-redirect','x-content-type-options']if key in response.headers};output={'status':response.status_code,'headers':selected}
 if method!='HEAD'and response.content and'json'in selected.get('content-type',''):
  output['json']=response.json()
 else:output.update(body_length=len(response.content),body_sha256=hashlib.sha256(response.content).hexdigest())
 response.close();checks.append({'name':name,'path':prepared.path_url,'method':method,'headers':dict(prepared.headers),'request_body_hex':raw.hex(),'request_body_sha256':hashlib.sha256(raw).hexdigest(),'expected':output,'pre_files':pre,'post_files':files()})
for role in ['admin','standard','basic','inactive','blocked',None,'invalid']:
 case('list-'+str(role),'',role=role)
 case('token-'+str(role),'dispatcharr-backup-rust-probe-a.zip/download-token/',role=role)
 case('download-'+str(role),'dispatcharr-backup-rust-probe-a.zip/download/',role=role)
 case('delete-'+str(role),'dispatcharr-backup-rust-probe-a.zip/delete/',method='DELETE',role=role)
 case('upload-'+str(role),'upload/',method='POST',role=role,files_input={'file':('fixture.zip',b'upload bytes','application/zip')})
 case('schedule-'+str(role),'schedule/',role=role)
for filename in ['dispatcharr-backup-rust-probe-a.zip','arbitrary-upload.txt','caf\xe9.zip','amp&plus+=.zip','missing.zip','a..b.zip','back\\slash.zip','fixture-directory']:
 case('token-name-'+filename,filename+'/download-token/')
 case('download-name-'+filename,filename+'/download/')
 case('delete-name-'+filename,filename+'/delete/',method='DELETE')
for role in [None,'standard','blocked','inactive','invalid']:
 for supplied in ['valid','bad','empty']:
  value=token('dispatcharr-backup-rust-probe-a.zip')if supplied=='valid'else('bad'if supplied=='bad'else'')
  case('token-download-'+str(role)+'-'+supplied,'dispatcharr-backup-rust-probe-a.zip/download/',role=role,query={'token':value})
for role in [None,'admin','invalid']:
 case('nonascii-token-'+str(role),'dispatcharr-backup-rust-probe-a.zip/download/',role=role,query={'token':'é'})
case('nul-token','dispatcharr-backup-rust-probe-a.zip/download/',role=None,query={'token':'\x00'})
for method in ['HEAD','POST','PUT','OPTIONS']:
 case('list-method-'+method,'',method=method)
 case('download-method-'+method,'dispatcharr-backup-rust-probe-a.zip/download/',method=method,role=None,query={'token':token('dispatcharr-backup-rust-probe-a.zip')})
for name,kwargs in [('missing',{}),('form-only',{'data':{'file':'text'}}),('json',{'data':'{}','headers':{'Content-Type':'application/json'}}),('plain',{'data':'x','headers':{'Content-Type':'text/plain'}}),('no-extension',{'files_input':{'file':('name',b'data')}}),('duplicate',{'files_input':{'file':('arbitrary-upload.txt',b'data')}}),('traversal-basename',{'files_input':{'file':('../chosen.zip',b'data')}}),('windows-basename',{'files_input':{'file':('C:\\folder\\chosen.zip',b'data')}}),('empty-file',{'files_input':{'file':('empty.zip',b'')}}),('multiple',{'files_input':[('file',('first.zip',b'first')),('file',('last.zip',b'last'))]})]:
 case('upload-body-'+name,'upload/',method='POST',**kwargs)
# Real large multipart transfers are represented as bounded generated segments,
# never expanded into fixture JSON or a whole uploaded body in observer memory.
boundary='rust-probe-backup-large-boundary'
for size in [1021,1022,1023,1024,1025,2048]:
 header=b'Content-Disposition: form-data; name="file"; filename="'+b'x'*(size-len(b'Content-Disposition: form-data; name="file"; filename=""\r\n\r\n'))+b'"\r\n\r\n'
 raw=('--'+boundary+'\r\n').encode()+header+b'data\r\n'+('--'+boundary+'--\r\n').encode()
 case('upload-header-bytes-'+str(size),'upload/',method='POST',data=raw,headers={'Content-Type':'multipart/form-data; boundary='+boundary})
large=[]
def segment(raw):return {'body_hex':raw.hex()}
def multipart_part(name,filename):return ('--'+boundary+'\r\nContent-Disposition: form-data; name="'+name+'"; filename="'+filename+'"\r\nContent-Type: application/octet-stream\r\n\r\n').encode()
for name,size,other in [('above-eight-mib',(8<<20)+1,False),('thirty-two-mib',32<<20,False),('ignored-large-other-file',32<<20,True)]:
 reset();pre=files();filename='large-fixture.bin';segments=[segment(multipart_part('other'if other else'file','ignored.bin'if other else filename)),{'repeat_hex':bytes(range(256)).hex(),'length':size}]
 if other:segments.extend([segment(b'\r\n'),segment(multipart_part('file',filename)),segment(b'small final file')])
 segments.append(segment(('\r\n--'+boundary+'--\r\n').encode()))
 def generate():
  for item in segments:
   if 'body_hex'in item:yield bytes.fromhex(item['body_hex'])
   else:
    pattern=bytes.fromhex(item['repeat_hex']);block=pattern*(65536//len(pattern));remaining=item['length']
    while remaining:count=min(remaining,len(block));yield block[:count];remaining-=count
 total=sum(len(bytes.fromhex(item['body_hex']))if'body_hex'in item else item['length']for item in segments)
 prepared=requests.Request('POST','http://127.0.0.1:5656/api/backups/upload/',headers={'Host':'localhost','Authorization':'Bearer '+roles['admin'],'Content-Type':'multipart/form-data; boundary='+boundary,'Content-Length':str(total)},data=generate()).prepare();prepared.headers.pop('Transfer-Encoding',None)
 response=requests.Session().send(prepared,timeout=90,allow_redirects=False);selected={key:response.headers[key]for key in ['content-type','content-length','content-disposition','www-authenticate','x-accel-redirect','x-content-type-options']if key in response.headers};output={'status':response.status_code,'headers':selected,'json':response.json()};response.close()
 large.append({'name':name,'path':prepared.path_url,'method':'POST','headers':dict(prepared.headers),'segments':segments,'body_length':total,'expected':output,'pre_files':pre,'post_files':files()})
reset()
print(json.dumps({'format_version':1,'scope':'job-free original backup HTTP/filesystem contracts; no backup/restore jobs enqueued','cases':checks,'large_uploads':large,'seed_files':[{'filename':name,'body_hex':data.hex(),'mtime':mtime}for name,data,mtime in seed],'seed_directories':['fixture-directory']},ensure_ascii=False,separators=(',',':')))
