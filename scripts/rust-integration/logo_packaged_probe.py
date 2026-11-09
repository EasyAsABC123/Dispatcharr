"""Original owned logo fixture replay against an immutable Rust application.
Runs only in the disposable original DB container. Original authors retain credit.
"""
import base64,hashlib,json,os,sys,time
from pathlib import Path
if os.environ.get('POSTGRES_PASSWORD')!='playback-fixture-only':raise RuntimeError('Disposable fixture required')
sys.path.insert(0,'/app');sys.argv.append('shell');os.environ.setdefault('DJANGO_SETTINGS_MODULE','dispatcharr.settings')
import django
django.setup()
import requests
from apps.channels.models import Logo,Channel,ChannelOverride
from django.db import connection
from django.core.cache import cache
from logo_marker_witness import clear_markers,marker_witness
fixture=json.loads(Path('/evidence/logo-fixture.json').read_text());target='http://candidate:9192';checks=[]
HEADERS=['content-type','content-length','cache-control','last-modified','content-disposition','x-content-type-options','content-security-policy','www-authenticate']
def digest(value):return hashlib.sha256(json.dumps(value,sort_keys=True,separators=(',',':'),ensure_ascii=False).encode()).hexdigest()
def blob(spec):return base64.b64decode(spec.get('prefix',''))+bytes([spec.get('byte',120)])*spec.get('repeat',0)
def wire(spec):
 if spec is None:return b'','application/json'
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
 out.extend(b'--rust-probe-boundary--\r\n');return bytes(out),'multipart/form-data; boundary=rust-probe-boundary'
def prepare(case):
 clear_markers(fixture['owned_paths'])
 for raw in fixture['owned_paths']:
  if not(raw.startswith('/data/logos/rust-probe-')or raw.startswith('/tmp/rust-probe-logo-fixture/')):raise RuntimeError('Unowned fixture path')
  path=Path(raw)
  if path.is_symlink()or path.is_file():path.unlink()
  elif path.is_dir():path.rmdir()
 for raw,body in fixture['files'].items():Path(raw).write_bytes(base64.b64decode(body));os.utime(raw,(1700000000.125,1700000000.125))
 outside=next(raw for raw in fixture['files']if raw.startswith('/tmp/rust-probe-logo-fixture/'));Path('/data/logos/rust-probe-logo-link.png').symlink_to(outside);Path('/data/logos/rust-probe-logo-dir').mkdir()
 Logo.objects.all().delete();Logo.objects.bulk_create([Logo(**row)for row in case['pre']['logos']])
 for row in case['pre']['channels']:Channel.objects.filter(pk=row['id']).update(logo_id=row['logo_id'])
 for row in case['pre']['overrides']:ChannelOverride.objects.filter(pk=row['id']).update(logo_id=row['logo_id'])
 with connection.cursor()as cursor:cursor.execute("SELECT setval('dispatcharr_channels_logo_id_seq',%s,true)",[case['sequence']])
 cache.delete('coresettings:default_user_agent')
 if case.get('cached_agent')is not None:cache.set('coresettings:default_user_agent',case['cached_agent'],300)
 requests.get('http://origin:8080/reset',timeout=5).raise_for_status()
def files():
 result={}
 for raw in fixture['owned_paths']:
  path=Path(raw)
  if path.is_symlink():result[raw]={'kind':'symlink','target':os.readlink(raw)}
  elif path.is_file():body=path.read_bytes();result[raw]={'kind':'file','size':len(body),'sha256':hashlib.sha256(body).hexdigest()}
  elif path.is_dir():result[raw]={'kind':'directory'}
  else:result[raw]={'kind':'missing'}
 return result
def db():return {'logos':list(Logo.objects.order_by('id').values('id','name','url')),'channels':list(Channel.objects.filter(pk__in=fixture['channel_ids']).order_by('id').values('id','logo_id')),'overrides':list(ChannelOverride.objects.filter(pk=fixture['override_id']).values('id','logo_id'))}
def normalize(value,clock):
 if clock and isinstance(value,dict)and isinstance(value.get('cache_url'),str):value={**value,'cache_url':value['cache_url'].rsplit('?v=',1)[0]+'?v=<mtime>'}
 return value
for case in fixture['cases']:
 prepare(case);raw,mime=wire(case['body']);failures=[]
 if hashlib.sha256(raw).hexdigest()!=case['request_body_sha256']:failures.append('request_bytes')
 headers={key.removeprefix('HTTP_').replace('_','-'):value for key,value in case['headers'].items()}
 for expected in case['expected']['responses']:
  response=requests.request(case['method'],target+case['path'],data=raw,headers={**headers,'Content-Type':mime},timeout=45,allow_redirects=False)
  selected={key:response.headers[key]for key in HEADERS if key in response.headers}
  if response.status_code!=expected['status']:failures.append('status')
  if digest(selected)!=digest(expected['headers']):failures.append('headers')
  if 'json'in expected:
   try:equal=digest(normalize(response.json(),case['clock_hash']))==digest(normalize(expected['json'],case['clock_hash']))
   except ValueError:equal=False
   if not equal:failures.append('json')
  elif len(response.content)!=expected['body_length']or hashlib.sha256(response.content).hexdigest()!=expected['body_sha256']:failures.append('bytes')
  response.close()
 if digest(db())!=digest(case['expected']['db']):failures.append('db')
 if digest(files())!=digest(case['expected']['files']):failures.append('files')
 if 'markers'in case['expected']and digest(marker_witness(fixture['owned_paths']))!=digest(case['expected']['markers']):failures.append('processed_file_marker')
 if digest(requests.get('http://origin:8080/counts',timeout=5).json())!=digest(case['expected']['origin_calls']):failures.append('origin_attempts')
 checks.append({'name':case['name'],'passed':not failures,'mismatches':failures})
print(json.dumps({'passed':all(c['passed']for c in checks),'checks':checks,'source_cases':len(checks),'source_http_responses':sum(len(c['expected']['responses'])for c in fixture['cases'])},separators=(',',':')))
