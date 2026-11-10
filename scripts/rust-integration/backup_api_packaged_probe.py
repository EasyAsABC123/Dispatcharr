"""Job-free original backup contracts against canonical native image, disposable shared files only.
Original authors retain full application credit. Raw fixture JWTs remain ignored.
"""
import hashlib,json,os
from pathlib import Path
if os.environ.get('POSTGRES_PASSWORD')!='playback-fixture-only'or os.environ.get('BACKUP_API_DISPOSABLE_FIXTURE')!='playback-fixture-only':raise RuntimeError('Explicit disposable backup file fixture required')
import requests
ROOT=Path('/data/backups');fixture=json.loads(Path('/evidence/backup-api-fixture.json').read_text())
def digest(value):return hashlib.sha256(json.dumps(value,sort_keys=True,separators=(',',':'),ensure_ascii=False).encode()).hexdigest()
def reset():
 for path in ROOT.iterdir():
  if path.is_dir():path.rmdir()
  else:path.unlink()
 for item in fixture['seed_files']:
  path=ROOT/item['filename'];path.write_bytes(bytes.fromhex(item['body_hex']));os.utime(path,(item['mtime'],item['mtime']))
 for name in fixture['seed_directories']:(ROOT/name).mkdir()
def snapshot():
 result={}
 for path in ROOT.iterdir():
  if path.is_dir():result[path.name]={'directory':True}
  else:
   hashed=hashlib.sha256()
   with path.open('rb')as file:
    for block in iter(lambda:file.read(65536),b''):hashed.update(block)
   result[path.name]={'size':path.stat().st_size,'sha256':hashed.hexdigest()}
 return result
def declared_204_delta(name,actual,expected,failures):
 known=['delete-admin','delete-name-dispatcharr-backup-rust-probe-a.zip','delete-name-arbitrary-upload.txt','delete-name-café.zip','delete-name-amp&plus+=.zip']
 if name not in known or failures!=['response']:return False
 if set(actual)!=set(['status','headers','body_length','body_sha256'])or set(expected)!=set(actual):return False
 if type(actual['status'])is not int or type(expected['status'])is not int or actual['status']!=204 or expected['status']!=204:return False
 if type(actual['body_length'])is not int or type(expected['body_length'])is not int or actual['body_length']!=0 or expected['body_length']!=0:return False
 empty=hashlib.sha256(b'').hexdigest()
 if actual['body_sha256']!=empty or expected['body_sha256']!=empty or'content-length'in actual['headers']or expected['headers'].get('content-length')!='40':return False
 normalized=json.loads(json.dumps(expected));normalized['headers'].pop('content-length');return digest(actual)==digest(normalized)
checks=[]
for case in fixture['cases']+fixture['large_uploads']:
 reset();assert snapshot()==case['pre_files'];segments=case.get('segments')
 def generate():
  for item in segments:
   if 'body_hex'in item:yield bytes.fromhex(item['body_hex'])
   else:
    pattern=bytes.fromhex(item['repeat_hex']);block=pattern*(65536//len(pattern));remaining=item['length']
    while remaining:count=min(remaining,len(block));yield block[:count];remaining-=count
 data=generate()if segments else bytes.fromhex(case['request_body_hex']);request=requests.Request(case['method'],'http://candidate:9192'+case['path'],headers=case['headers'],data=data).prepare();request.headers.pop('Transfer-Encoding',None)
 response=requests.Session().send(request,timeout=90,allow_redirects=False);selected={key:response.headers[key]for key in ['content-type','content-length','content-disposition','www-authenticate','x-accel-redirect','x-content-type-options']if key in response.headers};actual={'status':response.status_code,'headers':selected}
 if case['method']!='HEAD'and response.content and'json'in selected.get('content-type',''):actual['json']=response.json()
 else:actual.update(body_length=len(response.content),body_sha256=hashlib.sha256(response.content).hexdigest())
 response.close();failures=[]
 if digest(actual)!=digest(case['expected']):failures.append('response')
 if snapshot()!=case['post_files']:failures.append('filesystem')
 declared=declared_204_delta(case['name'],actual,case['expected'],failures)
 checks.append({'name':case['name'],'passed':not failures,'secondary_passed':not failures or declared,'declared_source_invalid_204_length_delta':declared,'mismatches':failures,'native_status':actual['status'],'source_status':case['expected']['status']})
print(json.dumps({'passed':all(v['passed']for v in checks),'secondary_passed':all(v['secondary_passed']for v in checks),'declared_source_invalid_204_length_deltas':sum(v['declared_source_invalid_204_length_delta']for v in checks),'source_cases':len(checks),'checks':checks},separators=(',',':')))
