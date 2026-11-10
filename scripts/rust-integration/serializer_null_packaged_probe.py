"""Frozen-image replay of original nonobject serializer and permission ordering.
Original Dispatcharr authors retain full application credit. Disposable only.
"""
import hashlib,json,os,sys
from pathlib import Path
if os.environ.get('SERIALIZER_NULL_DISPOSABLE_FIXTURE')!='playback-fixture-only'or os.environ.get('POSTGRES_PASSWORD')!='playback-fixture-only':raise RuntimeError('Explicit disposable serializer fixture required')
sys.path.insert(0,'/app');sys.argv.append('shell');os.environ.setdefault('DJANGO_SETTINGS_MODULE','dispatcharr.settings')
import django
django.setup()
from django.db import connection
import requests
fixture=json.loads(Path('/evidence/serializer-null-fixture.json').read_text())
def fingerprint():
 result={}
 with connection.cursor()as cursor:
  for table in fixture['protected_tables']:
   if not all(c in 'abcdefghijklmnopqrstuvwxyz0123456789_'for c in table):raise RuntimeError('Unexpected protected table')
   cursor.execute('SELECT md5(COALESCE(jsonb_agg(to_jsonb(t)ORDER BY t.id)::text,\'[]\'))FROM '+connection.ops.quote_name(table)+' t');result[table]=cursor.fetchone()[0]
 return result
checks=[]
for index,case in enumerate(fixture['cases']):
 before=fingerprint();raw=json.dumps(case['body'],ensure_ascii=False,separators=(',',':')).encode();assert hashlib.sha256(raw).hexdigest()==case['request_body_sha256']
 response=requests.request(case['method'],'http://candidate:9192'+case['path'],data=raw,headers=case['headers'],timeout=20,allow_redirects=False)
 selected={key:response.headers[key]for key in ['content-type','content-length','www-authenticate','x-content-type-options']if key in response.headers}
 actual={'status':response.status_code,'headers':selected}
 if 'json'in selected.get('content-type',''):actual['json']=response.json()
 else:actual.update(body_length=len(response.content),body_sha256=hashlib.sha256(response.content).hexdigest())
 response.close();expected={key:value for key,value in case['expected'].items()if key!='body_hex'};errors=[]
 if json.dumps(actual,sort_keys=True,separators=(',',':'))!=json.dumps(expected,sort_keys=True,separators=(',',':')):errors.append('response')
 if fingerprint()!=before:errors.append('model_mutation')
 checks.append({'case':index,'name':case['name'],'passed':not errors,'mismatches':errors,'native_status':actual['status'],'source_status':expected['status']})
print(json.dumps({'passed':all(case['passed']for case in checks),'source_cases':len(checks),'checks':checks},separators=(',',':')))
