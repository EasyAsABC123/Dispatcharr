"""Actual original OutputProfile contracts replayed against a frozen app image.
Original Dispatcharr authors retain full application credit and AGPL license.
Raw fixture inputs contain disposable credentials and remain ignored artifacts.
"""
import ast
import base64
import hashlib
import json
import os
import sys
import threading
import time
from pathlib import Path
from urllib.parse import urlencode
if (os.environ.get('POSTGRES_PASSWORD')!='playback-fixture-only'
        or os.environ.get('OUTPUT_PROFILE_DISPOSABLE_FIXTURE')!='playback-fixture-only'):
    raise RuntimeError('Explicit disposable OutputProfile fixture required')
sys.path.insert(0,'/app');sys.argv.append('shell');os.environ.setdefault('DJANGO_SETTINGS_MODULE','dispatcharr.settings')
import django
django.setup()
import requests
from django.db import connection,transaction
from django.core.cache import cache
from apps.accounts.models import User
from core.models import CoreSettings,OutputProfile
from core.utils import RedisClient
fixture=json.loads(Path('/evidence/output-profile-fixture.json').read_text())
PREFIX='rust-probe-output-profile-'
base=fixture['profiles'];users=[User.objects.get(pk=pk)for pk in fixture['user_ids']]
KEYS=['coresettings:group:stream_settings','coresettings:group:dvr_settings',
      'coresettings:groupver:stream_settings','coresettings:groupver:dvr_settings',
      'coresettings:default_user_agent','coresettings:default_user_agent:ver']
tree=ast.parse(Path('/integration/output_profile_fixture.py').read_text())
wanted={'clear_faults','install_fault','restore','wire','witness','cache_witness'}
exec(compile(ast.Module(body=[node for node in tree.body if isinstance(node,ast.FunctionDef)and node.name in wanted],type_ignores=[]),'frozen-original-fixture-helpers','exec'),globals())
def digest(value):return hashlib.sha256(json.dumps(value,sort_keys=True,separators=(',',':'),ensure_ascii=False,allow_nan=False).encode()).hexdigest()

def reset(case):
    restore()
    with connection.cursor()as cursor:
        for row in case['pre']['db']['users']:
            raw=None if row['sql_null']else json.dumps(row['custom_properties'])
            cursor.execute('UPDATE accounts_user SET custom_properties=%s::jsonb WHERE username=%s',[raw,PREFIX+row['suffix']])
        cursor.execute("DELETE FROM core_coresettings WHERE key IN('stream_settings','dvr_settings')")
        for row in case['pre']['db']['settings']:
            cursor.execute('INSERT INTO core_coresettings(id,key,name,value)VALUES(%s,%s,%s,%s::jsonb)',[row['id'],row['key'],row['name'],json.dumps(row['value'])])
        cursor.execute("SELECT setval('core_coresettings_id_seq',%s,%s)",case['settings_sequence'])
        cursor.execute("SELECT setval('core_outputprofile_id_seq',%s,true)",[case['sequence']])
    client=RedisClient.get_client()
    for key,value in case['pre']['cache'].items():
        cache.delete(key)
        if isinstance(value,dict)and value.get('redis_type')=='set':
            client.sadd(':1:'+key,*value['members'])
        elif key.endswith(':ver')or ':groupver:'in key:
            cache.set(key,123456,timeout=None)
        elif value is not None:
            cache.set(key,value,300)
    if case.get('setup')and 'sql_failure'in case['setup']:
        install_fault(case['setup']['sql_failure'])

checks=[];target='http://candidate:9192'
for index,case in enumerate(fixture['cases']):
    reset(case);failed=[]
    if digest(witness())!=digest(case['pre']['db']):raise RuntimeError('Original fixture DB restore differs at case '+str(index))
    if digest(cache_witness())!=digest(case['pre']['cache']):raise RuntimeError('Original fixture cache restore differs at case '+str(index))
    raw,mime=wire(case['body'])
    if hashlib.sha256(raw).hexdigest()!=case['request_body_sha256']:failed.append('request_bytes')
    response=requests.request(case['method'],target+case['path'],data=raw,
        headers={**case['headers'],'Content-Type':mime},timeout=25,allow_redirects=False)
    selected={key:response.headers[key]for key in ['content-type','content-length','www-authenticate','x-content-type-options']if key in response.headers}
    actual={'status':response.status_code,'headers':selected}
    if 'json'in selected.get('content-type',''):
        try:actual['json']=response.json()
        except ValueError:failed.append('response_json')
    else:actual.update(body_sha256=hashlib.sha256(response.content).hexdigest(),body_length=len(response.content))
    response.close()
    if digest(actual)!=digest(case['expected']['response']):failed.append('response')
    if digest(witness())!=digest(case['expected']['db']):failed.append('db_ids_sequences_values')
    if digest(cache_witness())!=digest(case['expected']['cache']):failed.append('cache')
    checks.append({'name':case['name'],'passed':not failed,'mismatches':failed})
print(json.dumps({'phase':'original-contracts','passed':all(c['passed']for c in checks),'checks':checks}),flush=True)
# Actual SQL ownership/admission proof through the packaged application.
restore();headers={**fixture['cases'][0]['headers'],'Content-Type':'application/json'}
timeouts=[];threads=[]
def abandoned(index):
    try:
        response=requests.post(target+'/api/core/outputprofiles/',json={
            'name':'rust-probe-output-profile-packaged-abandoned-'+str(index),
            'command':'ffmpeg','parameters':'-i pipe:0 -c copy -f mpegts pipe:1'},headers=headers,timeout=1)
        response.close()
    except requests.exceptions.RequestException:timeouts.append(index)
with transaction.atomic():
    with connection.cursor()as cursor:cursor.execute('LOCK TABLE core_outputprofile IN SHARE MODE')
    for index in range(4):
        thread=threading.Thread(target=abandoned,args=(index,));thread.start();threads.append(thread)
    deadline=time.monotonic()+5
    while True:
        with connection.cursor()as cursor:
            cursor.execute('SELECT pg_stat_clear_snapshot()')
            cursor.execute("SELECT count(*)FROM pg_stat_activity WHERE datname=current_database()AND wait_event_type='Lock'AND query LIKE 'INSERT INTO core_outputprofile(name,command,%'")
            count=cursor.fetchone()[0]
        if count==4:break
        if time.monotonic()>deadline:raise RuntimeError('Four packaged mutations did not reach SQL')
        time.sleep(.005)
    checks.append({'name':'four-packaged-owned-mutations','passed':count==4,'mismatches':[]})
    response=requests.post(target+'/api/core/outputprofiles/',data=b'{}',headers=headers,timeout=5)
    overloaded=response.status_code==503;response.close()
    for thread in threads:thread.join(2)
    checks.append({'name':'packaged-overload503-after-callers-timeout','passed':overloaded and len(timeouts)==4,'mismatches':[]if overloaded and len(timeouts)==4 else['admission']})
deadline=time.monotonic()+5
while OutputProfile.objects.filter(name__startswith='rust-probe-output-profile-packaged-abandoned-').count()!=4:
    if time.monotonic()>deadline:raise RuntimeError('Abandoned packaged mutations did not finish')
    time.sleep(.01)
response=requests.post(target+'/api/core/outputprofiles/',json={
    'name':'rust-probe-output-profile-packaged-recovered','command':'ffmpeg','parameters':'-i pipe:0 -c copy -f mpegts pipe:1'},headers=headers,timeout=5)
recovered=response.status_code==201;response.close()
checks.append({'name':'packaged-abandoned-completion-and-recovery','passed':recovered,'mismatches':[]if recovered else['recovery']})
print(json.dumps({'passed':all(c['passed']for c in checks),'checks':checks,
    'source_cases':len(fixture['cases']),'sql_fault_cases':sum(bool((c.get('setup')or{}).get('sql_failure'))for c in fixture['cases']),
    'resource_checks':3},separators=(',',':')))
