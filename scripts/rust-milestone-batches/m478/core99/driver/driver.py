"""Owned raw PG/Redis fixture and real packaged Main HTTP collector; no Django."""
import datetime
import hashlib
import json
import os
from pathlib import Path
import re
import sys
import time
from absolute_http import Observation
from native_state import default, import_existing, state, need

ROOT = Path('/native-root')
OUT = Path('/evidence')

def now(): return datetime.datetime.now(datetime.timezone.utc).isoformat()
def read(path): return json.loads(path.read_bytes())
def save(path, value):
    raw = (json.dumps(value, indent=2, allow_nan=False) + '\n').encode()
    need(len(raw) <= 134217728, 'capture128MiB')
    temporary = path.with_suffix(path.suffix + '.tmp'); temporary.write_bytes(raw); temporary.replace(path)

def main():
    need(os.environ.get('PLUGIN_REPO_DRIVER') == 'owned-native-pluginrepo-only', 'owned raw driver')
    action = os.environ['PLUGIN_REPO_ACTION']; epoch = os.environ.get('PLUGIN_UNSIGNED_EPOCH')
    label = os.environ['PLUGIN_REPO_LABEL']; need(re.fullmatch('[A-Za-z0-9-]{1,48}', label), 'bounded output label')
    database = os.environ['PLUGIN_REPO_DATABASE']; schema = read(ROOT / 'inputs/schema62-authority.json')
    path = OUT / (label + '.json'); need(not path.exists(), 'fresh observation output')
    if action == 'default': result = default(database, schema)
    elif action == 'state': result = state(database, schema)
    elif action == 'clear-owned-redis':
        from native_state import owned, redis_clients
        owned(database); clients = redis_clients(); result = {'before': [], 'cleared_owned_fixture_only': True}
        for index, client in clients.items():
            result['before'].append({'database':index, 'keys':client.dbsize()}); client.flushdb(); need(client.dbsize()==0, 'empty owned Redis'); client.close()
    else:
        authority = read(ROOT / 'inputs/authority.json')[epoch]
        source_path = ROOT / 'inputs' / authority['source_filename']; source_bytes = source_path.read_bytes()
        need(hashlib.sha256(source_bytes).hexdigest()==authority['source_sha256'], 'exact retained Source capture')
        source = json.loads(source_bytes); plan = read(ROOT / 'inputs' / authority['plan_filename'])
        if action == 'import': result = import_existing(database, source, schema)
        elif action == 'collect': result = collect(database, epoch, source, plan, schema, path)
        else: raise ValueError('known owned fixture action')
    need(not any(name=='django' or name.startswith(('django.', 'apps.', 'dispatcharr.')) for name in sys.modules), 'zero Original application imports')
    save(path, result); print(json.dumps({'action':action, 'output':str(path), 'Source_application_runs':0}))

def observed(path, body, headers, seconds=20):
    observation=Observation('http://127.0.0.1:9192',seconds=seconds)
    try:
        response=observation.request(path,headers,method='POST' if body is not None else 'GET',body=body)
        data=response.raw.read(1048577);need(len(data)<=1048576,'response1MiB')
        try:parsed=json.loads(data)
        except(ValueError,UnicodeDecodeError):parsed=None
        result={'status':response.status_code,'headers':list(response.raw.getheaders()),'response_body_hex':data.hex(),'response_body_bytes':len(data),'response_body_sha256':hashlib.sha256(data).hexdigest(),'json':parsed,'absolute_observation':observation.complete()}
    finally: close=observation.cancel()
    result['physical_close']=close
    return result

def collect(database, epoch, source, plan, schema, target):
    count=source['actual_request_count'];need(count in (16,24,1) and source['capture_complete'] is True and len(plan['cases'])==count,'complete retained epoch')
    result={'capture_only':True,'passed':False,'capture_complete':False,'actual_request_count':0,'completed_capture_count':0,'production_Main_exercised':True,'full_M8_accepted':False,'raw_body_byte_parity_claimed':False,'epoch':epoch,'requests':[],'readiness':[],'before_requests':None,'phase_began_utc':now()}
    save(target,result);ready=time.monotonic()+45
    while True:
        need(time.monotonic()<ready,'Main readiness45s')
        try:r=observed('/health/ready',None,{'Accept':'application/json'},seconds=1)
        except (ConnectionError,TimeoutError,OSError) as error:r={'error':{'type':type(error).__name__,'message':str(error)}}
        result['readiness'].append(r);save(target,result)
        if r.get('status')==200:break
        time.sleep(.05)
    result['before_requests']=state(database,schema);save(target,result)
    roles={row['alias']:row for row in plan['roles']};users={row['id']:row for row in source['seed']['users']};began=time.monotonic()
    try:
        for actual,spec in zip(source['requests'],plan['cases']):
            need(time.monotonic()-began<420,'business phase420s')
            keys=['index','name','actor','method','path','body_hex','body_sha256','body_bytes']
            need(all(actual[k]==spec[k] for k in keys),'exact retained literal input')
            need(spec['method']=='POST' and re.fullmatch(r'/api/plugins/repos/(?:preview/|[0-9]{1,12}/refresh/)?',spec['path']),'owned mutation routes only')
            body=bytes.fromhex(spec['body_hex']);need(len(body)==spec['body_bytes'] and hashlib.sha256(body).hexdigest()==spec['body_sha256'],'literal body bytes')
            headers={'Content-Type':spec['content_type'],'Accept':'application/json'}
            if spec['actor']!='anonymous':headers['X-Api-Key']=users[roles[spec['actor']]['id']]['api_key']
            row={**{k:spec[k] for k in keys},'raw_headers_private':headers,'before':state(database,schema),'began_utc':now(),'actual_request_invoked':True,'capture_complete':False}
            result['requests'].append(row);result['actual_request_count']+=1;save(target,result)
            try:row.update(observed(spec['path'],body,headers))
            except Exception as error:
                row['observation_error']={'type':type(error).__name__,'message':str(error)};raise
            row.update(completed_utc=now(),after=state(database,schema),capture_complete=True);result['completed_capture_count']+=1;save(target,result)
        result['capture_complete']=result['actual_request_count']==result['completed_capture_count']==count
        if epoch=='fresh404':
            body=json.dumps({'url':'http://11.253.250.2:18080/numeric-float.json','public_key':Path('/plugin-assets/manifest-public-key.asc').read_text()},separators=(',',':')).encode()
            headers={'Content-Type':'application/json','Accept':'application/json','X-Api-Key':users[roles['admin']['id']]['api_key']}
            diag={'name':'floating-point-signing-capability','Source_authority':'unmeasured numeric context; diagnostic cannot pass full domain','before':state(database,schema),'began_utc':now()}
            result['numeric_diagnostic']=diag;save(target,result);diag.update(observed('/api/plugins/repos/preview/',body,headers));diag.update(completed_utc=now(),after=state(database,schema),full_numeric_domain_accepted=False)
    finally:result['phase_completed_utc']=now();result['phase_elapsed_seconds']=time.monotonic()-began;save(target,result)
    return result

if __name__=='__main__':main()
