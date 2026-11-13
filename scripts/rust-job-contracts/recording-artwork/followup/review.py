#!/usr/bin/env python3
"""Recompute each actual acceptor in a separate Python process/module namespace."""
import argparse,hashlib,json,os,re,subprocess,sys
from pathlib import Path
ROOT=Path(__file__).resolve().parent
EXPECTED=[('physical',m,True,'physical')for m in('recording-read-Ready','update-Ready','update-error-Ready','Channels-reply','result-reply','oldSUCCESS-preserved','fresh-status-merge')]+[('refusal',m,True,'refusal')for m in('missing-recording','oversized-recording-properties','nonlocal-logo')]+[('refusal-corrected','duplicate-casefold-movie',True,'correction'),('refusal-setup-negative','duplicate-casefold-movie',False,'refusal')]
def dataset_rows(root=ROOT):
 rows=json.loads((root/'datasets.json').read_text())
 if type(rows)is not list or len(rows)!=12:raise ValueError('Exact12 retained attempt assignments')
 for i,(row,want)in enumerate(zip(rows,EXPECTED,strict=True),1):
  if row['key']!=f'case{i:02d}'or type(row['passed'])is not bool or(row['kind'],row['mode'],row['passed'],row['tools_bundle'])!=want:raise ValueError('Exact ordered physical/refusal/negative identity')
 return rows
def digest(path):return hashlib.sha256(Path(path).read_bytes()).hexdigest()
def projected_auth(value):
 count=0
 if type(value)is list:
  return sum(projected_auth(child)for child in value)
 if type(value)is not dict:return 0
 auth=(value.get('direction')=='frontend'and value.get('kind_hex')=='70')or(value.get('direction')=='backend'and value.get('kind_hex')=='52')
 cancel=value.get('direction')=='backend'and value.get('kind_hex')=='4b'
 if auth or cancel:
  flag='authentication_payload_projected'if auth else'cancel_secret_projected'
  if not(value.get(flag)is True and 'payload_base64'in value and value['payload_base64']is None and type(value.get('payload_bytes'))is int and value['payload_bytes']>=0 and re.fullmatch('[0-9a-f]{64}',value.get('payload_sha256',''))):raise ValueError('Actual PG authentication/cancel secret projection')
  count+=1
 return count+sum(projected_auth(child)for child in value.values())
def one(evidence,key):
 rows=dataset_rows();dataset=next(v for v in rows if v['key']==key);mapping=json.loads((ROOT/'evidence-map.json').read_text());evidence=Path(evidence)
 for name in dataset['files'].values():
  if digest(evidence/name)!=mapping[name]['sha256']:raise ValueError('Retained evidence hash')
 bundle=ROOT/'controllers'/dataset['tools_bundle'];toolmap=json.loads((bundle/'executed-tool-map.json').read_text())
 if any(digest(bundle/'tools'/name)!=value for name,value in toolmap.items()):raise ValueError('Exact executed tools')
 raw=json.loads((evidence/dataset['files']['raw']).read_text());host=json.loads((evidence/dataset['files']['host']).read_text());review=json.loads((evidence/dataset['files']['review']).read_text())
 if raw['cases'][-1]['name']!=dataset['mode']:raise ValueError('Actual raw case matches declared domain')
 auth_count=projected_auth(raw)
 if (evidence/dataset['files']['default-before']).read_bytes()!=(evidence/dataset['files']['default-after']).read_bytes():raise ValueError('Default raw manifest bytes changed')
 if not(host['default_database_unchanged']is True and host['actual_controller_settled']is True and host['cleanup_budget']['within_budget']is True):raise ValueError('Actual controller/default/cleanup settlement')
 if not all(p['physically_settled']is True and p['pipes_settled']is True and p['collection_invalid']is None for p in host['processes']):raise ValueError('Every actual command settled')
 queries=review['queries']
 if not(review['cleanup_passed']is True and len(queries)==5 and all(q['actual_exit']==0 and q['empty']is True and q['stdout_hex']==q['stderr_hex']==''for q in queries)):raise ValueError('Independent exact-owned cleanup')
 for index,q in enumerate(queries):
  expected='label=com.docker.compose.project='+host['project']if index<3 else'id='+host['owned_containers'][index-3]
  if expected not in q['argv']:raise ValueError('Exact owned query scope')
 if not all(p['original_process_absent']is True and type(p['starttime'])is str and p['starttime'].isdigit()for p in json.loads((evidence/dataset['files']['PID-absence']).read_text())):raise ValueError('Original observed PID absence')
 if dataset['passed']is True:
  if not(dataset['actual_wrapper_exit']==host['actual_host_exit']==host['actual_controller_exit']==0 and host['passed']is True and raw['passed']is True and review['passed']is True):raise ValueError('Actual passed process0')
  sys.path.insert(0,str(bundle/'tools'));from physical_contract import validate
  result=validate(0,raw,json.loads((bundle/'tools/reference.json').read_text()))
  if result!=host['native_acceptance']or result!=raw['native_acceptance']:raise ValueError('Exact recomputed typed acceptor')
  return {'key':key,'passed':True,'kind':dataset['kind'],'mode':dataset['mode'],'acceptance':result,'projected_PG_auth_observations':auth_count,'raw_authentication_replay_verified':False}
 if not(dataset['actual_wrapper_exit']==host['actual_host_exit']==host['actual_controller_exit']==1 and host['passed']is False and raw['passed']is False and review['passed']is False):raise ValueError('Literal preNative negative')
 case=raw['cases'][-1]
 if not(raw['error_type']=='IntegrityError'and 'unique_movie_name_year_language_no_external_id'in raw['error']and case.get('execution')is None and case['native_process']['spawn_state']=='not_started'):raise ValueError('Exact preserved setup negative')
 return {'key':key,'passed':False,'expected_preserved_negative':True,'Native_started':False,'mode':dataset['mode']}
def review(evidence):
 reports=[]
 for row in dataset_rows():
  argv=[sys.executable,str(Path(__file__).resolve()),'--evidence',str(Path(evidence).resolve()),'--one',row['key']]
  # Separate process avoids collisions between the three exact historical
  # physical_contract/seed_spec module namespaces. No application is started.
  proc=subprocess.run(argv,check=True,capture_output=True,text=True,timeout=30,env={**os.environ,'PYTHONDONTWRITEBYTECODE':'1'})
  if len(proc.stdout.encode())>1048576 or proc.stderr:raise ValueError('Bounded clean offline review output')
  reports.append(json.loads(proc.stdout))
 return {'passed':sum(v['passed']is True for v in reports)==11 and sum(v.get('expected_preserved_negative')is True for v in reports)==1,'passing_fixtures':11,'physical_checks':sum(v.get('acceptance',{}).get('physical_checks',0)for v in reports),'preserved_setup_negative':1,'reports':reports,'new_app_execution':False,'runtime_or_performance_transfer':False}
if __name__=='__main__':
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('--evidence',type=Path,required=True);p.add_argument('--one');a=p.parse_args();print(json.dumps(one(a.evidence,a.one)if a.one else review(a.evidence)))
