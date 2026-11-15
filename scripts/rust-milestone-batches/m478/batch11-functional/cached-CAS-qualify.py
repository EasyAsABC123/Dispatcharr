"""Read-only derived proof for actual Redis cached compare/pop scripts.
Original Dispatcharr/Redis authors retain credit; no runtime/Source replay.
"""
import argparse,hashlib,json,re,subprocess
from pathlib import Path
HERE=Path(__file__).resolve().parent

def matches(rows,index,payload,sha):
 row=rows[index]
 if row.get('db')!=1 or row.get('command')!='EVALSHA '+sha+' 1 celery '+payload:return False
 actual=rows[index+1:index+4]
 return len(actual)==3 and all(r.get('db')==1 and r.get('client_type')=='lua'and r.get('client_address')=='lua'and r.get('command')==wanted for r,wanted in zip(actual,['TYPE celery','LINDEX celery -1','RPOP celery']))

def qualify(binding_path,report_path):
 binding_raw=Path(binding_path).read_bytes();report_raw=Path(report_path).read_bytes();b=json.loads(binding_raw);d=json.loads(report_raw);r=Path(report_path);journal=r.parent.parent/'data/channel-maintenance'/r.parent.name/'redis-monitor-private.jsonl';raw=journal.read_bytes();rows=[json.loads(v)for v in raw.splitlines()]
 smap_raw=Path(b['candidate_source_map']).read_bytes();smap=json.loads(smap_raw);path='rust/crates/job-transport/src/transport.rs';source=subprocess.check_output(['git','show',b['candidate_git_revision']+':'+path]);file_sha=hashlib.sha256(source).hexdigest();assert file_sha==smap[path]
 assert hashlib.sha256(smap_raw).hexdigest()==b['candidate_source_map_sha256']
 script=re.search(rb'const CLAIM_CREATE: &str = r#"(.*?)"#;',source,re.S).group(1);sha=hashlib.sha1(script).hexdigest();assert b"redis.call('LINDEX',KEYS[1],-1)~=ARGV[1]"in script and b"redis.call('RPOP',KEYS[1]);return 1"in script
 old=d['wire'];assert old['journal_sha256']==hashlib.sha256(raw).hexdigest();assert len(d['cases'])==len(old['cases'])==9
 result={'passed':False,'scope':'Derived read-only cached-script predicate on retained actual batch11 frames; original failure remains immutable. No app/Source replay. Not full M6 or full channel parity.','binding_sha256':hashlib.sha256(binding_raw).hexdigest(),'original_report_sha256':hashlib.sha256(report_raw).hexdigest(),'journal_sha256':hashlib.sha256(raw).hexdigest(),'candidate_git':b['candidate_git_revision'],'candidate_image':b['candidate_image_id'],'source_map_sha256':hashlib.sha256(smap_raw).hexdigest(),'candidate_transport_sha256':file_sha,'claim_script_sha1':sha,'claim_script_sha256':hashlib.sha256(script).hexdigest(),'claim_script_utf8':script.decode(),'original_wire_passed':old['passed'],'original_report_passed':d['passed'],'cases':[]}
 for case,prior in zip(d['cases'],old['cases']):
  ident=case['task_id'];publications=[]
  for index,row in enumerate(rows):
   command=row.get('command','')
   if row.get('db')!=1 or not command.upper().startswith('LPUSH CELERY '):continue
   payload=command.split(' ',2)[2]
   try:
    frame=json.loads(payload)
    if isinstance(frame,str):frame=json.loads(frame)
   except(ValueError,TypeError):continue
   if frame.get('headers',{}).get('id')==ident:publications.append((index,payload))
  claims=[]
  if len(publications)==1:
   pub_index,payload=publications[0]
   claims=[{'journal_index':i,'actual_cached_script':rows[i],'actual_atomic_commands':rows[i+1:i+4]}for i in range(pub_index+1,len(rows))if matches(rows,i,payload,sha)]
  previous=prior['checks'];checks={'same_source_case_identity':case['name']==prior['name']and ident==prior['task_id'],'original_sole_wire_failure_is_literal_eval_predicate':previous.get('actual_compare_and_pop_script_with_same_task')is False and all(v is True for k,v in previous.items()if k!='actual_compare_and_pop_script_with_same_task'),'all_actual_body_results_SQL_events_passed':case['passed']is True,'exactly_one_actual_publication':len(publications)==1,'exactly_one_actual_cached_exact_raw_claim_and_atomic_RPOP':len(claims)==1}
  result['cases'].append({'name':case['name'],'task_id':ident,'checks':checks,'passed':all(checks.values()),'actual_CAS_evidence':claims})
 result['retained_independent_components']={'no_primary_or_cleanup_error':not any(d.get(k)for k in ['primary_error','observer_cleanup_error','trigger_cleanup_error']),'observer_role_normal0':d.get('observer_role_stop',{}).get('actual_exit')==0 and d['observer_role_stop']['all_original_process_generations_absent']is True and d['observer_role_stop']['stdout_EOF']is True and d['observer_role_stop']['stderr_EOF']is True and d['observer_role_stop']['forced']is False and not d['observer_role_stop']['failures'],'ordinary9':d['ordinary_cases_complete']is True,'HTTP98':d['HTTP98']['passed']is True,'physical':d['physical']['passed']is True,'observer_terminal':d['observer_final']['terminal_barrier_observed']is True and d['observer_final']['thread_settled']is True and d['observer_final']['observer_error']is None,'outer_cleanup_not_qualified_by_this_reader':True}
 result['derived_wire_passed']=all(c['passed']for c in result['cases']);result['passed']=result['derived_wire_passed']and all(result['retained_independent_components'].values());result['full_milestone_accepted']=False;result['performance_claims']=False;return result

if __name__=='__main__':
 p=argparse.ArgumentParser();p.add_argument('--binding',required=True);p.add_argument('--report',required=True);p.add_argument('--output',required=True);a=p.parse_args();result=qualify(a.binding,a.report);Path(a.output).write_text(json.dumps(result,indent=2)+'\n');print(json.dumps({'passed':result['passed'],'cases':len(result['cases']),'original_wire_passed':result['original_wire_passed'],'claim_script_sha1':result['claim_script_sha1']}));raise SystemExit(0 if result['passed']else 1)
