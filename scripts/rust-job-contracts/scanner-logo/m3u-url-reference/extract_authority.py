"""Offline scoped extraction from the failed Source3 cohort; never fabricates rows.
Original Dispatcharr/dependency authors retain code and algorithm credit.
"""
import argparse,hashlib,json
from pathlib import Path

def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def main():
 ap=argparse.ArgumentParser();ap.add_argument('--evidence',type=Path,required=True);ap.add_argument('--output',type=Path,required=True);a=ap.parse_args();e=a.evidence.resolve();out=a.output.resolve();out.mkdir(parents=True,exist_ok=False)
 raw=e/'source-url-hash-private.json';r=json.loads(raw.read_text());i=json.loads((e/'integration.json').read_text());plan=json.loads((e/'pair-tools/predeclared-plan.json').read_text());assert r['passed']is False and i['passed']is False and i['default_database_unchanged']is True and i['cleanup']=='owned project removed';assert len(r['cases'])==17 and len(r['checks'])==182 and sum(c['passed']is False for c in r['checks'])==1;assert [v['label']for v in r['cases']]==[v['label']for v in plan['actual_worker_steps'][:17]]
 case_summaries=[]
 for c,item in zip(r['cases'],plan['actual_worker_steps']):
  assert c['sealed_input']==item and len(c['results'])==1+len(item['expected_direct_child_families'])
  assert [v['headers']['task']for v in c['queue_frames']]==item['expected_direct_child_families']
  assert c['results'][c['parent_id']]['metadata']['result']is None
  assert all(v['metadata']['status']=='SUCCESS'and v['metadata']['task_id']==ident and v['metadata']['traceback']is None and 3500<=v['ttl']<=3600 for ident,v in c['results'].items())
  assert all(v['headers']['parent_id']==c['parent_id']and v['headers']['root_id']==c['parent_id']for v in c['queue_frames'])
  case_summaries.append({'label':c['label'],'group':c['group'],'created':item['account_created_by_this_parent'],'actual_task_count':len(c['results']),'account_id':c['account_id'],'proposed_native_in_domain':c['group']!=12,'selected_account_streams_after':len([v for v in c['after']['streams']if v['m3u_account_id']==c['account_id']]),'global_owned_streams_after':len(c['after']['streams'])})
 data={'passed':True,'scope':'Exactly17 completed actual pinned Original scanner/default-worker steps; failedcohort remainsfalse; no native acceptance','cohort_passed':False,'native_verified':False,'performance_verified':False,'Source_revision':i['original_revision'],'cohort_source_raw_sha256':sha(raw),'cohort_integration_sha256':sha(e/'integration.json'),'plan_sha256':sha(e/'pair-tools/predeclared-plan.json'),'provenance':r['provenance'],'cases':r['cases']};(out/'Source-URL-completed17.json').write_text(json.dumps(data,indent=2)+'\n')
 posts=[json.loads(v)for v in(e/'periodic-worker-postrun.jsonl').read_text().splitlines()];parents=[v for v in posts if v['task']=='core.tasks.scan_and_process_files'];root=parents[-1]['id'];pubs=[json.loads(v)for v in(e/'periodic-worker-children.jsonl').read_text().splitlines()];children=[v for v in pubs if v['headers'].get('root_id')==root];ids={root}|{v['headers']['id']for v in children};assert len(posts)==49 and len(ids)==3
 partial={'scope':'Partial step18 actual publication/postrun/SQL/filepost/queue observations only; NO retained backendmetadata/fullcase projection','cohort_passed':False,'step':'unsupported-list','parent_id':root,'full_backend_metadata_retained':False,'before_after_SQL_Channels_projection_retained':False,'Source_postruns':[v for v in posts if v['id']in ids],'publications':children,'controller_result_ID_TTL_check':next(v for v in r['checks']if v['name']=='unsupported-list-result-ID-and-TTL'),'backend_ledger_gap':'Unprefixed wildcard did not match configured celery-tasks: prefix; absence is not absence ofresults','step19':'UNEXECUTED'}
 for name in ['prerun','received','sql','filepost']:
  partial[name]=[json.loads(v)for v in(e/('periodic-worker-'+name+'.jsonl')).read_text().splitlines()if json.loads(v)['id']in ids]
 ledger=[]
 for line in(e/'transport-ledger.jsonl').read_text().splitlines():
  v=json.loads(line)
  if v['case']=='unsupported-list':ledger.append(v)
 assert ledger and all(v['backend']=={}for v in ledger);partial['transport_ledger']=ledger
 assert next(v for v in partial['Source_postruns']if v['task']=='apps.m3u.tasks.refresh_single_m3u_account')['result']is None
 (out/'Source-partial18-evidence.json').write_text(json.dumps(partial,indent=2)+'\n')
 summary={'passed':True,'scope':'Scoped Source authority only, not failedcohort or native parity','cohort_passed':False,'complete_steps':17,'complete_step_tasks':46,'all_observed_tasks_including_partial':49,'created_steps_complete':sum(v['created']for v in case_summaries),'existing_steps_complete':sum(not v['created']for v in case_summaries),'cases':case_summaries,'step18_full_backendmetadata':False,'step19_executed':False,'default_database_unchanged':True,'raw_binding':{'source':sha(raw),'integration':sha(e/'integration.json'),'transport_ledger':sha(e/'transport-ledger.jsonl')},'Source_dataset_sha256':sha(out/'Source-URL-completed17.json'),'partial18_sha256':sha(out/'Source-partial18-evidence.json'),'original_authors_credit':'Original Dispatcharr/dependency contributors retain code/algorithm/licenses'};(out/'verified-Source17.json').write_text(json.dumps(summary,indent=2)+'\n');print(json.dumps({'passed':True,'complete_steps':17,'tasks':46,'output':str(out)}))
if __name__=='__main__':main()
