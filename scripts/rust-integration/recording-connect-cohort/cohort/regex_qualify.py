"""Read-only paired qualification of explicitly captured regex scope only."""
import argparse,json,re
from pathlib import Path

def value(row):
 actual=row['actual'];after=actual['after'];message=after['last_message']
 message=re.sub(r'^Processing completed in [0-9]+\.[0-9] seconds\.', 'Processing completed in <actual elapsed> seconds.',message)
 backend=actual['backend'];assert backend['task_id']==actual['parent']
 return {'name':row['name'],'account_status':after['account_status'],'last_message':message,'channels':[{k:r[k]for k in('name','channel_number')}for r in after['rows']],'stream_links':after['stream_links'],'memberships':after['memberships'],'backend_status':backend['status'],'backend_result':backend['result'],'actual_child_count':len(backend['children'])}

def qualify(source,native):
 checks=[]
 for report in(source,native):
  assert report['status']=='passed'and len(report['cases'])==5
  assert all(r['status']=='passed'for r in report['cases'])
 for old,new in zip(source['cases'],native['cases']):
  checks.append({'cell':old['name'],'paired_captured_values_equal':value(old)==value(new)})
 return {'status':'passed'if all(c['paired_captured_values_equal']for c in checks)else'failed','checks':checks,'scope':'five realAPI→ordinaryworker→Source-regex→capturedSQL/profile/link/coarse-error/parent-status-result cells','Source_app_replays_of_previously_qualified_cases':0,'full_M6':False,'full_regex_grammar':False,'complete_Channels_progress_failure_payloads':False,'complete_backend_metadata_parity':False,'physical_cancellation':False,'performance_verified':False}
if __name__=='__main__':
 parser=argparse.ArgumentParser();parser.add_argument('source');parser.add_argument('native');parser.add_argument('output');args=parser.parse_args()
 result=qualify(json.loads(Path(args.source).read_text()),json.loads(Path(args.native).read_text()));Path(args.output).write_text(json.dumps(result,indent=2)+'\n');raise SystemExit(result['status']!='passed')
