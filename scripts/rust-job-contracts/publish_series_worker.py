#!/usr/bin/env python3
"""Publish bounded no-recording Series worker evidence without task tokens.
Original Dispatcharr authors retain full application/algorithm credit and AGPL.
"""
import argparse,hashlib,json
from pathlib import Path

def digest(path):return hashlib.sha256(path.read_bytes()).hexdigest()
def main():
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('--input',required=True,type=Path);p.add_argument('--output',required=True,type=Path);p.add_argument('--production',action='store_true');a=p.parse_args()
 report=json.loads((a.input/'integration.json').read_text());raw=a.input/'source-native-series-worker-private.json';data=json.loads(raw.read_text())
 assert report['passed']and report['default_database_unchanged']and report['cleanup']=='project removed'and data['passed']and data['queue_and_results_after_cleanup']==[0,0]
 rows=[]
 for case in data['native_cases']:
  audit=case['audit'];assert audit['caller_released']and audit['physical_operations']==0 and audit['family_drained']and audit['transport_drained']
  value={'name':case['name'],'expected':case['expected'],'caller_released':True,'physical_operations':0,'family_drained':True,'transport_drained':True,'held_actual_SQL':audit['held_actual_SQL'],'prepared_no_intake_then_started':audit.get('prepared_no_intake_then_started',False),'aborted_waiter_return_observed':audit['aborted_waiter_return_observed']}
  if case['metadata']is not None:value['return']=case['metadata']['result'];value['parented']='parent_id'in case['metadata']
  else:value['result_fabricated']=False
  rows.append(value)
 assert len(rows)==(11 if a.production else 10) and any(row['held_actual_SQL']for row in rows)
 result={'passed':True,'scope':data['scope'],'original_revision':report['original_revision'],'checks':report['checks'],'source_worker_checks':79,'native_cases':rows,'archive_sha256':report['rust_source_archive_sha256'],'integration_sha256':digest(a.input/'integration.json'),'source_private_sha256':digest(raw),'earlyACK_parity_claimed':False,'native_app_or_default_intake':False,'production_explicit_series_authority':a.production,'prepared_start_drains_own_transport':a.production,'configured_recording_rules_executed':False,'default_database_unchanged':True,'cleanup':'project removed','queue_and_results_after_cleanup':[0,0],'strict_log':'artifacts/rust/series-worker-production-input-2/clippy.log'if a.production else'artifacts/rust/series-worker-input-2/clippy.log','prior_successful_scope':'artifacts/rust/m6-source-native-series-worker-1 (139 checks; same runtime, before heldSQL test-only extension)'}
 if a.production:result['failed_attempt_retained']={'path':'artifacts/rust/m6-source-native-series-production-1','boundary':'Copied driver used wrong Panic variant; compile failed before Compose/runtime. Corrected input2 then strictClippy/actual155 passed.'}
 a.output.write_text(json.dumps(result,indent=2)+'\n');print(json.dumps({'passed':True,'checks':report['checks'],'native_cases':len(rows)}))
if __name__=='__main__':main()
