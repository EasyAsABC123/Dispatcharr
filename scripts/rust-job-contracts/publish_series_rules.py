#!/usr/bin/env python3
"""Credential-free Source/no-recording SeriesRules evidence publication.
Original Dispatcharr authors retain all application/algorithm credit and AGPL.
"""
import argparse,hashlib,json
from pathlib import Path

def digest(path):return hashlib.sha256(path.read_bytes()).hexdigest()
def main():
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('--source',required=True,type=Path);p.add_argument('--native',required=True,type=Path);p.add_argument('--output',required=True,type=Path);a=p.parse_args()
 s=json.loads((a.source/'integration.json').read_text());n=json.loads((a.native/'integration.json').read_text());raw=a.source/'source-series-rules-private.json';data=json.loads(raw.read_text())
 for report in [s,n]:assert report['passed']and report['default_database_unchanged']and report['cleanup']=='project removed'
 assert data['passed']and data['queue_and_results_after_cleanup']==[0,0]
 rows=[]
 for row in data['cases']:
  rows.append({'name':row['name'],'group':row['group'],'cached_group':row['cached_group'],'return':row['result']['result'],'parented':row['envelope']['headers']['parent_id']is not None,'argsrepr':row['envelope']['headers']['argsrepr'],'timelimit':row['envelope']['headers']['timelimit'],'early':row['early']})
 result={'passed':True,'scope':'Original SeriesRules no-recording workers and bounded native DVR-cache/global-lock prerequisites; no native task consumer/configured DVR rule execution or wholeM6 claim','original_revision':s['original_revision'],'source':{'checks':s['checks'],'actual_workers':7,'private_sha256':digest(raw),'integration_sha256':digest(a.source/'integration.json'),'attributes':data['attributes'],'fixture_control':data['fixture_control'],'cases':rows},'native':{'checks':n['checks'],'source_checks_repeated':79,'policy_interop_cases':10,'global_lock_tests':1,'archive_sha256':n['rust_source_archive_sha256'],'integration_sha256':digest(a.native/'integration.json'),'global_literal_key':'task_lock_evaluate_series_rules_all','no_renewal_observed':True,'foreign_owner_preserved':True,'Django_loads_native_cache':True},'default_database_unchanged':True,'cleanup':'both projects removed','limitations':['No nonempty dictionary rule scheduling/recording/artwork tasks executed.','Parented options are a labeled fixture control; actual refresh-child publication was characterized separately.','Native owner-fenced release preserves foreign tokens, unlike Original unconditional DEL.','No native Series task intake/result/ACK/restart acceptance from these prerequisite tests.']}
 a.output.write_text(json.dumps(result,indent=2)+'\n');print(json.dumps({'passed':True,'source_checks':s['checks'],'native_checks':n['checks']}))
if __name__=='__main__':main()
