#!/usr/bin/env python3
"""Publish Source-first typed refresh exhaustion evidence; no transport authority.
Original Dispatcharr authors retain application/algorithm credit and AGPL.
Framework authors retain all respective credit/licenses.
"""
import argparse,hashlib,json
from pathlib import Path
ROOT=Path(__file__).resolve().parents[2]
def sha(path):return hashlib.sha256(path.read_bytes()).hexdigest()
def load(path):return json.loads(path.read_text())
def main():
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('--output',type=Path,default=ROOT/'scripts/rust-job-contracts');a=p.parse_args();out=a.output.resolve();out.mkdir(exist_ok=True,parents=True)
 groups=[('normal','m6-original-epg-exhaustion-1','source-epg-exhaustion-private.json',15,['run_epg_exhaustion.py','source_epg_exhaustion.py']),('status_fault','m6-original-epg-exhaustion-fault-1','source-epg-exhaustion-fault-private.json',29,['run_epg_exhaustion_fault.py','source_epg_exhaustion_fault.py']),('terminal_fault','m6-original-epg-exhaustion-terminal-1','source-epg-exhaustion-terminal-private.json',17,['run_epg_exhaustion_terminal.py','source_epg_exhaustion_terminal.py'])]
 dataset={'format_version':1,'source_only':True,'groups':{}};evidence={};tools={}
 for group,name,raw_name,count,names in groups:
  path=ROOT/'artifacts/rust'/name;i=load(path/'integration.json');r=load(path/raw_name);assert i['passed']and i['cleanup']=='project removed'and i['default_database_unchanged']and r['passed']and len(r['checks'])==count
  cases=[]
  for case in r['cases']:
   assert case['metadata']['status']=='SUCCESS'and case['metadata']['result']is None and not case['children']
   cases.append({k:case[k]for k in ['name','source_id','metadata','index','refresh_lock','file_lock','parser_counter','source_state','terminal_fault_hits','fixture_control']if k in case})
  dataset['groups'][group]={'checks':r['checks'],'cases':cases}
  evidence[group]={'path':str(path.relative_to(ROOT)),'integration_sha256':sha(path/'integration.json'),'raw_report_sha256':sha(path/raw_name),'checks':count,'default_database_unchanged':True,'cleanup':'project removed'}
  for script in names+['epg_refresh_observer.py']:
   rel='scripts/rust-job-contracts/'+script;tool=path/'tool-source'/rel;assert sha(tool)==i['tool_source_sha256'][rel];tools[group+':'+rel]={'target':rel,'path':str(tool.relative_to(ROOT)),'sha256':sha(tool)}
 data=out/'source-epg-exhaustion-1.json';data.write_text(json.dumps(dataset,indent=2)+'\n')
 summary={'passed':True,'scope':'Actual pinned Original file/parser exhaustion at480, guarded status P0001 and terminal SELECT P0001; Source only','checks':61,'actual_worker_executions':8,'original_revision':'1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430','original_image':'ghcr.io/dispatcharr/dispatcharr@sha256:34f9011514edd31ef0ff111b5f256d2e37614fcb08a6ee1ed9eccec9251a856f','dataset':str(data.relative_to(ROOT)),'dataset_sha256':sha(data),'evidence':evidence,'selected_tools':tools,'native_execution_verified':False,'performance_verified':False,'limitations':['Normal/error/idle states and result are actual Source observations, not implications from static catches.','Terminal fault uses explicitly owned low-privilege role/RLS query-specific P0001 and sequence hit witness; no app/helper replacement.','No reset-before-try failure or other SQLSTATE authority.','Native terminal SELECT error mapping is not proved by this Source capture.'],'credit':'Original Dispatcharr authors retain full application/algorithm credit and AGPL; framework authors retain their credit/licenses.'}
 target=out/'verified-original-epg-exhaustion-1.json';target.write_text(json.dumps(summary,indent=2)+'\n');print(json.dumps({'passed':True,'summary':str(target),'sha256':sha(target)}))
if __name__=='__main__':main()
