#!/usr/bin/env python3
"""Publish Source-only representative guarded SQL errors; original authors retain AGPL credit."""
import hashlib,json
from pathlib import Path
ROOT=Path(__file__).resolve().parents[2]
def sha(path):return hashlib.sha256(path.read_bytes()).hexdigest()
def main():
 out=ROOT/'scripts/rust-job-contracts';data={'format_version':1,'source_only':True,'groups':{}};tools={};evidence={}
 for code,dirname,rawname,scripts in [('P0002','m6-original-epg-exhaustion-p0002-1','source-epg-exhaustion-p0002-private.json',['run_epg_exhaustion_p0002.py','source_epg_exhaustion_p0002.py']),('23514','m6-original-epg-exhaustion-servercode-1','source-epg-exhaustion-servercode-private.json',['run_epg_exhaustion_servercode.py','source_epg_exhaustion_servercode.py'])]:
  path=ROOT/'artifacts/rust'/dirname;i=json.loads((path/'integration.json').read_text());r=json.loads((path/rawname).read_text())
  assert i['passed']and i['default_database_unchanged']and i['cleanup']=='project removed'and r['passed']and len(r['checks'])==15 and r['fault_sqlstate']==code
  data['groups'][code]={'checks':r['checks'],'cases':[{key:c[key]for key in ['name','source_id','metadata','source_state','index','file_lock','parser_counter','refresh_lock']}for c in r['cases']]}
  evidence[code]={'artifact':str(path.relative_to(ROOT)),'raw_report_sha256':sha(path/rawname),'integration_sha256':sha(path/'integration.json'),'checks':15,'cleanup':'project removed','default_database_unchanged':True}
  for name in scripts+['epg_refresh_observer.py']:
   relative='scripts/rust-job-contracts/'+name;tool=path/'tool-source'/relative;assert sha(tool)==i['tool_source_sha256'][relative]
   if relative in tools:assert tools[relative]['sha256']==sha(tool)
   else:tools[relative]={'path':str(tool.relative_to(ROOT)),'sha256':sha(tool)}
 path=out/'source-original-epg-guarded-1.json';path.write_text(json.dumps(data,indent=2)+'\n')
 summary={'passed':True,'scope':'Actual unchanged Original guarded exhaustion status UPDATE P0002/23514; Source only','checks':30,'actual_worker_executions':4,'original_revision':'1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430','original_image':'ghcr.io/dispatcharr/dispatcharr@sha256:34f9011514edd31ef0ff111b5f256d2e37614fcb08a6ee1ed9eccec9251a856f','dataset':str(path.relative_to(ROOT)),'dataset_sha256':sha(path),'evidence':evidence,'selected_tools':tools,'earlier_guard_and_terminal_contract':'scripts/rust-job-contracts/verified-original-epg-exhaustion-1.json','native_execution_verified':False,'performance_verified':False,'credit':'Original Dispatcharr authors retain full application/algorithm credit and AGPL; framework authors retain their credit/licenses.'}
 path=out/'verified-original-epg-guarded-1.json';path.write_text(json.dumps(summary,indent=2)+'\n');print(json.dumps({'passed':True,'summary':str(path),'sha256':sha(path)}))
if __name__=='__main__':main()
