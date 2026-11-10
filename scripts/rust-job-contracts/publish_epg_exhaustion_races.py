#!/usr/bin/env python3
"""Publish actual Original exhaustion race observations; authors retain AGPL credit."""
import hashlib,json
from pathlib import Path
ROOT=Path(__file__).resolve().parents[2]
def sha(path):return hashlib.sha256(path.read_bytes()).hexdigest()
def main():
 path=ROOT/'artifacts/rust/m6-original-epg-exhaustion-races-1';i=json.loads((path/'integration.json').read_text());r=json.loads((path/'source-epg-exhaustion-races-private.json').read_text());assert i['passed']and i['cleanup']=='project removed'and i['default_database_unchanged']and r['passed']and len(r['checks'])==55 and len(r['cases'])==6
 data={'format_version':1,'source_only':True,'scope':r['scope'],'checks':r['checks'],'cases':r['cases'],'release_source_sha256':r['original_release_source_sha256'],'renewer_source_sha256':r['original_renewer_source_sha256']}
 out=ROOT/'scripts/rust-job-contracts';target=out/'source-original-epg-exhaustion-races-1.json';target.write_text(json.dumps(data,indent=2)+'\n')
 tools={}
 for name in ['run_epg_exhaustion_races.py','source_epg_exhaustion_races.py','epg_refresh_observer.py']:
  rel='scripts/rust-job-contracts/'+name;tool=path/'tool-source'/rel;assert sha(tool)==i['tool_source_sha256'][rel];tools[rel]={'path':str(tool.relative_to(ROOT)),'sha256':sha(tool)}
 summary={'passed':True,'scope':'Actual unchanged Original knownbusy480 UPDATE0 publication and release-token race return semantics; no native parity','checks':55,'actual_worker_executions':6,'original_revision':i['original_revision'],'dataset':str(target.relative_to(ROOT)),'dataset_sha256':sha(target),'raw_report_sha256':sha(path/'source-epg-exhaustion-races-private.json'),'integration_sha256':sha(path/'integration.json'),'private_release_implementation_sha256':sha(path/'source-release-private.json'),'selected_tools':tools,'default_database_unchanged':True,'cleanup':'project removed','native_execution_verified':False,'performance_verified':False,'observations':['Concurrent actual Django collector deletion after witnessed blocked status UPDATE yields affected0, real progress publication1, SUCCESS/null.','Replacing refresh key before finally yields UPDATE1, publication1, SUCCESS/null; Original unconditional DEL removes replacement.','Deleting refresh key before finally yields UPDATE1, publication1, SUCCESS/null.','All cases complete before renewal interval120s; both file/parser reasons are captured.'],'limitations':['Native must preserve foreign ownership; Source deletion of a replacement is not authorization to remove a foreign key.','Native UPDATE0/NotOwner controls remain historical conservative outcomes until independently mapped/tested.','No unbounded concurrency/timing generalization or renewer-race authority.'],'credit':'Original Dispatcharr authors retain full application/algorithm credit and AGPL; framework authors retain their credit/licenses.'}
 target=out/'verified-original-epg-exhaustion-races-1.json';target.write_text(json.dumps(summary,indent=2)+'\n');print(json.dumps({'passed':True,'summary':str(target),'sha256':sha(target)}))
if __name__=='__main__':main()
