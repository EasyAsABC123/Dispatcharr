#!/usr/bin/env python3
"""Publish bounded guarded-server refresh evidence; original authors retain AGPL credit."""
import argparse,hashlib,json,re
from pathlib import Path
ROOT=Path(__file__).resolve().parents[2]
def sha(path):return hashlib.sha256(path.read_bytes()).hexdigest()
def load(path):return json.loads(path.read_text())
def main():
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('--source',required=True,type=Path);a=p.parse_args();source=a.source.resolve();i=load(source/'integration.json')
 assert i['passed']and i['default_database_unchanged']and i['cleanup']=='project removed'
 names={'baseline':'source-native-epg-exhaustion-private.json','status_p0002':'source-native-epg-exhaustion-p0002-private.json','terminal':'source-native-epg-exhaustion-terminal-private.json','status_23514':'source-native-epg-exhaustion-servercode-private.json'}
 counts={'baseline':52,'status_p0002':25,'terminal':29,'status_23514':25};dataset={'format_version':1,'scope':'Physically settled guarded PostgreSQL ERROR in typed file/parser exhaustion only','groups':{}}
 for label,name in names.items():
  r=load(source/name);assert r['passed']and len(r['checks'])==counts[label]and all(c['passed']for c in r['checks'])
  data={'checks':r['checks'],'source_cases':[{k:c[k]for k in ['name','source_id','metadata','source_state','index','refresh_lock','file_lock','parser_counter','result_ttl','terminal_fault_hits','fixture_control']if k in c}for c in r['cases']],'native_cases':r['native_cases']}
  if label=='baseline':data.update(resources=r['native_resources'],operational_controls=r['native_operational_controls'])
  if label=='terminal':assert all(c['terminal_fault_hits']==1 for c in r['cases']+r['native_cases'])
  dataset['groups'][label]=data
 reg=load(source/'source-native-refresh-worker-exhaustion-private.json');assert reg['passed']and len(reg['checks'])==80
 dataset['regression']={'checks':reg['checks'],'cases':[{k:c[k]for k in ['name','scope','source_id','audit','metadata','metadata_sha256','repeat']}for c in reg['cases']]}
 dataset['normalizations']=['Source/native task UUIDs are separate publications and each result must match its own ID.','date_done is volatile; both result TTLs checked in [3590,3600].','Owned row IDs/status/message/properties/index/busy-key values match without normalization.']
 out=ROOT/'scripts/rust-job-contracts';data_path=out/'source-native-epg-guarded-1.json';data_path.write_text(json.dumps(dataset,indent=2)+'\n')
 files={}
 for rel in ['rust/crates/ingest-epg/src/file_job.rs','rust/crates/ingest-epg/src/reporter.rs','rust/crates/job-transport/src/refresh_worker.rs','rust/crates/job-transport/src/refresh_worker_peer.rs','rust/crates/ingest-epg/tests/file_job_exhaustion.rs']:
  path=source/'frozen'/rel;assert sha(path)==i['rust_source_sha256'][rel];files[rel]={'path':str(path.relative_to(ROOT)),'sha256':sha(path)}
 tools={}
 for name in ['run_native_epg_guarded.py','source_native_epg_exhaustion.py','source_native_epg_exhaustion_p0002.py','source_native_epg_exhaustion_terminal.py','source_native_epg_exhaustion_servercode.py','exhaustion_native_compare.py','source_native_refresh_worker_exhaustion.py','epg_refresh_observer.py']:
  rel='scripts/rust-job-contracts/'+name;path=source/'tool-source'/rel;assert sha(path)==i['tool_source_sha256'][rel];tools[rel]={'path':str(path.relative_to(ROOT)),'sha256':sha(path)}
 units={}
 for name in ['ingest-unit.log','job-unit.log']:
  m=re.search(r'test result: ok\. (\d+) passed; 0 failed; (\d+) ignored',(source/name).read_text());assert m;units[name]={'passed':int(m[1]),'external_ignored':int(m[2])}
 assert 'Finished `dev` profile'in(source/'clippy.log').read_text()and'3 passed; 0 failed'in(source/'native-refresh-worker-physical.log').read_text()
 summary={'passed':True,'scope':dataset['scope'],'guarded_component_checks':131,'targeted_refresh_regression_checks':80,'actual_source_native_worker_pairs':10,'terminal_select_native_fault_hits':[1,1],'resource_scenario_groups':7,'additional_worker_redis_resource_tests':3,'frozen_default_units':units,'strict_ingest_job_all_targets_clippy':True,
          'selected_runtime_and_test':files,'selected_tools':tools,'cargo_graph_changes':False,'lib_module_changes':False,'dataset':str(data_path.relative_to(ROOT)),'dataset_sha256':sha(data_path),'rust_source_archive':str((source/'rust-source.tar').relative_to(ROOT)),'rust_source_archive_sha256':i['rust_source_archive_sha256'],
          'raw_evidence_sha256':{name:sha(source/name)for name in ['integration.json',*names.values(),'source-native-refresh-worker-exhaustion-private.json','native-exhaustion-resources.log','native-refresh-worker-physical.log','clippy.log','ingest-unit.log','job-unit.log']},
          'server_error_boundary':{'stage':['guarded_status_UPDATE','guarded_terminal_SELECT','guarded_terminal_status_UPDATE'],'severity':'PostgreSQL ERROR only','failed_query_connection_close':'awaited before caught authority','lease':'actual Released required','diagnostic_field_max_bytes':4096,'diagnostic_total_max_bytes':8192,'sqlstate_allowlist':False,'diagnostics_are_python_exception_frames':False},
          'default_database_unchanged':True,'cleanup':'project removed','app_scheduler_default_consumer_202_verified':False,'wholeworkspace_validation':False,'performance_verified':False,
          'historical_scope_preserved':'Component52 remains immutable; its terminal and P0002 boundaries are not retrospectively reclassified.',
          'failed_attempt':{'path':'artifacts/rust/m6-source-native-epg-guarded-1','cause':'Second controller reused scratch-directory name and failed before P0002 execution; baseline52 and resource peer passed. Preserved/cleaned, exact same Rust reused.'},
          'limitations':['FATAL/PANIC, native pool/admission/capacity/reset-before-try, unclassified decoding, oversized diagnostic and IO/close ambiguity remain operational.','Coarse Failed outside explicit file/parser exhaustion is not promoted.','UPDATE0 and releaseNotOwner/PENDING remain conservative native controls pending independent Original race capture.','Aborted waiter return is unobserved.','Guarded terminal-status-update reuses verified reporter boundary; no separate actual in-progress double-update worker fault is claimed here.','No generic exception module/args/traceback rendering or unrelated frozen sibling acceptance.'],
          'credit':'Original Dispatcharr authors retain full application/algorithm credit and AGPL; framework authors retain their credit/licenses.'}
 target=out/'verified-source-native-epg-guarded-1.json';target.write_text(json.dumps(summary,indent=2)+'\n');print(json.dumps({'passed':True,'summary':str(target),'sha256':sha(target)}))
if __name__=='__main__':main()
