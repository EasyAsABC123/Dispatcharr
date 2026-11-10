#!/usr/bin/env python3
"""Bind typed exhaustion component52 to exact frozen inputs and scoped effects.
Original Dispatcharr authors retain application/algorithm credit and AGPL.
"""
import argparse,hashlib,json,re
from pathlib import Path
ROOT=Path(__file__).resolve().parents[2]
def sha(path):return hashlib.sha256(path.read_bytes()).hexdigest()
def load(path):return json.loads(path.read_text())
def main():
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('--source',required=True,type=Path);a=p.parse_args();source=a.source.resolve();i=load(source/'integration.json');r=load(source/'source-native-epg-exhaustion-private.json');reg=load(source/'source-native-refresh-worker-exhaustion-private.json')
 assert i['passed']and i['cleanup']=='project removed'and i['default_database_unchanged']and r['passed']and len(r['checks'])==52 and reg['passed']and len(reg['checks'])==80
 assert len(r['native_cases'])==4 and len(r['native_operational_controls'])==2 and not r['native_resources']['aborted_waiter_return_observed']
 data={'format_version':1,'scope':'Typed physically settled file/parser480 exhaustion and guarded status P0001 only; no app/default consumer',
       'checks':r['checks'],'source_cases':[{k:c[k]for k in ['name','source_id','metadata','index','source_state','refresh_lock','file_lock','parser_counter','result_ttl']}for c in r['cases']],
       'native_cases':r['native_cases'],'native_operational_controls':r['native_operational_controls'],'native_resources':r['native_resources'],
       'targeted_regression_checks':reg['checks'],'targeted_regression_cases':[{k:c[k]for k in ['name','scope','source_id','audit','metadata','metadata_sha256','repeat']}for c in reg['cases']],
       'normalizations':['Source/native task UUIDs are explicitly distinct publications; each result must match its own ID.','date_done is volatile; both result TTLs are checked in [3590,3600].','Source/native owned source IDs and status/message/properties/index/busy-key values match without normalization.']}
 out=ROOT/'scripts/rust-job-contracts';path=out/'source-native-epg-exhaustion-1.json';path.write_text(json.dumps(data,indent=2)+'\n')
 selected={}
 for name in ['rust/crates/ingest-epg/src/file_job.rs','rust/crates/ingest-epg/src/reporter.rs','rust/crates/job-transport/src/refresh_worker.rs','rust/crates/ingest-epg/tests/file_job_exhaustion.rs']:
  frozen=source/'frozen'/name;assert sha(frozen)==i['rust_source_sha256'][name];selected[name]={'path':str(frozen.relative_to(ROOT)),'sha256':sha(frozen)}
 tools={}
 for name in ['run_native_epg_exhaustion.py','source_native_epg_exhaustion.py','exhaustion_native_compare.py','source_native_refresh_worker_exhaustion.py','epg_refresh_observer.py']:
  relative='scripts/rust-job-contracts/'+name;frozen=source/'tool-source'/relative;assert sha(frozen)==i['tool_source_sha256'][relative];tools[relative]={'path':str(frozen.relative_to(ROOT)),'sha256':sha(frozen)}
 units={}
 for name in ['ingest-unit.log','job-unit.log']:
  match=re.search(r'test result: ok\. (\d+) passed; 0 failed; (\d+) ignored',(source/name).read_text());assert match;units[name]={'passed':int(match[1]),'external_ignored':int(match[2])}
 assert 'Finished `dev` profile'in(source/'clippy.log').read_text()and'3 passed; 0 failed'in(source/'native-refresh-worker-physical.log').read_text()
 summary={'passed':True,'scope':data['scope'],'component_checks':52,'targeted_regression_checks':80,'actual_source_native_worker_pairs':4,'operational_no_result_controls':2,'resource_scenario_groups':5,'additional_worker_redis_tests':3,'default_units':units,'strict_ingest_job_all_targets_clippy':True,
          'original_revision':i['original_revision'],'source_contract':'scripts/rust-job-contracts/verified-original-epg-exhaustion-1.json','selected_runtime_and_test':selected,'selected_tools':tools,'cargo_graph_changes':False,'lib_module_changes':False,
          'dataset':str(path.relative_to(ROOT)),'dataset_sha256':sha(path),'rust_source_archive':str((source/'rust-source.tar').relative_to(ROOT)),'rust_source_archive_sha256':i['rust_source_archive_sha256'],
          'raw_evidence_sha256':{name:sha(source/name)for name in ['integration.json','source-native-epg-exhaustion-private.json','source-native-refresh-worker-exhaustion-private.json','native-exhaustion-resources.log','native-refresh-worker-physical.log','clippy.log','ingest-unit.log','job-unit.log']},
          'default_database_unchanged':True,'cleanup':'project removed','wholeworkspace_validation':False,'app_default_consumer_scheduler_202_verified':False,'performance_verified':False,
          'prior_failed_attempt':{'path':'artifacts/rust/m6-source-native-epg-exhaustion-1','cause':'Test sink observed source_id instead of Source wire field source; four actual worker pairs passed before resource observer panic. Preserved/cleaned. Runtime unchanged; final test-only overlay fixes field.'},
          'limitations':['Source17 proves terminal SELECT P0001 still normal null; this historical native component leaves terminal SQL errors operational.','P0002 is a deliberate PENDING control here; actual Original counterpart and later guarded-stage mapping remain separate work.','Unclassified coarse Failed/channel/SQL/runtime/unsettled outcomes are not promoted.','Reset-before-try and native capacity failures are not treated as Source guarded errors.','Aborted logical waiter return is unobserved; ownership/drain evidence does not invent a return value.','Unrelated frozen sibling bytes receive no wholeworkspace acceptance.'],
          'credit':'Original Dispatcharr authors retain full application/algorithm credit and AGPL; framework authors retain their credit/licenses.'}
 target=out/'verified-source-native-epg-exhaustion-1.json';target.write_text(json.dumps(summary,indent=2)+'\n');print(json.dumps({'passed':True,'summary':str(target),'sha256':sha(target)}))
if __name__=='__main__':main()
