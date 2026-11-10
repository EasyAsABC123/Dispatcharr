#!/usr/bin/env python3
"""Hash-bind the isolated Source/native orphan component; no production claims.
Original Dispatcharr authors retain application/algorithm credit and AGPL.
Framework authors retain their respective credit/licenses.
"""
import argparse
import hashlib
import json
from pathlib import Path
from publish_epg_orphan import project_case
ROOT=Path(__file__).resolve().parents[2]
def sha(path):return hashlib.sha256(path.read_bytes()).hexdigest()
def load(path):return json.loads(path.read_text())
def main():
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('--source',required=True,type=Path);a=p.parse_args();source=a.source.resolve()
 integration=load(source/'integration.json');raw_path=source/'source-native-epg-orphan-private.json';raw=load(raw_path)
 assert integration['passed'] and integration['default_database_unchanged'] and integration['cleanup']=='project removed'
 assert raw['passed'] and len(raw['checks'])==125 and all(c['passed']for c in raw['checks'])
 assert len(raw['native_cases'])==11 and raw['native_resources']['resource_cases']==2
 assert 'Finished `dev` profile' in (source/'clippy.log').read_text()
 assert '2 passed; 0 failed' in (source/'unit.log').read_text()
 dataset={'format_version':1,'scope':'Nonconcurrent observed orphan graph/P0001 source-native component; no app/worker integration',
          'checks':raw['checks'],'versions':raw['versions'],'direct_source_cases':[project_case(c)for c in raw['cases']],
          'native_cases':raw['native_cases'],'actual_source_worker_cases':[project_case(c,True)for c in raw['worker_cases']],
          'native_resources':raw['native_resources'],'normalization':['Beat marker wall time: compare ident and changed-versus-original; native clock fixed 2021, Source wall clock. No other row/value differences allowed.']}
 out=ROOT/'scripts/rust-job-contracts';data=out/'source-native-epg-orphan-cleanup-1.json';data.write_text(json.dumps(dataset,indent=2,default=str)+'\n')
 relative='rust/crates/ingest-epg/src/orphan_cleanup.rs';module=source/'frozen'/relative
 assert sha(module)==integration['rust_source_sha256'][relative]
 source_lib=source/'frozen/rust/crates/ingest-epg/src/lib.rs'
 append='\n// Source orphan79 component; application/job integration remains gated.\n#[cfg(test)]\nmod orphan_cleanup;\n'
 assert source_lib.read_text().endswith(append)
 coupled={}
 for relative_coupled in ['rust/crates/ingest-epg/src/file_job.rs','rust/crates/job-transport/src/refresh_worker.rs']:
  old=ROOT/'artifacts/rust/m6-native-refresh-worker-1/frozen'/relative_coupled
  new=source/'frozen'/relative_coupled
  assert sha(old)==sha(new), 'Accepted worker76 runtime must remain byte-identical'
  coupled[relative_coupled]=sha(new)
 selected_tools={}
 for name in ['run_native_epg_orphan.py','source_native_epg_orphan.py','orphan_native_compare.py','epg_orphan_worker_observer.py']:
  relative_tool='scripts/rust-job-contracts/'+name;path=source/'tool-source'/relative_tool
  assert sha(path)==integration['tool_source_sha256'][relative_tool]
  selected_tools[relative_tool]={'path':str(path.relative_to(ROOT)),'sha256':sha(path)}
 # Full source captures unrelated, in-flight siblings. Their inclusion does not
 # extend the narrow component gate. Retain the complete map for reproducibility.
 summary={'passed':True,'scope':dataset['scope'],'checks':125,'direct_source_native_cases':11,'actual_source_worker_cases':9,
          'native_sql_ownership_cases':2,'pure_unit_tests':2,'strict_ingest_all_targets_clippy':True,
          'original_revision':integration['original_revision'],'dataset':str(data.relative_to(ROOT)),'dataset_sha256':sha(data),
          'source_evidence':'scripts/rust-job-contracts/verified-original-epg-orphan-cleanup-1.json',
          'raw_report':str(raw_path.relative_to(ROOT)),'raw_report_sha256':sha(raw_path),'integration_sha256':sha(source/'integration.json'),
          'rust_source_archive':str((source/'rust-source.tar').relative_to(ROOT)),'rust_source_archive_sha256':integration['rust_source_archive_sha256'],
          'selected_runtime':{relative:{'path':str(module.relative_to(ROOT)),'sha256':sha(module)}},
          'lib_registration':{'path':str(source_lib.relative_to(ROOT)),'sha256':sha(source_lib),'append_exact':append,'copy_entire_lib':False},
          'cargo_graph_changes':False,'selected_tools':selected_tools,
          'validation_logs':{name:sha(source/name)for name in ['clippy.log','unit.log','build.log','native-orphan-private.log']},
          'worker76_file_job_unchanged':True,'coupled_worker76_sha256':coupled,'default_database_unchanged':True,'cleanup':'project removed',
          'worker_or_app_integration_verified':False,'generic_collector_verified':False,'performance_verified':False,
          'limitations':['Unrelated Root ProfileCache and sibling bytes in full source capture are not accepted by this component gate.',
                         'No concurrent graph/marker writer acceptance; newly referenced intervals are rejected instead of cascading an unproved graph.',
                         'Only observed P0001 with settled rollback maps helper false; other SQL/transport or ambiguous rollback/commit faults remain operational.',
                         'Cancellation held an actual marker-lock reply and pool acquisition; no deliberately paused physical ROLLBACK reply proof.',
                         'Worker76 and FileJobRunner missing-source behavior remain unchanged; this helper is cfg(test) only.'],
          'credit':'Original Dispatcharr authors retain full application/algorithm credit and AGPL; Django and django-celery-beat authors retain framework credit/licenses.'}
 path=out/'verified-source-native-epg-orphan-cleanup-1.json';path.write_text(json.dumps(summary,indent=2)+'\n');print(json.dumps({'passed':True,'summary':str(path),'sha256':sha(path)}))
if __name__=='__main__':main()
