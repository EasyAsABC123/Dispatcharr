#!/usr/bin/env python3
"""Publish task-local EPG result DTO proof; original Dispatcharr authors retain AGPL credit."""
import argparse,hashlib,json,re
from pathlib import Path
ROOT=Path(__file__).resolve().parents[2]
def sha(path):return hashlib.sha256(path.read_bytes()).hexdigest()
def load(path):return json.loads(path.read_text())
def main():
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('--source',required=True,type=Path);a=p.parse_args();source=a.source.resolve();i=load(source/'integration.json');r=load(source/'source-native-epg-exhaustion-races-private.json')
 assert i['passed']and i['default_database_unchanged']and i['cleanup']=='project removed'and r['passed']and len(r['checks'])==93 and len(r['native_cases'])==6
 assert i['checks']==52 and i['regression_checks']==80 and sum(v['checks']for v in i['guarded_reports'].values())==79
 for c in r['native_cases']:
  if c['name'].startswith('replace_'):assert c['refresh_after']=='owned-foreign-token'
  if not c['name'].startswith('delete_'):assert c['audit']['domain_writes_after_intervention']==0
 aborted=[c for c in r['native_cases']if c['audit']['logical_waiter_aborted']];assert len(aborted)==2 and all(c['audit']['outcome']=='None'for c in aborted)
 out=ROOT/'scripts/rust-job-contracts';data={'format_version':1,'scope':'Explicit settled UPDATE0 and acknowledged NotOwner return semantics only for known exhaustion; legacy API unchanged','checks':r['checks'],'source_reference_cases':r['cases'],'native_cases':r['native_cases'],'effect_difference':'Original deletes replacement token; native preserves it. Native replacement occurs at RELEASE after domain work, deliberately avoiding stale writes; same interleaving is not claimed.','publication_scope':'Native actual typed UpdateEventSink callback payload versus actual Original Channels Redis delivery; native bus transport is separately gated, not inferred here.'}
 dataset=out/'source-native-epg-return-dto-1.json';dataset.write_text(json.dumps(data,indent=2)+'\n')
 selected={}
 for rel in ['rust/crates/ingest-epg/src/file_job.rs','rust/crates/job-transport/src/refresh_worker.rs','rust/crates/job-transport/src/refresh_worker_peer.rs','rust/crates/job-transport/src/refresh_worker_release_peer.rs','rust/crates/ingest-epg/tests/file_job_exhaustion.rs']:
  path=source/'frozen'/rel;assert sha(path)==i['rust_source_sha256'][rel];selected[rel]={'path':str(path.relative_to(ROOT)),'sha256':sha(path)}
 tools={}
 for name in ['run_native_epg_exhaustion_returns.py','source_native_epg_exhaustion_races.py','exhaustion_race_native_compare.py','source_native_epg_exhaustion.py','source_native_epg_exhaustion_p0002.py','source_native_epg_exhaustion_terminal.py','source_native_epg_exhaustion_servercode.py','exhaustion_native_compare.py','source_native_refresh_worker_exhaustion.py','epg_refresh_observer.py']:
  rel='scripts/rust-job-contracts/'+name;path=source/'tool-source'/rel;assert sha(path)==i['tool_source_sha256'][rel];tools[rel]={'path':str(path.relative_to(ROOT)),'sha256':sha(path)}
 units={}
 for name in ['ingest-unit.log','job-unit.log']:
  m=re.search(r'test result: ok\. (\d+) passed; 0 failed; (\d+) ignored',(source/name).read_text());assert m;units[name]={'passed':int(m[1]),'external_ignored':int(m[2])}
 assert 'Finished `dev` profile'in(source/'clippy.log').read_text()and'3 passed; 0 failed'in(source/'native-refresh-worker-physical.log').read_text()
 summary={'passed':True,'scope':data['scope'],'race_checks':93,'actual_source_native_race_pairs':6,'replayed_guarded_checks':131,'replayed_refresh_regression_checks':80,'actual_held0reply_caller_abort_cases':2,'native_no_domain_writes_after_loss':True,'foreign_state_preserved':True,'legacy_NotOwner_error_preserved':True,'frozen_default_units':units,'strict_ingest_job_all_targets_clippy':True,
          'selected_runtime_and_test':selected,'selected_tools':tools,'base_input':'artifacts/rust/epg-return-dto-reviewed-input-1/input.json','base_input_sha256':sha(ROOT/'artifacts/rust/epg-return-dto-reviewed-input-1/input.json'),'cargo_graph_changes':False,'lib_module_changes':False,
          'dataset':str(dataset.relative_to(ROOT)),'dataset_sha256':sha(dataset),'source_contract':'scripts/rust-job-contracts/verified-original-epg-exhaustion-races-1.json','rust_source_archive':str((source/'rust-source.tar').relative_to(ROOT)),'rust_source_archive_sha256':i['rust_source_archive_sha256'],
          'raw_evidence_sha256':{name:sha(source/name)for name in ['integration.json','source-native-epg-exhaustion-races-private.json','native-exhaustion-race-private.log','native-exhaustion-resources.log','native-refresh-worker-physical.log','clippy.log','ingest-unit.log','job-unit.log']},
          'default_database_unchanged':True,'cleanup':'project removed','wholeworkspace_validation':False,'application_consumer_status202_verified':False,'performance_verified':False,
          'limitations':['NotOwner is actual acknowledged0, not timeout/unknown/failed cleanup, and never claims Released.','No new shared-domain writes follow loss observation; owned task-result metadata is distinct reporting.','Replacement timing differs deliberately to preserve native fencing; same interleaving and Source foreign DEL are not reproduced.','Native publication payload callback is verified; no native Channels transport inference.','Coarse/unknown/unsettled/capacity/reset errors remain operational; broader races/renewal ticks unproved.'],
          'credit':'Original Dispatcharr authors retain full application/algorithm credit and AGPL; framework authors retain their respective credit/licenses.'}
 target=out/'verified-source-native-epg-return-dto-1.json';target.write_text(json.dumps(summary,indent=2)+'\n');print(json.dumps({'passed':True,'summary':str(target),'sha256':sha(target)}))
if __name__=='__main__':main()
